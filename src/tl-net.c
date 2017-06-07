#include <string.h>
#include <gio/gio.h>
#include "tl-net.h"
#include "tl-logger.h"
#include "tl-parser.h"

typedef struct _TLNetData
{
    gboolean initialized;
    gchar *conf_file_path;
    
    GSocketClient *vehicle_client;
    GSocketConnection *vehicle_connection;
    GInputStream *vehicle_input_stream;
    GOutputStream *vehicle_output_stream;
    GSource *vehicle_input_source;
    GSource *vehicle_output_source;
    
    GByteArray *vehicle_write_buffer;
    gboolean vehicle_write_request_answer;
    GQueue *vehicle_write_queue;
    GByteArray *vehicle_packet_read_buffer;
    gssize vehicle_packet_read_expect_length;
    
    GList *vehicle_server_list;
    GList *current_vehicle_server;
    
    gchar *vin;
    gchar *iccid;
    guint16 session;
    
    guint vehicle_connection_state;
    
    guint vehicle_connection_check_timeout;
    
    guint vehicle_connection_retry_count;
    guint vehicle_connection_retry_maximum;
    guint vehicle_connection_retry_cycle;
    gint64 vehicle_connection_login_request_timestamp;
    
    guint vehicle_connection_answer_timeout;
    gint64 vehicle_connection_request_timestamp;
    
    guint vehicle_connection_heartbeat_timeout;
    gint64 vehicle_connection_heartbeat_timestamp;
}TLNetData;

typedef struct _TLNetWriteBufferData
{
    GByteArray *buffer;
    gboolean request_answer;
}TLNetWriteBufferData;

typedef enum
{
    TL_NET_CONNECTION_STATE_IDLE = 0,
    TL_NET_CONNECTION_STATE_CONNECTING = 1,
    TL_NET_CONNECTION_STATE_CONNECTED = 2,
    TL_NET_CONNECTION_STATE_LOGINING = 3,
    TL_NET_CONNECTION_STATE_LOGINED = 4,
    TL_NET_CONNECTION_STATE_ANSWER_PENDING = 5
}TLNetConnectionState;

typedef enum
{
    TL_NET_COMMAND_TYPE_VEHICLE_LOGIN = 0x1,
    TL_NET_COMMAND_TYPE_REALTIME_DATA = 0x2,
    TL_NET_COMMAND_TYPE_REPEAT_DATA = 0x3,
    TL_NET_COMMAND_TYPE_VEHICLE_LOGOUT = 0x4,
    TL_NET_COMMAND_TYPE_PLATFORM_LOGIN = 0x5,
    TL_NET_COMMAND_TYPE_PLATFORM_LOGOUT = 0x6,
    TL_NET_COMMAND_TYPE_CLIENT_HEARTBEAT = 0x7,
    TL_NET_COMMAND_TYPE_SET_TIME = 0x8,
    TL_NET_COMMAND_TYPE_QUERY = 0x80,
    TL_NET_COMMAND_TYPE_SETUP = 0x81,
    TL_NET_COMMAND_TYPE_TERMINAL_CONTROL = 0x82
}TLNetCommandType;

typedef enum
{
    TL_NET_ANSWER_TYPE_SUCCEED = 0x1,
    TL_NET_ANSWER_TYPE_ERROR = 0x2,
    TL_NET_ANSWER_TYPE_VIN_DUPLICATED = 0x3,
    TL_NET_ANSWER_TYPE_COMMAND = 0xFE
}TLNetAnswerType;

typedef enum
{
    TL_NET_DATA_TYPE_TOTAL_VEHICLE = 0x1,
    TL_NET_DATA_TYPE_DRIVE_MOTOR = 0x2,
    TL_NET_DATA_TYPE_FUEL_BATTERY = 0x3,
    TL_NET_DATA_TYPE_ENGINE = 0x4,
    TL_NET_DATA_TYPE_VEHICLE_POSITION = 0x5,
    TL_NET_DATA_TYPE_EXTREMUM = 0x6,
    TL_NET_DATA_TYPE_ALARM = 0x7
}TLNetDataType;

typedef enum
{
    TL_NET_PACKET_ENCRYPTION_TYPE_NONE = 0x1,
    TL_NET_PACKET_ENCRYPTION_TYPE_RSA = 0x2,
    TL_NET_PACKET_ENCRYPTION_TYPE_AES = 0x3,
    TL_NET_PACKET_ENCRYPTION_TYPE_ABNORMAL = 0xFE,
    TL_NET_PACKET_ENCRYPTION_TYPE_INVALID = 0xFF,
}TLNetPacketEncryptionType;

static TLNetData g_tl_net_data = {0};

#define TL_NET_CONF_PATH_DEFAULT "/var/lib/tbox/conf"

static TLNetWriteBufferData *tl_net_write_buffer_data_new(GByteArray *ba)
{
    TLNetWriteBufferData *data;
    
    data = g_new0(TLNetWriteBufferData, 1);
    data->buffer = g_byte_array_ref(ba);
    
    return data;
}

static void tl_net_write_buffer_data_free(TLNetWriteBufferData *data)
{
    if(data==NULL)
    {
        return;
    }
    if(data->buffer!=NULL)
    {
        g_byte_array_unref(data->buffer);
    }
    g_free(data);
}

static gboolean tl_net_config_load(TLNetData *net_data,
    const gchar *conf_file)
{
    GKeyFile *keyfile;
    GError *error = NULL;
    gchar *svalue;
    gint ivalue;
    
    keyfile = g_key_file_new();
    
    if(!g_key_file_load_from_file(keyfile, conf_file, G_KEY_FILE_NONE, &error))
    {
        if(error!=NULL)
        {
            g_warning("TLNet load config file failed: %s", error->message);
            g_clear_error(&error);
        }
        return FALSE;
    }
    
    svalue = g_key_file_get_string(keyfile, "Network", "LastVIN", NULL);
    if(g_strcmp0(net_data->vin, svalue)==0)
    {
        ivalue = g_key_file_get_integer(keyfile, "Network", "LastSession",
            NULL);
        net_data->session = ivalue;
    }
    else
    {
        net_data->session = 0;
    }
    
    
    g_key_file_free(keyfile);
    
    return TRUE;
}

static gboolean tl_net_config_save(TLNetData *net_data, const gchar *conf_file)
{
    GKeyFile *keyfile;
    GError *error = NULL;
    
    keyfile = g_key_file_new();
    
    g_key_file_set_string(keyfile, "Network", "LastVIN", net_data->vin);
    g_key_file_set_integer(keyfile, "Network", "LastSession",
        net_data->session);
        
    if(!g_key_file_save_to_file(keyfile, conf_file, &error))
    {
        if(error!=NULL)
        {
            g_warning("TLNet failed to save config file: %s", error->message);
            g_clear_error(&error);
        }
    }

    g_key_file_free(keyfile);
    
    return TRUE;
}

static GByteArray *tl_net_packet_build(TLNetCommandType command,
    TLNetAnswerType answer, const guint8 *vin_code, guint vin_len,
    TLNetPacketEncryptionType encryption, GByteArray *payload)
{
    GByteArray *ba;
    guint8 byte_value;
    guint16 word_value;
    guint8 vin_code_buf[17] = {0};
    guint i;
    guint8 checksum = 0;
    
    ba = g_byte_array_new();
    
    g_byte_array_append(ba, (const guint8 *)"##", 2);
    
    byte_value = command;
    g_byte_array_append(ba, &byte_value, 1);
    
    byte_value = answer;
    g_byte_array_append(ba, &byte_value, 1);
    
    if(vin_len>17)
    {
        vin_len = 17;
    }
    memcpy(vin_code_buf, vin_code, vin_len);
    g_byte_array_append(ba, vin_code_buf, 17);
    
    byte_value = encryption;
    g_byte_array_append(ba, &byte_value, 1);
    
    if(payload!=NULL)
    {
        word_value = payload->len;
        word_value = g_htons(word_value);
        g_byte_array_append(ba, (const guint8 *)&word_value, 2);
        
        g_byte_array_append(ba, payload->data, payload->len);
    }
    else
    {
        word_value = 0;
        g_byte_array_append(ba, (const guint8 *)&word_value, 2);
    }

    for(i=2;i<ba->len;i++)
    {
        checksum ^= ba->data[i];
    }
    
    g_byte_array_append(ba, &checksum, 1);
    
    return ba;
}

static GByteArray *tl_net_login_packet_build(TLNetData *net_data)
{
    GByteArray *ba, *ret;
    GDateTime *dt;
    guint8 year, month, day;
    guint8 hour, min, sec;
    guint16 s_value;
    guint8 iccid_buf[20];
    guint iccid_len;
    GHashTable *log_table;
    TLLoggerLogItemData *log_item;
    guint8 battery_num = 0;
    guint8 battery_code_len;
    guint battery_code_total_len, battery_code_total_rlen;
    const gchar *battery_code;
    guint i;
    
    ba = g_byte_array_new();
    
    dt = g_date_time_new_now_local();
    year = (g_date_time_get_year(dt) - 2000);
    month = g_date_time_get_month(dt);
    day = g_date_time_get_day_of_month(dt);
    hour = g_date_time_get_hour(dt);
    min = g_date_time_get_minute(dt);
    sec = g_date_time_get_second(dt);
    g_date_time_unref(dt);
    
    g_byte_array_append(ba, &year, 1);
    g_byte_array_append(ba, &month, 1);
    g_byte_array_append(ba, &day, 1);
    g_byte_array_append(ba, &hour, 1);
    g_byte_array_append(ba, &min, 1);
    g_byte_array_append(ba, &sec, 1);
    
    net_data->session++;
    tl_net_config_save(net_data, net_data->conf_file_path);
    
    s_value = g_htons(net_data->session);
    g_byte_array_append(ba, (const guint8 *)&s_value, 2);
    
    iccid_len = strlen(net_data->iccid);
    if(iccid_len>20)
    {
        iccid_len = 20;
    }
    memcpy(iccid_buf, net_data->iccid, iccid_len);
    g_byte_array_append(ba, iccid_buf, 20);
    
    log_table = tl_logger_current_data_get(NULL);
    log_item = g_hash_table_lookup(log_table, TL_PARSER_BATTERY_NUMBER);
    if(log_item!=NULL)
    {
        battery_num = log_item->value;
        if(battery_num>250)
        {
            battery_num = 250;
        }
    }
    g_byte_array_append(ba, &battery_num, 1);
    
    battery_code = tl_parser_battery_code_get(&battery_code_len,
        &battery_code_total_len);
    if(battery_code==NULL || battery_num==0)
    {
        battery_code_len = 0;
    }
    g_byte_array_append(ba, &battery_code_len, 1);
    
    if(battery_code_len>0)
    {
        battery_code_total_rlen = (guint)battery_num * (guint)battery_code_len;
        if(battery_code_total_len>=battery_code_total_rlen)
        {
            g_byte_array_append(ba, (const guint8 *)battery_code,
                battery_code_total_rlen);
        }
        else
        {
            g_byte_array_append(ba, (const guint8 *)battery_code,
                battery_code_total_len);
            for(i=0;i<(battery_code_total_rlen - battery_code_total_len);i++)
            {
                g_byte_array_append(ba, (const guint8 *)"\0", 1);
            }
        }
    }
    
    ret = tl_net_packet_build(TL_NET_COMMAND_TYPE_VEHICLE_LOGIN,
        TL_NET_ANSWER_TYPE_COMMAND, (const guint8 *)net_data->vin,
        strlen(net_data->vin), TL_NET_PACKET_ENCRYPTION_TYPE_NONE, ba);
    g_byte_array_unref(ba);
    
    return ret;
}

static GByteArray *tl_net_heartbeat_packet_build(TLNetData *net_data)
{
    GByteArray *ba;
    
    ba = tl_net_packet_build(TL_NET_COMMAND_TYPE_CLIENT_HEARTBEAT,
        TL_NET_ANSWER_TYPE_COMMAND, (const guint8 *)net_data->vin,
        strlen(net_data->vin), TL_NET_PACKET_ENCRYPTION_TYPE_NONE, NULL);
        
    return ba;
}

static void tl_net_vehicle_connection_disconnect(TLNetData *net_data)
{
    if(net_data->vehicle_input_source!=NULL)
    {
        g_source_destroy(net_data->vehicle_input_source);
        g_source_unref(net_data->vehicle_input_source);
        net_data->vehicle_input_source = NULL;
    }
    if(net_data->vehicle_output_source!=NULL)
    {
        g_source_destroy(net_data->vehicle_output_source);
        g_source_unref(net_data->vehicle_output_source);
        net_data->vehicle_output_source = NULL;
    }
    if(net_data->vehicle_connection!=NULL)
    {
        g_object_unref(net_data->vehicle_connection);
        net_data->vehicle_connection = NULL;
    }
    if(net_data->vehicle_write_buffer!=NULL)
    {
        g_byte_array_unref(net_data->vehicle_write_buffer);
        net_data->vehicle_write_buffer = NULL;
    }
    if(net_data->vehicle_write_queue!=NULL)
    {
        g_queue_free_full(net_data->vehicle_write_queue,
            (GDestroyNotify)tl_net_write_buffer_data_free);
        net_data->vehicle_write_queue = g_queue_new();
    }
    
    net_data->vehicle_connection_state = TL_NET_CONNECTION_STATE_IDLE;
}

static gboolean tl_net_vehicle_connection_output_pollable_source_cb(
    GObject *pollable_stream, gpointer user_data)
{
    TLNetData *net_data = (TLNetData *)user_data;
    GError *error = NULL;
    gboolean ret = FALSE;
    gboolean disconnected = FALSE;
    gboolean wait_request = FALSE;
    gssize write_size;
    const gchar *host = NULL;
    TLNetWriteBufferData *write_buffer_data;
    
    if(net_data->current_vehicle_server!=NULL)
    {
        host = net_data->current_vehicle_server->data;
    }
    
    do
    {
        if(net_data->vehicle_write_buffer==NULL)
        {
            write_buffer_data = g_queue_pop_head(
                net_data->vehicle_write_queue);
            net_data->vehicle_write_buffer = g_byte_array_ref(
                write_buffer_data->buffer);
            net_data->vehicle_write_request_answer =
                write_buffer_data->request_answer;
            tl_net_write_buffer_data_free(write_buffer_data);
        }
        
        if(net_data->vehicle_write_buffer==NULL)
        {
            break;
        }
        
        while((write_size=g_pollable_output_stream_write_nonblocking(
            G_POLLABLE_OUTPUT_STREAM(net_data->vehicle_output_stream),
            net_data->vehicle_write_buffer->data,
            net_data->vehicle_write_buffer->len, NULL, &error))>0 &&
            error==NULL)
        {
            if(write_size>=net_data->vehicle_write_buffer->len)
            {
                if(net_data->vehicle_write_request_answer)
                {
                    wait_request = TRUE;
                }
                else
                {
                    g_byte_array_unref(net_data->vehicle_write_buffer);
                    net_data->vehicle_write_buffer = NULL;
                }
                break;
            }
            else
            {
                g_byte_array_remove_range(net_data->vehicle_write_buffer, 0,
                    write_size);
            }
        }
        
        if(error!=NULL)
        {
            if(error->code==G_IO_ERROR_WOULD_BLOCK)
            {
                ret = TRUE;
            }
            else
            {
                g_message("TLNet disconnected from host %s with error: %s",
                    host, error->message);
                disconnected = TRUE;
            }
            g_clear_error(&error);
            break;
        }
        
        if(wait_request)
        {
            break;
        }
    }
    while(net_data->vehicle_write_buffer!=NULL ||
        !g_queue_is_empty(net_data->vehicle_write_queue));

    if(!ret)
    {
        g_source_destroy(net_data->vehicle_output_source);
        g_source_unref(net_data->vehicle_output_source);
        net_data->vehicle_output_source = NULL;
    }
    
    if(disconnected)
    {
        tl_net_vehicle_connection_disconnect(net_data);
    }
    
    return ret;
}

static void tl_net_packet_parse(TLNetData *net_data, guint8 command,
    guint8 answer, const gchar *vin, guint8 encryption, const guint8 *payload,
    guint payload_len)
{
    switch(command)
    {
        case TL_NET_COMMAND_TYPE_VEHICLE_LOGIN:
        {
            if(answer==TL_NET_ANSWER_TYPE_SUCCEED)
            {
                net_data->vehicle_connection_state =
                    TL_NET_CONNECTION_STATE_LOGINED;
                net_data->vehicle_connection_retry_count = 0;
            }
            else if(answer!=TL_NET_ANSWER_TYPE_COMMAND)
            {
                net_data->vehicle_connection_state = 
                    TL_NET_CONNECTION_STATE_CONNECTED;
                net_data->vehicle_connection_login_request_timestamp =
                    g_get_monotonic_time();
            }
            break;
        }
        case TL_NET_COMMAND_TYPE_REALTIME_DATA:
        case TL_NET_COMMAND_TYPE_REPEAT_DATA:
        {
            if(answer==TL_NET_ANSWER_TYPE_SUCCEED)
            {

            }
            else if(answer!=TL_NET_ANSWER_TYPE_COMMAND)
            {
                
            }
            break;
        }
        default:
        {
            break;
        }
    }
    
}

static gboolean tl_net_vehicle_connection_input_pollable_source_cb(
    GObject *pollable_stream, gpointer user_data)
{
    TLNetData *net_data = (TLNetData *)user_data;
    GError *error = NULL;
    const gchar *host = NULL;
    gssize read_size;
    gssize i, j;
    guint16 expect_len;
    guint8 checksum, rchecksum;
    gchar vin_code[18] = {0};
    gchar buffer[4097];
    
    if(user_data==NULL)
    {
        return FALSE;
    }
    
    if(net_data->current_vehicle_server!=NULL)
    {
        host = net_data->current_vehicle_server->data;
    }
    
    while((read_size=g_pollable_input_stream_read_nonblocking(
        G_POLLABLE_INPUT_STREAM(pollable_stream), buffer, 4096,
        NULL, &error))>0 && error==NULL)
    {
        for(i=0;i<read_size;i++)
        {
            if(net_data->vehicle_packet_read_buffer->len==0)
            {
                if(buffer[i]=='#')
                {
                    g_byte_array_append(net_data->vehicle_packet_read_buffer,
                        (const guint8 *)"#", 1);
                }
            }
            else if(net_data->vehicle_packet_read_buffer->len==1)
            {
                if(buffer[i]=='#')
                {
                    g_byte_array_append(net_data->vehicle_packet_read_buffer,
                        (const guint8 *)"#", 1);
                    net_data->vehicle_packet_read_expect_length = 24;
                }
                else
                {
                    net_data->vehicle_packet_read_buffer->len = 0;
                }
            }
            else if(net_data->vehicle_packet_read_buffer->len>=2)
            {
                g_byte_array_append(net_data->vehicle_packet_read_buffer,
                    (const guint8 *)buffer+i, 1);
                
                if(net_data->vehicle_packet_read_buffer->len >
                    net_data->vehicle_packet_read_expect_length+1)
                {
                    net_data->vehicle_packet_read_buffer->len = 0;
                    net_data->vehicle_packet_read_expect_length = 0;
                }
                else if(net_data->vehicle_packet_read_buffer->len==24)
                {
                    memcpy(&expect_len,
                        net_data->vehicle_packet_read_buffer->data + 22, 2);
                    expect_len = g_ntohs(expect_len);
                    
                    if(expect_len>65531)
                    {
                        expect_len = 65531;
                    }
                    
                    net_data->vehicle_packet_read_expect_length = (guint)
                        expect_len + 24;
                }
                else if(net_data->vehicle_packet_read_buffer->len==
                    net_data->vehicle_packet_read_expect_length+1)
                {
                    /* Parse packet data. */
                    checksum = net_data->vehicle_packet_read_buffer->data[
                        net_data->vehicle_packet_read_expect_length];
                    rchecksum = 0;
                    for(j=2;j<net_data->vehicle_packet_read_expect_length;
                        j++)
                    {
                        rchecksum ^=
                            net_data->vehicle_packet_read_buffer->data[j];
                    }
                    
                    if(rchecksum==checksum)
                    {
                        memcpy(vin_code,
                            net_data->vehicle_packet_read_buffer->data+4, 17);

                        tl_net_packet_parse(net_data,
                            net_data->vehicle_packet_read_buffer->data[2],
                            net_data->vehicle_packet_read_buffer->data[3],
                            vin_code,
                            net_data->vehicle_packet_read_buffer->data[21],
                            net_data->vehicle_packet_read_buffer->data+24,
                            net_data->vehicle_packet_read_expect_length-24);
                    }
                    else
                    {
                        g_warning("TLNet got a packet with checksum error!");
                    }
                    
                    net_data->vehicle_packet_read_expect_length = 0;
                    net_data->vehicle_packet_read_buffer->len = 0;
                }
            }
        }
    }
    
    if(error!=NULL && error->code==G_IO_ERROR_WOULD_BLOCK)
    {
        g_clear_error(&error);
        return TRUE;
    }
    else
    {
        if(error==NULL)
        {
            g_message("TLNet disconnected from host %s normally.", host);
        }
        else
        {
            g_message("TLNet disconnected from host %s with error: %s",
                host, error->message);
            g_clear_error(&error);
        }
        
        tl_net_vehicle_connection_disconnect(net_data);
        
        return FALSE;
    }
    
    return TRUE;
}

static void tl_net_vehicle_connection_packet_output_request(
    TLNetData *net_data, GByteArray *packet, gboolean request_answer)
{
    TLNetWriteBufferData *write_buffer_data;
    
    if(net_data->vehicle_output_source==NULL)
    {
        net_data->vehicle_output_source =
            g_pollable_output_stream_create_source(
            G_POLLABLE_OUTPUT_STREAM(net_data->vehicle_output_stream), NULL);
        if(net_data->vehicle_output_source!=NULL)
        {
            g_source_set_callback(net_data->vehicle_output_source,
                (GSourceFunc)
                tl_net_vehicle_connection_output_pollable_source_cb, net_data,
                NULL);
            g_source_attach(net_data->vehicle_output_source, NULL);
        }
    }
    
    write_buffer_data = tl_net_write_buffer_data_new(packet);
    write_buffer_data->request_answer = request_answer;
    
    g_queue_push_tail(net_data->vehicle_write_queue,
        write_buffer_data);
}

static void tl_net_vehicle_connect_host_async_cb(GObject *source,
    GAsyncResult *res, gpointer user_data)
{
    TLNetData *net_data = (TLNetData *)user_data;
    GSocketConnection *connection;
    GError *error = NULL;
    const gchar *host = NULL;
    
    if(user_data==NULL)
    {
        return;
    }
    
    if(net_data->current_vehicle_server!=NULL)
    {
        host = net_data->current_vehicle_server->data;
    }
    
    connection = g_socket_client_connect_to_host_finish(G_SOCKET_CLIENT(
        source), res, &error);
    if(connection!=NULL)
    {
        net_data->vehicle_connection = connection;
        net_data->vehicle_connection_state = TL_NET_CONNECTION_STATE_CONNECTED;
        net_data->vehicle_connection_retry_count = 0;
        net_data->vehicle_connection_login_request_timestamp = 0;
        net_data->vehicle_write_request_answer = FALSE;
        
        net_data->vehicle_input_stream = g_io_stream_get_input_stream(
            G_IO_STREAM(connection));
        net_data->vehicle_output_stream = g_io_stream_get_output_stream(
            G_IO_STREAM(connection));
        
        if(net_data->vehicle_input_source!=NULL)
        {
            g_source_destroy(net_data->vehicle_input_source);
            g_source_unref(net_data->vehicle_input_source);
        }    
        net_data->vehicle_input_source = g_pollable_input_stream_create_source(
            G_POLLABLE_INPUT_STREAM(net_data->vehicle_input_stream), NULL);
        if(net_data->vehicle_input_source!=NULL)
        {
            g_source_set_callback(net_data->vehicle_input_source, (GSourceFunc)
                tl_net_vehicle_connection_input_pollable_source_cb, net_data,
                NULL);
            g_source_attach(net_data->vehicle_input_source, NULL);
        }
        if(net_data->vehicle_output_source!=NULL)
        {
            g_source_destroy(net_data->vehicle_output_source);
            g_source_unref(net_data->vehicle_output_source);
            net_data->vehicle_output_source = NULL;
        }
        
        g_message("TLNet connected to host %s.", host);
    }
    else
    {
        net_data->vehicle_connection_state = 0;
        
        if(error!=NULL)
        {
            g_warning("TLNet failed to connect to host %s: %s!", host,
                error->message);
        }
        else
        {
            g_warning("TLNet failed to connect to host %s with unknown "
                "error!", host);
        }
        
        if(net_data->current_vehicle_server!=NULL)
        {
            net_data->current_vehicle_server = g_list_next(
                net_data->current_vehicle_server);
        }
    }
    g_clear_error(&error);
}

static gboolean tl_net_vehicle_connection_check_timeout_cb(gpointer user_data)
{
    TLNetData *net_data = (TLNetData *)user_data;
    const gchar *server_host;
    gint64 now;
    GByteArray *ba;
    
    if(user_data==NULL)
    {
        return FALSE;
    }
    
    now = g_get_monotonic_time();
    
    switch(net_data->vehicle_connection_state)
    {
        case TL_NET_CONNECTION_STATE_ANSWER_PENDING:
        {
            if(now - net_data->vehicle_connection_request_timestamp >=
                net_data->vehicle_connection_answer_timeout * 1e6)
            {
                //TODO: Request TIMEOUT!
            }
            break;
        }
        case TL_NET_CONNECTION_STATE_LOGINED:
        {
            if(now - net_data->vehicle_connection_heartbeat_timestamp >=
                (gint64)net_data->vehicle_connection_heartbeat_timeout * 1e6)
            {
                ba = tl_net_heartbeat_packet_build(net_data);
                tl_net_vehicle_connection_packet_output_request(net_data,
                    ba, FALSE);
                g_byte_array_unref(ba);
                net_data->vehicle_connection_heartbeat_timestamp = now;
            }
            
            break;
        }
        case TL_NET_CONNECTION_STATE_LOGINING:
        {
            if(now -
                net_data->vehicle_connection_login_request_timestamp >=
                (gint64)net_data->vehicle_connection_answer_timeout * 1e6)
            {
                net_data->vehicle_connection_state =
                    TL_NET_CONNECTION_STATE_CONNECTED;
                net_data->vehicle_connection_login_request_timestamp = now;
            }
            
            break;
        }
        case TL_NET_CONNECTION_STATE_CONNECTED:
        {
            if(net_data->vehicle_connection_retry_count >
                net_data->vehicle_connection_retry_maximum)
            {
                if(net_data->current_vehicle_server!=NULL)
                {
                    net_data->current_vehicle_server = g_list_next(
                        net_data->current_vehicle_server);
                }
                tl_net_vehicle_connection_disconnect(net_data);
            }
            else
            {
                if(now -
                    net_data->vehicle_connection_login_request_timestamp >=
                    (gint64)net_data->vehicle_connection_retry_cycle * 1e6)
                {
                    ba = tl_net_login_packet_build(net_data);
                    tl_net_vehicle_connection_packet_output_request(net_data,
                        ba, TRUE);
                    g_byte_array_unref(ba);
                    
                    net_data->vehicle_connection_state =
                        TL_NET_CONNECTION_STATE_LOGINING;
                    net_data->vehicle_connection_retry_count++;
                    net_data->vehicle_connection_login_request_timestamp = now;
                }
            }
            
            break;
        }
        case TL_NET_CONNECTION_STATE_IDLE:
        {
            if(net_data->vehicle_server_list==NULL)
            {
                break;
            }
    
            if(net_data->current_vehicle_server==NULL)
            {
                net_data->current_vehicle_server =
                    net_data->vehicle_server_list;
            }
    
            server_host = net_data->current_vehicle_server->data;
    
            net_data->vehicle_connection_state =
                TL_NET_CONNECTION_STATE_CONNECTING;
            g_socket_client_connect_to_host_async(net_data->vehicle_client,
                server_host, 0, NULL, tl_net_vehicle_connect_host_async_cb,
                net_data);
            
            break;
        }
        default:
        {
            break;
        }
    }
    
    return TRUE;
}

gboolean tl_net_init(const gchar *vin, const gchar *iccid,
    const gchar *conf_path, const gchar *fallback_vehicle_server_host,
    guint16 fallback_vehicle_server_port)
{    
    if(g_tl_net_data.initialized)
    {
        g_warning("TLNet already initialized!");
        return TRUE;
    }
    
    g_tl_net_data.vehicle_connection_retry_maximum = 3;
    g_tl_net_data.vehicle_connection_retry_cycle = 10;
    
    g_tl_net_data.vehicle_connection_answer_timeout = 60;
    g_tl_net_data.vehicle_connection_heartbeat_timeout = 10;
    
    g_tl_net_data.vehicle_connection_state = 0;
    
    g_tl_net_data.vehicle_packet_read_buffer = g_byte_array_new();
    g_tl_net_data.vehicle_write_queue = g_queue_new();
    
    if(fallback_vehicle_server_host!=NULL)
    {
        if(fallback_vehicle_server_port!=0)
        {
            g_tl_net_data.vehicle_server_list = g_list_prepend(
                g_tl_net_data.vehicle_server_list, g_strdup_printf("%s:%u",
                fallback_vehicle_server_host, fallback_vehicle_server_port));
        }
        else
        {
            g_tl_net_data.vehicle_server_list = g_list_prepend(
                g_tl_net_data.vehicle_server_list, g_strdup(
                fallback_vehicle_server_host));
        }
    }
    
    if(conf_path==NULL)
    {
        conf_path = TL_NET_CONF_PATH_DEFAULT;
    }
    g_tl_net_data.conf_file_path = g_build_filename(conf_path,
        "network.conf", NULL);
    tl_net_config_load(&g_tl_net_data, g_tl_net_data.conf_file_path);
    
    
    g_tl_net_data.vehicle_client = g_socket_client_new();
    g_socket_client_set_timeout(g_tl_net_data.vehicle_client, 60);
    
    g_tl_net_data.current_vehicle_server = g_tl_net_data.vehicle_server_list;
    
    g_tl_net_data.vehicle_connection_check_timeout = g_timeout_add_seconds(2,
        tl_net_vehicle_connection_check_timeout_cb, &g_tl_net_data);
    
    g_tl_net_data.initialized = TRUE;
    
    return TRUE;
}

void tl_net_uninit()
{
    if(!g_tl_net_data.initialized)
    {
        return;
    }
    
    if(g_tl_net_data.vehicle_connection_check_timeout>0)
    {
        g_source_remove(g_tl_net_data.vehicle_connection_check_timeout);
        g_tl_net_data.vehicle_connection_check_timeout = 0;
    }
    if(g_tl_net_data.vehicle_input_source!=NULL)
    {
        g_source_destroy(g_tl_net_data.vehicle_input_source);
        g_source_unref(g_tl_net_data.vehicle_input_source);
        g_tl_net_data.vehicle_input_source = NULL;
    }
    if(g_tl_net_data.vehicle_output_source!=NULL)
    {
        g_source_destroy(g_tl_net_data.vehicle_output_source);
        g_source_unref(g_tl_net_data.vehicle_output_source);
        g_tl_net_data.vehicle_output_source = NULL;
    }
    
    if(g_tl_net_data.conf_file_path!=NULL)
    {
        g_free(g_tl_net_data.conf_file_path);
        g_tl_net_data.conf_file_path = NULL;
    }
    
    if(g_tl_net_data.vehicle_connection!=NULL)
    {
        g_object_unref(g_tl_net_data.vehicle_connection);
        g_tl_net_data.vehicle_connection = NULL;
    }
    
    if(g_tl_net_data.vehicle_client!=NULL)
    {
        g_object_unref(g_tl_net_data.vehicle_client);
        g_tl_net_data.vehicle_client = NULL;
    }
    
    if(g_tl_net_data.vehicle_server_list!=NULL)
    {
        g_list_free_full(g_tl_net_data.vehicle_server_list, g_free);
        g_tl_net_data.vehicle_server_list = NULL;
    }
    
    if(g_tl_net_data.vehicle_write_buffer!=NULL)
    {
        g_byte_array_unref(g_tl_net_data.vehicle_write_buffer);
        g_tl_net_data.vehicle_write_buffer = NULL;
    }
    if(g_tl_net_data.vehicle_packet_read_buffer!=NULL)
    {
        g_byte_array_unref(g_tl_net_data.vehicle_packet_read_buffer);
        g_tl_net_data.vehicle_packet_read_buffer = NULL;
    }
    
    if(g_tl_net_data.vehicle_write_queue!=NULL)
    {
        g_queue_free_full(g_tl_net_data.vehicle_write_queue,
            (GDestroyNotify)tl_net_write_buffer_data_free);
        g_tl_net_data.vehicle_write_queue = NULL;
    }
    
    g_tl_net_data.initialized = FALSE;
}
