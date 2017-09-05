#include <string.h>
#include <stdio.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include "tl-net.h"
#include "tl-logger.h"
#include "tl-parser.h"
#include "tl-gps.h"
#include "tl-serial.h"

#define TL_NET_BACKLOG_MAXIMUM 45
#define TL_NET_LOG_TO_DISK_TRIGGER 2048

#define TL_NET_PACKET_FILE_HEADER ((const guint8 *)"TLNP")

typedef struct _TLNetData
{
    gboolean initialized;
    gchar *conf_file_path;
    gchar *log_path;
    gchar hwversion[6];
    gchar fwversion[6];
    
    GSocketClient *vehicle_client;
    GSocketConnection *vehicle_connection;
    GInputStream *vehicle_input_stream;
    GOutputStream *vehicle_output_stream;
    GSource *vehicle_input_source;
    GSource *vehicle_output_source;
    
    GByteArray *vehicle_write_buffer;
    GByteArray *vehicle_write_buffer_dup;
    gboolean vehicle_write_request_answer;
    guint8 vehicle_write_request_type;
    gint64 vehicle_write_request_timestamp;
    GQueue *vehicle_write_queue;
    GByteArray *vehicle_packet_read_buffer;
    gssize vehicle_packet_read_expect_length;
    
    GThread *vehicle_data_log_thread;
    gboolean vehicle_data_log_thread_work_flag;
    
    GList *vehicle_server_list;
    GList *current_vehicle_server;
    
    gchar *vin;
    gchar *iccid;
    guint16 session;
    
    gboolean first_connected;
    
    gint64 realtime_now;
    gint64 realtime_now2;
    gint64 time_now;
    
    guint vehicle_connection_state;
    
    guint vehicle_connection_check_timeout;
    
    guint vehicle_connection_retry_count;
    guint vehicle_connection_retry_maximum;
    guint vehicle_connection_retry_cycle;
    gint64 vehicle_connection_login_request_timestamp;
    
    gint64 vehicle_connection_timestamp;
    
    guint vehicle_connection_answer_timeout;
    gint64 vehicle_connection_request_timestamp;
    
    guint vehicle_connection_heartbeat_timeout;
    gint64 vehicle_connection_heartbeat_timestamp;
    
    guint vehicle_data_retry_count;
    gboolean vehicle_data_report_is_emergency;
    guint vehicle_data_report_normal_timeout;
    guint vehicle_data_report_emergency_timeout;
    gint64 vehicle_data_report_timestamp;
    
    GTree *vehicle_data_tree;
    GMutex vehicle_data_mutex;
    
    GQueue *vehicle_backlog_data_queue;
    GMutex vehicle_backlog_data_mutex;
    
    guint vehicle_data_report_timer_timeout;
    
    guint vehicle_last_alarm_level;
    
    GHashTable *vehicle_data_file_buffer_table;
    guint vehicle_data_file_node_count;
    GSList *vehicle_data_file_remove_list;
    
    gint64 vehicle_data_last_timestamp;
    GHashTable *pending_vehicle_data_timestamp_table;
    
    guint vehicle_connection_change_server_timeout_id;
}TLNetData;

typedef struct _TLNetWriteBufferData
{
    GByteArray *buffer;
    gint64 timestamp;
    gboolean request_answer;
    guint8 request_type;
}TLNetWriteBufferData;

typedef struct _TLNetBacklogData
{
    gint64 timestamp;
    GByteArray *backlog;
}TLNetBacklogData;

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
    TL_NET_PACKET_ENCRYPTION_TYPE_NONE = 0x1,
    TL_NET_PACKET_ENCRYPTION_TYPE_RSA = 0x2,
    TL_NET_PACKET_ENCRYPTION_TYPE_AES = 0x3,
    TL_NET_PACKET_ENCRYPTION_TYPE_ABNORMAL = 0xFE,
    TL_NET_PACKET_ENCRYPTION_TYPE_INVALID = 0xFF,
}TLNetPacketEncryptionType;

typedef enum
{
    TL_NET_VEHICLE_DATA_TYPE_TOTAL_DATA = 0x1,
    TL_NET_VEHICLE_DATA_TYPE_DRIVE_MOTOR = 0x2,
    TL_NET_VEHICLE_DATA_TYPE_FUEL_BATTERY = 0x3,
    TL_NET_VEHICLE_DATA_TYPE_ENGINE = 0x4,
    TL_NET_VEHICLE_DATA_TYPE_VEHICLE_POSITION = 0x5,
    TL_NET_VEHICLE_DATA_TYPE_EXTREMUM = 0x6,
    TL_NET_VEHICLE_DATA_TYPE_ALARM = 0x7,
    TL_NET_VEHICLE_DATA_TYPE_RECHARGABLE_DEVICE_VOLTAGE = 0x8,
    TL_NET_VEHICLE_DATA_TYPE_RECHARGABLE_DEVICE_TEMPERATURE = 0x9,
    TL_NET_VEHICLE_DATA_TYPE_LAST
}TLNetVehicleDataType;

static TLNetData g_tl_net_data = {0};

#define TL_NET_CONF_PATH_DEFAULT "/var/lib/tbox/conf"

static void tl_net_vehicle_packet_build_total_data(GByteArray *packet,
    GHashTable *log_table);
static void tl_net_vehicle_packet_build_drive_motor_data(GByteArray *packet,
    GHashTable *log_table);
static void tl_net_vehicle_packet_build_extreme_data(GByteArray *packet,
    GHashTable *log_table);
static void tl_net_vehicle_packet_build_alarm_data(GByteArray *packet,
    GHashTable *log_table);
static gboolean tl_net_vehicle_packet_build_rechargable_device_voltage_data(
    GByteArray *packet, GHashTable *log_table, guint16 start_frame);
static void tl_net_vehicle_packet_build_rechargable_device_temp_data(
    GByteArray *packet, GHashTable *log_table);
static void tl_net_vehicle_packet_build_vehicle_position_data(
    GByteArray *packet, GHashTable *log_table);
    
static void tl_net_command_vehicle_data_query(TLNetData *net_data,
    const guint8 *payload, guint payload_len);
static void tl_net_command_vehicle_setup(TLNetData *net_data,
    const guint8 *payload, guint payload_len);
static void tl_net_command_terminal_control(TLNetData *net_data,
    const guint8 *payload, guint payload_len);

static inline guint16 tl_net_crc16_compute(const guchar *data_p,
    gsize length)
{
    guchar x;
    guint16 crc = 0xFFFF;
    while(length--)
    {
        x = crc >> 8 ^ *data_p++;
        x ^= x>>4;
        crc = (crc << 8) ^ ((guint16)(x << 12)) ^ ((guint16)(x <<5)) ^
            ((guint16)x);
    }
    return crc;
}


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

static void tl_net_backlog_data_free(TLNetBacklogData *data)
{
    if(data==NULL)
    {
        return;
    }
    if(data->backlog!=NULL)
    {
        g_byte_array_unref(data->backlog);
    }
    g_free(data);
}

static gboolean tl_net_config_load(TLNetData *net_data,
    const gchar *conf_file)
{
    GKeyFile *keyfile;
    GError *error = NULL;
    const gchar *svalue;
    gint ivalue, h, m;
    guint i;
    gchar server_key[16] = {0};
    
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
    
    ivalue = g_key_file_get_integer(keyfile, "Connnection", "AnswerTimeout",
        NULL);
    if(ivalue>0 && ivalue<=600)
    {
        net_data->vehicle_connection_answer_timeout = ivalue;
    }
    
    ivalue = g_key_file_get_integer(keyfile, "Connnection", "HeartbeatTimeout",
        NULL);
    if(ivalue>0 && ivalue<=240)
    {
        net_data->vehicle_connection_heartbeat_timeout = ivalue;
    }
    
    ivalue = g_key_file_get_integer(keyfile, "Connnection",
        "ReportNormalTimeout", NULL);
    if(ivalue>0 && ivalue<=600)
    {
        net_data->vehicle_data_report_normal_timeout = ivalue;
    }
    
    ivalue = g_key_file_get_integer(keyfile, "Connnection",
        "ReportEmergencyTimeout", NULL);
    if(ivalue>0 && ivalue<=600)
    {
        net_data->vehicle_data_report_emergency_timeout = ivalue;
    }
    
    ivalue = g_key_file_get_integer(keyfile, "Connnection",
        "LocalLogUpdateTimeout", NULL);
    if(ivalue>0 && ivalue<=60000)
    {
        tl_logger_log_update_timeout_set(ivalue);
    }
    
    for(i=0;i<5;i++)
    {
        snprintf(server_key, 15, "Host%u", i+1);
        svalue = g_key_file_get_string(keyfile, "Server", server_key, NULL);
        if(svalue!=NULL)
        {
            net_data->vehicle_server_list = g_list_append(
                net_data->vehicle_server_list, g_strdup(svalue));
        }
    }
    
    if(g_key_file_has_key(keyfile, "Config", "GravityThreshold", NULL))
    {
        ivalue = g_key_file_get_integer(keyfile, "Config",
            "GravityThreshold", NULL);
        if(ivalue>=0 && ivalue<=100)
        {
            tl_serial_gravity_threshold_set(ivalue);
        }
    }
    
    if(g_key_file_has_key(keyfile, "Config", "DailyAlarmClockHour", NULL))
    {
        h = g_key_file_get_integer(keyfile, "Config",
            "DailyAlarmClockHour", NULL);
        m = g_key_file_get_integer(keyfile, "Config",
            "DailyAlarmClockMinute", NULL);
        if(m<0 || m>=60)
        {
            m = 0;
        }
            
        if(h>=0 && h<=23)
        {
            tl_serial_power_on_daily_set(h, m);
        }
    }
    
    g_key_file_free(keyfile);
    
    return TRUE;
}

static void tl_net_config_sync()
{
    GKeyFile *keyfile;
    GError *error = NULL;
    guint i;
    const GList *list_foreach;
    gchar server_key[16] = {0};
    
    if(!g_tl_net_data.initialized)
    {
        return;
    }
    
    if(g_tl_net_data.conf_file_path==NULL)
    {
        return;
    }
    
    keyfile = g_key_file_new();
    
    g_key_file_load_from_file(keyfile, g_tl_net_data.conf_file_path,
        G_KEY_FILE_NONE, NULL);
        
    g_key_file_set_string(keyfile, "Network", "LastVIN", g_tl_net_data.vin);
    g_key_file_set_integer(keyfile, "Network", "LastSession",
        g_tl_net_data.session);
    g_key_file_set_integer(keyfile, "Connnection", "AnswerTimeout",
        g_tl_net_data.vehicle_connection_answer_timeout);
    g_key_file_set_integer(keyfile, "Connnection", "HeartbeatTimeout",
        g_tl_net_data.vehicle_connection_heartbeat_timeout);
    g_key_file_set_integer(keyfile, "Connnection", "ReportNormalTimeout",
        g_tl_net_data.vehicle_data_report_normal_timeout);
    g_key_file_set_integer(keyfile, "Connnection", "ReportEmergencyTimeout",
        g_tl_net_data.vehicle_data_report_emergency_timeout);
    g_key_file_set_integer(keyfile, "Connnection", "LocalLogUpdateTimeout",
        tl_logger_log_update_timeout_get());
        
    for(list_foreach=g_tl_net_data.vehicle_server_list, i=0;
        list_foreach!=NULL && g_list_next(list_foreach)!=NULL && i<5;
        list_foreach=g_list_next(list_foreach), i++)
    {
        if(list_foreach->data==NULL)
        {
            continue;
        }
        
        snprintf(server_key, 15, "Host%u", i+1);
        g_key_file_set_string(keyfile, "Server", server_key,
            (const gchar *)list_foreach->data);
    }
    
    g_key_file_save_to_file(keyfile, g_tl_net_data.conf_file_path, &error);
    if(error!=NULL)
    {
        g_warning("TLNet failed to save config data: %s", error->message);
        g_clear_error(&error);
    }
    
    g_key_file_free(keyfile);
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
    guint8 iccid_buf[20] = {0};
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
    tl_net_config_sync();
    
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

static GByteArray *tl_net_vehicle_data_packet_build(TLNetData *net_data,
    gboolean is_repeat, gint64 timestamp, GByteArray *payload)
{
    GByteArray *ba, *ret;
    GDateTime *dt;
    TLNetCommandType command;
    guint8 year, month, day, hour, min, sec;
    
    if(payload==NULL)
    {
        return NULL;
    }
    
    ba = g_byte_array_new();
    
    dt = g_date_time_new_from_unix_local(timestamp);
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
    
    g_byte_array_append(ba, payload->data, payload->len);
    
    command = is_repeat ? TL_NET_COMMAND_TYPE_REPEAT_DATA :
        TL_NET_COMMAND_TYPE_REALTIME_DATA;
    
    ret = tl_net_packet_build(command, TL_NET_ANSWER_TYPE_COMMAND,
        (const guint8 *)net_data->vin,
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
    gint64 now;
    
    if(net_data->current_vehicle_server!=NULL)
    {
        host = net_data->current_vehicle_server->data;
    }
    
    now = g_get_monotonic_time();
    
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
            net_data->vehicle_write_request_type = 
                write_buffer_data->request_type;
            net_data->vehicle_write_request_timestamp =
                write_buffer_data->timestamp;
            tl_net_write_buffer_data_free(write_buffer_data);
            net_data->vehicle_data_retry_count = 0;
            
            if(net_data->vehicle_write_buffer_dup!=NULL)
            {
                g_byte_array_unref(net_data->vehicle_write_buffer_dup);
                net_data->vehicle_write_buffer_dup = NULL;
            }
            if(net_data->vehicle_write_buffer!=NULL)
            {
                net_data->vehicle_write_buffer_dup = g_byte_array_new();
                g_byte_array_append(net_data->vehicle_write_buffer_dup,
                    net_data->vehicle_write_buffer->data,
                    net_data->vehicle_write_buffer->len);
            }
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
                    net_data->vehicle_connection_request_timestamp =
                        g_get_monotonic_time();
                }
                else
                {
                    g_byte_array_unref(net_data->vehicle_write_buffer);
                    net_data->vehicle_write_buffer = NULL;
                    
                    if(net_data->vehicle_write_request_type==
                        TL_NET_COMMAND_TYPE_REALTIME_DATA || 
                        net_data->vehicle_write_request_type==
                        TL_NET_COMMAND_TYPE_REPEAT_DATA)
                    {
                        g_mutex_lock(&(net_data->vehicle_data_mutex));
                        g_tree_remove(net_data->vehicle_data_tree,
                            &(net_data->vehicle_write_request_timestamp));
                        net_data->vehicle_write_request_timestamp = 0;
                        g_mutex_unlock(&(net_data->vehicle_data_mutex));
                    }
                }
                break;
            }
            else
            {
                g_byte_array_remove_range(net_data->vehicle_write_buffer, 0,
                    write_size);
            }
        }
        
        net_data->vehicle_connection_timestamp = now;
        
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

static void tl_net_connection_continue_write(TLNetData *net_data)
{
    if(g_queue_is_empty(net_data->vehicle_write_queue) &&
        net_data->vehicle_write_buffer==NULL)
    {
        if(net_data->vehicle_output_source!=NULL)
        {
            g_source_destroy(net_data->vehicle_output_source);
            g_source_unref(net_data->vehicle_output_source);
            net_data->vehicle_output_source = NULL;
        }
        return;
    }
    
    if(net_data->vehicle_output_source==NULL)
    {
        net_data->vehicle_output_source =
            g_pollable_output_stream_create_source(G_POLLABLE_OUTPUT_STREAM(
            net_data->vehicle_output_stream), NULL);
        if(net_data->vehicle_output_source!=NULL)
        {
            g_source_set_callback(net_data->vehicle_output_source,
                (GSourceFunc)
                tl_net_vehicle_connection_output_pollable_source_cb, net_data,
                NULL);
            g_source_attach(net_data->vehicle_output_source, NULL);
        }
    }
}

static void tl_net_packet_parse(TLNetData *net_data, guint8 command,
    guint8 answer, const gchar *vin, guint8 encryption, const guint8 *payload,
    guint payload_len)
{
    if(g_strcmp0(vin, net_data->vin)!=0)
    {
        return;
    }
    
    switch(command)
    {
        case TL_NET_COMMAND_TYPE_VEHICLE_LOGIN:
        {
            net_data->vehicle_write_request_type = 0;
            net_data->vehicle_write_request_answer = FALSE;
            if(net_data->vehicle_write_buffer!=NULL)
            {
                g_byte_array_unref(net_data->vehicle_write_buffer);
                net_data->vehicle_write_buffer = NULL;
            }
            if(answer==TL_NET_ANSWER_TYPE_SUCCEED)
            {
                net_data->vehicle_connection_state =
                    TL_NET_CONNECTION_STATE_LOGINED;
                net_data->vehicle_connection_retry_count = 0;
                net_data->vehicle_data_last_timestamp = 0;
                net_data->first_connected = TRUE;
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
        case TL_NET_COMMAND_TYPE_CLIENT_HEARTBEAT:
        {
            break;
        }
        case TL_NET_COMMAND_TYPE_QUERY:
        {
            if(answer==TL_NET_ANSWER_TYPE_COMMAND)
            {
                tl_net_command_vehicle_data_query(net_data, payload,
                    payload_len);
            }
            break;
        }
        case TL_NET_COMMAND_TYPE_SETUP:
        {
            if(answer==TL_NET_ANSWER_TYPE_COMMAND)
            {
                tl_net_command_vehicle_setup(net_data, payload, payload_len);
            }
            break;
        }
        case TL_NET_COMMAND_TYPE_TERMINAL_CONTROL:
        {
            if(answer==TL_NET_ANSWER_TYPE_COMMAND)
            {
                tl_net_command_terminal_control(net_data, payload,
                    payload_len);
            }
            break;
        }
        default:
        {
            g_message("Got unknown command type %u.", command);
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
    gint64 now;
    
    if(user_data==NULL)
    {
        return FALSE;
    }
    
    if(net_data->current_vehicle_server!=NULL)
    {
        host = net_data->current_vehicle_server->data;
    }
    
    now = g_get_monotonic_time();
    
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
        
        net_data->vehicle_connection_timestamp = now;
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
    TLNetData *net_data, GByteArray *packet, gboolean request_answer,
    guint8 request_type, gint64 timestamp)
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
    write_buffer_data->request_type = request_type;
    write_buffer_data->timestamp = timestamp;
    
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
        
        if(net_data->vehicle_connection_retry_count >
            net_data->vehicle_connection_retry_maximum)
        {
            if(net_data->current_vehicle_server!=NULL &&
                g_list_next(net_data->current_vehicle_server)!=NULL)
            {
                net_data->current_vehicle_server = g_list_next(
                    net_data->current_vehicle_server);
            }
            net_data->vehicle_connection_retry_count = 0;
        }
        else
        {
            net_data->vehicle_connection_retry_count++;
        }
    }
    g_clear_error(&error);
}

static gboolean tl_net_vehicle_data_traverse(gpointer key, gpointer value,
    gpointer user_data)
{
    TLNetData *net_data = (TLNetData *)user_data;
    gint64 *timestamp = (gint64 *)key;
    GByteArray *ba = (GByteArray *)value;
    GByteArray *packet;
    gboolean is_repeat;

    if(user_data==NULL)
    {
        return FALSE;
    }
    if(key==NULL)
    {
        return FALSE;
    }

    if(ba==NULL)
    {
        g_tree_remove(net_data->vehicle_data_tree, key);
        return TRUE;
    }

    is_repeat = (*timestamp < net_data->realtime_now -
        (gint64)net_data->vehicle_connection_answer_timeout *
        G_TIME_SPAN_SECOND);
    
    packet = tl_net_vehicle_data_packet_build(net_data, is_repeat,
        *timestamp, ba);
    if(packet==NULL)
    {
        return FALSE;
    }
    
    g_debug("Vehicle data packet timestamp %"G_GINT64_FORMAT"\n", *timestamp);
    
    tl_net_vehicle_connection_packet_output_request(net_data,
        packet, FALSE, is_repeat ? TL_NET_COMMAND_TYPE_REPEAT_DATA :
        TL_NET_COMMAND_TYPE_REALTIME_DATA, *timestamp);

    return TRUE;
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
        case TL_NET_CONNECTION_STATE_LOGINED:
        {
            GDateTime *dt;
            
            if(now - net_data->vehicle_connection_timestamp >
                (gint64)net_data->vehicle_connection_answer_timeout * 1e6)
            {
                g_warning("TLNet connection timeout!");
                tl_net_vehicle_connection_disconnect(net_data);
                break;
            }
            
            if(g_queue_is_empty(net_data->vehicle_write_queue) &&
                net_data->vehicle_write_buffer==NULL)
            {
                dt = g_date_time_new_now_local();
                net_data->realtime_now = g_date_time_to_unix(dt);
                net_data->time_now = now;
                g_date_time_unref(dt);
                
                g_mutex_lock(&(net_data->vehicle_data_mutex));
                g_tree_foreach(net_data->vehicle_data_tree,
                    tl_net_vehicle_data_traverse, net_data);
                g_mutex_unlock(&(net_data->vehicle_data_mutex));
            }
            
            if(g_queue_is_empty(net_data->vehicle_write_queue) &&
                net_data->vehicle_write_buffer==NULL)
            {
                if(now - net_data->vehicle_connection_heartbeat_timestamp >=
                    (gint64)net_data->vehicle_connection_heartbeat_timeout *
                    1e6)
                {
                    ba = tl_net_heartbeat_packet_build(net_data);
                    tl_net_vehicle_connection_packet_output_request(net_data,
                        ba, FALSE, TL_NET_COMMAND_TYPE_CLIENT_HEARTBEAT, 0);
                    g_byte_array_unref(ba);
                    net_data->vehicle_connection_heartbeat_timestamp = now;
                }
            }
            
            tl_net_connection_continue_write(net_data);
            
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
                if(net_data->current_vehicle_server!=NULL &&
                    g_list_next(net_data->current_vehicle_server)!=NULL)
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
                        ba, TRUE, TL_NET_COMMAND_TYPE_VEHICLE_LOGIN, 0);
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

static gboolean tl_net_vehicle_data_report_timeout(gpointer user_data)
{
    TLNetData *net_data = (TLNetData *)user_data;
    gboolean updated = FALSE;
    GHashTable *current_data_table;
    TLLoggerLogItemData *log_item_data;
    gint64 now, report_timeout;
    GByteArray *packet, *packet_dup;
    GDateTime *dt;
    gint64 timestamp;
    TLNetBacklogData *backlog_data;
    guint battery_frame_count = 0;
    
    if(user_data==NULL)
    {
        return FALSE;
    }
    
    current_data_table = tl_logger_current_data_get(&updated);
    if(current_data_table==NULL || !updated)
    {
        return TRUE;
    }
    
    dt = g_date_time_new_now_local();
    timestamp = g_date_time_to_unix(dt);
    g_date_time_unref(dt);
        
    packet = g_byte_array_new();
        
    tl_net_vehicle_packet_build_total_data(packet, current_data_table);
    tl_net_vehicle_packet_build_drive_motor_data(packet,
        current_data_table);
    tl_net_vehicle_packet_build_vehicle_position_data(packet,
        current_data_table);
    tl_net_vehicle_packet_build_extreme_data(packet, current_data_table);
    tl_net_vehicle_packet_build_alarm_data(packet, current_data_table);
    while(tl_net_vehicle_packet_build_rechargable_device_voltage_data(packet,
        current_data_table, battery_frame_count))
    {
        battery_frame_count += 200;
    }
    tl_net_vehicle_packet_build_rechargable_device_temp_data(packet,
        current_data_table);
    
    g_mutex_lock(&(net_data->vehicle_backlog_data_mutex));
    
    while(g_queue_get_length(net_data->vehicle_backlog_data_queue)>=
        TL_NET_BACKLOG_MAXIMUM)
    {
        backlog_data  = g_queue_pop_head(net_data->vehicle_backlog_data_queue);
        tl_net_backlog_data_free(backlog_data);
    }
    
    backlog_data = g_new0(TLNetBacklogData, 1);
    backlog_data->backlog = packet;
    backlog_data->timestamp = timestamp;
    g_queue_push_tail(net_data->vehicle_backlog_data_queue, backlog_data);
    
    g_mutex_unlock(&(net_data->vehicle_backlog_data_mutex));
    
    log_item_data = g_hash_table_lookup(current_data_table,
        TL_PARSER_VEHICLE_FAULT_LEVEL);
    if(log_item_data!=NULL)
    {
        net_data->vehicle_data_report_is_emergency =
            (log_item_data->value >= 3);
            
        if(net_data->vehicle_last_alarm_level<3 &&
            net_data->vehicle_data_report_is_emergency)
        {
            while(g_queue_get_length(net_data->vehicle_backlog_data_queue)>0)
            {
                backlog_data =
                    g_queue_pop_head(net_data->vehicle_backlog_data_queue);
                if(timestamp - backlog_data->timestamp <= 3e7)
                {
                    g_mutex_lock(&(net_data->vehicle_data_mutex));
                    g_tree_replace(net_data->vehicle_data_tree,
                        g_memdup(&(backlog_data->timestamp), sizeof(gint64)),
                        g_byte_array_ref(backlog_data->backlog));
                    g_mutex_unlock(&(net_data->vehicle_data_mutex));
                }
                
                tl_net_backlog_data_free(backlog_data);
            }
        }
        net_data->vehicle_last_alarm_level = log_item_data->value;
    }
    
    if(!net_data->first_connected)
    {
        return TRUE;
    }
    
    now = g_get_monotonic_time();
    
    report_timeout = net_data->vehicle_data_report_is_emergency ?
        net_data->vehicle_data_report_emergency_timeout :
        net_data->vehicle_data_report_normal_timeout;
    report_timeout *= 1e6;
    
    if(now - net_data->vehicle_data_report_timestamp >=
        report_timeout)
    {
        packet_dup = g_byte_array_new();
        g_byte_array_append(packet_dup, packet->data, packet->len);
        
        g_mutex_lock(&(net_data->vehicle_data_mutex));
        g_tree_replace(net_data->vehicle_data_tree, g_memdup(&timestamp,
            sizeof(gint64)), packet_dup);
        g_mutex_unlock(&(net_data->vehicle_data_mutex));
        
        net_data->vehicle_data_report_timestamp = now;
    }
    
    return TRUE;
}

static gint tl_net_int64ptr_compare(gconstpointer a, gconstpointer b,
    gpointer user_data)
{
    gint64 *x = (gint64 *)a;
    gint64 *y = (gint64 *)b;
    
    if(a==NULL && b==NULL)
    {
        return 0;
    }
    else if(a==NULL)
    {
        return -1;
    }
    else if(b==NULL)
    {
        return 1;
    }
    else if(*x==*y)
    {
        return 0;
    }
    else if(*x>*y)
    {
        return 1;
    }
    else
    {
        return -1;
    }
}

static guint tl_net_vehicle_data_from_file(TLNetData *net_data,
    const gchar *filepath, guint count, gboolean *remove_file)
{
    guint ret = 0;
    FILE *fp1, *fp2 = NULL;
    gboolean flag = FALSE;
    gchar buffer[4096];
    ssize_t rsize;
    gchar *tmp_file;
    guint i;
    guint expect_length = 0;
    guint16 expect_crc, crc;
    guint32 timestamp_high, timestamp_low;
    gint64 timestamp = 0;
    GByteArray *parse_buffer;
    GByteArray *packet;
    
    if(filepath==NULL)
    {
        return 0;
    }
    
    fp1 = fopen(filepath, "r");
    if(fp1==NULL)
    {
        return 0;
    }
    
    parse_buffer = g_byte_array_new();
    
    while(!feof(fp1))
    {
        rsize = fread(buffer, 1, 4096, fp1);
        if(!flag)
        {
            for(i=0;i<rsize;i++)
            {
                if(parse_buffer->len < 4)
                {
                    if(buffer[i]==TL_NET_PACKET_FILE_HEADER[parse_buffer->len])
                    {
                        g_byte_array_append(parse_buffer,
                            (const guint8 *)buffer+i, 1);
                    }
                }
                else if(parse_buffer->len < 18)
                {
                    g_byte_array_append(parse_buffer,
                        (const guint8 *)buffer+i, 1);
                    if(parse_buffer->len==18)
                    {
                        memcpy(&timestamp_high, parse_buffer->data + 4, 4);
                        memcpy(&timestamp_low, parse_buffer->data + 8, 4);
                        memcpy(&expect_length, parse_buffer->data + 12, 4);
                        memcpy(&expect_crc, parse_buffer->data + 16, 2);
                        
                        timestamp_high = g_ntohl(timestamp_high);
                        timestamp_low = g_ntohl(timestamp_low);
                        expect_length = g_ntohl(expect_length);
                        expect_crc = g_ntohs(expect_crc);
                        
                        if(expect_length==0)
                        {
                            g_byte_array_unref(parse_buffer);
                            parse_buffer = g_byte_array_new();
                            continue;
                        }
                        
                        timestamp = ((guint64)timestamp_high << 32) |
                            timestamp_low;
                    }
                }
                else if(parse_buffer->len < 18 + expect_length)
                {
                    g_byte_array_append(parse_buffer,
                        (const guint8 *)buffer+i, 1);
                        
                    if(parse_buffer->len == 18 + expect_length)
                    {
                        crc = tl_net_crc16_compute(parse_buffer->data + 18,
                            expect_length);
                        if(crc==expect_crc)
                        {
                            packet = g_byte_array_new();
                            g_byte_array_append(packet,
                                parse_buffer->data + 18, expect_length);
                            
                            g_mutex_lock(&(net_data->vehicle_data_mutex));
                            g_tree_insert(net_data->vehicle_data_tree,
                                g_memdup(&timestamp, sizeof(gint64)), packet);
                            g_mutex_unlock(&(net_data->vehicle_data_mutex));
                            
                            ret++;
                            
                            if(ret + count >= TL_NET_LOG_TO_DISK_TRIGGER/2)
                            {
                                flag = TRUE;
                                tmp_file = g_strdup_printf("%s.new",
                                    filepath);
                                fp2 = fopen(tmp_file, "w");
                                
                                break;
                            }
                        }
                        
                        g_byte_array_unref(parse_buffer);
                        parse_buffer = g_byte_array_new();
                        expect_length = 0;
                    }
                }
            }
        }
        else if(fp2!=NULL)
        {
            fwrite(buffer, 1, rsize, fp2);
        }
        else
        {
            break;
        }
    }
    
    g_byte_array_unref(parse_buffer);
    
    fclose(fp1);
    if(fp2!=NULL)
    {
        fclose(fp2);
        if(tmp_file!=NULL)
        {
            g_rename(tmp_file, filepath);
        }
    }
    if(tmp_file!=NULL)
    {
        g_free(tmp_file);
    }
    
    if(!flag)
    {
        if(remove_file!=NULL)
        {
            *remove_file = TRUE;
        }
    }
    
    return ret;
}

static gboolean tl_net_vehicle_data_to_file_traverse(gpointer key,
    gpointer value, gpointer user_data)
{
    TLNetData *net_data = (TLNetData *)user_data;
    gint64 *timestamp = (gint64 *)key;
    GByteArray *packet = (GByteArray *)value;
    GDateTime *dt;
    GByteArray *file_buffer;
    gchar *datestr;
    guint packet_size;
    guint16 checksum;
    guint32 timestamp_high, timestamp_low;
    
    if(user_data==NULL || key==NULL)
    {
        return TRUE;
    }
    
    if(value==NULL)
    {
        net_data->vehicle_data_file_remove_list = g_slist_prepend(
            net_data->vehicle_data_file_remove_list, key);
        return FALSE;
    }
    
    if(net_data->realtime_now2 - *timestamp <= (gint64)7 * G_TIME_SPAN_DAY)
    {
        dt = g_date_time_new_from_unix_local(*timestamp);
        datestr = g_date_time_format(dt, "%Y%m%d");
        g_date_time_unref(dt);
        
        timestamp_high = (*(const guint64*)timestamp >> 32);
        timestamp_low = (*(const guint64*)timestamp & 0xFFFFFFFF);
        
        timestamp_high = g_htonl(timestamp_high);
        timestamp_low = g_htonl(timestamp_low);
        
        packet_size = g_htonl(packet->len);
        
        checksum = tl_net_crc16_compute(packet->data, packet->len);
        checksum = g_htons(checksum);
        
        file_buffer = g_hash_table_lookup(
            net_data->vehicle_data_file_buffer_table, datestr);
        if(file_buffer!=NULL)
        {
            g_byte_array_append(file_buffer, TL_NET_PACKET_FILE_HEADER, 4);
            g_byte_array_append(file_buffer, (const guint8 *)
                &timestamp_high, 4);
            g_byte_array_append(file_buffer, (const guint8 *)
                &timestamp_low, 4);
            g_byte_array_append(file_buffer, (const guint8 *)&packet_size, 4);
            g_byte_array_append(file_buffer, (const guint8 *)&checksum, 2);
            g_byte_array_append(file_buffer, packet->data, packet->len);
            
            g_free(datestr);
        }
        else
        {
            file_buffer = g_byte_array_new();
            
            g_byte_array_append(file_buffer, TL_NET_PACKET_FILE_HEADER, 4);
            g_byte_array_append(file_buffer, (const guint8 *)
                &timestamp_high, 4);
            g_byte_array_append(file_buffer, (const guint8 *)
                &timestamp_low, 4);
            g_byte_array_append(file_buffer, (const guint8 *)&packet_size, 4);
            g_byte_array_append(file_buffer, (const guint8 *)&checksum, 2);
            g_byte_array_append(file_buffer, packet->data, packet->len);
            
            g_hash_table_replace(net_data->vehicle_data_file_buffer_table,
                datestr, file_buffer);
        }
        
        net_data->vehicle_data_file_node_count++;
    }

    net_data->vehicle_data_file_remove_list = g_slist_prepend(
        net_data->vehicle_data_file_remove_list, key);
    
    return (net_data->vehicle_data_file_node_count >
        TL_NET_LOG_TO_DISK_TRIGGER/2);
}

static gpointer tl_net_vehicle_data_log_thread(gpointer user_data)
{
    TLNetData *net_data = (TLNetData *)user_data;
    guint tree_len;
    GSList *list_foreach, *file_list;
    GDateTime *dt;
    GHashTableIter iter;
    const gchar *datestr;
    GByteArray *file_buffer;
    gchar *filename, *filepath;
    FILE *fp;
    int year, month, day;
    GDir *dir;
    const gchar *dfname;
    gint64 timestamp;
    guint i, ret;
    gboolean remove_file;
    
    if(user_data==NULL)
    {
        return NULL;
    }
    
    net_data->vehicle_data_log_thread_work_flag = TRUE;
    
    while(net_data->vehicle_data_log_thread_work_flag)
    {
        g_mutex_lock(&(net_data->vehicle_data_mutex));
        tree_len = g_tree_nnodes(net_data->vehicle_data_tree);
        g_mutex_unlock(&(net_data->vehicle_data_mutex));
        
        if(tree_len > TL_NET_LOG_TO_DISK_TRIGGER)
        {
            net_data->vehicle_data_file_remove_list = NULL;
            net_data->vehicle_data_file_node_count = 0;
            net_data->vehicle_data_file_buffer_table = g_hash_table_new_full(
                g_str_hash, g_str_equal, g_free, (GDestroyNotify)
                g_byte_array_unref);
            
            dt = g_date_time_new_now_local();
            net_data->realtime_now2 = g_date_time_to_unix(dt);
            g_date_time_unref(dt);
            
            g_mutex_lock(&(net_data->vehicle_data_mutex));
            g_tree_foreach(net_data->vehicle_data_tree,
                tl_net_vehicle_data_to_file_traverse, user_data);
            
            for(list_foreach=net_data->vehicle_data_file_remove_list;
                list_foreach!=NULL;list_foreach=g_slist_next(list_foreach))
            {
                if(list_foreach->data==NULL)
                {
                    continue;
                }
                g_tree_remove(net_data->vehicle_data_tree, list_foreach->data);
            }            
            g_mutex_unlock(&(net_data->vehicle_data_mutex));
            g_slist_free(net_data->vehicle_data_file_remove_list);
            net_data->vehicle_data_file_remove_list = NULL;
            
            g_hash_table_iter_init(&iter,
                net_data->vehicle_data_file_buffer_table);
            while(g_hash_table_iter_next(&iter, (gpointer *)&datestr,
                (gpointer *)&file_buffer))
            {
                if(datestr==NULL || file_buffer==NULL)
                {
                    continue;
                }
                filename = g_strdup_printf("tn-%s.tn", datestr);
                filepath = g_build_filename(net_data->log_path,
                    filename, NULL);
                g_free(filename);
                
                fp = fopen(filepath, "a");
                
                if(fp!=NULL)
                {
                    fwrite(file_buffer->data, 1, file_buffer->len, fp);
                }
                
                fclose(fp);
                
                g_free(filepath);
            }
            
            g_hash_table_unref(net_data->vehicle_data_file_buffer_table);
            net_data->vehicle_data_file_buffer_table = NULL;
            
            file_list = NULL;
            dir = g_dir_open(net_data->log_path, 0, NULL);
            if(dir!=NULL)
            {
                while((dfname=g_dir_read_name(dir))!=NULL)
                {
                    if(g_str_has_suffix(dfname, ".tn"))
                    {
                        year = 0;
                        month = 1;
                        day = 1;
                        sscanf(dfname, "tn-%04d%02d%02d.tn", &year, &month,
                            &day);
                        
                        dt = g_date_time_new_local(year, month, day, 0, 0, 0);
                        timestamp = g_date_time_to_unix(dt);
                        g_date_time_unref(dt);
                        
                        if(net_data->realtime_now2 - timestamp >=
                            (gint64)8 * G_TIME_SPAN_DAY)
                        {
                            filepath = g_build_filename(net_data->log_path,
                                dfname, NULL);
                            file_list = g_slist_prepend(file_list, filepath);
                        }
                    }
                }
                
                g_dir_close(dir);
            }
                
            for(list_foreach=file_list;list_foreach!=NULL;
                list_foreach=g_slist_next(list_foreach))
            {
                if(list_foreach->data==NULL)
                {
                    continue;
                }
                filepath = list_foreach->data;
                g_remove(filepath);
            }
            g_slist_free_full(file_list, g_free);
            file_list = NULL;
        }
        else if(tree_len==0)
        {
            file_list = NULL;
            dir = g_dir_open(net_data->log_path, 0, NULL);
            i = 0;
            if(dir!=NULL)
            {
                while((dfname=g_dir_read_name(dir))!=NULL &&
                    i<TL_NET_LOG_TO_DISK_TRIGGER/2)
                {
                    if(g_str_has_suffix(dfname, ".tn"))
                    {
                        year = 0;
                        month = 1;
                        day = 1;
                        sscanf(dfname, "tn-%04d%02d%02d.tn", &year, &month,
                            &day);
                        
                        dt = g_date_time_new_local(year, month, day, 0, 0, 0);
                        timestamp = g_date_time_to_unix(dt);
                        g_date_time_unref(dt);
                        
                        if(net_data->realtime_now2 - timestamp <
                            (gint64)7 * G_TIME_SPAN_DAY)
                        {
                            filepath = g_build_filename(net_data->log_path,
                                dfname, NULL);

                            remove_file = FALSE;
                            ret = tl_net_vehicle_data_from_file(net_data,
                                filepath, i, &remove_file);
                                
                            i += ret;

                            if(remove_file)
                            {
                                file_list = g_slist_prepend(file_list,
                                    filepath);
                            }
                            else
                            {
                                g_free(filepath);
                            }
                        }
                    }
                }
                
                g_dir_close(dir);
            }
            
            for(list_foreach=file_list;list_foreach!=NULL;
                list_foreach=g_slist_next(list_foreach))
            {
                if(list_foreach->data==NULL)
                {
                    continue;
                }
                filepath = list_foreach->data;
                g_remove(filepath);
            }
            g_slist_free_full(file_list, g_free);
            file_list = NULL;
        }
        
        g_usleep(1000000);
    }
    
    return NULL;
}

static void tl_net_reset_arguments()
{
    g_tl_net_data.vehicle_connection_answer_timeout = 60;
    g_tl_net_data.vehicle_connection_heartbeat_timeout = 10;
    g_tl_net_data.vehicle_data_report_normal_timeout = 5;
    g_tl_net_data.vehicle_data_report_emergency_timeout = 1;
    tl_logger_log_update_timeout_set(10000);
    
    tl_net_config_sync();
}

gboolean tl_net_init(const gchar *vin, const gchar *iccid,
    const gchar *conf_path, const gchar *log_path,
    const gchar *fallback_vehicle_server_host,
    guint16 fallback_vehicle_server_port)
{
    FILE *fp;
    
    if(g_tl_net_data.initialized)
    {
        g_warning("TLNet already initialized!");
        return TRUE;
    }
    
    if(g_tl_net_data.vin!=NULL)
    {
        g_free(g_tl_net_data.vin);
    }
    g_tl_net_data.vin = g_strdup(vin);
    
    if(g_tl_net_data.iccid!=NULL)
    {
        g_free(g_tl_net_data.iccid);
    }
    g_tl_net_data.iccid = g_strdup(iccid);
    
    if(g_tl_net_data.log_path!=NULL)
    {
        g_free(g_tl_net_data.log_path);
    }
    g_tl_net_data.log_path = g_strdup(log_path);
    
    g_tl_net_data.vehicle_connection_retry_maximum = 3;
    g_tl_net_data.vehicle_connection_retry_cycle = 10;
    
    g_tl_net_data.vehicle_connection_answer_timeout = 60;
    g_tl_net_data.vehicle_connection_heartbeat_timeout = 10;
    
    g_tl_net_data.vehicle_data_report_normal_timeout = 5;
    g_tl_net_data.vehicle_data_report_emergency_timeout = 1;
    g_tl_net_data.vehicle_data_report_is_emergency = FALSE;
    
    g_tl_net_data.vehicle_connection_state = 0;
    g_tl_net_data.vehicle_last_alarm_level = 0;
    
    fp = fopen("/etc/tboxhwver", "r");
    if(fp!=NULL)
    {
        fread(g_tl_net_data.hwversion, 1, 5, fp);
        fclose(fp);
    }
    
    fp = fopen("/etc/tboxfwver", "r");
    if(fp!=NULL)
    {
        fread(g_tl_net_data.fwversion, 1, 5, fp);
        fclose(fp);
    }
    
    g_mutex_init(&(g_tl_net_data.vehicle_data_mutex));
    g_mutex_init(&(g_tl_net_data.vehicle_backlog_data_mutex));
    
    g_tl_net_data.vehicle_data_tree = g_tree_new_full(tl_net_int64ptr_compare,
        NULL, g_free, (GDestroyNotify)g_byte_array_unref);
    g_tl_net_data.vehicle_backlog_data_queue = g_queue_new();
    
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
        "settings.conf", NULL);
    tl_net_config_load(&g_tl_net_data, g_tl_net_data.conf_file_path);
    
    
    g_tl_net_data.vehicle_client = g_socket_client_new();
    g_socket_client_set_timeout(g_tl_net_data.vehicle_client, 60);
    
    g_tl_net_data.current_vehicle_server = g_tl_net_data.vehicle_server_list;
    
    g_tl_net_data.vehicle_connection_check_timeout = g_timeout_add_seconds(2,
        tl_net_vehicle_connection_check_timeout_cb, &g_tl_net_data);
    
    g_tl_net_data.vehicle_data_report_timer_timeout = g_timeout_add_seconds(1,
        tl_net_vehicle_data_report_timeout, &g_tl_net_data);
    
    
    g_tl_net_data.vehicle_data_log_thread = g_thread_new(
        "tl-net-vehicle-data-log-thread", tl_net_vehicle_data_log_thread,
        &g_tl_net_data);
    
    g_tl_net_data.initialized = TRUE;
    
    return TRUE;
}

void tl_net_uninit()
{
    if(!g_tl_net_data.initialized)
    {
        return;
    }
    
    if(g_tl_net_data.vehicle_connection_change_server_timeout_id>0)
    {
        g_source_remove(
            g_tl_net_data.vehicle_connection_change_server_timeout_id);
        g_tl_net_data.vehicle_connection_change_server_timeout_id = 0;
    }
    
    if(g_tl_net_data.vehicle_data_report_timer_timeout>0)
    {
        g_source_remove(g_tl_net_data.vehicle_data_report_timer_timeout);
        g_tl_net_data.vehicle_data_report_timer_timeout = 0;
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
    
    if(g_tl_net_data.vehicle_data_log_thread!=NULL)
    {
        g_tl_net_data.vehicle_data_log_thread_work_flag = FALSE;
        g_thread_join(g_tl_net_data.vehicle_data_log_thread);
        g_tl_net_data.vehicle_data_log_thread = NULL;
    }
    
    if(g_tl_net_data.vehicle_write_queue!=NULL)
    {
        g_queue_free_full(g_tl_net_data.vehicle_write_queue,
            (GDestroyNotify)tl_net_write_buffer_data_free);
        g_tl_net_data.vehicle_write_queue = NULL;
    }
    
    g_mutex_lock(&(g_tl_net_data.vehicle_data_mutex));
    if(g_tl_net_data.vehicle_data_tree!=NULL)
    {
        g_tree_unref(g_tl_net_data.vehicle_data_tree);
        g_tl_net_data.vehicle_data_tree = NULL;
    }
    g_mutex_unlock(&(g_tl_net_data.vehicle_data_mutex));
    
    g_mutex_lock(&(g_tl_net_data.vehicle_backlog_data_mutex));
    if(g_tl_net_data.vehicle_backlog_data_queue!=NULL)
    {
        g_queue_free_full(g_tl_net_data.vehicle_backlog_data_queue,
            (GDestroyNotify)tl_net_backlog_data_free);
        g_tl_net_data.vehicle_backlog_data_queue = NULL;
    }
    g_mutex_unlock(&(g_tl_net_data.vehicle_backlog_data_mutex));
    
    g_mutex_clear(&(g_tl_net_data.vehicle_data_mutex));
    g_mutex_clear(&(g_tl_net_data.vehicle_backlog_data_mutex));
    
    if(g_tl_net_data.iccid!=NULL)
    {
        g_free(g_tl_net_data.iccid);
        g_tl_net_data.iccid = NULL;
    }
    if(g_tl_net_data.vin!=NULL)
    {
        g_free(g_tl_net_data.vin);
        g_tl_net_data.vin = NULL;
    }
    if(g_tl_net_data.log_path!=NULL)
    {
        g_free(g_tl_net_data.log_path);
        g_tl_net_data.log_path = NULL;
    }
    
    g_tl_net_data.initialized = FALSE;
}

static void tl_net_vehicle_packet_build_total_data(GByteArray *packet,
    GHashTable *log_table)
{
    guint8 u8_value;
    guint16 u16_value;
    guint32 u32_value;
    const TLLoggerLogItemData *item_data;
    gdouble temp;
    
    u8_value = TL_NET_VEHICLE_DATA_TYPE_TOTAL_DATA;
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_VEHICLE_STATE);
    if(item_data!=NULL)
    {
        if(item_data->value==0)
        {
            u8_value = 2;
        }
        else if(item_data->value==1)
        {
            u8_value = 1;
        }
        else
        {
            u8_value = 3;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_BATTERY_STATE);
    if(item_data!=NULL)
    {
        if(item_data->value==6)
        {
            u8_value = 1;
        }
        else if(item_data->value==7)
        {
            u8_value = 2;
        }
        else if(item_data->value==8)
        {
            u8_value = 4;
        }
        else if(item_data->value==0xA)
        {
            u8_value = 0xFE;
        }
        else if(item_data->value>=0 && item_data->value<=5)
        {
            u8_value = 3;
        }
        else
        {
            u8_value = 0xFF;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_RUNNING_MODE);
    if(item_data!=NULL)
    {
        if(item_data->value==1)
        {
            u8_value = 1;
        }
        else if(item_data->value==3)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = 0xFF;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_VEHICLE_SPEED);
    if(item_data!=NULL)
    {
        temp = (gdouble)item_data->value * item_data->unit +
            item_data->offset;
        temp *= 10;
        u16_value = temp;
        if(u16_value <= 2200)
        {
            u16_value = g_htons(u16_value);
        }
        else
        {
            u16_value = g_htons(0xFFFE);
        }
    }
    else
    {
        u16_value = 0xFFFF;
    }
    g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_TOTAL_MILEAGE);
    if(item_data!=NULL)
    {
        u32_value = (gdouble)item_data->value * item_data->unit +
            item_data->offset;
        u32_value *= 10;
        if(u32_value <= 9999999)
        {
            u32_value = g_htonl(u32_value);
        }
        else
        {
            u32_value = g_htonl(0xFFFFFFFE);
        }
    }
    else
    {
        u32_value = 0xFFFFFFFF;
    }
    g_byte_array_append(packet, (const guint8 *)&u32_value, 4);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_TOTAL_VOLTAGE);
    if(item_data!=NULL)
    {
        temp = (gdouble)item_data->value * item_data->unit +
            item_data->offset;
        temp *= 10;
        u16_value = temp;
        if(u16_value <= 10000)
        {
            u16_value = g_htons(u16_value);
        }
        else
        {
            u16_value = g_htons(0xFFFE);
        }
    }
    else
    {
        u16_value = 0xFFFF;
    }
    g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_TOTAL_CURRENT);
    if(item_data!=NULL)
    {
        temp = ((gdouble)item_data->value * item_data->unit +
            item_data->offset) + 1000;
        temp *= 10;
        u16_value = temp;
        if(u16_value <= 20000)
        {
            u16_value = g_htons(u16_value);
        }
        else
        {
            u16_value = g_htons(0xFFFE);
        }
    }
    else
    {
        u16_value = 0xFFFF;
    }
    g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_SOC_STATE);
    if(item_data!=NULL)
    {
        temp = (gdouble)item_data->value * item_data->unit +
            item_data->offset;
        if(temp>100)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = temp;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_DC2DC_STATE);
    if(item_data!=NULL)
    {
        if(item_data->value==1)
        {
            u8_value = 1;
        }
        else if(item_data->value==0 || item_data->value==2)
        {
            u8_value = 2;
        }
        else
        {
            u8_value = 0xFE;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_GEAR_SHIFT_STATE);
    if(item_data!=NULL)
    {
        if(item_data->value==0)
        {
            u8_value = 0;
        }
        else if(item_data->value==1)
        {
            u8_value = 0xE;
        }
        else if(item_data->value==2)
        {
            u8_value = 0xD;
        }
        else if(item_data->value==3)
        {
            u8_value = 0xF;
        }
        else
        {
            u8_value = 0;
        }
    }
    else
    {
        u8_value = 0;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_INSULATION_RESISTANCE);
    if(item_data!=NULL)
    {
        temp = (gdouble)item_data->value * item_data->unit + item_data->offset;
        temp *= 10;
        u16_value = temp;
        u16_value = g_htons(u16_value);
    }
    else
    {
        u16_value = 0xFFFF;
    }
    g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_ACCELERATOR_LEVEL);
    if(item_data!=NULL)
    {
        if(item_data->value>100)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = (gdouble)item_data->value * item_data->unit;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_BRAKE_LEVEL);
    if(item_data!=NULL)
    {
        if(item_data->value>101)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
}

static void tl_net_vehicle_packet_build_drive_motor_data(GByteArray *packet,
    GHashTable *log_table)
{
    guint8 u8_value;
    guint16 u16_value;
    const TLLoggerLogItemData *item_data;
    GHashTable *index_table;
    GHashTableIter iter;
    GHashTable *state_table, *controller_temp_table, *spin_speed_table;
    GHashTable *torque_table, *temperature_table, *controller_voltage_table;
    GHashTable *controller_current_table;
    gdouble controller_voltage_unit = 1.0;
    gdouble controller_current_unit = 1.0;
    gdouble controller_temp_unit = 1.0;
    gdouble torque_unit = 1.0;
    gdouble spin_speed_unit = 1.0;
    gdouble temperature_unit = 1.0;
    
    gint controller_temp_offset = 0;
    gint torque_offset = 0;
    gint temperature_offset = 0;
    gint controller_current_offset = 0;
    gint spin_speed_offset = 0;
    gint controller_voltage_offset = 0;
    guint table_size, i = 0;
    gpointer key;
    gint index;
    const gint64 *raw_value;
    gdouble temp;
    
    u8_value = TL_NET_VEHICLE_DATA_TYPE_DRIVE_MOTOR;
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_DRIVE_MOTOR_INDEX);
    if(item_data==NULL || !item_data->list_index ||
        item_data->index_table==NULL)  
    {
        u8_value = 0;
        g_byte_array_append(packet, &u8_value, 1);
        return;
    }
    index_table = item_data->index_table;
    
    table_size = g_hash_table_size(index_table);
    if(table_size>253)
    {
        table_size = 253;
    }
    u8_value = table_size;
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_DRIVE_MOTOR_STATE);
    if(item_data!=NULL && item_data->list_parent!=NULL)
    {
        state_table = item_data->list_table;
    }
    else
    {
        state_table = NULL;
    }

    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_DRIVE_MOTOR_CONTROLLER_TEMPERATURE);
    if(item_data!=NULL && item_data->list_parent!=NULL)
    {
        controller_temp_table = item_data->list_table;
        controller_temp_unit = item_data->unit;
        controller_temp_offset = item_data->offset;
    }
    else
    {
        controller_temp_table = NULL;
    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_DRIVE_MOTOR_SPIN_SPEED);
    if(item_data!=NULL && item_data->list_parent!=NULL)
    {
        spin_speed_table = item_data->list_table;
        spin_speed_offset = item_data->offset;
        spin_speed_unit = item_data->unit;
    }
    else
    {
        spin_speed_table = NULL;
    }
        
    item_data = g_hash_table_lookup(log_table, TL_PARSER_DRIVE_MOTOR_TORQUE);
    if(item_data!=NULL && item_data->list_parent!=NULL)
    {
        torque_table = item_data->list_table;
        torque_offset = item_data->offset;
        torque_unit = item_data->unit;
    }
    else
    {
        torque_table = NULL;
    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_DRIVE_MOTOR_TEMPERATURE);
    if(item_data!=NULL && item_data->list_parent!=NULL)
    {
        temperature_table = item_data->list_table;
        temperature_offset = item_data->offset;
        temperature_unit = item_data->unit;
    }
    else
    {
        temperature_table = NULL;
    }
        
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_DRIVE_MOTOR_CONTROLLER_VOLTAGE);
    if(item_data!=NULL && item_data->list_parent!=NULL)
    {
        controller_voltage_table = item_data->list_table;
        controller_voltage_unit = item_data->unit;
        controller_voltage_offset = item_data->offset;
    }
    else
    {
        controller_voltage_table = NULL;
    }
        
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_DRIVE_MOTOR_CONTROLLER_CURRENT);
    if(item_data!=NULL && item_data->list_parent!=NULL)
    {
        controller_current_table = item_data->list_table;
        controller_current_unit = item_data->unit;
        controller_current_offset = item_data->offset;
    }
    else
    {
        controller_current_table = NULL;
    }
    
    g_hash_table_iter_init(&iter, index_table);
    while(g_hash_table_iter_next(&iter, &key, NULL) && i<253)
    {
        if(key==NULL)
        {
            continue;
        }
        
        i++;
        sscanf((const gchar *)key, "%u", &index);
        
        u8_value = index;
        g_byte_array_append(packet, &u8_value, 1);
        
        u8_value = 0xFF;
        if(state_table!=NULL)
        {
            raw_value = g_hash_table_lookup(state_table, key);
            if(raw_value!=NULL)
            {
                switch(*raw_value)
                {
                    case 0:
                    {
                        u8_value = 3;
                        break;
                    }
                    case 1:
                    {
                        u8_value = 4;
                        break;
                    }
                    case 3:
                    {
                        u8_value = 1;
                        break;
                    }
                    case 4:
                    {
                        u8_value = 2;
                        break;
                    }
                    case 5:
                    {
                        u8_value = 0xFE;
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
            }
        }
        g_byte_array_append(packet, &u8_value, 1);
        
        u8_value = 0xFF;
        if(controller_temp_table!=NULL)
        {
            raw_value = g_hash_table_lookup(controller_temp_table, key);
            if(raw_value!=NULL)
            {
                temp = (gdouble)(*raw_value) * controller_temp_unit +
                    controller_temp_offset;
                if(temp <= 210 && temp >= -40)
                {
                    u8_value = temp + 40;
                }
                else
                {
                    u8_value = 0xFE;
                }
            }
        }
        g_byte_array_append(packet, &u8_value, 1);
        
        u16_value = 0xFFFF;
        if(spin_speed_table!=NULL)
        {
            raw_value = g_hash_table_lookup(spin_speed_table, key);
            if(raw_value!=NULL)
            {
                temp = (gdouble)(*raw_value) * spin_speed_unit +
                    spin_speed_offset;
                if(temp <= 45531 && temp >= -20000)
                {
                    u16_value = temp + 20000;
                }
                else
                {
                    u16_value = 0xFFFE;
                }
            }
        }
        u16_value = g_htons(u16_value);
        g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
        
        u16_value = 0xFFFF;
        if(torque_table!=NULL)
        {
            raw_value = g_hash_table_lookup(torque_table, key);
            if(raw_value!=NULL)
            {
                temp = (gdouble)(*raw_value) * torque_unit + torque_offset;
                temp *= 10;
                if(temp <= 45531 && temp >= -20000)
                {
                    u16_value = temp + 20000;
                }
                else
                {
                    u16_value = 0xFFFE;
                }
            }
        }
        u16_value = g_htons(u16_value);
        g_byte_array_append(packet, (const guint8 *)&u16_value, 2);

        u8_value = 0xFF;
        if(temperature_table!=NULL)
        {
            raw_value = g_hash_table_lookup(temperature_table, key);
            if(raw_value!=NULL)
            {
                temp = (gdouble)(*raw_value) * temperature_unit +
                    temperature_offset;
                if(temp <= 210 && temp >= -40)
                {
                    u8_value = temp + 40;
                }
                else
                {
                    u8_value = 0xFE;
                }
            }
        }
        g_byte_array_append(packet, &u8_value, 1);
        
        u16_value = 0xFFFF;
        if(controller_voltage_table!=NULL)
        {
            raw_value = g_hash_table_lookup(controller_voltage_table, key);
            if(raw_value!=NULL)
            {
                temp = (gdouble)(*raw_value) * controller_voltage_unit +
                    controller_voltage_offset;
                temp *= 10;
                if(temp<=60000 && temp>=0)
                {
                    u16_value = temp;
                }
                else
                {
                    u16_value = 0xFFFE;
                }
            }
        }
        u16_value = g_htons(u16_value);
        g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
        
        u16_value = 0xFFFF;
        if(controller_current_table!=NULL)
        {
            raw_value = g_hash_table_lookup(controller_current_table, key);
            if(raw_value!=NULL)
            {
                temp = ((gdouble)*raw_value * controller_current_unit +
                    controller_current_offset) * 10;
                temp += 10000;
                if(temp<=20000 && temp>=0)
                {
                    u16_value = temp;
                }
                else
                {
                    u16_value = 0xFFFE;
                }
            }
        }
        u16_value = g_htons(u16_value);
        g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
    }
}

static void tl_net_vehicle_packet_build_extreme_data(GByteArray *packet,
    GHashTable *log_table)
{
    guint8 u8_value;
    guint16 u16_value;
    const TLLoggerLogItemData *item_data;
    
    u8_value = TL_NET_VEHICLE_DATA_TYPE_EXTREMUM;
    g_byte_array_append(packet, &u8_value, 1);

    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_SUBSYSTEM_MAX_VOLTAGE_ID);
    if(item_data!=NULL)
    {
        if(item_data->value>=250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value + 1;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_CELL_MAX_VOLTAGE_ID);
    if(item_data!=NULL)
    {
        if(item_data->value>=250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value + 1;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_CELL_MAX_VOLTAGE);
    if(item_data!=NULL)
    {
        if(item_data->value>15000)
        {
            u16_value = 0xFFFE;
        }
        else
        {
            u16_value = (gdouble)item_data->value * item_data->unit +
                item_data->offset;
        }
    }
    else
    {
        u16_value = 0xFFFF;
    }
    u16_value = g_htons(u16_value);
    g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_SUBSYSTEM_MIN_VOLTAGE_ID);
    if(item_data!=NULL)
    {
        if(item_data->value>=250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value + 1;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_CELL_MIN_VOLTAGE_ID);
    if(item_data!=NULL)
    {
        if(item_data->value>=250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value + 1;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_CELL_MIN_VOLTAGE);
    if(item_data!=NULL)
    {
        if(item_data->value>15000)
        {
            u16_value = 0xFFFE;
        }
        else
        {
            u16_value = (gdouble)item_data->value * item_data->unit +
                item_data->offset;
        }
    }
    else
    {
        u16_value = 0xFFFF;
    }
    u16_value = g_htons(u16_value);
    g_byte_array_append(packet, (const guint8 *)&u16_value, 2);

    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_SUBSYSTEM_MAX_TEMPERATURE_ID);
    if(item_data!=NULL)
    {
        if(item_data->value>=250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value + 1;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_CELL_MAX_TEMPERATURE_ID);
    if(item_data!=NULL)
    {
        if(item_data->value>=250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value + 1;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_CELL_MAX_TEMPERATURE);
    if(item_data!=NULL)
    {
        if(item_data->value>250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = (gdouble)item_data->value * item_data->unit +
                item_data->offset + 40;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);

    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_SUBSYSTEM_MIN_TEMPERATURE_ID);
    if(item_data!=NULL)
    {
        if(item_data->value>=250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value + 1;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_CELL_MIN_TEMPERATURE_ID);
    if(item_data!=NULL)
    {
        if(item_data->value>=250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value + 1;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_CELL_MIN_TEMPERATURE);
    if(item_data!=NULL)
    {
        if(item_data->value>250)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = (gdouble)item_data->value * item_data->unit +
                item_data->offset + 40;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
}

static void tl_net_vehicle_packet_build_alarm_data(GByteArray *packet,
    GHashTable *log_table)
{
    guint8 u8_value;
    guint32 u32_value;
    const TLLoggerLogItemData *item_data;
    
    u8_value = TL_NET_VEHICLE_DATA_TYPE_ALARM;
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_VEHICLE_FAULT_LEVEL);
    if(item_data!=NULL)
    {
        if(item_data->value>3)
        {
            u8_value = 0xFE;
        }
        else
        {
            u8_value = item_data->value;
        }
    }
    else
    {
        u8_value = 0xFF;
    }
    g_byte_array_append(packet, &u8_value, 1);
    
    u32_value = 0;
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_TEMPERATURE_DIFF);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 0);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_BATTERY_OVERHEAT);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 1);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_BATTERY_OVERVOLTAGE);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 2);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_BATTERY_UNDERVOLTAGE);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 3);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_SOC_LOW);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 4);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_BATTERY_CELL_OVERVOLTAGE);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 5);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_BATTERY_CELL_UNDERVOLTAGE);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 6);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_SOC_HIGH);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 7);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_SOC_JUMP);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 8);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_BATTERY_MISMATCH);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 9);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_BATTERY_CONSIST);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 10);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_BAD_INSULATION);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 11);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_DC2DC_OVERHEAT);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 12);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_EVP);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 13);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_DC2DC);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 14);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_DRIVE_MOTOR_CONTROLLER_TEMPERATURE);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 15);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_EMERGENCY_OFF_PILOT);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 16);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_DRIVE_MOTOR_TEMPERATURE);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 17);
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_ALARM_SOC_OVERCHARGE);
    if(item_data!=NULL)
    {
        if(item_data->value!=0)
        {
            u32_value |= (1 << 18);
        }
    }
    g_byte_array_append(packet, (const guint8 *)&u32_value, 4);
    
    u8_value = 0;
    g_byte_array_append(packet, &u8_value, 1);
    g_byte_array_append(packet, &u8_value, 1);
    g_byte_array_append(packet, &u8_value, 1);
    g_byte_array_append(packet, &u8_value, 1);
}

static gboolean tl_net_vehicle_packet_build_rechargable_device_voltage_data(
    GByteArray *packet, GHashTable *log_table, guint16 start_frame)
{
    guint8 u8_value;
    guint16 u16_value;
    const TLLoggerLogItemData *item_data;
    GHashTable *index_table;
    GHashTableIter iter, iter2;
    guint table_size;
    guint16 cell_number;
    const gchar *key, *key2, *index_str;
    guint16 battery_voltage, battery_current;
    gint64 *raw_value;
    gdouble temp;
    GHashTable *cell_number_table = NULL;
    GHashTable *cell_index_table[3] = {NULL, NULL, NULL};
    GHashTable *cell_g0_voltage_table[4] = {NULL, NULL, NULL, NULL};
    GHashTable *cell_g1_voltage_table[4] = {NULL, NULL, NULL, NULL};
    GHashTable *cell_g2_voltage_table[4] = {NULL, NULL, NULL, NULL};
    gboolean have_more_data = FALSE;
    guint16 values[200];
    guint16 frame_number;
    guint subsys_id;
    guint index, i, j;
    gdouble cell_voltage_units[3][4];
    gint cell_voltage_offset[3][4];

    u8_value = TL_NET_VEHICLE_DATA_TYPE_RECHARGABLE_DEVICE_VOLTAGE;
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_VOLTAGE_SUBSYSTEM_INDEX);
    if(item_data==NULL || !item_data->list_index ||
        item_data->index_table==NULL)  
    {
        u8_value = 0xFF;
        g_byte_array_append(packet, &u8_value, 1);
        return FALSE;
    }
    index_table = item_data->index_table;
    
    table_size = g_hash_table_size(index_table);
    
    if(table_size>250)
    {
        table_size = 250;
    }
    if(table_size==0)
    {
        u8_value = 0xFF;
        g_byte_array_append(packet, &u8_value, 1);
        return FALSE;
    }
    u8_value = table_size;
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_TOTAL_VOLTAGE);
    if(item_data!=NULL)
    {
        temp = (gdouble)item_data->value * item_data->unit +
            item_data->offset;
        temp *= 10;
        battery_voltage = temp;
        if(battery_voltage <= 10000)
        {
            battery_voltage = g_htons(battery_voltage);
        }
        else
        {
            battery_voltage = g_htons(0xFFFE);
        }
    }
    else
    {
        battery_voltage = 0xFFFF;
    }
    
    item_data = g_hash_table_lookup(log_table, TL_PARSER_TOTAL_CURRENT);
    if(item_data!=NULL)
    {
        temp = ((gdouble)item_data->value * item_data->unit +
            item_data->offset) + 1000;
        temp *= 10;
        battery_current = temp / table_size;
        if(battery_current <= 20000)
        {
            battery_current = g_htons(battery_current);
        }
        else
        {
            battery_current = g_htons(0xFFFE);
        }
    }
    else
    {
        battery_current = 0xFFFF;
    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_CELL_NUMBER);
    if(item_data!=NULL)
    {
        cell_number_table = item_data->list_table;
    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_CELL_START_ID);
    if(item_data!=NULL)
    {
        cell_index_table[0] = item_data->index_table;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_CELL_START_ID);
    if(item_data!=NULL)
    {
        cell_index_table[1] = item_data->index_table;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_CELL_START_ID);
    if(item_data!=NULL)
    {
        cell_index_table[2] = item_data->index_table;
    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_CELL_P0_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g0_voltage_table[0] = item_data->list_table;
        cell_voltage_units[0][0] = item_data->unit;
        cell_voltage_offset[0][0] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_CELL_P1_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g0_voltage_table[1] = item_data->list_table;
        cell_voltage_units[0][1] = item_data->unit;
        cell_voltage_offset[0][1] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_CELL_P2_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g0_voltage_table[2] = item_data->list_table;
        cell_voltage_units[0][2] = item_data->unit;
        cell_voltage_offset[0][2] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_CELL_P3_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g0_voltage_table[3] = item_data->list_table;
        cell_voltage_units[0][3] = item_data->unit;
        cell_voltage_offset[0][3] = item_data->offset;
    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_CELL_P0_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g1_voltage_table[0] = item_data->list_table;
        cell_voltage_units[1][0] = item_data->unit;
        cell_voltage_offset[1][0] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_CELL_P1_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g1_voltage_table[1] = item_data->list_table;
        cell_voltage_units[1][1] = item_data->unit;
        cell_voltage_offset[1][1] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_CELL_P2_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g1_voltage_table[2] = item_data->list_table;
        cell_voltage_units[1][2] = item_data->unit;
        cell_voltage_offset[1][2] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_CELL_P3_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g1_voltage_table[3] = item_data->list_table;
        cell_voltage_units[1][3] = item_data->unit;
        cell_voltage_offset[1][3] = item_data->offset;
    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_CELL_P0_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g2_voltage_table[0] = item_data->list_table;
        cell_voltage_units[2][0] = item_data->unit;
        cell_voltage_offset[2][0] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_CELL_P1_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g2_voltage_table[1] = item_data->list_table;
        cell_voltage_units[2][1] = item_data->unit;
        cell_voltage_offset[2][1] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_CELL_P2_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g2_voltage_table[2] = item_data->list_table;
        cell_voltage_units[2][2] = item_data->unit;
        cell_voltage_offset[2][2] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_CELL_P3_VOLTAGE);
    if(item_data!=NULL)
    {
        cell_g2_voltage_table[3] = item_data->list_table;
        cell_voltage_units[2][3] = item_data->unit;
        cell_voltage_offset[2][3] = item_data->offset;
    }

    g_hash_table_iter_init(&iter, index_table);
    while(g_hash_table_iter_next(&iter, (gpointer *)&key, NULL))
    {
        if(key==NULL)
        {
            continue;
        }
        sscanf(key, "%u", &subsys_id);
        u8_value = subsys_id + 1;
        g_byte_array_append(packet, &u8_value, 1);
        
        g_byte_array_append(packet, (const guint8 *)&battery_voltage, 2);
        g_byte_array_append(packet, (const guint8 *)&battery_current, 2);
        
        cell_number = 0;
        if(cell_number_table!=NULL)
        {
            raw_value = g_hash_table_lookup(cell_number_table, key);
            if(raw_value!=NULL)
            {
                cell_number = *raw_value;
            }
        }
        
        u16_value = g_htons(cell_number);
        g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
        
        if(cell_number > start_frame)
        {
            frame_number = cell_number - start_frame;
            if(frame_number > 200)
            {
                have_more_data = TRUE;
                frame_number = 200;
            }
        }
        else
        {
            cell_number = 0;
        }
        
        if(cell_number>0)
        {
            u16_value = start_frame + 1;
            u16_value = g_htons(u16_value);
        }
        else
        {
            u16_value = 0;
        }
        g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
        
        u8_value = frame_number;
        g_byte_array_append(packet, &u8_value, 1);
        
        memset(values, 0, sizeof(guint16) * 200);
        
        for(j=0;j<3;j++)
        {
            if(cell_index_table[j]==NULL)
            {
                continue;
            }
            g_hash_table_iter_init(&iter2, cell_index_table[j]);
            while(g_hash_table_iter_next(&iter2, (gpointer *)&key2, NULL))
            {
                index_str = g_strrstr(key2, ":");
                if(index_str!=NULL)
                {
                    if(sscanf(index_str, ":%u", &index)<1)
                    {
                        continue;
                    }
                }
                else
                {
                    if(sscanf(key2, "%u", &index)<1)
                    {
                        continue;
                    }
                }
                
                if(index >= 200 + start_frame)
                {
                    continue;
                }
                
                for(i=0;i<4;i++)
                {
                    raw_value = NULL;
                    if(cell_g0_voltage_table[i]!=NULL)
                    {
                        raw_value = g_hash_table_lookup(
                            cell_g0_voltage_table[i], key2);
                    }
                    if(raw_value!=NULL)
                    {
                        if(index + i < 200)
                        {
                            temp = (gdouble)(*raw_value) *
                                cell_voltage_units[j][i] +
                                cell_voltage_offset[j][i];
                            u16_value = temp;
                            if(u16_value > 60000)
                            {
                                u16_value = 0xFFFE;
                            }
                            values[index + i] = g_htons(u16_value);
                        }
                    }
                    
                    raw_value = NULL;
                    if(cell_g1_voltage_table[i]!=NULL)
                    {
                        raw_value = g_hash_table_lookup(
                            cell_g1_voltage_table[i], key2);
                    }
                    if(raw_value!=NULL)
                    {
                        if(index + i < 200)
                        {
                            temp = (gdouble)(*raw_value) *
                                cell_voltage_units[j][i] +
                                cell_voltage_offset[j][i];
                            u16_value = temp;
                            if(u16_value > 60000)
                            {
                                u16_value = 0xFFFE;
                            }
                            values[index + i] = g_htons(u16_value);
                        }
                    }
                    
                    raw_value = NULL;
                    if(cell_g2_voltage_table[i]!=NULL)
                    {
                        raw_value = g_hash_table_lookup(
                            cell_g2_voltage_table[i], key2);
                    }
                    if(raw_value!=NULL)
                    {
                        if(index + i < 200)
                        {
                            temp = (gdouble)(*raw_value) *
                                cell_voltage_units[j][i] +
                                cell_voltage_offset[j][i];
                            u16_value = temp;
                            if(u16_value > 60000)
                            {
                                u16_value = 0xFFFE;
                            }
                            values[index + i] = g_htons(u16_value);
                        }
                    }
                }
            }
        }
        
        for(i=0;i<frame_number;i++)
        {
            g_byte_array_append(packet, (const guint8 *)(values + i), 2);
        }
    }
    
    return have_more_data;
}

static void tl_net_vehicle_packet_build_rechargable_device_temp_data(
    GByteArray *packet, GHashTable *log_table)
{
    guint8 u8_value;
    guint16 u16_value;
    const TLLoggerLogItemData *item_data;
    GHashTable *index_table;
    GHashTableIter iter, iter2;
    guint table_size;
    guint16 sensor_number;
    const gchar *key, *key2, *index_str;
    gdouble temp;
    gint64 *raw_value;
    GHashTable *sensor_number_table = NULL;
    GHashTable *ts_index_table[3] = {NULL, NULL, NULL};
    GHashTable *ts_g0_temp_table[4] = {NULL, NULL, NULL, NULL};
    GHashTable *ts_g1_temp_table[4] = {NULL, NULL, NULL, NULL};
    GHashTable *ts_g2_temp_table[4] = {NULL, NULL, NULL, NULL};
    gdouble ts_temp_units[3][4];
    gint ts_temp_offset[3][4];
    guint subsys_id;

    guint8 *values;
    guint index, i, j;

    u8_value = TL_NET_VEHICLE_DATA_TYPE_RECHARGABLE_DEVICE_TEMPERATURE;
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_TEMPERATURE_SUBSYSTEM_INDEX);
    if(item_data==NULL || !item_data->list_index ||
        item_data->index_table==NULL)  
    {
        u8_value = 0xFF;
        g_byte_array_append(packet, &u8_value, 1);
        return;
    }
    index_table = item_data->index_table;
    
    table_size = g_hash_table_size(index_table);
    
    if(table_size>250)
    {
        table_size = 250;
    }
    if(table_size==0)
    {
        u8_value = 0xFF;
        g_byte_array_append(packet, &u8_value, 1);
        return;
    }
    u8_value = table_size;
    g_byte_array_append(packet, &u8_value, 1);
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_TEMPERATURE_SENSOR_NUMBER);
    if(item_data!=NULL)
    {
        sensor_number_table = item_data->list_table;
    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_TS_START_ID);
    if(item_data!=NULL)
    {
        ts_index_table[0] = item_data->index_table;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_TS_START_ID);
    if(item_data!=NULL)
    {
        ts_index_table[1] = item_data->index_table;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_TS_START_ID);
    if(item_data!=NULL)
    {
        ts_index_table[2] = item_data->index_table;
    }
    
    for(j=0;j<3;j++)
    {
        for(i=0;i<4;i++)
        {
            ts_temp_units[j][i] = 1.0;
            ts_temp_offset[j][i] = 0;
        }
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_TS_P0_VALUE);
    if(item_data!=NULL)
    {
        ts_g0_temp_table[0] = item_data->list_table;
        ts_temp_units[0][0] = item_data->unit;
        ts_temp_offset[0][0] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_TS_P1_VALUE);
    if(item_data!=NULL)
    {
        ts_g0_temp_table[1] = item_data->list_table;
        ts_temp_units[0][1] = item_data->unit;
        ts_temp_offset[0][1] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_TS_P2_VALUE);
    if(item_data!=NULL)
    {
        ts_g0_temp_table[2] = item_data->list_table;
        ts_temp_units[0][2] = item_data->unit;
        ts_temp_offset[0][2] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G0_TS_P3_VALUE);
    if(item_data!=NULL)
    {
        ts_g0_temp_table[3] = item_data->list_table;
        ts_temp_units[0][3] = item_data->unit;
        ts_temp_offset[0][3] = item_data->offset;
    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_TS_P0_VALUE);
    if(item_data!=NULL)
    {
        ts_g1_temp_table[0] = item_data->list_table;
        ts_temp_units[1][0] = item_data->unit;
        ts_temp_offset[1][0] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_TS_P1_VALUE);
    if(item_data!=NULL)
    {
        ts_g1_temp_table[1] = item_data->list_table;
        ts_temp_units[1][1] = item_data->unit;
        ts_temp_offset[1][1] = item_data->offset;

    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_TS_P2_VALUE);
    if(item_data!=NULL)
    {
        ts_g1_temp_table[2] = item_data->list_table;
        ts_temp_units[1][2] = item_data->unit;
        ts_temp_offset[1][2] = item_data->offset;

    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G1_TS_P3_VALUE);
    if(item_data!=NULL)
    {
        ts_g1_temp_table[3] = item_data->list_table;
        ts_temp_units[1][3] = item_data->unit;
        ts_temp_offset[1][3] = item_data->offset;

    }
    
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_TS_P0_VALUE);
    if(item_data!=NULL)
    {
        ts_g2_temp_table[0] = item_data->list_table;
        ts_temp_units[2][0] = item_data->unit;
        ts_temp_offset[2][0] = item_data->offset;

    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_TS_P1_VALUE);
    if(item_data!=NULL)
    {
        ts_g2_temp_table[1] = item_data->list_table;
        ts_temp_units[2][1] = item_data->unit;
        ts_temp_offset[2][1] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_TS_P2_VALUE);
    if(item_data!=NULL)
    {
        ts_g2_temp_table[2] = item_data->list_table;
        ts_temp_units[2][2] = item_data->unit;
        ts_temp_offset[2][2] = item_data->offset;
    }
    item_data = g_hash_table_lookup(log_table,
        TL_PARSER_BATTERY_G2_TS_P3_VALUE);
    if(item_data!=NULL)
    {
        ts_g2_temp_table[3] = item_data->list_table;
        ts_temp_units[2][3] = item_data->unit;
        ts_temp_offset[2][3] = item_data->offset;
    }

    g_hash_table_iter_init(&iter, index_table);
    while(g_hash_table_iter_next(&iter, (gpointer *)&key, NULL))
    {
        if(key==NULL)
        {
            continue;
        }
        sscanf(key, "%u", &subsys_id);
        u8_value = subsys_id + 1;
        g_byte_array_append(packet, &u8_value, 1);
        
        sensor_number = 0;
        if(sensor_number_table!=NULL)
        {
            raw_value = g_hash_table_lookup(sensor_number_table, key);
            if(raw_value!=NULL)
            {
                sensor_number = *raw_value;
            }
        }
        
        u16_value = g_htons(sensor_number);
        g_byte_array_append(packet, (const guint8 *)&u16_value, 2);

        values = g_new0(guint8, sensor_number);
        
        for(j=0;j<3;j++)
        {
            if(ts_index_table[j]==NULL)
            {
                continue;
            }
            g_hash_table_iter_init(&iter2, ts_index_table[j]);
            while(g_hash_table_iter_next(&iter2, (gpointer *)&key2, NULL))
            {
                index_str = g_strrstr(key2, ":");
                if(index_str!=NULL)
                {
                    if(sscanf(index_str, ":%u", &index)<1)
                    {
                        continue;
                    }
                }
                else
                {
                    if(sscanf(key2, "%u", &index)<1)
                    {
                        continue;
                    }
                }

                if(index>=sensor_number)
                {
                    continue;
                }
                
                for(i=0;i<4;i++)
                {
                    raw_value = NULL;
                    if(ts_g0_temp_table[i]!=NULL)
                    {
                        raw_value = g_hash_table_lookup(ts_g0_temp_table[i],
                            key2);
                    }
                    if(raw_value!=NULL)
                    {
                        temp = (gdouble)(*raw_value) * ts_temp_units[j][i] +
                            ts_temp_offset[j][i];
                        temp += 40;
                        if(index+i < sensor_number)
                        {
                            values[index + i] = temp;
                        }
                    }
                    
                    raw_value = NULL;
                    if(ts_g1_temp_table[i]!=NULL)
                    {
                        raw_value = g_hash_table_lookup(ts_g1_temp_table[i],
                            key2);
                    }
                    if(raw_value!=NULL)
                    {
                        temp = (gdouble)(*raw_value) * ts_temp_units[j][i] +
                            ts_temp_offset[j][i];
                        temp += 40;
                        if(index+i < sensor_number)
                        {
                            values[index + i] = temp;
                        }
                    }
                    
                    raw_value = NULL;
                    if(ts_g2_temp_table[i]!=NULL)
                    {
                        raw_value = g_hash_table_lookup(ts_g2_temp_table[i],
                            key2);
                    }
                    if(raw_value!=NULL)
                    {
                        temp = (gdouble)(*raw_value) * ts_temp_units[j][i] +
                            ts_temp_offset[j][i];
                        temp += 40;
                        if(index+i < sensor_number)
                        {
                            values[index + i] = temp;
                        }
                    }
                }
            }
        }
        
        for(i=0;i<sensor_number;i++)
        {
            g_byte_array_append(packet, values + i, 1);
        }

        g_free(values);
    }
}

static void tl_net_vehicle_packet_build_vehicle_position_data(
    GByteArray *packet, GHashTable *log_table)
{
    guint8 u8_value;
    guint32 u32_value;
    guint8 state = 0x1;
    guint32 latitude = 0, longitude = 0;
    
    u8_value = TL_NET_VEHICLE_DATA_TYPE_VEHICLE_POSITION;
    g_byte_array_append(packet, &u8_value, 1);
    
    tl_gps_state_get(&state, &latitude, &longitude);
    
    g_byte_array_append(packet, &state, 1);
    
    u32_value = g_htonl(longitude);
    g_byte_array_append(packet, (const guint8 *)&u32_value, 4);
    
    u32_value = g_htonl(latitude);
    g_byte_array_append(packet, (const guint8 *)&u32_value, 4);
}

static void tl_net_command_vehicle_data_query(TLNetData *net_data,
    const guint8 *payload, guint payload_len)
{
    guint8 args_num;
    guint8 arg_id;
    guint8 i;
    guint8 date[6];
    
    guint8 remote_domain_len = 0;
    guint8 public_domain_len = 0;
    
    guint8 answer_count = 0;
    
    GByteArray *packet, *ba;
    guint16 u16_value;
    guint8 u8_value;
    gchar *server_host = NULL;
    guint16 server_port = 0;
    const gchar *tmp1, *tmp2;
    size_t len;
    
    if(payload_len < 7)
    {
        return;
    }

    memcpy(date, payload, 6);
    args_num = payload[6];
    
    if(args_num > 252 || payload_len < (guint)args_num + 7)
    {
        return;
    }
    
    if(net_data->vehicle_server_list!=NULL)
    {
        tmp1 = (const gchar *)net_data->vehicle_server_list->data;
        if(tmp1!=NULL)
        {
            tmp2 = g_strrstr_len(tmp1, -1, ":");
            if(tmp2!=NULL)
            {
                if(tmp2 > tmp1)
                {
                    sscanf(tmp2, ":%hu", &server_port);
                    server_host = g_strndup(tmp1, tmp2 - tmp1);
                }
            }
            else
            {
                server_host = g_strdup(tmp1);
                server_port = 8700;
            }
            
            if(server_host!=NULL)
            {
                len = strlen(server_host);
                if(len > 255)
                {
                    remote_domain_len = 255;
                }
                else
                {
                    remote_domain_len = len;
                }
            }
        }
    }
    
    packet = g_byte_array_new();
    g_byte_array_append(packet, date, 6);
    g_byte_array_append(packet, &args_num, 1);
    
    for(i=0;i<args_num;i++)
    {
        arg_id = payload[i+7];
        
        switch(arg_id)
        {
            case 1:
            {
                u16_value = tl_logger_log_update_timeout_get();
                u16_value = g_htons(u16_value);
                
                u8_value = 1;
                g_byte_array_append(packet, &u8_value, 1);
                g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
                
                answer_count++;
                
                break;
            }
            case 2:
            {
                u16_value = net_data->vehicle_data_report_normal_timeout;
                if(u16_value==0 || u16_value > 600)
                {
                    u16_value = 0xFFFE;
                }
                u16_value = g_htons(u16_value);
                
                u8_value = 2;
                g_byte_array_append(packet, &u8_value, 1);
                g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
                
                answer_count++;
                
                break;
            }
            case 3:
            {
                u16_value = net_data->vehicle_data_report_emergency_timeout *
                    1000;
                if(u16_value==0 || u16_value > 60000)
                {
                    u16_value = 0xFFFE;
                }
                u16_value = g_htons(u16_value);
                
                u8_value = 3;
                g_byte_array_append(packet, &u8_value, 1);
                g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
                
                answer_count++;
                
                break;
            }
            case 4:
            {
                u8_value = 4;
                g_byte_array_append(packet, &u8_value, 1);
                g_byte_array_append(packet, &remote_domain_len, 1);
                
                answer_count++;
                
                break;
            }
            case 5:
            {
                u8_value = 5;
                g_byte_array_append(packet, &u8_value, 1);
                
                if(server_host!=NULL)
                {
                    g_byte_array_append(packet, (const guint8 *)server_host,
                        remote_domain_len);
                }
                
                break;
            }
            case 6:
            {
                u16_value = server_port;
                if(u16_value==0 || u16_value > 65531)
                {
                    u16_value = 0xFFFE;
                }
                u16_value = g_htons(u16_value);
                
                u8_value = 6;
                g_byte_array_append(packet, &u8_value, 1);
                g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
                
                answer_count++;
                break;
            }
            case 7:
            {
                u8_value = 7;
                g_byte_array_append(packet, &u8_value, 1);
                g_byte_array_append(packet, (const guint8 *)
                    net_data->hwversion, 5);
                
                answer_count++;
                
                break;
            }
            case 8:
            {
                u8_value = 8;
                g_byte_array_append(packet, &u8_value, 1);
                g_byte_array_append(packet, (const guint8 *)
                    net_data->fwversion, 5);
                
                answer_count++;
                
                break;
            }
            case 9:
            {
                u8_value = 9;
                g_byte_array_append(packet, &u8_value, 1);
                
                u8_value = net_data->vehicle_connection_heartbeat_timeout;
                if(u8_value==0 || u8_value > 240)
                {
                    u8_value = 0xFE;
                }
                g_byte_array_append(packet, &u8_value, 1);
                
                answer_count++;
                
                break;
            }
            case 0xA:
            {
                u16_value = net_data->vehicle_connection_answer_timeout;
                if(u16_value==0 || u16_value > 600)
                {
                    u16_value = 0xFFFE;
                }
                u16_value = g_htons(u16_value);
                
                u8_value = 0xA;
                g_byte_array_append(packet, &u8_value, 1);
                g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
                
                answer_count++;
                
                break;
            }
            case 0xB:
            {
                u16_value = net_data->vehicle_connection_answer_timeout;
                if(u16_value==0 || u16_value > 600)
                {
                    u16_value = 0xFFFE;
                }
                u16_value = g_htons(u16_value);
                
                u8_value = 0xB;
                g_byte_array_append(packet, &u8_value, 1);
                g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
                
                answer_count++;
                
                break;
            }
            case 0xC:
            {                
                u8_value = 0xC;
                g_byte_array_append(packet, &u8_value, 1);
                
                u8_value = 0xFF;
                g_byte_array_append(packet, &u8_value, 1);
                
                answer_count++;
                
                break;
            }
            case 0xD:
            {
                u8_value = 0xD;
                g_byte_array_append(packet, &u8_value, 1);
                
                u8_value = public_domain_len;
                g_byte_array_append(packet, &u8_value, 1);
                
                answer_count++;
                
                break;
            }
            case 0xE:
            {
                u8_value = 0xE;
                g_byte_array_append(packet, &u8_value, 1);
                
                answer_count++;
                
                break;
            }
            case 0xF:
            {
                u8_value = 0xF;
                g_byte_array_append(packet, &u8_value, 1);
                
                u16_value = 0;
                g_byte_array_append(packet, (const guint8 *)&u16_value, 2);
                
                answer_count++;
                
                break;
            }
            case 0x10:
            {
                u8_value = 0x10;
                g_byte_array_append(packet, &u8_value, 1);
                u8_value = 0x2;
                g_byte_array_append(packet, &u8_value, 1);
                
                answer_count++;
                
                break;
            }
            default:
            {
                g_message("TLNet unknown query arugment %u", arg_id);
                break;
            }
        }
    }
    
    if(server_host!=NULL)
    {
        g_free(server_host);
    }
    
    packet->data[6] = answer_count;
    
    ba = tl_net_packet_build(TL_NET_COMMAND_TYPE_QUERY,
        TL_NET_ANSWER_TYPE_SUCCEED, (const guint8 *)net_data->vin,
        strlen(net_data->vin), TL_NET_PACKET_ENCRYPTION_TYPE_NONE, packet);
    g_byte_array_unref(packet);
    tl_net_vehicle_connection_packet_output_request(net_data, ba, FALSE,
        TL_NET_COMMAND_TYPE_QUERY, 0);
    g_byte_array_unref(ba);
}

static gboolean tl_net_command_vehicle_setup_server_host_timeout_cb(
    gpointer user_data)
{
    TLNetData *net_data = (TLNetData *)user_data;
    
    if(user_data==NULL)
    {
        return FALSE;
    }
    
    net_data->current_vehicle_server = net_data->vehicle_server_list;
    net_data->vehicle_connection_retry_count = 0;
    tl_net_vehicle_connection_disconnect(net_data);
    
    net_data->vehicle_connection_change_server_timeout_id = 0;
    
    return FALSE;
}

static void tl_net_command_vehicle_setup(TLNetData *net_data,
    const guint8 *payload, guint payload_len)
{
    guint8 args_num;
    guint8 arg_id;
    guint8 date[6];
    guint i, j;
    gboolean flag = TRUE;
    guint8 remote_domain_len = 0;
    guint8 public_domain_len = 0;
    guint16 u16_value;
    guint8 u8_value;
    
    gchar *remote_domain_address;
    
    gboolean error = FALSE;
    
    gboolean log_update_timeout_set = FALSE;
    guint log_update_timeout = 10000;
    gboolean report_normal_timeout_set = FALSE;
    guint report_normal_timeout = 5;
    gboolean report_emergency_timeout_set = FALSE;
    guint report_emergency_timeout = 1;
    gboolean remote_domain_set = FALSE;
    gchar remote_domain[256] = {0};
    gboolean remote_domain_port_set = FALSE;
    guint16 remote_domain_port = 8700;
    gboolean heartbeat_timeout_set = FALSE;
    guint heartbeat_timeout = 10;
    gboolean answer_timeout_set = FALSE;
    guint answer_timeout = 60;
    
    gboolean need_config_sync = FALSE;
    GList *list_foreach, *list_next;
    
    if(payload_len < 7)
    {
        return;
    }

    memcpy(date, payload, 6);
    args_num = payload[6];
    
    if(args_num > 252 || payload_len < (guint)args_num + 7)
    {
        return;
    }
    
    for(i=7;i<payload_len && flag;)
    {
        arg_id = payload[i];
        i++;
        
        switch(arg_id)
        {
            case 1:
            {
                if(i+2>payload_len)
                {
                    flag = FALSE;
                    break;
                }
                memcpy(&u16_value, payload+i, 2);
                i+=2;
                u16_value = g_ntohs(u16_value);
                
                if(u16_value >= 1000 && u16_value <= 60000)
                {
                    log_update_timeout = u16_value;
                    log_update_timeout_set = TRUE;
                }
                if(u16_value < 1000)
                {
                    log_update_timeout = 1000;
                    log_update_timeout_set = TRUE;
                }
                else if(u16_value > 60000 && u16_value < 0xFFFE)
                {
                    log_update_timeout = 60000;
                    log_update_timeout_set = TRUE;
                }
                else if(u16_value==0xFFFE)
                {
                    error = TRUE;
                    flag = FALSE;
                }
                
                break;
            }
            case 2:
            {
                if(i+2>payload_len)
                {
                    flag = FALSE;
                    break;
                }
                memcpy(&u16_value, payload+i, 2);
                i+=2;
                u16_value = g_ntohs(u16_value);
                
                if(u16_value >=1 && u16_value <= 600)
                {
                    report_normal_timeout = u16_value;
                    report_normal_timeout_set = TRUE;
                }
                else if(u16_value==0)
                {
                    report_normal_timeout = 1;
                    report_normal_timeout_set = TRUE;
                }
                else if(u16_value > 600 && u16_value < 0xFFFE)
                {
                    report_normal_timeout = 600;
                    report_normal_timeout_set = TRUE;
                }
                else if(u16_value==0xFFFE)
                {
                    error = TRUE;
                    flag = FALSE;
                }
                
                break;
            }
            case 3:
            {
                if(i+2>payload_len)
                {
                    flag = FALSE;
                    break;
                }
                memcpy(&u16_value, payload+i, 2);
                i+=2;
                u16_value = g_ntohs(u16_value);
                
                if(u16_value >=1000 && u16_value <= 60000)
                {
                    report_emergency_timeout = u16_value / 1000;
                    report_emergency_timeout_set = TRUE;
                }
                else if(u16_value < 1000)
                {
                    report_emergency_timeout = 1;
                    report_emergency_timeout_set = TRUE;
                }
                else if(u16_value > 60000 && u16_value < 0xFFFE)
                {
                    report_emergency_timeout = 60;
                    report_emergency_timeout_set = TRUE;
                }
                else if(u16_value==0xFFFE)
                {
                    error = TRUE;
                    flag = FALSE;
                }
                
                break;
            }
            case 4:
            {
                if(i+1>payload_len)
                {
                    flag = FALSE;
                    break;
                }
                
                remote_domain_len = payload[i];
                i+=1;
                
                break;
            }
            case 5:
            {
                if(i+remote_domain_len>payload_len)
                {
                    flag = FALSE;
                    break;
                }

                memcpy(remote_domain, payload+i, remote_domain_len);
                i+=remote_domain_len;
                remote_domain_set = TRUE;
                
                break;
            }
            case 6:
            {
                if(i+2>payload_len)
                {
                    flag = FALSE;
                    break;
                }
                memcpy(&u16_value, payload+i, 2);
                i+=2;
                u16_value = g_ntohs(u16_value);
                
                if(u16_value>0 && u16_value<0xFFFE)
                {
                    remote_domain_port = u16_value;
                    remote_domain_port_set = TRUE;
                }
                else if(u16_value==0)
                {
                    remote_domain_port = 8700;
                    remote_domain_port_set = TRUE;
                }
                else if(u16_value==0xFFFE)
                {
                    flag = FALSE;
                    error = TRUE;
                }

                break;
            }
            case 9:
            {
                if(i+1>payload_len)
                {
                    flag = FALSE;
                    break;
                }

                u8_value = payload[i];
                i+=1;
                
                if(u8_value > 0 && u8_value < 254)
                {
                    heartbeat_timeout = u8_value;
                    heartbeat_timeout_set = TRUE;
                }
                else if(u8_value==0)
                {
                    heartbeat_timeout = 1;
                    heartbeat_timeout_set = TRUE;
                }
                else if(u8_value==0xFE)
                {
                    flag = FALSE;
                    error = TRUE;
                }

                break;
            }
            case 0xA:
            {
                if(i+2>payload_len)
                {
                    flag = FALSE;
                    break;
                }

                memcpy(&u16_value, payload+i, 2);
                i+=2;
                u16_value = g_ntohs(u16_value);
                
                if(answer_timeout_set)
                {
                    break;
                }
                
                if(u16_value >= 15 && u16_value <= 600)
                {
                    answer_timeout = u16_value;
                    answer_timeout_set = TRUE;
                }
                else if(u16_value < 15)
                {
                    answer_timeout = 15;
                    answer_timeout_set = TRUE;
                }
                else if(u16_value > 600 && u16_value < 0xFFFE)
                {
                    answer_timeout = 600;
                    answer_timeout_set = TRUE;
                }
                else if(u16_value==0xFFFE)
                {
                    flag = FALSE;
                    error = TRUE;
                }
                
                break;
            }
            case 0xB:
            {
                if(i+2>payload_len)
                {
                    flag = FALSE;
                    break;
                }

                memcpy(&u16_value, payload+i, 2);
                i+=2;
                u16_value = g_ntohs(u16_value);
                                
                if(u16_value >= 15 && u16_value <= 600)
                {
                    answer_timeout = u16_value;
                    answer_timeout_set = TRUE;
                }
                else if(u16_value < 15)
                {
                    answer_timeout = 15;
                    answer_timeout_set = TRUE;
                }
                else if(u16_value > 600 && u16_value < 0xFFFE)
                {
                    answer_timeout = 600;
                    answer_timeout_set = TRUE;
                }
                else if(u16_value==0xFFFE)
                {
                    flag = FALSE;
                    error = TRUE;
                }

                break;
            }
            case 0xC:
            {   
                if(i+1>payload_len)
                {
                    flag = FALSE;
                    break;
                }
                
                i+=1;

                break;
            }
            case 0xD:
            {
                if(i+1>payload_len)
                {
                    flag = FALSE;
                    break;
                }
                
                public_domain_len = payload[i];
                i+=1;

                break;
            }
            case 0xE:
            {
                if(i+public_domain_len>payload_len)
                {
                    flag = FALSE;
                    break;
                }
                
                i+=public_domain_len;

                break;
            }
            case 0xF:
            {
                if(i+2>payload_len)
                {
                    flag = FALSE;
                    break;
                }

                i+=2;
                
                break;
            }
            case 0x10:
            {
                if(i+1>payload_len)
                {
                    flag = FALSE;
                    break;
                }

                i+=1;
                
                break;
            }
            default:
            {
                g_message("TLNet unknown query arugment %u", arg_id);
                flag = FALSE;
                break;
            }
        }
    }
    
    
    if(!error)
    {
        if(log_update_timeout_set)
        {
            tl_logger_log_update_timeout_set(log_update_timeout);
            need_config_sync = TRUE;
        }
        
        if(report_normal_timeout_set)
        {
            net_data->vehicle_data_report_normal_timeout =
                report_normal_timeout;
            need_config_sync = TRUE;
        }
        
        if(report_emergency_timeout_set)
        {
            net_data->vehicle_data_report_emergency_timeout = 
                report_emergency_timeout;
            need_config_sync = TRUE;
        }
        
        if(remote_domain_set)
        {
            if(remote_domain_port_set)
            {
                remote_domain_address = g_strdup_printf("%s:%hu",
                    remote_domain, remote_domain_port);
            }
            else
            {
                remote_domain_address = g_strdup_printf("%s:8700",
                    remote_domain);
            }
            
            for(list_foreach=net_data->vehicle_server_list, j=0;
                list_foreach!=NULL;)
            {
                if(j<3)
                {
                    list_foreach = g_list_next(list_foreach);
                    j++;
                }
                else if(g_list_next(list_foreach)!=NULL)
                {
                    list_next = g_list_next(list_foreach);
                    if(g_list_next(list_next)!=NULL)
                    {
                        g_free(g_list_next(list_next)->data);
                        net_data->vehicle_server_list = g_list_delete_link(
                            net_data->vehicle_server_list, list_next);
                    }
                }
            }
            
            net_data->vehicle_server_list = g_list_prepend(
                net_data->vehicle_server_list, remote_domain_address);
            
            if(net_data->vehicle_connection_change_server_timeout_id > 0)
            {
                g_source_remove(
                    net_data->vehicle_connection_change_server_timeout_id);
            }
            net_data->vehicle_connection_change_server_timeout_id =
                g_timeout_add_seconds(5,
                    tl_net_command_vehicle_setup_server_host_timeout_cb,
                    net_data);
                    
            need_config_sync = TRUE;
        }
        
        if(heartbeat_timeout_set)
        {
            net_data->vehicle_connection_heartbeat_timeout =
                heartbeat_timeout;
            need_config_sync = TRUE;
        }
        
        if(answer_timeout_set)
        {
            net_data->vehicle_connection_answer_timeout =
                answer_timeout;
            need_config_sync = TRUE;
        }
    }
    
    if(need_config_sync)
    {
        tl_net_config_sync();
    }
}

static void tl_net_command_terminal_update(TLNetData *net_data,
    const guint8 *payload, guint payload_len)
{
    gchar *url = NULL;
    gchar *apn = NULL;
    gchar *user = NULL;
    gchar *password = NULL;
    gchar address[7] = {0};
    guint16 port;
    gchar factory_code[5] = {0};
    gchar hwversion[6] = {0};
    gchar fwversion[6] = {0};
    guint i;
    guint used_len = 0;
    guint16 timeout = 0;
    gchar *url_scheme = NULL;
    GError *error = NULL;
    GDateTime *dt;
    gint64 timestamp;
    FILE *fp;
    
    /* URL;APN;USER;PASS;ADDR;PORT;FCODE;HWVER;FWVER;TIMEOUT */
    
    for(i=0;i<payload_len;i++)
    {
        if(payload[i]==';')
        {
            url = g_strndup((const gchar *)payload, i);
            i++;
            used_len = i;
            break;
        }
    }
    
    for(i=used_len;i<payload_len;i++)
    {
        if(payload[i]==';')
        {
            apn = g_strndup((const gchar *)payload+used_len, i-used_len);
            i++;
            used_len = i;
            break;
        }
    }
    
    for(i=used_len;i<payload_len;i++)
    {
        if(payload[i]==';')
        {
            user = g_strndup((const gchar *)payload+used_len, i-used_len);
            i++;
            used_len = i;
            break;
        }
    }
    
    for(i=used_len;i<payload_len;i++)
    {
        if(payload[i]==';')
        {
            password = g_strndup((const gchar *)payload+used_len, i-used_len);
            i++;
            used_len = i;
            break;
        }
    }
    
    if(used_len+7 <= payload_len)
    {
        memcpy(address, payload+used_len, 6);
    }
    used_len += 7;
    
    if(used_len+3 <= payload_len)
    {
        memcpy(&port, payload+used_len, 2);
        port = g_ntohs(port);
    }
    used_len += 3;
    
    if(used_len+5 <= payload_len)
    {
        memcpy(factory_code, payload+used_len, 4);
    }
    used_len += 5;
    
    if(used_len+6 <= payload_len)
    {
        memcpy(hwversion, payload+used_len, 5);
    }
    used_len += 6;
    
    if(used_len+6 <= payload_len)
    {
        memcpy(fwversion, payload+used_len, 5);
    }
    used_len += 6;
    
    if(used_len+2 <= payload_len)
    {
        memcpy(&timeout, payload+used_len, 2);
        timeout = g_ntohs(timeout);
    }
    
    g_message("Got update request, URL %s, address %02X:%02X:%02X:%02X:"
        "%02X:%02X, port %u, APN %s, user %s, password %s.", url, address[0],
        address[1], address[2], address[3], address[4], address[5], port,
        apn, user, password);
    
    if(url!=NULL)
    {
        url_scheme = g_uri_parse_scheme(url);
        if(url_scheme==NULL || g_ascii_strcasecmp(url_scheme, "http")!=0 ||
            g_ascii_strcasecmp(url_scheme, "https")!=0 ||
            g_ascii_strcasecmp(url_scheme, "ftp")!=0)
        {
            g_free(url);
            url = NULL;
        }
        g_free(url_scheme);
    }
    
    dt = g_date_time_new_now_local();
    timestamp = g_date_time_to_unix(dt);
    g_date_time_unref(dt);
    
    fp = fopen("/var/lib/tbox/conf/tl-update.conf", "w");
    if(fp!=NULL)
    {
        fprintf(fp, "TIMESTAMP=%"G_GINT64_FORMAT"\n", timestamp);
        
        if(url!=NULL && strlen(url)>0)
        {
            fprintf(fp, "URL=%s\n", url);
        }
        
        if(apn!=NULL && strlen(apn)>0)
        {
            fprintf(fp, "APN=%s\n", apn);
        }
        
        if(user!=NULL && strlen(user)>0)
        {
            fprintf(fp, "USER=%s\n", user);
        }
        
        if(password!=NULL && strlen(password)>0)
        {
            fprintf(fp, "PASSWORD=%s\n", password);
        }
        
        if(address[0]==0 && address[1]==0)
        {
            fprintf(fp, "HOST=%u.%u.%u.%u\n", address[2], address[3],
                address[4], address[5]);
        }
        else
        {
            fprintf(fp, "HOST=%6s\n", address);
        }
        
        fprintf(fp, "PORT=%u\n", port);
        fprintf(fp, "FACTORYCODE=%s\n", factory_code);
        fprintf(fp, "HWVERSION=%s\n", hwversion);
        fprintf(fp, "FWVERSION=%s\n", fwversion);
        fprintf(fp, "TIMEOUT=%u\n", timeout);
        
        fclose(fp);
    }
   
    if(!g_spawn_command_line_async("/usr/bin/tl-update", &error))
    {
        g_warning("TLNet failed to update!");
        g_clear_error(&error);
    }
    
    g_free(url);
    g_free(apn);
    g_free(user);
    g_free(password);
}

static void tl_net_command_terminal_control(TLNetData *net_data,
    const guint8 *payload, guint payload_len)
{
    guint8 command;
    GError *error = NULL;
    FILE *fp;
    
    if(payload_len < 7)
    {
        return;
    }
    
    command = payload[6];
    
    switch(command)
    {
        case 1:
        {
            tl_net_command_terminal_update(net_data, payload, payload_len);
            break;
        }
        case 2:
        {
            if(!g_spawn_command_line_async("/sbin/poweroff", &error))
            {
                g_warning("TLNet failed to power off!");
                g_clear_error(&error);
            }
            break;
        }
        case 3:
        {
            if(!g_spawn_command_line_async("/sbin/reboot", &error))
            {
                g_warning("TLNet failed to reset!");
                g_clear_error(&error);
            }
            break;
        }
        case 4:
        {
            tl_net_reset_arguments();
            break;
        }
        case 5:
        {
            if(!g_spawn_command_line_async("/usr/bin/poff gprs",
                &error))
            {
                g_warning("TLNet failed to disconnect network!");
                g_clear_error(&error);
            }
            else
            {
                fp = fopen("/tmp/gprs-off", "w");
                if(fp!=NULL)
                {
                    fprintf(fp, "DISCONNECT");
                    fclose(fp);
                }
            }
            break;
        }
        default:
        {
            g_message("TLNet unknown command in terminal control %u.",
                command);
            break;
        }
    }
}
