#include <string.h>
#include "tl-net.h"
#include "tl-logger.h"
#include "tl-parser.h"

typedef struct _TLNetData
{
    gboolean initialized;
    gchar *conf_file_path;
    int fd;
    GList *vehicle_server_list;
    gchar *pending_vehicle_server;
    gchar *vin;
    gchar *iccid;
    guint16 session;
    
    gboolean vehicle_login_state;
}TLNetData;

typedef enum
{
    TL_NET_COMMAND_TYPE_VEHICLE_LOGIN = 0x1,
    TL_NET_COMMAND_TYPE_REALTIME_DATA = 0x2,
    TL_NET_COMMAND_TYPE_REPEAT_DATA = 0x3,
    TL_NET_COMMAND_TYPE_VEHICLE_LOGOUT = 0x4,
    TL_NET_COMMAND_TYPE_PLATFORM_LOGIN = 0x5,
    TL_NET_COMMAND_TYPE_PLATFORM_LOGOUT = 0x6,
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



gboolean tl_net_init(const gchar *vin, const gchar *iccid,
    const gchar *conf_path, const gchar *fallback_vehicle_server_host,
    guint16 fallback_vehicle_server_port)
{    
    if(g_tl_net_data.initialized)
    {
        g_warning("TLNet already initialized!");
        return TRUE;
    }
    
    if(conf_path==NULL)
    {
        conf_path = TL_NET_CONF_PATH_DEFAULT;
    }
    g_tl_net_data.conf_file_path = g_build_filename(conf_path,
        "network.conf", NULL);
    tl_net_config_load(&g_tl_net_data, g_tl_net_data.conf_file_path);
    
    
    
    
    
    g_tl_net_data.initialized = TRUE;
    
    return TRUE;
}

void tl_net_uninit()
{
    if(!g_tl_net_data.initialized)
    {
        return;
    }
    
    if(g_tl_net_data.conf_file_path!=NULL)
    {
        g_free(g_tl_net_data.conf_file_path);
        g_tl_net_data.conf_file_path = NULL;
    }
    
    g_tl_net_data.initialized = FALSE;
}
