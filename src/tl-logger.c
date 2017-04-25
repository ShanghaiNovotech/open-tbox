#include "tl-logger.h"

#define TL_LOGGER_STORAGE_BASE_PATH_DEFAULT "/var/lib/tbox/log"

#define TL_LOGGER_LOG_ITEM_HEAD_MAGIC "TLIH"
#define TL_LOGGER_LOG_ITEM_TAIL_MAGIC "TLIT"



typedef struct _TLLoggerData
{
    gboolean initialized;
    gchar *storage_base_path;
    gint64 last_timestamp;
    gint64 new_timestamp;
    GHashTable *last_log_data;
    guint log_update_timeout_id;
}TLLoggerData;

static TLLoggerData g_tl_logger_data = {0};

static void tl_logger_log_item_data_free(TLLoggerLogItemData *data)
{
    if(data==NULL)
    {
        return;
    }
    if(data->name!=NULL)
    {
        g_free(data->name);
    }
    g_free(data);
}

static gboolean tl_logger_log_update_timer_cb(gpointer user_data)
{
    TLLoggerData *logger_data = (TLLoggerData *)user_data;
    
    if(logger_data->new_timestamp > logger_data->last_timestamp +
        (gint64)10000000)
    {
        /* Update log data to file. */
        
        
        logger_data->last_timestamp = logger_data->new_timestamp;
    }
    
    return TRUE;
}

gboolean tl_logger_init(const gchar *storage_base_path)
{
    if(g_tl_logger_data.initialized)
    {
        g_warning("TLLogger already initialized!");
        return TRUE;
    }
    
    if(storage_base_path!=NULL)
    {
        g_tl_logger_data.storage_base_path = g_strdup(storage_base_path);
    }
    else
    {
        g_tl_logger_data.storage_base_path = g_strdup(
            TL_LOGGER_STORAGE_BASE_PATH_DEFAULT);
    }
    
    g_tl_logger_data.log_update_timeout_id = g_timeout_add_seconds(10,
        tl_logger_log_update_timer_cb, &g_tl_logger_data);
    
    g_tl_logger_data.initialized = TRUE;
    
    return TRUE;
}

void tl_logger_uninit()
{
    if(!g_tl_logger_data.initialized)
    {
        return;
    }
    
    if(g_tl_logger_data.log_update_timeout_id>0)
    {
        g_source_remove(g_tl_logger_data.log_update_timeout_id);
        g_tl_logger_data.log_update_timeout_id = 0;
    }
    
    if(g_tl_logger_data.last_log_data!=NULL)
    {
        g_hash_table_unref(g_tl_logger_data.last_log_data);
        g_tl_logger_data.last_log_data = NULL;
    }
    
    if(g_tl_logger_data.storage_base_path!=NULL)
    {
        g_free(g_tl_logger_data.storage_base_path);
        g_tl_logger_data.storage_base_path = NULL;
    }
    
    g_tl_logger_data.initialized = FALSE;
}

void tl_logger_update_current_data(const TLLoggerLogItemData *item_data)
{
    TLLoggerLogItemData *idata;
    
    if(item_data==NULL || item_data->name==NULL)
    {
        return;
    }
    if(g_tl_logger_data.last_log_data==NULL)
    {
        g_tl_logger_data.last_log_data = g_hash_table_new_full(g_str_hash,
            g_str_equal, NULL, (GDestroyNotify)tl_logger_log_item_data_free);
    }
    if(g_hash_table_contains(g_tl_logger_data.last_log_data, item_data->name))
    {
        idata = g_hash_table_lookup(g_tl_logger_data.last_log_data,
            item_data->name);
        if(idata!=NULL)
        {
            idata->value = item_data->value;
            idata->unit = item_data->unit;
            idata->source = item_data->source;
        }
    }
    else
    {
        idata = g_new(TLLoggerLogItemData, 1);
        idata->name = g_strdup(item_data->name);
        idata->value = item_data->value;
        idata->unit = item_data->unit;
        idata->source = item_data->source;
        g_hash_table_replace(g_tl_logger_data.last_log_data, idata->name,
            idata);
    }
    
    g_tl_logger_data.new_timestamp = g_get_monotonic_time();
}
