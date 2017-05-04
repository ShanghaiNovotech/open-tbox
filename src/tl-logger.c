#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <json.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "tl-logger.h"

#define TL_LOGGER_STORAGE_BASE_PATH_DEFAULT "/var/lib/tbox/log"

#define TL_LOGGER_LOG_ITEM_HEAD_MAGIC (const guint8 *)"TLIH"
#define TL_LOGGER_LOG_ITEM_TAIL_MAGIC (const guint8 *)"TLIT"
#define TL_LOGGER_LOG_SIZE_MAXIUM 8 * 1024 * 1024

typedef struct _TLLoggerData
{
    gboolean initialized;
    gchar *storage_base_path;
    gint64 last_timestamp;
    gint64 new_timestamp;
    
    GMutex cached_log_mutex;
    GQueue *cached_log_data;
    GQueue *write_log_queue;
    GList *last_saved_data;
    GHashTable *last_log_data;
    guint log_update_timeout_id;
    
    GThread *write_thread;
    gboolean write_thread_work_flag;
    
    GThread *archive_thread;
    GThread *query_thread;
}TLLoggerData;

static TLLoggerData g_tl_logger_data = {0};

static TLLoggerLogItemData *tl_logger_log_item_data_dup(
    TLLoggerLogItemData *data)
{
    TLLoggerLogItemData *new_data;
    
    if(data==NULL)
    {
        return NULL;
    }
    
    new_data = g_new0(TLLoggerLogItemData, 1);
    new_data->name = g_strdup(data->name);
    new_data->value = data->value;
    new_data->unit = data->unit;
    new_data->source = data->source;
    
    return new_data;
}

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

static inline guint16 tl_logger_crc16_compute(const guchar *data_p,
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


/*
 * Log Frame:
 * | Head Magic (4B) | Length (4B) | CRC16 (2B) | JSON Data | Tail Magic (4B) |
 * 
 */

static GByteArray *tl_logger_log_to_file_data(GHashTable *log_data)
{
    GByteArray *ba;
    GHashTableIter iter;
    TLLoggerLogItemData *item_data;
    json_object *root, *child, *item_object;
    const gchar *json_data;
    guint32 json_len, belen;
    guint16 crc, becrc;
    
    ba = g_byte_array_new();
    
    g_byte_array_append(ba, TL_LOGGER_LOG_ITEM_HEAD_MAGIC, 4);
    g_byte_array_append(ba, (const guint8 *)"\0\0\0\0", 4);
    g_byte_array_append(ba, (const guint8 *)"\0\0", 2);
    
    root = json_object_new_array();
    
    g_hash_table_iter_init(&iter, log_data);
    while(g_hash_table_iter_next(&iter, NULL, (gpointer *)&item_data))
    {
        item_object = json_object_new_object();
        
        child = json_object_new_string(item_data->name);
        json_object_object_add(item_object, "name", child);
        
        child = json_object_new_int64(item_data->value);
        json_object_object_add(item_object, "value", child);
        
        child = json_object_new_double(item_data->unit);
        json_object_object_add(item_object, "unit", child);
        
        child = json_object_new_int(item_data->source);
        json_object_object_add(item_object, "source", child);
        
        json_object_array_add(root, item_object);
    }
    
    json_data = json_object_to_json_string(root);
    json_len = strlen(json_data);
    g_byte_array_append(ba, (const guint8 *)json_data, json_len);
    
    crc = tl_logger_crc16_compute((const guchar *)json_data, json_len);
    
    json_object_put(root);
    
    belen = g_htonl(json_len);
    memcpy(ba->data+4, &belen, 4);
    
    becrc = g_htons(crc);
    memcpy(ba->data+8, &becrc, 2);
    
    g_byte_array_append(ba, TL_LOGGER_LOG_ITEM_TAIL_MAGIC, 4);
    
    return ba;
}

static gpointer tl_logger_log_query_thread(gpointer user_data)
{
    return NULL;
}

static gpointer tl_logger_log_archive_thread(gpointer user_data)
{
    GDir *log_dir;
    
    
    
    return NULL;
}

static gpointer tl_logger_log_write_thread(gpointer user_data)
{
    TLLoggerData *logger_data = (TLLoggerData *)user_data;
    GHashTable *item_data;
    GByteArray *ba;
    int fd = -1;
    ssize_t written_size = 0, rsize;
    gchar *lastlog_filename = NULL, *datestr, *basename;
    gint64 last_write_time = G_MININT64, write_time;
    TLLoggerLogItemData *time_item;
    GDateTime *dt;
    
    if(user_data==NULL)
    {
        return NULL;
    }
    
    logger_data->write_thread_work_flag = TRUE;
    
    while(logger_data->write_thread_work_flag)
    {
        g_mutex_lock(&(logger_data->cached_log_mutex));
        
        logger_data->last_saved_data = g_queue_pop_head_link(
            logger_data->write_log_queue);
        
        if(logger_data->last_saved_data==NULL)
        {
            g_usleep(100000);
            g_mutex_unlock(&(logger_data->cached_log_mutex));
            continue;
        }
        
        item_data = logger_data->last_saved_data->data;
        if(item_data==NULL)
        {
            g_list_free_full(logger_data->last_saved_data,
                (GDestroyNotify)tl_logger_log_item_data_free);
            logger_data->last_saved_data = NULL;
            
            g_usleep(100000);
            g_mutex_unlock(&(logger_data->cached_log_mutex));
            continue;
        }
        
        time_item = g_hash_table_lookup(item_data, "time");
        if(time_item==NULL)
        {
            g_list_free_full(logger_data->last_saved_data,
                (GDestroyNotify)tl_logger_log_item_data_free);
            logger_data->last_saved_data = NULL;
            
            g_usleep(100000);
            g_mutex_unlock(&(logger_data->cached_log_mutex));
            continue;
        }
        write_time = time_item->value;
        
        if(fd>=0 && write_time < last_write_time)
        {
            /* Log time is not monotonic! */
            close(fd);
            fd = -1;
            
            g_queue_free_full(logger_data->cached_log_data,
                (GDestroyNotify)g_hash_table_unref);
            logger_data->cached_log_data = g_queue_new();
            logger_data->last_saved_data = NULL;
        }
        last_write_time = write_time;
        
        ba = tl_logger_log_to_file_data(item_data);
        
        g_queue_push_tail_link(logger_data->cached_log_data,
            logger_data->last_saved_data);
        
        g_mutex_unlock(&(logger_data->cached_log_mutex));
        
        if(fd<0)
        {
            if(lastlog_filename!=NULL)
            {
                g_free(lastlog_filename);
            }
            dt = g_date_time_new_from_unix_local(write_time);
            datestr = g_date_time_format(dt, "%Y%m%d%H%M%S");
            basename = g_strdup_printf("tbl-%s.tl", datestr);
            g_free(datestr);
            g_date_time_unref(dt);
            
            lastlog_filename = g_build_filename(
                TL_LOGGER_STORAGE_BASE_PATH_DEFAULT, basename, NULL);
            g_free(basename);
            
            fd = open(lastlog_filename, O_WRONLY | O_CREAT);
            written_size = 0;
        }
        
        rsize = write(fd, ba->data, ba->len);
        g_byte_array_unref(ba);
        
        if(rsize>0)
        {
            written_size += rsize;
        }
        
        if(rsize<=0 || written_size>=TL_LOGGER_LOG_SIZE_MAXIUM)
        {
            close(fd);
            fd = -1;
            
            g_mutex_lock(&(logger_data->cached_log_mutex));
            g_queue_free_full(logger_data->cached_log_data,
                (GDestroyNotify)g_hash_table_unref);
            logger_data->cached_log_data = g_queue_new();
            logger_data->last_saved_data = NULL;
            g_mutex_unlock(&(logger_data->cached_log_mutex));
        }
        
    }
    
    if(lastlog_filename!=NULL)
    {
        g_free(lastlog_filename);
    }
    
    return NULL;
}

static gboolean tl_logger_log_update_timer_cb(gpointer user_data)
{
    TLLoggerData *logger_data = (TLLoggerData *)user_data;
    TLLoggerLogItemData *item_data, *dup_data;
    GHashTableIter iter;
    GDateTime *dt;
    GHashTable *dup_table;
    
    if(logger_data->new_timestamp > logger_data->last_timestamp +
        (gint64)10000000)
    {
        dt = g_date_time_new_now_local();
        dup_table = g_hash_table_new_full(g_str_hash,
            g_str_equal, NULL, (GDestroyNotify)tl_logger_log_item_data_free);
        item_data = g_new0(TLLoggerLogItemData, 1);
        item_data->name = g_strdup("time");
        item_data->value = g_date_time_to_unix(dt);
        g_date_time_unref(dt);
        item_data->unit = 1.0;
        item_data->source = 0;
        g_hash_table_replace(dup_table, item_data->name, item_data);
        
        g_hash_table_iter_init(&iter, logger_data->last_log_data);
        while(g_hash_table_iter_next(&iter, NULL, (gpointer *)&item_data))
        {
            if(item_data==NULL)
            {
                continue;
            }
            dup_data = tl_logger_log_item_data_dup(item_data);
            g_hash_table_replace(dup_table, dup_data->name, dup_data);
        }
        
        g_mutex_lock(&(logger_data->cached_log_mutex));
        g_queue_push_tail(logger_data->write_log_queue, dup_data);
        g_mutex_unlock(&(logger_data->cached_log_mutex));
                
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
    
    g_mutex_init(&(g_tl_logger_data.cached_log_mutex));
    g_tl_logger_data.cached_log_data = g_queue_new();
    g_tl_logger_data.write_log_queue = g_queue_new();
    
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
    
    g_tl_logger_data.write_thread = g_thread_new("tl-logger-write-thread",
        tl_logger_log_write_thread, &g_tl_logger_data);
    
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
    
    if(g_tl_logger_data.write_thread!=NULL)
    {
        g_tl_logger_data.write_thread_work_flag = FALSE;
        g_thread_join(g_tl_logger_data.write_thread);
        g_tl_logger_data.write_thread = NULL;
    }
    
    if(g_tl_logger_data.last_log_data!=NULL)
    {
        g_hash_table_unref(g_tl_logger_data.last_log_data);
        g_tl_logger_data.last_log_data = NULL;
    }
    
    if(g_tl_logger_data.write_log_queue!=NULL)
    {
        g_queue_free_full(g_tl_logger_data.write_log_queue,
            (GDestroyNotify)g_hash_table_unref);
        g_tl_logger_data.write_log_queue = NULL;
    }
    if(g_tl_logger_data.cached_log_data!=NULL)
    {
        g_queue_free_full(g_tl_logger_data.cached_log_data,
            (GDestroyNotify)g_hash_table_unref);
        g_tl_logger_data.cached_log_data = NULL;
    }
    
    if(g_tl_logger_data.storage_base_path!=NULL)
    {
        g_free(g_tl_logger_data.storage_base_path);
        g_tl_logger_data.storage_base_path = NULL;
    }
    
    g_mutex_clear(&(g_tl_logger_data.cached_log_mutex));
    
    g_tl_logger_data.initialized = FALSE;
}

void tl_logger_current_data_update(const TLLoggerLogItemData *item_data)
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

GHashTable *tl_logger_current_data_get()
{
    return g_tl_logger_data.last_log_data;
}
