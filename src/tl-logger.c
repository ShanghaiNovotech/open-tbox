#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <json.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <glib/gstdio.h>
#include <gio/gio.h>
#include <errno.h>
#include <sys/statvfs.h>
#include "tl-logger.h"

#define TL_LOGGER_STORAGE_BASE_PATH_DEFAULT "/var/lib/tbox/log"

#define TL_LOGGER_LOG_ITEM_HEAD_MAGIC ((const guint8 *)"TLIH")
#define TL_LOGGER_LOG_ITEM_TAIL_MAGIC ((const guint8 *)"TLIT")
#define TL_LOGGER_LOG_SIZE_MAXIUM 8 * 1024 * 1024

#define TL_LOGGER_LOG_PARSE_MAXIUM_SIZE 8 * 1024 * 1024

#define TL_LOGGER_LOG_FREE_SPACE_MINIUM 200UL * 1024 * 1024
#define TL_LOGGER_LOG_FREE_NODE_MINIUM 2048

typedef struct _TLLoggerQueryData
{
    gboolean begin_time_set;
    gint64 begin_time;
    gboolean end_time_set;
    gint64 end_time;
    TLLoggerQueryResultCallback query_result_cb;
    gpointer query_result_user_data;
}TLLoggerQueryData;

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
    gboolean archive_thread_work_flag;
    gint archive_thread_wait_countdown;
    
    GThread *query_thread;
    gboolean query_thread_work_flag;
    GQueue *query_queue;
    TLLoggerQueryData *query_working_data;
    GMutex query_queue_mutex;
    gboolean query_work_flag;
}TLLoggerData;

typedef struct _TLLoggerFileStat
{
    gchar *name;
    guint64 size;
}TLLoggerFileStat;

typedef gboolean (*TLLoggerLogQueryCallback)(TLLoggerData *logger_data,
    GByteArray *ba, TLLoggerQueryData *query_data);

static TLLoggerData g_tl_logger_data = {0};

static TLLoggerLogItemData *tl_logger_log_item_data_dup(
    TLLoggerLogItemData *data)
{
    TLLoggerLogItemData *new_data;
    GHashTableIter iter;
    gpointer key;
    gint64 *value;
    
    if(data==NULL)
    {
        return NULL;
    }
    
    new_data = g_new0(TLLoggerLogItemData, 1);
    new_data->name = g_strdup(data->name);
    new_data->value = data->value;
    new_data->unit = data->unit;
    new_data->source = data->source;
    new_data->list_parent = g_strdup(data->list_parent);
    
    if(data->list_table!=NULL && data->list_item>0)
    {
        new_data->list_table = g_hash_table_new_full(g_direct_hash,
            g_direct_equal, NULL, g_free);
        
        g_hash_table_iter_init(&iter, data->list_table);
        while(g_hash_table_iter_next(&iter, &key, (gpointer *)&value))
        {
            g_hash_table_replace(new_data->list_table, key, g_memdup(value,
                sizeof(gint64)));
        }
    }
    
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
    if(data->list_parent!=NULL)
    {
        g_free(data->list_parent);
    }
    if(data->list_table!=NULL)
    {
        g_hash_table_unref(data->list_table);
    }
    g_free(data);
}

static void tl_logger_file_stat_free(TLLoggerFileStat *data)
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

static int tl_logger_file_stat_compare(const TLLoggerFileStat *a,
    const TLLoggerFileStat *b, gpointer user_data)
{
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
    return g_strcmp0(a->name, b->name);
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

static gboolean tl_logger_log_query_from_file(TLLoggerData *logger_data,
    const gchar *filename, TLLoggerQueryData *query_data,
    TLLoggerLogQueryCallback callback)
{
    GError *error = NULL;
    GFile *input_file;
    GZlibDecompressor *decompressor;
    GFileInputStream *file_istream;
    GInputStream *decompress_istream;
    guint8 buffer[4096];
    gssize read_size;
    GByteArray *parse_array;
    guint i;
    gboolean ret = TRUE;
    guint32 except_len = 0;
    guint16 except_crc;
    guint16 real_crc;
    
    if(callback==NULL)
    {
        return FALSE;
    }
    
    input_file = g_file_new_for_path(filename);
    if(input_file==NULL)
    {
        return FALSE;
    }
    
    file_istream = g_file_read(input_file, NULL, &error);
    g_object_unref(input_file);
    
    if(file_istream==NULL)
    {
        g_warning("TLLogger cannot open input stream from %s: %s", filename,
            error->message);
        g_clear_error(&error);
        return FALSE;
    }
    
    decompressor = g_zlib_decompressor_new(G_ZLIB_COMPRESSOR_FORMAT_ZLIB);
    decompress_istream = g_converter_input_stream_new(G_INPUT_STREAM(
        file_istream), G_CONVERTER(decompressor));
    g_object_unref(file_istream);
    g_object_unref(decompressor);
    
    parse_array = g_byte_array_new();
    while((read_size=g_input_stream_read(decompress_istream, buffer, 4096,
        NULL, NULL))>0 && logger_data->query_work_flag)
    {
        for(i=0;i<read_size;i++)
        {
            if(parse_array->len<4 &&
                buffer[i]==TL_LOGGER_LOG_ITEM_HEAD_MAGIC[parse_array->len])
            {
                g_byte_array_append(parse_array, buffer + i, 1);
            }
            else if(parse_array->len>=4)
            {
                g_byte_array_append(parse_array, buffer + i, 1);
                
                if(except_len==0 && parse_array->len>=6)
                {
                    memcpy(&except_len, parse_array->data + 4, 4);
                    except_len = g_ntohl(except_len);
                }
                
                if(parse_array->len>=14 && parse_array->len==except_len &&
                    memcmp(parse_array->data + parse_array->len - 4,
                    TL_LOGGER_LOG_ITEM_TAIL_MAGIC, 4)==0)
                {
                    /* Parse the data */
                    memcpy(&except_crc, parse_array->data + 8, 2);
                    except_crc = g_ntohs(except_crc);
                    
                    real_crc = tl_logger_crc16_compute(parse_array->data + 10,
                        except_len);
                    
                    if(except_crc==real_crc)
                    {
                        ret = callback(logger_data, parse_array, query_data);
                    }
                    else
                    {
                        g_warning("TLLogger detected incorrect CRC in "
                            "log item!");
                    }
                    
                    g_byte_array_unref(parse_array);
                    parse_array = g_byte_array_new();
                }
                
                if(parse_array->len > except_len+14 || 
                    parse_array->len > TL_LOGGER_LOG_PARSE_MAXIUM_SIZE)
                {
                    g_byte_array_unref(parse_array);
                    parse_array = g_byte_array_new();
                }
            }
            
            if(!ret)
            {
                break;
            }
        }
        
        if(!ret)
        {
            break;
        }
    }
    
    g_object_unref(decompress_istream);
    
    g_byte_array_unref(parse_array);
    
    return TRUE;
}

static gboolean tl_logger_log_query_from_cache(TLLoggerData *logger_data,
    TLLoggerQueryData *query_data)
{
    GList *list_foreach;
    GHashTable *log_data, *dup_table;
    GHashTableIter iter;
    TLLoggerLogItemData *log_item_data, *dup_item_data;
    GSList *query_result_list = NULL, *slist_foreach;
    
    g_mutex_lock(&(logger_data->cached_log_mutex));
    
    for(list_foreach=g_queue_peek_head_link(logger_data->cached_log_data);
        list_foreach!=NULL;list_foreach=g_list_next(list_foreach))
    {        
        log_data = list_foreach->data;
        if(log_data==NULL)
        {
            continue;
        }
        
        log_item_data = g_hash_table_lookup(log_data, "time");
        if(log_item_data==NULL)
        {
            continue;
        }
        
        if(query_data->end_time_set && query_data->end_time <
            log_item_data->value)
        {
            break;
        }
        
        if(query_data->begin_time_set && query_data->begin_time >
            log_item_data->value)
        {
            continue;
        }
        
        dup_table = g_hash_table_new_full(g_str_hash, g_str_equal, NULL,
            (GDestroyNotify)tl_logger_log_item_data_free);
        g_hash_table_iter_init(&iter, log_data);
        while(g_hash_table_iter_next(&iter, NULL, (gpointer *)&log_item_data))
        {
            if(log_item_data==NULL)
            {
                continue;
            }
            
            dup_item_data = tl_logger_log_item_data_dup(log_item_data);
            g_hash_table_replace(dup_table, dup_item_data->name, dup_item_data);
        }
        
        query_result_list = g_slist_prepend(query_result_list, dup_table);
    }
    
    g_mutex_unlock(&(logger_data->cached_log_mutex));
    
    for(slist_foreach=query_result_list;slist_foreach!=NULL;
        slist_foreach=g_slist_next(slist_foreach))
    {
        dup_table = slist_foreach->data;
        if(dup_table==NULL)
        {
            continue;
        }
        
        if(query_data->query_result_cb!=NULL)
        {
            query_data->query_result_cb(query_data->begin_time_set,
                query_data->begin_time, query_data->end_time_set,
                query_data->end_time, dup_table,
                query_data->query_result_user_data);
        }
    }
    
    g_slist_free_full(query_result_list, (GDestroyNotify)g_hash_table_unref);
    
    return TRUE;
}

/*
 * Log Frame:
 * | Head Magic (4B) | Length (4B) | CRC16 (2B) | JSON Data | Tail Magic (4B) |
 * 
 */

static GByteArray *tl_logger_log_to_file_data(GHashTable *log_data)
{
    GByteArray *ba;
    GHashTableIter iter, iter2;
    TLLoggerLogItemData *item_data;
    json_object *root, *child, *item_object, *array, *dict;
    const gchar *json_data;
    guint32 json_len, belen;
    guint16 crc, becrc;
    gpointer key;
    gint64 *valueptr;
    gchar keystr[16] = {0};
    
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
        
        child = json_object_new_int(item_data->offset);
        json_object_object_add(item_object, "offset", child);
        
        child = json_object_new_double(item_data->unit);
        json_object_object_add(item_object, "unit", child);
        
        child = json_object_new_int(item_data->source);
        json_object_object_add(item_object, "source", child);
        
        if(item_data->list_item && item_data->list_table!=NULL)
        {
            child = json_object_new_int(1);
            json_object_object_add(item_object, "listindex", child);
            
            array = json_object_new_array();
            g_hash_table_iter_init(&iter2, item_data->list_table);
            while(g_hash_table_iter_next(&iter2, &key, NULL))
            {
                child = json_object_new_int(GPOINTER_TO_INT(key));
                json_object_array_add(array, child);
            }
            json_object_object_add(item_object, "index", array);
        }
        else if(item_data->list_parent!=NULL && item_data->list_table!=NULL)
        {
            child = json_object_new_string(item_data->list_parent);
            json_object_object_add(item_object, "listparent", child);
            dict = json_object_new_object();
            g_hash_table_iter_init(&iter2, item_data->list_table);
            while(g_hash_table_iter_next(&iter2, &key, (gpointer *)&valueptr))
            {
                if(valueptr==NULL)
                {
                    continue;
                }
                snprintf(keystr, 15, "%d", GPOINTER_TO_INT(key));
                child = json_object_new_int64(*valueptr);
                json_object_object_add(dict, keystr, child);
            }
            json_object_object_add(item_object, "valuetable", dict);
        }
        
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

static void tl_logger_archives_clear_old(TLLoggerData *logger_data,
    guint64 freespace, guint64 freeinodes)
{
    GDir *log_dir;
    GError *error = NULL;
    const gchar *filename;
    struct stat statbuf;
    TLLoggerFileStat *file_stat;
    GSequence *file_sequence;
    GSequenceIter *iter;
    gchar *fullpath;
    
    log_dir = g_dir_open(logger_data->storage_base_path, 0, &error);
    if(error!=NULL)
    {
        g_warning("TLLogger cannot open log storage directionary: %s",
            error->message);
        return;
    }
    
    file_sequence = g_sequence_new((GDestroyNotify)tl_logger_file_stat_free);
    
    while((filename=g_dir_read_name(log_dir))!=NULL)
    {
        if(g_str_has_suffix(filename, ".tlz"))
        {
            fullpath = g_build_filename(logger_data->storage_base_path,
                filename, NULL);
            if(stat(fullpath, &statbuf)==0)
            {
                file_stat = g_new0(TLLoggerFileStat, 1);
                file_stat->name = fullpath;
                file_stat->size = statbuf.st_size;
                
                g_sequence_insert_sorted(file_sequence, file_stat, 
                    (GCompareDataFunc)tl_logger_file_stat_compare,
                    logger_data);
            }
            else
            {
                g_warning("TLLogger cannot stat archived log file %s: %s",
                    filename, strerror(errno));
                g_free(fullpath);
            }
            
        }
    }
    
    for(iter=g_sequence_get_begin_iter(file_sequence);
        !g_sequence_iter_is_end(iter);iter=g_sequence_iter_next(iter))
    {
        file_stat = g_sequence_get(iter);
        if(file_stat==NULL)
        {
            continue;
        }
        
        if(freespace > TL_LOGGER_LOG_FREE_SPACE_MINIUM &&
            freeinodes > TL_LOGGER_LOG_FREE_NODE_MINIUM)
        {
            break;
        }
        
        if(g_remove(file_stat->name)==0)
        {
            freespace += file_stat->size;
            freeinodes++;
        }
        else
        {
            g_warning("TLLogger failed to remove old archive file %s: %s",
                file_stat->name, strerror(errno));
            break;
        }
    }
    
    g_sequence_free(file_sequence);
    
    g_dir_close(log_dir);
}

static gboolean tl_logger_log_archive_compress_file(TLLoggerData *logger_data,
    const gchar *file)
{
    gboolean ret = TRUE;
    GZlibCompressor *compressor;
    GFileOutputStream *file_ostream;
    GOutputStream *compress_ostream;
    GFile *output_file;
    gssize write_size;
    GError *error = NULL;
    FILE *fp;
    gchar *tmpname;
    gchar buff[4096];
    size_t rsize;
    gchar *newname;
    
    if(file==NULL)
    {
        return FALSE;
    }
    
    tmpname = g_build_filename(logger_data->storage_base_path, "tlz.tmp",
        NULL);
    output_file = g_file_new_for_path(tmpname);
    if(output_file==NULL)
    {
        g_free(tmpname);
        return FALSE;
    }
    
    file_ostream = g_file_replace(output_file, NULL, FALSE,
        G_FILE_CREATE_PRIVATE, NULL, &error);
    g_object_unref(output_file);
    
    if(file_ostream==NULL)
    {
        g_warning("TLLogger cannot open output file stream: %s",
            error->message);
        g_clear_error(&error);
        g_free(tmpname);
        return FALSE;
    }
    
    compressor = g_zlib_compressor_new(G_ZLIB_COMPRESSOR_FORMAT_ZLIB, 5);
    compress_ostream = g_converter_output_stream_new(G_OUTPUT_STREAM(
        file_ostream), G_CONVERTER(compressor));
    g_object_unref(file_ostream);
    g_object_unref(compressor);
    
    fp = fopen(file, "r");
    if(fp==NULL)
    {
        g_warning("TLLogger failed to open origin log file for new "
            "archived log: %s", strerror(errno));
        g_output_stream_close(compress_ostream, NULL, NULL);
        g_object_unref(compress_ostream);
        return FALSE;
    }
    
    while((rsize=fread(buff, 1, 4096, fp))>0)
    {
        write_size += g_output_stream_write(compress_ostream, buff,
            rsize, NULL, &error);
        if(error!=NULL)
        {
            g_warning("TLLogger cannot write archive: %s", error->message);
            g_clear_error(&error);
            ret = FALSE;
            break;
        }
    }
    fclose(fp);

    g_output_stream_close(compress_ostream, NULL, &error);
    if(error!=NULL)
    {
        g_warning("TLLogger cannot close archive file stream: %s",
            error->message);
        g_clear_error(&error);
        ret = FALSE;
    }
    g_object_unref(compress_ostream);

    if(ret)
    {
        newname = g_strdup_printf("%sz", file);
        g_rename(tmpname, newname);
        g_free(newname);
    }
    
    g_free(tmpname);

    return ret;
}

static gboolean tl_logger_log_query_file_cb(TLLoggerData *logger_data,
    GByteArray *ba, TLLoggerQueryData *query_data)
{
    struct json_tokener *tokener;
    struct json_object *root = NULL, *node, *child, *array, *dict;
    guint array_len, array2_len, i, j;
    gboolean ret = TRUE;
    gboolean data_completed = TRUE;
    const gchar *name, *listparent;
    
    GHashTable *log_table, *value_table;
    TLLoggerLogItemData *log_item_data;
    gint64 log_value;
    gint log_source;
    gint log_offset;
    gdouble log_unit;
    gboolean list_index;
    gint64 i64value;
    gint ivalue;
    struct json_object_iter json_iter;
    
    const gchar *json_data = (const gchar *)ba->data + 10;
    guint json_len = ba->len - 14;
    
    tokener = json_tokener_new();
    root = json_tokener_parse_ex(tokener, json_data, json_len);
    json_tokener_free(tokener);
    
    if(root==NULL)
    {
        g_warning("TLLogger failed to parse JSON data!");
        return FALSE;
    }
    
    array_len = json_object_array_length(root);
    log_table = g_hash_table_new_full(g_str_hash, g_str_equal, NULL,
        (GDestroyNotify)tl_logger_log_item_data_free);
    
    for(i=0;i<array_len;i++)
    {
        node = json_object_array_get_idx(root, i);
        if(node==NULL)
        {
            continue;
        }
        
        json_object_object_get_ex(node, "name", &child);
        if(child==NULL)
        {
            continue;
        }
        
        name = json_object_get_string(child);
        
        json_object_object_get_ex(node, "value", &child);
        if(child!=NULL)
        {
            log_value = json_object_get_int64(child);
        }
        else
        {
            log_value = 0;
        }
        
        json_object_object_get_ex(node, "unit", &child);
        if(child!=NULL)
        {
            log_unit = json_object_get_double(child);
        }
        else
        {
            log_unit = 1.0;
        }
        
        json_object_object_get_ex(node, "offset", &child);
        if(child!=NULL)
        {
            log_offset = json_object_get_int(child);
        }
        else
        {
            log_offset = 0;
        }
        
        json_object_object_get_ex(node, "source", &child);
        if(child!=NULL)
        {
            log_source = json_object_get_int(child);
        }
        else
        {
            log_source = 0;
        }
        
        list_index = FALSE;
        value_table = NULL;
        json_object_object_get_ex(node, "listindex", &child);
        if(child!=NULL)
        {
            list_index = (json_object_get_int(child)!=0);
            array = NULL;
            if(list_index)
            {
                json_object_object_get_ex(node, "index", &array);
                if(array==NULL)
                {
                    list_index = FALSE;
                }
            }
            if(list_index)
            {
                array2_len = json_object_array_length(array);
                value_table = g_hash_table_new_full(g_direct_hash,
                    g_direct_equal, NULL, g_free);
                for(j=0;j<array2_len;j++)
                {
                    child = json_object_array_get_idx(array, j);
                    if(child==NULL)
                    {
                        continue;
                    }
                    i64value = json_object_get_int64(child);
                    ivalue = i64value;
                    g_hash_table_replace(value_table, GINT_TO_POINTER(ivalue),
                        g_memdup(&i64value, sizeof(gint64)));
                }   
            }
        }
        
        listparent = NULL;
        dict = NULL;
        if(!list_index)
        {
            json_object_object_get_ex(node, "listparent", &child);
            if(child!=NULL)
            {
                listparent = json_object_get_string(child);
            }
        }
        if(listparent!=NULL)
        {
            json_object_object_get_ex(node, "valuetable", &dict);
            if(dict!=NULL)
            {
                value_table = g_hash_table_new_full(g_direct_hash,
                    g_direct_equal, NULL, g_free);
                json_object_object_foreachC(dict, json_iter)
                {
                    if(json_iter.key!=NULL && json_iter.val!=NULL &&
                        sscanf(json_iter.key, "%u", &ivalue)>0)
                    {
                        i64value = json_object_get_int64(json_iter.val);
                        g_hash_table_replace(value_table,
                            GINT_TO_POINTER(ivalue),
                            g_memdup(&i64value, sizeof(gint64)));
                    }
                }
            }
        }
        
        if(g_strcmp0(name, "time")==0)
        {
            log_value = json_object_get_int64(child);
        
            if(query_data->end_time_set && query_data->end_time < log_value)
            {
                ret = FALSE;
                data_completed = FALSE;
                break;
            }
        
            if(query_data->begin_time_set &&
                query_data->begin_time > log_value)
            {
                data_completed = FALSE;
                break;
            }
        }
        
        log_item_data = g_new0(TLLoggerLogItemData, 1);
        log_item_data->name = g_strdup(name);
        log_item_data->value = log_value;
        log_item_data->source = log_source;
        log_item_data->unit = log_unit;
        log_item_data->offset = log_offset;
        
        if(list_index && value_table!=NULL)
        {
            log_item_data->list_index = TRUE;
            log_item_data->list_table = value_table;
        }
        else if(listparent!=NULL && value_table!=NULL)
        {
            log_item_data->list_parent = g_strdup(listparent);
            log_item_data->list_table = value_table;
        }
        else if(value_table!=NULL)
        {
            g_hash_table_unref(value_table);
        }
        
        g_hash_table_replace(log_table, log_item_data->name, log_item_data);
    }
    
    json_object_put(root);
    
    if(data_completed && !g_hash_table_contains(log_table, "time"))
    {
        data_completed = FALSE;
    }
    
    if(data_completed)
    {
        if(query_data->query_result_cb!=NULL)
        {
            query_data->query_result_cb(query_data->begin_time_set,
                query_data->begin_time, query_data->end_time_set,
                query_data->end_time, log_table,
                query_data->query_result_user_data);
        }
    }
    g_hash_table_unref(log_table);
    
    return ret;
}

static gpointer tl_logger_log_query_thread(gpointer user_data)
{
    TLLoggerData *logger_data = (TLLoggerData *)user_data;
    TLLoggerQueryData *query_data;
    GDir *log_dir;
    GError *error = NULL;
    const gchar *filename;
    
    if(user_data==NULL)
    {
        return NULL;
    }
    
    logger_data->query_thread_work_flag = TRUE;
    
    while(logger_data->query_thread_work_flag)
    {
        g_mutex_lock(&(logger_data->query_queue_mutex));
        query_data = g_queue_pop_head(logger_data->query_queue);
        logger_data->query_working_data = query_data;
        logger_data->query_work_flag = TRUE;
        g_mutex_unlock(&(logger_data->query_queue_mutex));
        
        if(query_data!=NULL)
        {
            G_STMT_START
            {
                log_dir = g_dir_open(logger_data->storage_base_path, 0,
                    &error);
                if(error!=NULL)
                {
                    g_warning("TLLogger cannot open log storage "
                        "directionary: %s", error->message);
                    break;
                }
                
                while(logger_data->query_work_flag &&
                    (filename=g_dir_read_name(log_dir))!=NULL)
                {
                    if(g_str_has_suffix(filename, ".tlz"))
                    {
                        tl_logger_log_query_from_file(logger_data, filename,
                            query_data, tl_logger_log_query_file_cb);
                    }
                }
            }
            G_STMT_END;
            
            g_dir_close(log_dir);
            
            tl_logger_log_query_from_cache(logger_data, query_data);
            
            logger_data->query_working_data = NULL;
            g_free(query_data);
        }
        else
        {
            g_usleep(100000);
        }
    }
    
    return NULL;
}

static gpointer tl_logger_log_archive_thread(gpointer user_data)
{
    TLLoggerData *logger_data = (TLLoggerData *)user_data;
    GDir *log_dir;
    GError *error = NULL;
    const gchar *filename;
    gchar *fullpath;
    struct statvfs statbuf;
    guint64 freespace;
    guint64 freeinodes;
    
    if(user_data==NULL)
    {
        return NULL;
    }
    
    log_dir = g_dir_open(logger_data->storage_base_path, 0, &error);
    if(error!=NULL)
    {
        g_warning("TLLogger cannot open log storage directionary: %s",
            error->message);
        g_clear_error(&error);
        return NULL;
    }
    
    logger_data->archive_thread_work_flag = TRUE;
    
    while(logger_data->archive_thread_work_flag)
    {
        if(logger_data->archive_thread_wait_countdown>0)
        {
            logger_data->archive_thread_wait_countdown--;
            g_usleep(1000000);
        }
        else /* Scan unarchived log every 60s. */
        {
            if(statvfs(logger_data->storage_base_path, &statbuf)==0)
            {
                freespace = (guint64)statbuf.f_bavail * statbuf.f_bsize;
                freeinodes = statbuf.f_favail;
                
                if(freespace < TL_LOGGER_LOG_FREE_SPACE_MINIUM ||
                    freeinodes < TL_LOGGER_LOG_FREE_NODE_MINIUM)
                {
                    /* Clear the disk for more space or inodes */
                    tl_logger_archives_clear_old(logger_data, freespace,
                        freeinodes);
                }
            }
            else
            {
                g_warning("TLLogger cannot stat storage directory: %s",
                    strerror(errno));
            }
            
            while((filename=g_dir_read_name(log_dir))!=NULL)
            {
                if(g_str_has_suffix(filename, ".tl"))
                {
                    fullpath = g_build_filename(
                        logger_data->storage_base_path, filename, NULL);
                    if(tl_logger_log_archive_compress_file(logger_data,
                        fullpath))
                    {
                        g_remove(fullpath);
                    }
                    g_free(fullpath);
                }
            }
            
            g_dir_rewind(log_dir);
            
            logger_data->archive_thread_wait_countdown = 60;
        }
    }
    
    g_dir_close(log_dir);
    
    return NULL;
}

static gpointer tl_logger_log_write_thread(gpointer user_data)
{
    TLLoggerData *logger_data = (TLLoggerData *)user_data;
    GHashTable *item_data;
    GByteArray *ba;
    int fd = -1;
    ssize_t written_size = 0, rsize;
    gchar *lastlog_filename = NULL;
    gchar *datestr, *filename, *fullpath;
    gchar *lastlog_basename = NULL;
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
            fsync(fd);
            close(fd);
            fd = -1;
            
            if(lastlog_filename!=NULL && lastlog_basename!=NULL)
            {
                filename = g_strdup_printf("%s.tl", lastlog_basename);
                fullpath = g_build_filename(
                    logger_data->storage_base_path, filename, NULL);
                g_free(filename);
                
                g_rename(lastlog_basename, fullpath);
                
                g_free(fullpath);
            }
            
            g_queue_free_full(logger_data->cached_log_data,
                (GDestroyNotify)g_hash_table_unref);
            logger_data->cached_log_data = g_queue_new();
            logger_data->last_saved_data = NULL;
            
            logger_data->archive_thread_wait_countdown = 0;
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
            if(lastlog_basename!=NULL)
            {
                g_free(lastlog_basename);
            }
            dt = g_date_time_new_from_unix_local(write_time);
            datestr = g_date_time_format(dt, "%Y%m%d%H%M%S");
            lastlog_basename = g_strdup_printf("tbl-%s", datestr);
            filename = g_strdup_printf("%s.tlw", lastlog_basename);
            g_free(datestr);
            g_date_time_unref(dt);
            
            lastlog_filename = g_build_filename(
                logger_data->storage_base_path, filename, NULL);
            g_free(filename);
            
            fd = open(lastlog_filename, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
            written_size = 0;
        }
        
        rsize = write(fd, ba->data, ba->len);
        g_byte_array_unref(ba);
        fsync(fd);
        
        if(rsize>0)
        {
            written_size += rsize;
        }
        
        if(rsize<=0 || written_size>=TL_LOGGER_LOG_SIZE_MAXIUM)
        {
            fsync(fd);
            close(fd);
            fd = -1;
            
            if(lastlog_filename!=NULL && lastlog_basename!=NULL)
            {
                filename = g_strdup_printf("%s.tl", lastlog_basename);
                fullpath = g_build_filename(
                    logger_data->storage_base_path, filename, NULL);
                g_free(filename);
                
                g_rename(lastlog_basename, fullpath);
                
                g_free(fullpath);
            }
            
            g_mutex_lock(&(logger_data->cached_log_mutex));
            g_queue_free_full(logger_data->cached_log_data,
                (GDestroyNotify)g_hash_table_unref);
            logger_data->cached_log_data = g_queue_new();
            logger_data->last_saved_data = NULL;
            g_mutex_unlock(&(logger_data->cached_log_mutex));
            
            logger_data->archive_thread_wait_countdown = 0;
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
        g_queue_push_tail(logger_data->write_log_queue, dup_table);
        g_mutex_unlock(&(logger_data->cached_log_mutex));
                
        logger_data->last_timestamp = logger_data->new_timestamp;
    }
    
    return TRUE;
}

gboolean tl_logger_init(const gchar *storage_base_path)
{
    GDir *log_dir;
    GError *error = NULL;
    const gchar *filename;
    gchar *fullpath, *newpath;
    size_t slen;
    
    if(g_tl_logger_data.initialized)
    {
        g_warning("TLLogger already initialized!");
        return TRUE;
    }
    
    g_mutex_init(&(g_tl_logger_data.cached_log_mutex));
    g_mutex_init(&(g_tl_logger_data.query_queue_mutex));
    
    g_tl_logger_data.cached_log_data = g_queue_new();
    g_tl_logger_data.write_log_queue = g_queue_new();
    g_tl_logger_data.query_queue = g_queue_new();
    
    if(storage_base_path!=NULL)
    {
        g_tl_logger_data.storage_base_path = g_strdup(storage_base_path);
    }
    else
    {
        g_tl_logger_data.storage_base_path = g_strdup(
            TL_LOGGER_STORAGE_BASE_PATH_DEFAULT);
    }
    
    log_dir = g_dir_open(g_tl_logger_data.storage_base_path, 0, &error);
    if(error!=NULL)
    {
        g_warning("TLLogger cannot open log storage directionary: %s",
            error->message);
        g_free(g_tl_logger_data.storage_base_path);
        g_tl_logger_data.storage_base_path = NULL;
        return FALSE;
    }
    
    while((filename=g_dir_read_name(log_dir))!=NULL)
    {
        if(g_str_has_suffix(filename, ".tlw"))
        {
            fullpath = g_build_filename(g_tl_logger_data.storage_base_path,
                filename, NULL);
            newpath = g_strdup(fullpath);
            slen = strlen(newpath);
            newpath[slen-1] = '\0';
            
            g_rename(fullpath, newpath);
            
            g_free(newpath);
            g_free(fullpath);
        }
    }
    
    g_dir_close(log_dir);
    
    g_tl_logger_data.log_update_timeout_id = g_timeout_add_seconds(10,
        tl_logger_log_update_timer_cb, &g_tl_logger_data);
    
    g_tl_logger_data.write_thread = g_thread_new("tl-logger-write-thread",
        tl_logger_log_write_thread, &g_tl_logger_data);
        
    g_tl_logger_data.archive_thread = g_thread_new("tl-logger-archive-thread",
        tl_logger_log_archive_thread, &g_tl_logger_data);
        
    g_tl_logger_data.query_thread = g_thread_new("tl-logger-query-thread",
        tl_logger_log_query_thread, &g_tl_logger_data);
    
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
    
    if(g_tl_logger_data.query_thread!=NULL)
    {
        g_tl_logger_data.query_thread_work_flag = FALSE;
        g_thread_join(g_tl_logger_data.query_thread);
        g_tl_logger_data.query_thread = NULL;
    }
    
    if(g_tl_logger_data.write_thread!=NULL)
    {
        g_tl_logger_data.write_thread_work_flag = FALSE;
        g_thread_join(g_tl_logger_data.write_thread);
        g_tl_logger_data.write_thread = NULL;
    }
    
    if(g_tl_logger_data.archive_thread!=NULL)
    {
        g_tl_logger_data.archive_thread_work_flag = FALSE;
        g_tl_logger_data.archive_thread_wait_countdown = 0;
        g_thread_join(g_tl_logger_data.archive_thread);
        g_tl_logger_data.archive_thread = NULL;
    }

    if(g_tl_logger_data.last_log_data!=NULL)
    {
        g_hash_table_unref(g_tl_logger_data.last_log_data);
        g_tl_logger_data.last_log_data = NULL;
    }
    
    if(g_tl_logger_data.query_queue!=NULL)
    {
        g_queue_free_full(g_tl_logger_data.query_queue, g_free);
        g_tl_logger_data.query_queue = NULL;
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
    g_mutex_clear(&(g_tl_logger_data.query_queue_mutex));
    
    g_tl_logger_data.initialized = FALSE;
}

void tl_logger_current_data_update(const TLLoggerLogItemData *item_data)
{
    TLLoggerLogItemData *idata, *pdata;
    gint pindex = 0;
    
    if(item_data==NULL || item_data->name==NULL)
    {
        return;
    }
    if(g_tl_logger_data.last_log_data==NULL)
    {
        g_tl_logger_data.last_log_data = g_hash_table_new_full(g_str_hash,
            g_str_equal, NULL, (GDestroyNotify)tl_logger_log_item_data_free);
    }
    
    if(!item_data->list_index && item_data->list_parent!=NULL)
    {
        pdata = g_hash_table_lookup(g_tl_logger_data.last_log_data,
            item_data->list_parent);
        if(pdata==NULL)
        {
            return;
        }
        pindex = pdata->value;
        
        if(pindex<=0)
        {
            return;
        }
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
            idata->offset = item_data->offset;
            
            if(item_data->list_index)
            {
                pindex = item_data->value;
                g_hash_table_replace(idata->list_table,
                    GINT_TO_POINTER(pindex), g_memdup(&(item_data->value),
                    sizeof(gint64)));
            }
            else if(item_data->list_parent!=NULL)
            {
                g_hash_table_replace(idata->list_table,
                    GINT_TO_POINTER(pindex), g_memdup(&(item_data->value),
                    sizeof(gint64)));
            }
        }
    }
    else
    {
        idata = g_new0(TLLoggerLogItemData, 1);
        idata->name = g_strdup(item_data->name);
        idata->value = item_data->value;
        idata->unit = item_data->unit;
        idata->source = item_data->source;
        idata->list_index = item_data->list_index;
        idata->offset = item_data->offset;
        
        if(item_data->list_index)
        {
            idata->list_table = g_hash_table_new_full(g_direct_hash,
                g_direct_equal, NULL, g_free);
            idata->list_index = item_data->list_index;
            pindex = item_data->value;
            g_hash_table_replace(idata->list_table,
                GINT_TO_POINTER(pindex), g_memdup(&(item_data->value),
                sizeof(gint64)));
        }
        else if(item_data->list_parent!=NULL)
        {
            idata->list_parent = g_strdup(item_data->list_parent);
            idata->list_table = g_hash_table_new_full(g_direct_hash,
                g_direct_equal, NULL, g_free);
            
            g_hash_table_replace(idata->list_table,
                GINT_TO_POINTER(pindex), g_memdup(&(item_data->value),
                sizeof(gint64)));
        }
        
        g_hash_table_replace(g_tl_logger_data.last_log_data, idata->name,
            idata);
    }
    
    g_tl_logger_data.new_timestamp = g_get_monotonic_time();
}

GHashTable *tl_logger_current_data_get(gboolean *updated)
{
    static gint64 latest_timestamp = 0;
    
    if(g_tl_logger_data.new_timestamp!=latest_timestamp)
    {
        if(updated!=NULL)
        {
            *updated = TRUE;
        }
        latest_timestamp = g_tl_logger_data.new_timestamp;
    }
    else
    {
        if(updated!=NULL)
        {
            *updated = FALSE;
        }
    }
    
    return g_tl_logger_data.last_log_data;
}

void *tl_logger_log_query_start(gboolean begin_time_set, gint64 begin_time,
    gboolean end_time_set, gint64 end_time,
    TLLoggerQueryResultCallback callback, gpointer user_data)
{
    TLLoggerQueryData *query_data;
    
    if(!g_tl_logger_data.initialized)
    {
        return NULL;
    }
    
    query_data = g_new0(TLLoggerQueryData, 1);
    query_data->begin_time_set = begin_time_set;
    query_data->end_time_set = end_time_set;
    query_data->begin_time = begin_time;
    query_data->end_time = end_time;
    query_data->query_result_cb = callback;
    query_data->query_result_user_data = user_data;
    
    g_mutex_lock(&(g_tl_logger_data.query_queue_mutex));
    g_queue_push_tail(g_tl_logger_data.query_queue, query_data);
    
    g_mutex_unlock(&(g_tl_logger_data.query_queue_mutex));
    
    return query_data;
}

void tl_logger_log_query_stop(void *handler)
{
    if(!g_tl_logger_data.initialized)
    {
        return;
    }
    
    g_mutex_lock(&(g_tl_logger_data.query_queue_mutex));
    if(handler==NULL || handler==g_tl_logger_data.query_working_data)
    {
        g_tl_logger_data.query_work_flag = FALSE;
    }
    if(handler!=NULL)
    {
        g_queue_remove(g_tl_logger_data.query_queue, handler);
    }
    else
    {
        g_queue_free_full(g_tl_logger_data.query_queue, g_free);
        g_tl_logger_data.query_queue = g_queue_new();
    }
    g_mutex_unlock(&(g_tl_logger_data.query_queue_mutex));
}
