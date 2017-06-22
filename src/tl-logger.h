#ifndef HAVE_TL_LOGGER_H
#define HAVE_TL_LOGGER_H

#include <glib.h>

typedef struct _TLLoggerLogItemData
{
    gchar *name;
    gint64 value;
    gint offset;
    gdouble unit;
    guint list_item;
    gint8 source;
    gboolean list_index;
    gchar *list_parent;
    GHashTable *list_table;
}TLLoggerLogItemData;

typedef void (*TLLoggerQueryResultCallback)(gboolean begin_time_set,
    gint64 begin_time, gboolean end_time_set, gint64 end_time,
    GHashTable *log_table, gpointer user_data);

gboolean tl_logger_init(const gchar *storage_base_path);
void tl_logger_uninit();
void tl_logger_current_data_update(const TLLoggerLogItemData *item_data);
GHashTable *tl_logger_current_data_get(gboolean *updated);

void *tl_logger_log_query_start(gboolean begin_time_set, gint64 begin_time,
    gboolean end_time_set, gint64 end_time,
    TLLoggerQueryResultCallback callback, gpointer user_data);
void tl_logger_log_query_stop(void *handler);

#endif
