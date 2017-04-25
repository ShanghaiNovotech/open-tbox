#ifndef HAVE_TL_LOGGER_H
#define HAVE_TL_LOGGER_H

#include <glib.h>

typedef struct _TLLoggerLogItemData
{
    gchar *name;
    gint64 value;
    gdouble unit;
    gint8 source;
}TLLoggerLogItemData;

gboolean tl_logger_init(const gchar *storage_base_path);
void tl_logger_uninit();
void tl_logger_update_current_data(const TLLoggerLogItemData *item_data);

#endif
