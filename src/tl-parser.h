#ifndef HAVE_TL_PARSER_H
#define HAVE_TL_PARSER_H

#include <glib.h>

typedef struct _TLParserSignalData
{
    int id;
    gchar *name;
    gboolean endian;
    guint firstbyte;
    guint firstbit;
    guint bitlength;
    gdouble unit;
    int offset;
    int source;
}TLParserSignalData;

#define TL_PARSER_BATTERY_NUMBER "BMS08_BatNumber"

gboolean tl_parser_init();
void tl_parser_uninit();
gboolean tl_parser_load_parse_file(const gchar *file);
gboolean tl_parser_parse_can_data(const gchar *device,
    int can_id, const guint8 *data, gsize len);
const gchar *tl_parser_battery_code_get(guint8 *single_bat_code_len,
    guint *bat_code_total_len);

#endif
