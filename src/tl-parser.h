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

gboolean tl_parser_init();
void tl_parser_uninit();
gboolean tl_parser_load_parse_file(const gchar *file);
gboolean tl_parser_parse_can_data(const gchar *device,
    int can_id, const guint8 *data, gsize len);

#endif
