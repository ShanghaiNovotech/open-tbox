#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "tl-parser.h"

typedef enum 
{
    TL_PARSER_PRIMARY_STATE_NONE,
    TL_PARSER_PRIMARY_STATE_SIGNAL,
    TL_PARSER_PRIMARY_STATE_NAME,
    TL_PARSER_PRIMARY_STATE_REV
}TLParserPrimaryState;

typedef struct _TLParserData
{
    gboolean initialized;
    GMarkupParseContext *parser_context;
    GHashTable *parser_table;
    gboolean data_flag;
    TLParserPrimaryState primary_state;
    gchar *name;
    guint rev;
    gboolean use_ext_id;
}TLParserData;

static TLParserData g_tl_parser_data = {0};

static void tl_parser_signal_data_free(TLParserSignalData *data)
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

static void tl_parser_signal_data_list_free(GSList *list)
{
    if(list==NULL)
    {
        return;
    }
    g_slist_free_full(list, (GDestroyNotify)tl_parser_signal_data_free);
}

static void tl_parser_markup_parser_start_element(GMarkupParseContext *context,
    const gchar *element_name, const gchar **attribute_names,
    const gchar **attribute_values, gpointer user_data, GError **error)
{
    TLParserData *parser_data = (TLParserData *)user_data;
    int i;
    TLParserSignalData *signal_data;
    gboolean have_id = FALSE;
    GSList *signal_list;
    
    if(user_data==NULL)
    {
        return;
    }
    
    if(!parser_data->data_flag && g_strcmp0(element_name, "tbox")==0)
    {
        parser_data->data_flag = TRUE;
    }
    else if(parser_data->data_flag && g_strcmp0(element_name, "signal")==0)
    {
        parser_data->primary_state = TL_PARSER_PRIMARY_STATE_SIGNAL;
        signal_data = g_new0(TLParserSignalData, 1);
        have_id = FALSE;
        
        for(i=0;attribute_names[i]!=NULL;i++)
        {
            if(g_strcmp0(attribute_names[i], "id")==0)
            {
                if(sscanf(attribute_values[i], "%d", &(signal_data->id))>=1)
                {
                    have_id = TRUE;
                    if(signal_data->id >= 2048)
                    {
                        parser_data->use_ext_id = TRUE;
                    }
                    g_debug("Parsed CAN-Bus ID %d\n", signal_data->id);
                }
            }
            else if(g_strcmp0(attribute_names[i], "name")==0)
            {
                signal_data->name = g_strdup(attribute_names[i]);
            }
            else if(g_strcmp0(attribute_names[i], "byteorder")==0)
            {
                if(g_ascii_strcasecmp(attribute_names[i], "BE")==0)
                {
                    signal_data->endian = TRUE;
                }
            }
            else if(g_strcmp0(attribute_names[i], "firstbyte")==0)
            {
                sscanf(attribute_values[i], "%u", &(signal_data->firstbyte));
            }
            else if(g_strcmp0(attribute_names[i], "firstbit")==0)
            {
                sscanf(attribute_values[i], "%u", &(signal_data->firstbit));
            }
            else if(g_strcmp0(attribute_names[i], "bitlength")==0)
            {
                sscanf(attribute_values[i], "%u", &(signal_data->bitlength));
            }
            else if(g_strcmp0(attribute_names[i], "unit")==0)
            {
                sscanf(attribute_values[i], "%lf", &(signal_data->unit));
            }
            else if(g_strcmp0(attribute_names[i], "offset")==0)
            {
                sscanf(attribute_values[i], "%d", &(signal_data->offset));
            }
            else if(g_strcmp0(attribute_names[i], "source")==0)
            {
                sscanf(attribute_values[i], "%d", &(signal_data->source));
            }
        }
        
        if(have_id)
        {
            if(g_hash_table_contains(parser_data->parser_table,
                GINT_TO_POINTER(signal_data->id)))
            {
                signal_list = g_hash_table_lookup(parser_data->parser_table,
                    GINT_TO_POINTER(signal_data->id));
                signal_list = g_slist_prepend(signal_list, signal_data);
            }
            else
            {
                signal_list = g_slist_append(NULL, signal_data);
            }
            g_hash_table_replace(parser_data->parser_table,
                GINT_TO_POINTER(signal_data->id), signal_list);
        }
        else
        {
            tl_parser_signal_data_free(signal_data);
        }
    }
    else if(parser_data->data_flag && g_strcmp0(element_name, "name")==0)
    {
        parser_data->primary_state = TL_PARSER_PRIMARY_STATE_NAME;
    }
    else if(parser_data->data_flag && g_strcmp0(element_name, "rev")==0)
    {
        parser_data->primary_state = TL_PARSER_PRIMARY_STATE_REV;
    }
}

static void tl_parser_markup_parser_end_element(GMarkupParseContext *context,
    const gchar *element_name, gpointer user_data, GError **error)
{
    TLParserData *parser_data = (TLParserData *)user_data;
    
    if(user_data==NULL)
    {
        return;
    }
    
    if(!parser_data->data_flag)
    {
        return;
    }
    
    if(g_strcmp0(element_name, "tbox")==0)
    {
        parser_data->data_flag = FALSE;
    }
    else if(parser_data->primary_state!=TL_PARSER_PRIMARY_STATE_NONE)
    {
        parser_data->primary_state = TL_PARSER_PRIMARY_STATE_NONE;
    }
}

static void tl_parser_markup_parser_text(GMarkupParseContext *context,
    const gchar *text, gsize text_len, gpointer user_data, GError **error)
{
    
}

static GMarkupParser g_tl_parser_markup_parser =
{
    .start_element = tl_parser_markup_parser_start_element,
    .end_element = tl_parser_markup_parser_end_element,
    .text = tl_parser_markup_parser_text
};

gboolean tl_parser_init()
{
    if(g_tl_parser_data.initialized)
    {
        g_warning("TLParser already initialized!");
        return TRUE;
    }
    
    g_tl_parser_data.data_flag = FALSE;
    g_tl_parser_data.primary_state = TL_PARSER_PRIMARY_STATE_NONE;
    g_tl_parser_data.parser_table = g_hash_table_new_full(g_direct_hash,
        g_direct_equal, NULL, (GDestroyNotify)tl_parser_signal_data_list_free);
    
    g_tl_parser_data.initialized = TRUE;
    
    return TRUE;
}

void tl_parser_uninit()
{
    if(!g_tl_parser_data.initialized)
    {
        return;
    }
    if(g_tl_parser_data.parser_context!=NULL)
    {
        g_markup_parse_context_free(g_tl_parser_data.parser_context);
        g_tl_parser_data.parser_context = NULL;
    }
    if(g_tl_parser_data.parser_table!=NULL)
    {
        g_hash_table_unref(g_tl_parser_data.parser_table);
        g_tl_parser_data.parser_table = NULL;
    }
    
    g_tl_parser_data.initialized = FALSE;
}

gboolean tl_parser_load_parse_file(const gchar *file)
{
    gchar buffer[4096];
    FILE *fp;
    size_t rsize;
    GError *error = NULL;
    
    if(!g_tl_parser_data.initialized)
    {
        g_warning("TLParser is not initialized yet!");
        return FALSE;
    }
    
    if(file==NULL)
    {
        return FALSE;
    }
    
    fp = fopen(file, "r");
    if(fp==NULL)
    {
        g_warning("TLParser failed to open file %s: %s", file,
            strerror(errno));
        return FALSE;
    }
    
    g_hash_table_remove_all(g_tl_parser_data.parser_table);
    if(g_tl_parser_data.name!=NULL)
    {
        g_free(g_tl_parser_data.name);
        g_tl_parser_data.name = NULL;
    }
    g_tl_parser_data.rev = 0;
    g_tl_parser_data.use_ext_id = FALSE;
    
    g_tl_parser_data.parser_context = g_markup_parse_context_new(
        &g_tl_parser_markup_parser, 0, &g_tl_parser_data, NULL);
    while(!feof(fp))
    {
        if((rsize=fread(buffer, 4096, 1, fp))>0)
        {
            g_markup_parse_context_parse(g_tl_parser_data.parser_context,
                buffer, rsize, &error);
            if(error!=NULL)
            {
                g_warning("TLParser failed to parse file: %s", error->message);
                g_clear_error(&error);
            }
        }
    }
    g_markup_parse_context_end_parse(g_tl_parser_data.parser_context, &error);
    if(error!=NULL)
    {
        g_warning("TLParser parse file with error: %s", error->message);
        g_clear_error(&error);
    }
    if(g_tl_parser_data.parser_context!=NULL)
    {
        g_markup_parse_context_free(g_tl_parser_data.parser_context);
        g_tl_parser_data.parser_context = NULL;
    }
    
    return TRUE;
}

gboolean tl_parser_parse_can_data(const gchar *device,
    int can_id, guint8 *data, gsize len)
{
    GSList *signal_list, *list_foreach;
    TLParserSignalData *signal_data;
    gboolean parsed = FALSE;
    guint source = 0;
    guint firstbyte, rbits;
    guint64 value;
    gint i, x, y;
    
    if(!g_tl_parser_data.initialized || g_tl_parser_data.parser_table==NULL)
    {
        return FALSE;
    }
    
    signal_list = g_hash_table_lookup(g_tl_parser_data.parser_table,
        GINT_TO_POINTER(can_id));
        
    if(signal_list==NULL)
    {
        return FALSE;
    }
    
    if(sscanf(device, "can%u", &source)>0)
    {
        source += 1;
    }
    
    for(list_foreach=signal_list;list_foreach!=NULL;
        list_foreach=g_slist_next(list_foreach))
    {
        signal_data = list_foreach->data;
        
        if(signal_data->source>0 && signal_data->source!=source)
        {
            continue;
        }
        
        firstbyte = signal_data->firstbit / 8;
        
        if(firstbyte >= len)
        {
            continue;
        }
        
        value = 0;
        
        if(signal_data->endian) /* BE */
        {
            rbits = 8 - (signal_data->firstbit%8) + firstbyte * 8;
            for(i=0;i<signal_data->bitlength && i<rbits;i++)
            {
                x = (rbits - i) / 8;
                y = (signal_data->firstbit + i) % 8;
                value = value << 1;
                value |= ((data[x] >> y) & 1);
            }
        }
        else
        {
            rbits = len * 8 - signal_data->firstbit;
            for(i=0;i<signal_data->bitlength && i<rbits;i++)
            {
                x = (signal_data->firstbit + i) / 8;
                y = (signal_data->firstbit + i) % 8;
                value = value << 1;
                value |= ((data[x] >> y) & 1);
            }
        }
        
        g_debug("Got %s value %"G_GUINT64_FORMAT".", signal_data->name, value);
    }
    
    return parsed;
}
