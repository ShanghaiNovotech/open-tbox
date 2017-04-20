#include <unistd.h>
#include <glib.h>


#include "tl-logger.h"
#include "tl-canbus.h"
#include "tl-net.h"
#include "tl-parser.h"

static GMainLoop *g_tl_main_loop = NULL;
static gboolean g_tl_main_cmd_daemon = FALSE;

static GOptionEntry g_tl_main_cmd_entries[] =
{
    { "daemon", 'D', 0, G_OPTION_ARG_NONE, &g_tl_main_cmd_daemon,
        "Start as daemon", NULL },
    { NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL }
};

int main(int argc, char *argv[])
{
    GError *error = NULL;
    GOptionContext *context;
    
    context = g_option_context_new("- TBox Logger");
    g_option_context_set_ignore_unknown_options(context, TRUE);
    g_option_context_add_main_entries(context, g_tl_main_cmd_entries, "TL");
    if(!g_option_context_parse(context, &argc, &argv, &error))
    {
        g_warning("Option parsing failed: %s\n", error->message);
        g_clear_error(&error);
    }
    
    g_tl_main_loop = g_main_loop_new(NULL, FALSE);
    
    if(g_tl_main_cmd_daemon)
    {
        daemon(0, 0);
    }
    
    if(!tl_parser_init())
    {
        g_error("Cannot initialize parser!");
        return 2;
    }
    if(!tl_canbus_init())
    {
        g_error("Cannot initialize CAN-Bus!");
        return 3;
    }
    
    g_main_loop_run(g_tl_main_loop);
    g_main_loop_unref(g_tl_main_loop);
    
    tl_canbus_uninit();
    tl_parser_uninit();
    
    return 0;
}
