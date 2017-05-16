#include <unistd.h>
#include <glib.h>

#include "tl-logger.h"
#include "tl-canbus.h"
#include "tl-net.h"
#include "tl-parser.h"

static GMainLoop *g_tl_main_loop = NULL;
static gboolean g_tl_main_cmd_daemon = FALSE;
static gchar *g_tl_main_cmd_log_storage_path = NULL;
static gchar *g_tl_main_cmd_conf_path = NULL;
static gchar *g_tl_main_cmd_vin_code = NULL;
static gchar *g_tl_main_cmd_iccid_code = NULL;
static gchar *g_tl_main_cmd_fallback_vehicle_server_host = NULL;
static gint g_tl_main_cmd_fallback_vehicle_server_port = 0;

static GOptionEntry g_tl_main_cmd_entries[] =
{
    { "daemon", 'D', 0, G_OPTION_ARG_NONE, &g_tl_main_cmd_daemon,
        "Start as daemon", NULL },
    { "vin", 'N', 0, G_OPTION_ARG_STRING, &g_tl_main_cmd_vin_code,
        "Set VIN code", NULL },
    { "iccid", 'I', 0, G_OPTION_ARG_STRING, &g_tl_main_cmd_iccid_code,
        "Set ICCID code", NULL },
    { "log-storage-path", 'L', 0, G_OPTION_ARG_STRING,
        &g_tl_main_cmd_log_storage_path, "Set log storage path", NULL },
    { "config-path", 'C', 0, G_OPTION_ARG_STRING,
        &g_tl_main_cmd_conf_path, "Set config path", NULL },
    { "fallback-vehicle-server-host", 0, 0, G_OPTION_ARG_STRING,
        &g_tl_main_cmd_fallback_vehicle_server_host,
        "Set fallback vehicle server host", NULL },
    { "fallback-vehicle-server-port", 0, 0, G_OPTION_ARG_INT,
        &g_tl_main_cmd_fallback_vehicle_server_port,
        "Set fallback vehicle server port", NULL },
    { NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL }
};

int main(int argc, char *argv[])
{
    GError *error = NULL;
    GOptionContext *context;
    gchar *conf_file_path;
    gchar *parse_file_path;
    
    context = g_option_context_new("- TBox Logger");
    g_option_context_set_ignore_unknown_options(context, TRUE);
    g_option_context_add_main_entries(context, g_tl_main_cmd_entries, "TL");
    if(!g_option_context_parse(context, &argc, &argv, &error))
    {
        g_warning("Option parsing failed: %s\n", error->message);
        g_clear_error(&error);
    }
    
    if(g_tl_main_cmd_vin_code==NULL)
    {
        g_error("VIN code should be specified!");
        return 1;
    }
    
    if(g_tl_main_cmd_iccid_code==NULL)
    {
        g_error("ICCID code should be specified!");
        return 1;
    }
    
    g_tl_main_loop = g_main_loop_new(NULL, FALSE);
    
    if(g_tl_main_cmd_daemon)
    {
        daemon(0, 0);
    }
    
    if(!tl_logger_init(g_tl_main_cmd_log_storage_path))
    {
        g_error("Cannot initialize logger!");
        return 2;
    }
    
    if(!tl_parser_init())
    {
        g_error("Cannot initialize parser!");
        return 3;
    }
    
    if(!tl_canbus_init())
    {
        g_error("Cannot initialize CAN-Bus!");
        return 4;
    }
    
    if(!tl_net_init(g_tl_main_cmd_vin_code, g_tl_main_cmd_iccid_code,
        g_tl_main_cmd_conf_path, g_tl_main_cmd_fallback_vehicle_server_host,
        g_tl_main_cmd_fallback_vehicle_server_port))
    {
        g_warning("Cannot initialize net module! Data may not be uploaded.");
    }
    
    parse_file_path = g_build_filename(g_tl_main_cmd_conf_path,
        "tboxparse.xml", NULL);
    tl_parser_load_parse_file(parse_file_path);
    g_free(parse_file_path);
    
    g_main_loop_run(g_tl_main_loop);
    g_main_loop_unref(g_tl_main_loop);
    
    tl_net_uninit();
    tl_canbus_uninit();
    tl_parser_uninit();
    tl_logger_uninit();
    
    return 0;
}
