#include <unistd.h>
#include <glib.h>

#include "tl-main.h"
#include "tl-logger.h"
#include "tl-canbus.h"
#include "tl-net.h"
#include "tl-parser.h"
#include "tl-gps.h"
#include "tl-serial.h"

static GMainLoop *g_tl_main_loop = NULL;
static gboolean g_tl_main_cmd_daemon = FALSE;
static gchar *g_tl_main_cmd_log_storage_path = NULL;
static gchar *g_tl_main_cmd_conf_path = NULL;
static gchar *g_tl_main_cmd_vin_code = NULL;
static gchar *g_tl_main_cmd_iccid_code = NULL;
static gchar *g_tl_main_cmd_fallback_vehicle_server_host = NULL;
static gint g_tl_main_cmd_fallback_vehicle_server_port = 0;
static gchar *g_tl_main_cmd_serial_port = NULL;
static gboolean g_tl_main_cmd_shutdown = FALSE;

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
    { "stm-serial-port", 0, 0, G_OPTION_ARG_STRING, &g_tl_main_cmd_serial_port,
        "Set STM8 connection serial port", NULL },
    { NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL }
};


static gboolean tl_main_shutdown_request_timeout(gpointer user_data)
{
    g_warning("Serial port waiting timeout, start to shutdown!");
    
    tl_main_shutdown();
    
    return FALSE;
}

int main(int argc, char *argv[])
{
    GError *error = NULL;
    GOptionContext *context;
    const gchar *conf_file_path;
    const gchar *log_file_path;
    const gchar *serial_port;
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
    
    
    if(g_tl_main_cmd_conf_path!=NULL)
    {
        conf_file_path = g_tl_main_cmd_conf_path;
    }
    else
    {
        conf_file_path = "/var/lib/tbox/conf";
    }
    
    if(g_tl_main_cmd_log_storage_path!=NULL)
    {
        log_file_path = g_tl_main_cmd_log_storage_path;
    }
    else
    {
        log_file_path = "/var/lib/tbox/log";
    }
    
    if(g_tl_main_cmd_serial_port!=NULL)
    {
        serial_port = g_tl_main_cmd_serial_port;
    }
    else
    {
        serial_port = "/dev/ttymxc3";
    }
    
    if(!tl_logger_init(log_file_path))
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
    
    if(!tl_gps_init())
    {
        g_warning("Cannot initialize GPS!");
    }
    
    if(!tl_net_init(g_tl_main_cmd_vin_code, g_tl_main_cmd_iccid_code,
        g_tl_main_cmd_conf_path, log_file_path,
        g_tl_main_cmd_fallback_vehicle_server_host,
        g_tl_main_cmd_fallback_vehicle_server_port))
    {
        g_warning("Cannot initialize net module! Data may not be uploaded.");
    }
    
    if(!tl_serial_init(serial_port))
    {
        g_warning("Cannot initialize serial port for STM8!");
    }
    
    parse_file_path = g_build_filename(conf_file_path, "tboxparse.xml", NULL);
    tl_parser_load_parse_file(parse_file_path);
    g_free(parse_file_path);
    
    g_main_loop_run(g_tl_main_loop);
    g_main_loop_unref(g_tl_main_loop);
    
    tl_net_uninit();
    tl_gps_uninit();
    tl_canbus_uninit();
    tl_parser_uninit();
    tl_logger_uninit();
    
    tl_serial_uninit();
    
    if(g_tl_main_cmd_shutdown)
    {
        g_spawn_command_line_async("/sbin/poweroff", NULL);
    }
    
    return 0;
}

void tl_main_request_shutdown()
{
    g_message("Prepare to shutdown.");
    
    tl_net_uninit();
    tl_gps_uninit();
    tl_canbus_uninit();
    tl_parser_uninit();
    tl_logger_uninit();
    
    sync();
    
    tl_serial_request_shutdown();
    
    g_timeout_add_seconds(180, tl_main_shutdown_request_timeout, NULL);
}

void tl_main_shutdown()
{
    g_main_loop_quit(g_tl_main_loop);
    g_tl_main_cmd_shutdown = TRUE;
}
