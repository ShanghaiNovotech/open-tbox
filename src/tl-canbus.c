#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <linux/can.h>
#include <linux/can/raw.h>
#include "tl-canbus.h"
#include "tl-parser.h"
#include "tl-serial.h"
#include "tl-main.h"

#define TL_CANBUS_NO_DATA_TIMEOUT 180

typedef struct _TLCANBusSocketData
{
    gchar *device;
    int fd;
    GIOChannel *channel;
    guint watch_id;
}TLCANBusSocketData;

typedef struct _TLCANBusData
{
    gboolean initialized;
    GHashTable *socket_table;
    gint64 data_timestamp;
    guint check_timeout_id;
}TLCANBusData;

static TLCANBusData g_tl_canbus_data = {0};

static void tl_canbus_socket_data_free(TLCANBusSocketData *data)
{
    if(data==NULL)
    {
        return;
    }
    if(data->watch_id>0)
    {
        g_source_remove(data->watch_id);
    }
    if(data->channel!=NULL)
    {
        g_io_channel_unref(data->channel);
    }
    if(data->fd>0)
    {
        close(data->fd);
    }
    g_free(data);
}

static gboolean tl_canbus_socket_io_channel_watch(GIOChannel *source,
    GIOCondition condition, gpointer user_data)
{
    TLCANBusSocketData *socket_data = (TLCANBusSocketData *)user_data;
    struct canfd_frame frame;
    ssize_t rsize;
    
    if(user_data==NULL)
    {
        return FALSE;
    }
    
    if(condition & G_IO_IN)
    {
        rsize = read(socket_data->fd, &frame, CAN_MTU);
        if(rsize>0)
        {
            if(rsize>=(ssize_t)CAN_MTU)
            {
                tl_parser_parse_can_data(socket_data->device,
                    frame.can_id, frame.data, frame.len);
                    
                g_tl_canbus_data.data_timestamp = g_get_monotonic_time();
            }
            else
            {
                g_warning("TLCANBus received an incompleted packet "
                    "on device %s with size %ld", socket_data->device,
                    (long)rsize);
            }
        }
    }
    
    return TRUE;
}

static gboolean tl_canbus_open_socket(const gchar *device)
{
    int fd;
    struct ifreq ifr;
    struct sockaddr_can addr;
    GIOChannel *channel;
    TLCANBusSocketData *socket_data;
    
    fd = socket(PF_CAN, SOCK_RAW, CAN_RAW);
    if(fd < 0)
    {
        g_warning("TLCANBus Failed to open CAN socket %s: %s", device,
            strerror(errno));
        return FALSE;
    }
    strncpy(ifr.ifr_name, device, IFNAMSIZ - 1);
    ifr.ifr_name[IFNAMSIZ - 1] = '\0';
    ifr.ifr_ifindex = if_nametoindex(ifr.ifr_name);
    
    if(ifr.ifr_ifindex==0)
    {
        close(fd);
        g_warning("TLCANBus Failed to get interface index on "
            "device %s: %s", device, strerror(errno));
        return FALSE;
    }
    
    addr.can_family = AF_CAN;
    addr.can_ifindex = ifr.ifr_ifindex;

    if(bind(fd, (struct sockaddr *)&addr, sizeof(addr))<0)
    {
        close(fd);
        g_warning("TLCANBus Failed to bind CAN socket %s: %s", device,
            strerror(errno));
        return FALSE;
    }
    
    channel = g_io_channel_unix_new(fd);
    if(channel==NULL)
    {
        close(fd);
        g_warning("TLCANBus Failed to create IO channel!");
        return FALSE;
    }
    
    socket_data = g_new0(TLCANBusSocketData, 1);
    socket_data->fd = fd;
    socket_data->device = g_strdup(device);
    socket_data->channel = channel;
    
    socket_data->watch_id = g_io_add_watch(channel, G_IO_IN,
        tl_canbus_socket_io_channel_watch, socket_data);
    
    g_hash_table_replace(g_tl_canbus_data.socket_table, GINT_TO_POINTER(fd),
        socket_data);
    
    return TRUE;
}

static GSList *tl_canbus_scan_devices()
{
    GSList *device_list = NULL;
    struct ifaddrs *addrs = NULL, *addrs_foreach;
    
    if(getifaddrs(&addrs)!=0)
    {
        g_warning("TLCANBus Cannot get CANBus interface data: %s",
            strerror(errno));
        return NULL;
    }
    
    for(addrs_foreach=addrs;addrs_foreach!=NULL;
        addrs_foreach=addrs_foreach->ifa_next)
    {
        if(!g_str_has_prefix(addrs_foreach->ifa_name, "can"))
        {
            continue;
        }
        if(addrs_foreach->ifa_flags & IFF_UP)
        {
            device_list = g_slist_prepend(device_list,
                g_strdup(addrs_foreach->ifa_name));
        }
    }
    
    freeifaddrs(addrs);
    
    return device_list;
}

static gboolean tl_canbus_check_timeout_cb(gpointer user_data)
{
    TLCANBusData *canbus_data = (TLCANBusData *)user_data;
    gint64 now;
    
    if(user_data==NULL)
    {
        return FALSE;
    }
    
    now = g_get_monotonic_time();
    
    if(now - canbus_data->data_timestamp > (gint64)TL_CANBUS_NO_DATA_TIMEOUT)
    {
        g_message("TLCANBus no CANBus data received for 3min, "
            "start to shutdown.");
        tl_main_request_shutdown();
    }
    
    return TRUE;
}

gboolean tl_canbus_init()
{
    GSList *device_list, *list_foreach;
    
    if(g_tl_canbus_data.initialized)
    {
        g_warning("TLCANBus already initialized!");
        return TRUE;
    }
    
    g_tl_canbus_data.socket_table = g_hash_table_new_full(g_direct_hash,
        g_direct_equal, NULL, (GDestroyNotify)tl_canbus_socket_data_free);
    
    device_list = tl_canbus_scan_devices();
    if(device_list==NULL)
    {
        g_warning("TLCANBus no CANBus device detected!");
    }
    
    for(list_foreach=device_list;list_foreach!=NULL;
        list_foreach=g_slist_next(list_foreach))
    {
        tl_canbus_open_socket((const gchar *)list_foreach->data);
    }
    
    g_slist_free_full(device_list, g_free);
    
    g_tl_canbus_data.data_timestamp = g_get_monotonic_time();
    g_tl_canbus_data.initialized = TRUE;
    
    g_tl_canbus_data.check_timeout_id = g_timeout_add_seconds(5,
        tl_canbus_check_timeout_cb, &g_tl_canbus_data);
    
    return TRUE;
}

void tl_canbus_uninit()
{
    if(!g_tl_canbus_data.initialized)
    {
        return;
    }
    
    if(g_tl_canbus_data.check_timeout_id>0)
    {
        g_source_remove(g_tl_canbus_data.check_timeout_id);
        g_tl_canbus_data.check_timeout_id = 0;
    }
    
    if(g_tl_canbus_data.socket_table!=NULL)
    {
        g_hash_table_unref(g_tl_canbus_data.socket_table);
        g_tl_canbus_data.socket_table = NULL;
    }
    
    g_tl_canbus_data.initialized = FALSE;
}
