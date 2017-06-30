#ifndef HAVE_TL_NET_H
#define HAVE_TL_NET_H

#include <glib.h>

gboolean tl_net_init(const gchar *vin, const gchar *iccid,
    const gchar *conf_path, const gchar *log_path,
    const gchar *fallback_vehicle_server_host,
    guint16 fallback_vehicle_server_port);
void tl_net_uninit();

#endif
