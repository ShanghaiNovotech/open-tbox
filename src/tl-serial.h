#ifndef HAVE_TL_SERIAL_H
#define HAVE_TL_SERIAL_H

#include <glib.h>

gboolean tl_serial_init(const gchar *port);
void tl_serial_uninit();

void tl_serial_request_shutdown();
void tl_serial_power_on_time_set(gint64 time);
void tl_serial_power_on_daily_set(gint hour, gint minute);
void tl_serial_gravity_threshold_set(guint8 threshold);

#endif
