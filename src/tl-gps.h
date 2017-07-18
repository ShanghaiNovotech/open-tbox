#ifndef HAVE_TL_GPS_H
#define HAVE_TL_GPS_H

#include <glib.h>

gboolean tl_gps_init();
void tl_gps_uninit();
void tl_gps_state_get(guint8 *state, guint32 *latitude, guint32 *longitude);

#endif
