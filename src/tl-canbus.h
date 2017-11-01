#ifndef HAVE_TL_CANBUS_H
#define HAVE_TL_CANBUS_H

#include <glib.h>

gboolean tl_canbus_init(gboolean use_vcan);
void tl_canbus_uninit();

#endif
