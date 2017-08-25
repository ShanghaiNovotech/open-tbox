#ifndef HAVE_TL_SERIAL_H
#define HAVE_TL_SERIAL_H

#include <glib.h>

gboolean tl_serial_init(const gchar *port);
void tl_serial_uninit();

void tl_serial_request_shutdown();

#endif
