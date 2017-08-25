bin_PROGRAMS=tbox-logger iccid-fetch

noinst_HEADERS=tl-main.h tl-canbus.h tl-net.h tl-logger.h tl-parser.h \
    tl-gps.h tl-serial.h

tbox_logger_CFLAGS=@GLIB2_CFLAGS@ @JSONC_CFLAGS@ @LIBGPS_CFLAGS@ \
    -DPREFIXDIR=\"$(prefix)\"
tbox_logger_DEPENDENCIES=@LIBOBJS@
tbox_logger_SOURCES=main.c tl-canbus.c tl-net.c tl-logger.c tl-parser.c \
    tl-gps.c tl-serial.c
tbox_logger_LDFLAGS=-export-dynamic -no-undefined \
    -export-symbols-regex "^[[^_]].*"
tbox_logger_LDADD=@LIBOBJS@ @GLIB2_LIBS@ @JSONC_LIBS@ @LIBGPS_LIBS@

iccid_fetch_CFLAGS=@GLIB2_CFLAGS@ -DPREFIXDIR=\"$(prefix)\"
iccid_fetch_DEPENDENCIES=@LIBOBJS@
iccid_fetch_SOURCES=iccid-fetch.c
iccid_fetch_LDFLAGS=-export-dynamic -no-undefined \
    -export-symbols-regex "^[[^_]].*"
iccid_fetch_LDADD=@LIBOBJS@ @GLIB2_LIBS@

if DEBUG_MODE
    tbox_logger_CFLAGS += -DDEBUG_MODE=1 -g
    iccid_fetch_CFLAGS += -DDEBUG_MODE=1 -g
endif

