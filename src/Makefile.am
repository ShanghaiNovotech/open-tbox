bin_PROGRAMS=tbox-logger

noinst_HEADERS=tl-canbus.h tl-net.h tl-logger.h tl-parser.h

tbox_logger_CFLAGS=@GLIB2_CFLAGS@ @LIBSOUP_CFLAGS@ -DPREFIXDIR=\"$(prefix)\"
tbox_logger_DEPENDENCIES=@LIBOBJS@
tbox_logger_SOURCES=main.c tl-canbus.c tl-net.c tl-logger.c tl-parser.c
tbox_logger_LDFLAGS=-export-dynamic -no-undefined \
    -export-symbols-regex "^[[^_]].*"
tbox_logger_LDADD=@LIBOBJS@ @GLIB2_LIBS@ @LIBSOUP_LIBS@

if DEBUG_MODE
    tbox_logger_CFLAGS += -DDEBUG_MODE=1 -g
endif

