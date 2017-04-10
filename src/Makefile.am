bin_PROGRAMS=tbox-logger

noinst_HEADERS=

tbox_logger_CFLAGS=@GLIB2_CFLAGS@  -DPREFIXDIR=\"$(prefix)\"
tbox_logger_DEPENDENCIES=@LIBOBJS@
tbox_logger_SOURCES=main.c
tbox_logger_LDFLAGS=-export-dynamic -no-undefined \
    -export-symbols-regex "^[[^_]].*"
tbox_logger_LDADD=@LIBOBJS@ @GLIB2_LIBS@

if DEBUG_MODE
    tbox_logger_CFLAGS += -DDEBUG_MODE=1 -g
endif

