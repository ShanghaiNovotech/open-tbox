# -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_CONFIG_MACRO_DIR(m4)
m4_define([tbox_logger_major_version], [1])
m4_define([tbox_logger_minor_version], [0])
m4_define([tbox_logger_micro_version], [0])
m4_define([tbox_logger_version], [tbox_logger_major_version.tbox_logger_minor_version.tbox_logger_micro_version])

AC_INIT([TBoxLogger], [tbox_logger_version])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([Makefile.am])
AC_CONFIG_HEADERS([config.h])

AM_SILENT_RULES([yes])

TBOX_LOGGER_MAJOR_VERSION=tbox_logger_major_version
TBOX_LOGGER_MINOR_VERSION=tbox_logger_minor_version
TBOX_LOGGER_MICRO_VERSIOn=tbox_logger_micro_version
TBOX_LOGGER_VERSION=tbox_logger_version
TBOX_LOGGER_LIB_VERSION=tbox_logger_lib_version

AC_DEFINE(TBOX_LOGGER_MAJOR_VERSION, [tbox_logger_major_version],
    [Define to the Luna Pudge Server major version])
AC_DEFINE(TBOX_LOGGER_MINOR_VERSION, [tbox_logger_minor_version],
    [Define to the Luna Pudge Server minor version])
AC_DEFINE(TBOX_LOGGER_MICRO_VERSION, [tbox_logger_micro_version],
    [Define to the Luna Pudge Server micro version])

# Checks for programs.
AC_PROG_CC
AC_PROG_CXX
AC_PROG_CC_C_O
AC_PROG_INSTALL
AC_PROG_LN_S
AM_PROG_CC_C_O
AC_PROG_LIBTOOL
AM_PROG_LIBTOOL
CFLAGS="-Wall"

# Checks for libraries.
AC_CHECK_LIB([pthread], [main],, AC_MSG_ERROR($missing_library))
PKG_CHECK_MODULES([GLIB2], [glib-2.0 >= 2.32, gthread-2.0 >= 2.32, \
    gmodule-2.0 >= 2.32, gio-2.0 >= 2.32])
PKG_CHECK_MODULES([LIBSOUP], [libsoup-2.4 >= 2.0])

AC_ARG_ENABLE(debug, AS_HELP_STRING([--enable-debug], \
    [enable debug mode by default]), enable_debug=yes, \
    enable_debug=no)
AM_CONDITIONAL(DEBUG_MODE, test "x$enable_debug" = "xyes")

# Checks for headers

AC_SUBST(TBOX_LOGGER_VERSION)

AC_CONFIG_FILES(
    Makefile src/Makefile
)
AC_OUTPUT

echo "
$PACKAGE $VERSION

configure complete, now type 'make'
"


