# Super primitive configure script

AC_INIT(dagmulticaster, 0.0.1, salcock@wand.net.nz)

AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR(libndag-server/ndagmulticaster.c)
AM_CONFIG_HEADER(config.h)

m4_ifdef([AM_SILENT_RULES],[AM_SILENT_RULES([yes])])

AC_PREFIX_DEFAULT(/usr/local/)

AC_PROG_CC
AC_PROG_INSTALL

AC_PROG_LIBTOOL

AC_CHECK_LIB([trace], [trace_get_payload_length],,trace_found=0)
AC_CHECK_LIB([dag], [dag_attach_stream],,dag_found=0)
AC_CHECK_LIB([pthread], [pthread_create],,pthread_found=0)

AC_CONFIG_FILES([Makefile libndag-server/Makefile src/Makefile])

if test "$trace_found" = 0; then
        AC_MSG_ERROR(Required library libtrace not found; use LDFLAGS to specify library location)
fi

if test "$dag_found" = 0; then
        AC_MSG_ERROR(Required library libdag not found; use LDFLAGS to specify library location)
fi

if test "$pthread_found" = 0; then
        AC_MSG_ERROR(Required library libpthread not found; use LDFLAGS to specify library location)
fi

AC_SUBST([ADD_LIBS])
AC_SUBST([ADD_LDFLAGS])
AC_SUBST([ADD_INCLS])
AC_SUBST([LTLIBOBJS])

AC_OUTPUT

