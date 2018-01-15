# Super primitive configure script

AC_INIT(dagmulticaster, 0.0.1, salcock@wand.net.nz)

AM_INIT_AUTOMAKE([subdir-objects])
AC_CONFIG_SRCDIR(libndag-server/ndagmulticaster.c)
AM_CONFIG_HEADER(config.h)
AC_CONFIG_MACRO_DIR([m4])

m4_ifdef([AM_SILENT_RULES],[AM_SILENT_RULES([yes])])

AC_PREFIX_DEFAULT(/usr/local/)

AC_PROG_CC
AC_PROG_INSTALL

AC_PROG_LIBTOOL

gcc_PACKED

AC_CHECK_LIB([trace], [trace_get_payload_length],,trace_found=0)
AC_CHECK_LIB([wandio], [wandio_create],,wandio_found=0)
AC_CHECK_LIB([dag], [dag_attach_stream],,dag_found=0)
AC_CHECK_LIB([dagconf], [dag_config_init],,dagconf_found=0)
AC_CHECK_LIB([pthread], [pthread_create],,pthread_found=0)
AC_CHECK_LIB([numa], [numa_node_of_cpu],,numa_found=0)
AC_CHECK_LIB([z], [deflate],,zlib_found=0)

EXTRA_LIBS=""


AC_SEARCH_LIBS(processWdcapPacket, wdcap, have_wdcap=yes, have_wdcap=no)

AS_IF([test "x$have_wdcap" = "xyes"], [
        EXTRA_LIBS="$EXTRA_LIBS -lwdcap"
        AC_DEFINE(HAVE_WDCAP, 1, "Compiled with libwdcap support")
        with_wdcap=yes], [
        AC_DEFINE(HAVE_WDCAP, 0, "Compiled with libwdcap support")
        with_wdcap=no]
)

AC_CHECK_FUNCS(sendmmsg)

AC_CONFIG_FILES([Makefile libndag-server/Makefile src/Makefile
                testclient/Makefile])

if test "$trace_found" = 0; then
        AC_MSG_ERROR(Required library libtrace not found; use LDFLAGS to specify library location)
fi

if test "$wandio_found" = 0; then
        AC_MSG_ERROR(Required library libwandio not found; use LDFLAGS to specify library location)
fi

if test "$dag_found" = 0; then
        AC_MSG_ERROR(Required library libdag not found; use LDFLAGS to specify library location)
fi

if test "$dagconf_found" = 0; then
        AC_MSG_ERROR(Required library libdagconf not found; use LDFLAGS to specify library location)
fi

if test "$pthread_found" = 0; then
        AC_MSG_ERROR(Required library libpthread not found; use LDFLAGS to specify library location)
fi

if test "$numa_found" = 0; then
        AC_MSG_ERROR(Required library libnuma not found; use LDFLAGS to specify library location)
fi

AM_CONDITIONAL([HAVE_WDCAP], [test "x$with_wdcap" != "xno"])

AC_SUBST([ADD_LIBS])
AC_SUBST([EXTRA_LIBS])
AC_SUBST([ADD_LDFLAGS])
AC_SUBST([ADD_INCLS])
AC_SUBST([LTLIBOBJS])

AC_OUTPUT

# Function for reporting whether an option was set or not
reportopt() {
        if test x"$2" = xtrue -o x"$2" = xyes; then
                AC_MSG_NOTICE([$1: Yes])
        else 
                AC_MSG_NOTICE([$1: No])
        fi
}


reportopt "Building wdcapsniffer (requires libwdcap)" $with_wdcap
