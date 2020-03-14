# Super primitive configure script

AC_INIT(libndagserver, 0.0.1, salcock@wand.net.nz)

AM_INIT_AUTOMAKE([subdir-objects])
AC_CONFIG_SRCDIR(ndagmulticaster.c)
AM_CONFIG_HEADER(config.h)
AC_CONFIG_MACRO_DIR([../m4])

m4_ifdef([AM_SILENT_RULES],[AM_SILENT_RULES([yes])])

AC_PREFIX_DEFAULT(/usr/local/)

AC_PROG_CC
AC_PROG_INSTALL

AC_PROG_LIBTOOL

gcc_PACKED

AC_CHECK_LIB([trace], [trace_get_payload_length],,trace_found=0)
AC_CHECK_LIB([wandio], [wandio_create],,wandio_found=0)
AC_CHECK_LIB([pthread], [pthread_create],,pthread_found=0)
AC_CHECK_LIB([dag], [dag_attach_stream],dag_found=1,dag_found=0)
#AC_CHECK_LIB([numa], [numa_node_of_cpu],,numa_found=0)

EXTRA_LIBS=""


AC_CHECK_FUNCS(sendmmsg)

AC_CONFIG_FILES([Makefile])

if test "$dag_found" = 1; then
        EXTRA_LIBS="$EXTRA_LIBS -ldag"
        AC_DEFINE(HAVE_DAGAPI, 1, "Compiled with libdag support")
fi

if test "$trace_found" = 0; then
        AC_MSG_ERROR(Required library libtrace not found; use LDFLAGS to specify library location)
fi

if test "$wandio_found" = 0; then
        AC_MSG_ERROR(Required library libwandio not found; use LDFLAGS to specify library location)
fi

if test "$pthread_found" = 0; then
        AC_MSG_ERROR(Required library libpthread not found; use LDFLAGS to specify library location)
fi

if test "$numa_found" = 0; then
        AC_MSG_ERROR(Required library libnuma not found; use LDFLAGS to specify library location)
fi



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

