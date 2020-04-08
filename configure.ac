# Super primitive configure script

AC_INIT(dagmulticaster, 0.0.1, salcock@wand.net.nz)

AM_INIT_AUTOMAKE([foreign subdir-objects])
AC_CONFIG_SRCDIR(src/telescope.c)
AM_CONFIG_HEADER(config.h)
AC_CONFIG_MACRO_DIR([m4])

m4_ifdef([AM_SILENT_RULES],[AM_SILENT_RULES([yes])])

AC_PREFIX_DEFAULT(/usr/local/)

AC_PROG_CC
AC_PROG_INSTALL

AC_PROG_LIBTOOL

gcc_PACKED

AC_CHECK_LIB([ndagserver], [ndag_close_multicaster_socket],,
  [AC_MSG_ERROR(Required library libndagserver not found)]
)
AC_CHECK_LIB([trace], [trace_get_payload_length],,
  [AC_MSG_ERROR(Required library libtrace not found)]
)
AC_CHECK_LIB([wandio], [wandio_create],,
  [AC_MSG_ERROR(Required library libwandio not found)]
)
AC_CHECK_LIB([pthread], [pthread_create],,
  [AC_MSG_ERROR(Required library libpthread not found)]
)
AC_CHECK_LIB([dag], [dag_attach_stream],,
  [AC_MSG_ERROR(Required library libdag not found)]
)
AC_CHECK_LIB([dagconf], [dag_config_init],,
  [AC_MSG_ERROR(Required library libdagconf not found)]
)
AC_CHECK_LIB([numa], [numa_node_of_cpu],,
  [AC_MSG_ERROR(Required library libnuma not found)]
)
AC_CHECK_LIB([yaml], [yaml_parser_initialize],,
  [AC_MSG_ERROR(Required library libyaml not found)]
)

AC_CHECK_FUNCS([recvmmsg sendmmsg],,
  [AC_MSG_ERROR(Required functions (recvmmsg, sendmmsg) not found.)]
)

AC_CONFIG_FILES([Makefile src/Makefile])

AC_OUTPUT
