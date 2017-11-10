lib_LTLIBRARIES=libndagserver.la
include_HEADERS=ndagmulticaster.h

libndagserver_la_SOURCES=ndagmulticaster.c ndagmulticaster.h byteswap.h \
                byteswap.c

libndagserver_la_LIBADD = @ADD_LIBS@
libndagserver_la_LDFLAGS = @ADD_LDFLAGS@ -version-info 0:1:0
libndagserver_la_CPPFLAGS = -Werror
