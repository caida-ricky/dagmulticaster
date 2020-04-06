bin_PROGRAMS=ndag-telescope

ndag_telescope_SOURCES=telescope.c telescope.h \
			dagmultiplexer.h dagmultiplexer.c \
			darkfilter.c darkfilter.h \
			configparser.c \
			byteswap.c byteswap.h

ndag_telescope_LDADD = @ADD_LIBS@ -lndagserver
ndag_telescope_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
ndag_telescope_CFLAGS=-I"$(top_srcdir)/libndag-server"

