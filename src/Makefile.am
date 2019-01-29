bin_PROGRAMS=ndag-telescope

if HAVE_WDCAP
bin_PROGRAMS+=ndag-wdcapsniffer

ndag_wdcapsniffer_SOURCES=wdcapsniffer.c dagmultiplexer.h wdcapsniffer.h common.c
ndag_wdcapsniffer_LDADD = @ADD_LIBS@ @EXTRA_LIBS@ -lndagserver
ndag_wdcapsniffer_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
ndag_wdcapsniffer_CFLAGS=-I"$(top_srcdir)/libndag-server"
endif

ndag_telescope_SOURCES=telescope.c dagmultiplexer.h common.c darkfilter.c \
        darkfilter.h telescope.h configparser.c
ndag_telescope_LDADD = @ADD_LIBS@ -lndagserver
ndag_telescope_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
ndag_telescope_CFLAGS=-I"$(top_srcdir)/libndag-server"

