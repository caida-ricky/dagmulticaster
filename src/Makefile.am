bin_PROGRAMS=telescope

if HAVE_WDCAP
bin_PROGRAMS+=wdcapsniffer

wdcapsniffer_SOURCES=wdcapsniffer.c dagmultiplexer.h wdcapsniffer.h
wdcapsniffer_LDADD = @ADD_LIBS@ @EXTRA_LIBS@ -lndagserver
wdcapsniffer_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
wdcapsniffer_CFLAGS=-I"$(top_srcdir)/libndag-server"
endif

telescope_SOURCES=telescope.c dagmultiplexer.h
telescope_LDADD = @ADD_LIBS@ -lndagserver
telescope_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
telescope_CFLAGS=-I"$(top_srcdir)/libndag-server"

