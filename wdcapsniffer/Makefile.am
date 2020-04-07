if HAVE_WDCAP
bin_PROGRAMS=ndag-wdcapsniffer

ndag_wdcapsniffer_SOURCES=\
	wdcapsniffer.c wdcapsniffer.h \
	dagmultiplexer.c dagmultiplexer.h \
	byteswap.c byteswap.h

ndag_wdcapsniffer_LDADD = @ADD_LIBS@ @EXTRA_LIBS@ -lndagserver
ndag_wdcapsniffer_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
ndag_wdcapsniffer_CFLAGS=-I"$(top_srcdir)/libndag-server"
endif

