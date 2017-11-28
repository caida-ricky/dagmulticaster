bin_PROGRAMS=telescope

telescope_SOURCES=telescope.c dagmultiplexer.h
telescope_LDADD = @ADD_LIBS@ -lndagserver
telescope_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
telescope_CFLAGS=-I"$(top_srcdir)/libndag-server"
