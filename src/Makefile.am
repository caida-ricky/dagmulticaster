bin_PROGRAMS=prototype

prototype_SOURCES=prototype.c dagmultiplexer.h
prototype_LDADD = @ADD_LIBS@ -lndagserver
prototype_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
prototype_CFLAGS=-I"$(top_srcdir)/libndag-server"
