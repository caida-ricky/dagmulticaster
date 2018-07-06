bin_PROGRAMS=ndag-testclient

ndag_testclient_SOURCES=testclient.c message_queue.c testclient.h message_queue.h
ndag_testclient_LDADD = @ADD_LIBS@ -lndagserver
ndag_testclient_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
ndag_testclient_CFLAGS=-I"$(top_srcdir)/libndag-server"
