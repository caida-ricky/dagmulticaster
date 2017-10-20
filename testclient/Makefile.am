bin_PROGRAMS=testclient

testclient_SOURCES=testclient.c message_queue.c testclient.h message_queue.h
testclient_LDADD = @ADD_LIBS@ -lndagserver
testclient_LDFLAGS=-L"$(top_srcdir)/libndag-server/.libs"
testclient_CFLAGS=-I"$(top_srcdir)/libndag-server"
