#ifndef TESTCLIENT_H_
#define TESTCLIENT_H_

#include "message_queue.h"
#include <pthread.h>

typedef struct controlparams {
  char *groupaddr;
  char *portstr;
  char *localiface;
  uint32_t maxthreads;
  pthread_t tid;
} controlparams_t;

typedef struct streamsource {
  uint16_t monitor;
  char *groupaddr;
  char *localiface;
  uint16_t port;
} streamsource_t;

typedef struct streamsock {
  char *groupaddr;
  int sock;
  struct addrinfo *srcaddr;
  uint16_t port;
  uint32_t expectedseq;
  uint16_t monitorid;
} streamsock_t;

typedef struct recvthread {
  streamsock_t *sources;
  uint16_t sourcecount;
  pthread_t tid;
  libtrace_message_queue_t mqueue;
  int threadindex;

  uint64_t records_received;
} recvthread_t;

enum {
  NDAG_CLIENT_HALT = 0x01,
  NDAG_CLIENT_RESTARTED = 0x02,
  NDAG_CLIENT_NEWGROUP = 0x03
};

typedef struct ndagreadermessage {
  uint8_t type;
  streamsource_t contents;
} mymessage_t;
#endif

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
