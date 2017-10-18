#ifndef DAGMULTIPLEXER_H_
#define DAGMULTIPLEXER_H_

#define DAG_POLL_MINDATA 8000
#define DAG_POLL_MAXWAIT 100000
#define DAG_POLL_FREQ 10000

#define DAG_MULTIPLEX_PORT_INCR 2
#define DAG_MULTIPLEX_BEACON_FREQ 1000      // milliseconds

#include "ndagmulticaster.h"

typedef struct streamparams {
    int dagfd;
    int streamnum;
    uint16_t monitorid;
    uint16_t exportport;
    uint8_t compressflag;
    char *multicastgroup;
} streamparams_t;

typedef struct dsthread {

    streamparams_t params;
    pthread_t tid;


} dagstreamthread_t;


typedef struct beaconthread {
    pthread_t tid;
    ndag_beacon_params_t params;
} beaconthread_t;

#endif

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
