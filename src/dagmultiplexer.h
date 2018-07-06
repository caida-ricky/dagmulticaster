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
    char *sourceaddr;
    char *dagdevname;
    uint64_t globalstart;
    uint16_t mtu;
} streamparams_t;

typedef struct dsthread {

    streamparams_t params;
    pthread_t tid;
    int threadstarted;

    struct iovec *iovs;
    uint16_t iov_alloc;
    uint8_t streamstarted;
    uint32_t idletime;

    void *extra;

} dagstreamthread_t;


typedef struct beaconthread {
    pthread_t tid;
    ndag_beacon_params_t *params;
} beaconthread_t;

extern volatile int halted;
extern volatile int paused;

inline int is_paused(void) {
    return paused;
}

inline int is_halted(void) {
    return halted;
}

inline void halt_program(void) {
    halted = 1;
}

inline void pause_program(void) {
    if (paused) {
        paused = 0;
    } else {
        paused = 1;
    }
}

void halt_signal(int signal);
void toggle_pause_signal(int signal);

int init_dag_stream(dagstreamthread_t *dst, ndag_encap_params_t *state);
void dag_stream_loop(dagstreamthread_t *dst, ndag_encap_params_t *state,
        uint16_t(*walk_records)(char **, char *, dagstreamthread_t *,
            uint16_t *, ndag_encap_params_t *));
void halt_dag_stream(dagstreamthread_t *dst, ndag_encap_params_t *state);
int create_multiplex_beaconer(beaconthread_t *bthread);
int run_dag_streams(int dagfd, uint16_t firstport,
        ndag_beacon_params_t *bparams,
        streamparams_t *sparams,
        void *initdata,
        void *(*initfunc)(void *),
        void *(*processfunc)(void *),
        void (*destroyfunc)(void *));


#endif

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
