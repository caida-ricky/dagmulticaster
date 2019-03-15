#ifndef DAGMULTIPLEXER_H_
#define DAGMULTIPLEXER_H_

#define DAG_POLL_MINDATA 8000
#define DAG_POLL_MAXWAIT 100000
#define DAG_POLL_FREQ 10000

#define DAG_MULTIPLEX_PORT_INCR 2
#define DAG_MULTIPLEX_BEACON_FREQ 1000 // milliseconds

#define ENCAP_OVERHEAD (sizeof(ndag_common_t) + sizeof(ndag_encap_t))

#include "ndagmulticaster.h"

/* Our color type, currently 8 bit. Used as a bit-field. */
typedef uint8_t color_t;

/* One slot per bit in the color. Note that no bit (color 0) will drop packets
 * and does not require a slot. */
#define DAG_COLOR_SLOTS (sizeof(color_t) * 8)

/* Parameters to configure a (multicast) sink. */
typedef struct streamsink {
  color_t color;
  uint16_t monitorid;
  uint16_t exportport;
  char *multicastgroup;
  char *sourceaddr;
  char *name; // non-owning reference, owned by config
  uint16_t mtu;
} streamsink_t;

/* Configuration parameters for the dag stream. */
typedef struct streamparams {
  int dagfd;
  int streamnum;
  uint8_t compressflag;
  char *dagdevname;
  uint64_t globalstart;
  int statinterval;
  char *statdir;
  int sinkcnt;
  streamsink_t *sinks;
} streamparams_t;

/* Stats for a multicast sink. */
typedef struct streamsinkstats {
  char *name;            // non-owning reference, owned by config
  uint64_t tx_datagrams; // number of multicast datagrams tx'd
  uint64_t tx_records;   // number of ERF records (packets) tx'd
  uint64_t tx_bytes;     // number of bytes tx'd
  uint64_t tx_wbytes;    // number of "wire" bytes tx'd (excl. ERF headers)
} streamsinkstats_t;

/* Performance stats. */
typedef struct streamstats {
  /* DAG stream walking stats. */
  uint64_t walked_buffers; // number of stream buffers walked
  uint64_t walked_records; // number of ERF records (packets) walked
  uint64_t walked_bytes;   // number of bytes walked
  uint64_t walked_wbytes;  // number of "wire" bytes walked (excl. ERF headers)

  /* Filtered out stats. */
  streamsinkstats_t filtered_out;

  /* nDAG transmit stats. */
  streamsinkstats_t sinks[DAG_COLOR_SLOTS];

  /* Error stats. */
  uint64_t dropped_records;   // number of records dropped (according to DAG)
  uint64_t truncated_records; // number of records truncated
} streamstats_t;

/* Data to manage one iovec. */
typedef struct iov_data {
  struct iovec *vec;
  uint16_t len;
  uint16_t maxsize;
} iov_data_t;

/* State to configure and run a dagstream thread. */
typedef struct dsthread {
  streamparams_t params;
  streamstats_t stats;
  pthread_t tid;
  int threadstarted;
  pthread_mutex_t *dagmutex;

  /* One entry for each color. */
  iov_data_t iovs[DAG_COLOR_SLOTS];
  /* Number of entries in use. */
  uint16_t inuse;

  uint8_t streamstarted;
  uint32_t idletime;

  /* Application specific storage. */
  void *extra;
} dagstreamthread_t;

typedef struct beaconthread {
  pthread_t tid;
  ndag_beacon_params_t *params;
} beaconthread_t;

extern volatile int halted;
extern volatile int paused;

inline int is_paused(void)
{
  return paused;
}

inline int is_halted(void)
{
  return halted;
}

inline void halt_program(void)
{
  halted = 1;
}

inline void pause_program(void)
{
  if (paused) {
    paused = 0;
  } else {
    paused = 1;
  }
}

void halt_signal(int signal);
void toggle_pause_signal(int signal);

int init_dag_stream(dagstreamthread_t *dst);
int init_dag_sink(ndag_encap_params_t *state, streamsink_t *params,
                  int streamnum, uint64_t globalstart);
void dag_stream_loop(dagstreamthread_t *dst, ndag_encap_params_t *state,
                     void (*walk_records)(char **, char *, dagstreamthread_t *,
                                          uint16_t *, uint16_t *,
                                          ndag_encap_params_t *));
void halt_dag_stream(dagstreamthread_t *dst);
void halt_dag_sink(ndag_encap_params_t *state);
int create_multiplex_beaconer(beaconthread_t *bthread);
int run_dag_streams(int dagfd, uint16_t firstport, int beaconcnt,
                    ndag_beacon_params_t *bparams, streamparams_t *sparams,
                    void *initdata, void *(*initfunc)(void *),
                    void *(*processfunc)(void *), void (*destroyfunc)(void *));

#endif

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
