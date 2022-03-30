#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>

#include <pthread.h>
#include <dagapi.h>
#include <dag_config_api.h>
#include <libtrace.h>
#include <numa.h>

#include <wandio.h>

#include "byteswap.h"
#include "dagmultiplexer.h"
#include "ndagmulticaster.h"

volatile int halted = 0;
volatile int paused = 0;

extern inline int is_paused(void);
extern inline int is_halted(void);
extern inline void halt_program(void);
extern inline void pause_program(void);

void halt_signal(int signal) {
    (void) signal;
    halt_program();
}

void toggle_pause_signal(int signal) {
    (void) signal;
    pause_program();
}

static int get_nb_cores() {
        int numCPU;
#ifdef _SC_NPROCESSORS_ONLN
        /* Most systems do this now */
        numCPU = sysconf(_SC_NPROCESSORS_ONLN);

#else
        int mib[] = {CTL_HW, HW_AVAILCPU};
        size_t len = sizeof(numCPU);

        /* get the number of CPUs from the system */
        sysctl(mib, 2, &numCPU, &len, NULL, 0);
#endif
        return numCPU <= 0 ? 1 : numCPU;
}

static int get_next_thread_cpu(char *dagdevname, uint8_t *cpumap,
        uint16_t streamnum) {

    int i, cpuid = -1;
    dag_card_ref_t cardref = NULL;
    dag_component_t root = NULL;
    dag_component_t streamconf = NULL;
    attr_uuid_t any;
    void *ptr;
    mem_node_t *meminfo;

    cardref = dag_config_init(dagdevname);
    root = dag_config_get_root_component(cardref);
    streamconf = dag_component_get_subcomponent(root, kComponentStream,
            streamnum);

    any = dag_component_get_attribute_uuid(streamconf,
            kStructAttributeMemNode);

    if (dag_config_get_struct_attribute(cardref, any, &ptr) != 0) {
        cpuid = -1;
        goto endcpucheck;
    }

    meminfo = (mem_node_t *)ptr;

    for (i = 1; i < get_nb_cores(); i++) {
        if (numa_node_of_cpu(i) == meminfo->node && cpumap[i] == 0) {
            cpumap[i] = 1;
            cpuid = i;
            goto endcpucheck;
        }
    }

endcpucheck:
    dag_config_dispose(cardref);
    return cpuid;
}

int init_dag_stream(dagstreamthread_t *dst) {
    struct timeval maxwait, poll;
    dag_size_t mindata;

    /* Set polling parameters.
     * TODO: are these worth making configurable?
     * Currently defined in dagmultiplexer.h.
     */
    mindata = DAG_POLL_MINDATA;
    maxwait.tv_sec = 0;
    maxwait.tv_usec = DAG_POLL_MAXWAIT;
    poll.tv_sec = 0;
    poll.tv_usec = DAG_POLL_FREQ;

    pthread_mutex_lock(dst->dagmutex);
    if (dag_attach_stream64(dst->params.dagfd,
            dst->params.streamnum, 0, 8 * 1024 * 1024) != 0) {
        if (errno == ENOMEM) {
            pthread_mutex_unlock(dst->dagmutex);
            return 0;
        }

        fprintf(stderr, "Failed to attach to DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
        pthread_mutex_unlock(dst->dagmutex);
        return -1;
    }

    if (dag_set_stream_poll64(dst->params.dagfd, dst->params.streamnum,
            mindata, &maxwait, &poll) != 0) {
        fprintf(stderr, "Failed to set polling parameters for DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
        pthread_mutex_unlock(dst->dagmutex);
        return -1;
    }

    /* Start stream. */
    if (dag_start_stream(dst->params.dagfd, dst->params.streamnum) != 0) {
        fprintf(stderr, "Failed to start DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
        pthread_mutex_unlock(dst->dagmutex);
        return -1;
    }

    pthread_mutex_unlock(dst->dagmutex);
    dst->streamstarted = 1;
    return 0;
}

int init_dag_sink(ndag_encap_params_t *state, streamsink_t *params, int streamnum,
        uint64_t globalstart) {
    int sock;
    struct addrinfo *targetinfo;

    /* Create an exporting socket. */
    sock = ndag_create_multicaster_socket(params->exportport,
        params->multicastgroup, params->sourceaddr, &targetinfo, params->ttl);
    if (sock == -1) {
        fprintf(stderr, "Failed to create multicaster socket for DAG stream %d\n",
            streamnum);
        return -1;
    }

    ndag_init_encap(state, sock, targetinfo, params->monitorid, streamnum,
        globalstart, params->mtu, NDAG_PKT_ENCAPERF, 0);
    return sock;
}

static inline void log_stats(dagstreamthread_t *dst, struct timeval now) {
    iow_t *logf = NULL;
    char buf[1024];
    int i;

    if (dst->params.statdir) {
        snprintf(buf, sizeof(buf), "%s/ndag.stream-%02d.stats",
                 dst->params.statdir, dst->params.streamnum);
        if ((logf = wandio_wcreate(buf, WANDIO_COMPRESS_NONE,
                                   0, O_CREAT)) == NULL) {
            /* Could be transient failure, so just log and move on. */
            fprintf(stderr, "Failed to create stats file %s\n", buf);
            return;
        }
        wandio_printf(logf, "time %d\n"
                 "stats_interval %d\n"
                 "stream %d\n"
                 "walked_buffers %"PRIu64"\n"
                 "walked_records %"PRIu64"\n"
                 "walked_bytes %"PRIu64"\n"
                 "walked_wire_bytes %"PRIu64"\n"
                 "filtered_out_records %"PRIu64"\n"
                 "filtered_out_bytes %"PRIu64"\n"
                 "filtered_out_wire_bytes %"PRIu64"\n"
                 "dropped_records %"PRIu64"\n"
                 "truncated_records %"PRIu64"\n",
                 (int)now.tv_sec,
                 dst->params.statinterval,
                 dst->params.streamnum,
                 dst->stats.walked_buffers,
                 dst->stats.walked_records,
                 dst->stats.walked_bytes,
                 dst->stats.walked_wbytes,
                 dst->stats.filtered_out.tx_records,
                 dst->stats.filtered_out.tx_bytes,
                 dst->stats.filtered_out.tx_wbytes,
                 dst->stats.dropped_records,
                 dst->stats.truncated_records);
        for (i = 0; i < dst->inuse; ++i) {
            wandio_printf(logf,
                 "sink=%s tx_datagrams %"PRIu64"\n"
                 "sink=%s tx_records %"PRIu64"\n"
                 "sink=%s tx_bytes %"PRIu64"\n"
                 "sink=%s tx_wire_bytes %"PRIu64"\n",
                 dst->stats.sinks[i].name,
                 dst->stats.sinks[i].tx_datagrams,
                 dst->stats.sinks[i].name,
                 dst->stats.sinks[i].tx_records,
                 dst->stats.sinks[i].name,
                 dst->stats.sinks[i].tx_bytes,
                 dst->stats.sinks[i].name,
                 dst->stats.sinks[i].tx_wbytes);
        }
        wandio_wdestroy(logf);
    } else {
        fprintf(stderr,
                "STATS %d stream:%d "
                "walked_buffers:%"PRIu64" "
                "walked_records:%"PRIu64" "
                "walked_bytes:%"PRIu64" "
                "walked_wire_bytes:%"PRIu64" "
                "filtered_out_records %"PRIu64" "
                "filtered_out_bytes %"PRIu64" "
                "filtered_out_wire_bytes %"PRIu64" "
                "dropped_records:%"PRIu64" "
                "truncated_records %"PRIu64"\n",
                (int)now.tv_sec,
                dst->params.streamnum,
                dst->stats.walked_buffers,
                dst->stats.walked_records,
                dst->stats.walked_bytes,
                dst->stats.walked_wbytes,
                dst->stats.filtered_out.tx_records,
                dst->stats.filtered_out.tx_bytes,
                dst->stats.filtered_out.tx_wbytes,
                dst->stats.dropped_records,
                dst->stats.truncated_records);
        for (i = 0; i < dst->inuse; ++i) {
            fprintf(stderr,
                 "%s_tx_datagrams %"PRIu64"\n"
                 "%s_tx_records %"PRIu64"\n"
                 "%s_tx_bytes %"PRIu64"\n"
                 "%s_tx_wire_bytes %"PRIu64"\n",
                 dst->stats.sinks[i].name,
                 dst->stats.sinks[i].tx_datagrams,
                 dst->stats.sinks[i].name,
                 dst->stats.sinks[i].tx_records,
                 dst->stats.sinks[i].name,
                 dst->stats.sinks[i].tx_bytes,
                 dst->stats.sinks[i].name,
                 dst->stats.sinks[i].tx_wbytes);
        }
    }
}

/* MAYBE: Don't assume DAG_COLOR_SLOTS and pass an argument instead? */
void dag_stream_loop(dagstreamthread_t *dst, ndag_encap_params_t *state,
        void(*walk_records)(char **, char *, dagstreamthread_t *,
            uint16_t *, uint16_t *, ndag_encap_params_t *)) {
    void *bottom, *top;
    struct timeval timetaken, endtime, starttime, now;
    uint32_t nextstat = 0;
    int i;

    bottom = NULL;
    top = NULL;

    fprintf(stderr, "In main per-thread loop: %d\n", dst->params.streamnum);
    gettimeofday(&starttime, NULL);
    if (dst->params.statinterval) {
        nextstat = ((starttime.tv_sec / dst->params.statinterval) *
                    dst->params.statinterval) + dst->params.statinterval;
    }

    /* Count datagrams and records. */
    uint16_t savedtosend[DAG_COLOR_SLOTS];
    uint16_t records_walked[DAG_COLOR_SLOTS];

    /* DO dag_advance_stream WHILE not interrupted and not error */
    while (!halted && !paused) {

        /* Reset stats in each loop. */
        memset(savedtosend, 0, sizeof(savedtosend));
        memset(records_walked, 0, sizeof(records_walked));

        // Should we log stats now?
        // TODO: consider checking the time every N iterations
        gettimeofday(&now, NULL);
        if (nextstat > 0 && now.tv_sec >= nextstat) {
            log_stats(dst, now);
            nextstat += dst->params.statinterval;
        }
        //fprintf(stderr, "dag_stream_loop: before dag_advance_stream. streamnum %d\n",dst->params.streamnum);
        top = dag_advance_stream(dst->params.dagfd, dst->params.streamnum,
                (uint8_t **)(&bottom));
        if (top == NULL) {
            fprintf(stderr, "Error while advancing DAG stream %d: %s\n",
                    dst->params.streamnum, strerror(errno));
            break;
        }

        if (bottom == top) {
            dst->idletime += DAG_POLL_MAXWAIT;

            if (dst->idletime > 5 * 1000000) {
                for (i = 0; i < dst->inuse; ++i) {
                    if (ndag_send_keepalive(&state[i]) < 0) {
                        break;
                    }
                }
                dst->idletime = 0;
            }
            continue;
        }
        //fprintf(stderr, "dag_stream_loop: before ndag_reset_encap_state. inuse %d\n",dst->inuse);
        for (i = 0; i < dst->inuse; ++i) {
            ndag_reset_encap_state(&state[i]);
        }
        //fprintf(stderr, "dag_stream_loop: before walk_records\n");
        walk_records((char **)(&bottom), (char *)top, dst, savedtosend,
                records_walked, state);

        /* Record stats. */
        dst->stats.walked_buffers++;
        for (i = 0; i < dst->inuse; ++i) {
            dst->stats.sinks[i].tx_records += records_walked[i];
            dst->stats.sinks[i].tx_datagrams += savedtosend[i];
            /* Account for message headers. */
            dst->stats.sinks[i].tx_bytes += savedtosend[i] * ENCAP_OVERHEAD;
        }


        for (i = 0; i < dst->inuse; ++i) {
            if (savedtosend[i] > 0) {
                if (ndag_send_encap_records(&state[i], savedtosend[i]) == 0) {
                    break;
                }
            }
        }
    }

    gettimeofday(&endtime, NULL);
    timersub(&endtime, &starttime, &timetaken);
    fprintf(stderr, "Halting stream %d after processing %lu records in %d.%d seconds\n",
            dst->params.streamnum, dst->stats.walked_records, (int)timetaken.tv_sec,
            (int)timetaken.tv_usec);
}

void halt_dag_stream(dagstreamthread_t *dst) {
    if (dst->streamstarted) {
        if (dag_stop_stream(dst->params.dagfd, dst->params.streamnum) != 0) {
            fprintf(stderr, "Error while stopping DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
        }
    }

    if (dag_detach_stream(dst->params.dagfd, dst->params.streamnum) != 0) {
        fprintf(stderr, "Error while detaching DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
    }
}

void halt_dag_sink(ndag_encap_params_t *state) {
    if (state) {
        ndag_close_multicaster_socket(state->sock, state->target);
    }
}

int create_multiplex_beaconer(beaconthread_t *bthread) {

    int ret;

#ifdef __linux__
    pthread_attr_t attrib;
    cpu_set_t cpus;
    //int i;
#endif

#ifdef __linux__

    /* This thread is low impact so can be bound to core 0 */
    CPU_ZERO(&cpus);
    CPU_SET(0, &cpus);
    pthread_attr_init(&attrib);
    pthread_attr_setaffinity_np(&attrib, sizeof(cpus), &cpus);
    ret = pthread_create(&(bthread->tid), &attrib, ndag_start_beacon,
            (void *)(bthread->params));
    pthread_attr_destroy(&attrib);

#else
    ret = pthread_create(&(bthread->tid), NULL, ndag_start_beacon,
            (void *)(bthread->params));
#endif


    if (ret != 0) {
        return -1;
    }

    return 1;

}

static int start_dag_thread(dagstreamthread_t *nextslot,
        uint8_t *cpumap, void *(*processfunc)(void *)) {

    int ret, nextdagcpu;
#ifdef __linux__
    pthread_attr_t attrib;
    cpu_set_t cpus;
    //int i;
#endif

    pthread_mutex_lock(nextslot->dagmutex);
    /* Attach to a stream */
    if (dag_attach_stream64(nextslot->params.dagfd,
            nextslot->params.streamnum, 0, 8 * 1024 * 1024) != 0) {
        if (errno == ENOMEM) {
            pthread_mutex_unlock(nextslot->dagmutex);
            return 0;
        }

        fprintf(stderr, "Failed to attach to DAG stream %d: %s\n",
                nextslot->params.streamnum, strerror(errno));
        pthread_mutex_unlock(nextslot->dagmutex);
        return -1;
    }

    /* Check buffer size: if zero, we can save ourselves a thread because
     * we're not going to get any packets on this stream.
     */
    if (dag_get_stream_buffer_size64(nextslot->params.dagfd,
            nextslot->params.streamnum) <= 0) {
        dag_detach_stream(nextslot->params.dagfd, nextslot->params.streamnum);
        pthread_mutex_unlock(nextslot->dagmutex);
        return 0;
    }

    dag_detach_stream(nextslot->params.dagfd, nextslot->params.streamnum);
    nextdagcpu = get_next_thread_cpu(nextslot->params.dagdevname, cpumap,
            nextslot->params.streamnum);
    pthread_mutex_unlock(nextslot->dagmutex);
    if (nextdagcpu == -1) {
        /* TODO better error handling */
        /* TODO allow users to decide that they want more than one stream per
         * CPU */
        fprintf(stderr,
                "Not enough CPUs for the number of threads requested?\n");
        return -1;
    }

#ifdef __linux__

    /* Control which core this thread is bound to */
    CPU_ZERO(&cpus);
    CPU_SET(nextdagcpu, &cpus);
    pthread_attr_init(&attrib);
    pthread_attr_setaffinity_np(&attrib, sizeof(cpus), &cpus);
    ret = pthread_create(&nextslot->tid, &attrib, processfunc,
            (void *)nextslot);
    pthread_attr_destroy(&attrib);

#else
    ret = pthread_create(&nextslot->tid, NULL, processfunc,
            (void *)nextslot);
#endif

    if (ret != 0) {
        return -1;
    }

    return 1;
}

static void dst_destroy(dagstreamthread_t *dst,
                        void (*destroyfunc)(void *)) {
    int i;

    if (dst->params.sinks != NULL) {
        free(dst->params.sinks);
        dst->params.sinkcnt = 0;
    }

    for (i = 0; i < DAG_COLOR_SLOTS; ++i) {
        if (dst->iovs[i].vec != NULL) {
            free(dst->iovs[i].vec);
            dst->iovs[i].vec = NULL;
            dst->iovs[i].len = 0;
        }
    }
    if (destroyfunc) {
        destroyfunc(dst->extra);
    }
}

int run_dag_streams(int dagfd, uint16_t firstport,
        int beaconcnt, ndag_beacon_params_t *bparams,
        streamparams_t *sparams,
        void *initdata,
        void *(*initfunc)(void *),
        void *(*processfunc)(void *),
        void (*destroyfunc)(void *)) {

    dagstreamthread_t *dagthreads = NULL;
    int maxstreams = 0, errorstate = 0;
    sigset_t sig_before, sig_block_all;
    int ret, i, j;
    int threadcount = 0;
    int filteroffset = 0;
    beaconthread_t *beacons = NULL;
    uint8_t *cpumap = NULL;
    pthread_mutex_t dagmutex;


    pthread_mutex_init(&dagmutex, NULL);
    cpumap = (uint8_t *)malloc(sizeof(uint8_t) * get_nb_cores());
    memset(cpumap, 0, sizeof(uint8_t) * get_nb_cores());

    fprintf(stderr, "Starting DAG streams.\n");
    /* Determine maximum stream count and allocate memory for threads */
    maxstreams = dag_rx_get_stream_count(dagfd);
    if (maxstreams < 0) {
        fprintf(stderr, "Failed to get RX stream count from DAG device: %s\n",
                strerror(errno));
        errorstate = 1;
        goto halteverything;
    }

    /* TODO: Might need a better approach here. */
    filteroffset = maxstreams * 4;

    beacons = (beaconthread_t *)(malloc(sizeof(beaconthread_t) * beaconcnt));
    if (beacons == NULL) {
        fprintf(stderr, "Failed to alloce memory for beacon threads\n");
        errorstate = 1;
        goto halteverything;
    }

    dagthreads = (dagstreamthread_t *)(
            malloc(sizeof(dagstreamthread_t) * maxstreams));
    if (dagthreads == NULL) {
        fprintf(stderr, "Failed to alloce memory for dag threads\n");
        errorstate = 1;
        goto halteverything;
    }

    sigemptyset(&sig_block_all);
    if (pthread_sigmask(SIG_SETMASK, &sig_block_all, &sig_before) < 0) {
        fprintf(stderr, "Unable to disable signals before starting threads.\n");
        errorstate = 1;
        goto halteverything;
    }

    /* Create reading thread for each available stream */
    /* note that "available" streams is likely more than the number we're
       actually using, so several of these will not be used */
    for (i = 0; i < maxstreams; i++) {
        dagstreamthread_t *dst = &(dagthreads[i]);
        if (initfunc) {
            dst->extra = initfunc(initdata);
        } else {
            dst->extra = NULL;
        }

        dst->params = *sparams;
        dst->params.sinks =
            (streamsink_t *) malloc(sizeof(streamsink_t) * dst->params.sinkcnt);
        memcpy(dst->params.sinks, sparams->sinks,
                sizeof(streamsink_t) * dst->params.sinkcnt);
        for (j = 0; j < DAG_COLOR_SLOTS; ++j) {
            dst->iovs[j].vec = (struct iovec *) malloc(sizeof(struct iovec) * 2);
            dst->iovs[j].len = 2;
        }
        dst->idletime = 0;
        for (j = 0; j < dst->params.sinkcnt; ++j) {
            dst->params.sinks[j].exportport = firstport + (j * filteroffset)
                + (threadcount * DAG_MULTIPLEX_PORT_INCR);
            assert(dst->params.sinks[j].exportport <= 65534);
        }
        dst->params.streamnum = i * 2;
        dst->streamstarted = 0;
        dst->threadstarted = 0;
        memset(&dst->stats, 0, sizeof(streamstats_t));
        dst->dagmutex = &dagmutex;

        ret = start_dag_thread(dst, cpumap, processfunc);

        if (ret < 0) {
            fprintf(stderr, "Error creating new thread for DAG processing\n");
            errorstate = 1;
            goto halteverything;
        }

        if (ret == 0) {
            /* we're not going to use this thread, might as well clean up now */
            dst_destroy(dst, destroyfunc);
            continue;
        }

        dst->threadstarted = 1;
        threadcount += 1;
    }

    if (pthread_sigmask(SIG_SETMASK, &sig_before, NULL)) {
        fprintf(stderr, "Unable to re-enable signals after thread creation.\n");
        errorstate = 1;
        goto halteverything;
    }

    if (threadcount == 0) {
        fprintf(stderr, "Failed to create any usable DAG threads. Exiting.\n");
        errorstate = 1;
        goto halteverything;
    }

    /* Start one thread per filter destination. */
    for (i = 0; i < beaconcnt; ++i) {
        /* Pass non-owning pointer beacon. Will be cleaned up in telescope. */
        beacons[i].params = &bparams[i];
        beacons[i].params->numstreams = threadcount;
        beacons[i].params->streamports =
            (uint16_t *)malloc(sizeof(uint16_t) * threadcount);

        // TODO: This is not right anymore?
        for (j = 0; j < threadcount; j++) {
            beacons[i].params->streamports[j] =
                filteroffset * i  + firstport + (DAG_MULTIPLEX_PORT_INCR * j);
        }

        /* Create beaconing thread */
        ret = create_multiplex_beaconer(&beacons[i]);
        if (ret < 0) {
            fprintf(stderr, "Failed to create beaconing thread. Exiting.\n");
            errorstate = 1;
            goto halteverything;
        }
    }

    /* Join on all threads */
    for (i = 0; i < threadcount; i++) {
        pthread_join(dagthreads[i].tid, NULL);
    }
    ndag_interrupt_beacon();
    for (i = 0; i < beaconcnt; ++i) {
        pthread_join(beacons[i].tid, NULL);
    }

halteverything:
    if (dagthreads) {
        if (errorstate) {
            halt_program();
            for (i = 0; i < threadcount; i++) {
                if (dagthreads[i].threadstarted)
                    pthread_join(dagthreads[i].tid, NULL);
            }
        }
        for (i = 0; i < threadcount; i++) {
            dst_destroy(&(dagthreads[i]), destroyfunc);
        }
        free(dagthreads);
    }

    for (i = 0; i < beaconcnt; ++i) {
        if (beacons[i].params->streamports) {
            free(beacons[i].params->streamports);
        }
    }
    if (beacons) {
        free(beacons);
    }
    fprintf(stderr, "All DAG streams have been halted.\n");
    free(cpumap);
    pthread_mutex_destroy(&dagmutex);
    return errorstate;
}



// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

