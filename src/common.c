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

int init_dag_stream(dagstreamthread_t *dst, ndag_encap_params_t *state) {

    struct timeval maxwait, poll;
    dag_size_t mindata;
    int sock;
    struct addrinfo *targetinfo;

    /* Set polling parameters
     * TODO: are these worth making configurable?
     * Currently defined in dagmultiplexer.h.
     */
    mindata = DAG_POLL_MINDATA;
    maxwait.tv_sec = 0;
    maxwait.tv_usec = DAG_POLL_MAXWAIT;
    poll.tv_sec = 0;
    poll.tv_usec = DAG_POLL_FREQ;

    if (dag_set_stream_poll64(dst->params.dagfd, dst->params.streamnum,
            mindata, &maxwait, &poll) != 0) {
        fprintf(stderr, "Failed to set polling parameters for DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
        return -1;
    }

    /* Start stream */
    if (dag_start_stream(dst->params.dagfd, dst->params.streamnum) != 0) {
        fprintf(stderr, "Failed to start DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
        return -1;
    }

    dst->streamstarted = 1;

    /* Create an exporting socket */
    sock = ndag_create_multicaster_socket(dst->params.exportport,
            dst->params.multicastgroup, dst->params.sourceaddr, &targetinfo);
    if (sock == -1) {
        fprintf(stderr, "Failed to create multicaster socket for DAG stream %d\n",
                dst->params.streamnum);
        return -1;
    }

    ndag_init_encap(state, sock, targetinfo, dst->params.monitorid,
            dst->params.streamnum, dst->params.globalstart, dst->params.mtu,
            0);
    return sock;
}

static inline void log_stats(dagstreamthread_t *dst, struct timeval now) {
    iow_t *logf = NULL;
    char buf[1024];

    if (dst->params.statdir) {
        snprintf(buf, sizeof(buf), "%s/ndag.stream-%02d.stats",
                 dst->params.statdir, dst->params.streamnum);
        if ((logf = wandio_wcreate(buf, WANDIO_COMPRESS_NONE,
                                   0, O_CREAT)) == NULL) {
            /* could be transient failure, so just log and move on */
            fprintf(stderr, "Failed to create stats file %s\n", buf);
            return;
        }
        snprintf(buf, sizeof(buf),
                 "time %d\n"
                 "stats_interval %d\n"
                 "stream %d\n"
                 "walked_buffers %"PRIu64"\n"
                 "walked_records %"PRIu64"\n"
                 "walked_bytes %"PRIu64"\n"
                 "tx_datagrams %"PRIu64"\n"
                 "tx_records %"PRIu64"\n"
                 "tx_bytes %"PRIu64"\n"
                 "dropped_records %"PRIu64"\n"
                 "truncated_records %"PRIu64"\n",
                 (int)now.tv_sec,
                 dst->params.statinterval,
                 dst->params.streamnum,
                 dst->stats.walked_buffers,
                 dst->stats.walked_records,
                 dst->stats.walked_bytes,
                 dst->stats.tx_datagrams,
                 dst->stats.tx_records,
                 dst->stats.tx_bytes,
                 dst->stats.dropped_records,
                 dst->stats.truncated_records);
        wandio_wwrite(logf, buf, strlen(buf));
        wandio_wdestroy(logf);
    } else {
        fprintf(stderr,
                "STATS %d stream:%d "
                "walked_buffers:%"PRIu64" "
                "walked_records:%"PRIu64" "
                "walked_bytes:%"PRIu64" "
                "tx_datagrams:%"PRIu64" "
                "tx_bytes:%"PRIu64" "
                "tx_records:%"PRIu64" "
                "dropped_records:%"PRIu64" "
                "truncated_records %"PRIu64"\n",
                (int)now.tv_sec,
                dst->params.streamnum,
                dst->stats.walked_buffers,
                dst->stats.walked_records,
                dst->stats.walked_bytes,
                dst->stats.tx_datagrams,
                dst->stats.tx_records,
                dst->stats.tx_bytes,
                dst->stats.dropped_records,
                dst->stats.truncated_records);
    }
}

void dag_stream_loop(dagstreamthread_t *dst, ndag_encap_params_t *state,
        uint16_t(*walk_records)(char **, char *, dagstreamthread_t *,
                uint16_t *, ndag_encap_params_t *)) {
    void *bottom, *top;
    struct timeval timetaken, endtime, starttime, now;
    uint32_t nextstat = 0;
    uint16_t reccnt = 0;
    uint64_t allrecords = 0;

    bottom = NULL;
    top = NULL;

    fprintf(stderr, "In main per-thread loop: %d\n", dst->params.streamnum);
    gettimeofday(&starttime, NULL);
    if (dst->params.statinterval) {
        nextstat = ((starttime.tv_sec / dst->params.statinterval) *
                    dst->params.statinterval) + dst->params.statinterval;
    }
    /* DO dag_advance_stream WHILE not interrupted and not error */
    while (!halted && !paused) {
        uint16_t savedtosend = 0;

        // should we log stats now?
        // TODO: consider checking the time every N iterations
        gettimeofday(&now, NULL);
        if (now.tv_sec >= nextstat) {
            log_stats(dst, now);
            nextstat += dst->params.statinterval;
        }

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
                if (ndag_send_keepalive(state) < 0) {
                    break;
                }
                dst->idletime = 0;
            }
            continue;
        }

        ndag_reset_encap_state(state);

        reccnt = walk_records((char **)(&bottom), (char *)top, dst,
                &savedtosend, state);
        dst->stats.walked_buffers++;
        allrecords += reccnt;
        dst->stats.tx_records += reccnt;
        dst->stats.tx_datagrams += savedtosend;
        /* account for message headers: */
        dst->stats.tx_bytes += savedtosend * ENCAP_OVERHEAD;

        if (savedtosend > 0) {
            if (ndag_send_encap_records(state, savedtosend) == 0) {
                break;
            }
        }
    }

    gettimeofday(&endtime, NULL);
    timersub(&endtime, &starttime, &timetaken);
    fprintf(stderr, "Halting stream %d after processing %lu records in %d.%d seconds\n",
            dst->params.streamnum, allrecords, (int)timetaken.tv_sec,
            (int)timetaken.tv_usec);
}

void halt_dag_stream(dagstreamthread_t *dst, ndag_encap_params_t *state) {

    if (state) {
        ndag_close_multicaster_socket(state->sock, state->target);
    }

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

    /* Attach to a stream */
    if (dag_attach_stream64(nextslot->params.dagfd,
            nextslot->params.streamnum, 0, 8 * 1024 * 1024) != 0) {
        if (errno == ENOMEM)
            return 0;

        fprintf(stderr, "Failed to attach to DAG stream %d: %s\n",
                nextslot->params.streamnum, strerror(errno));
        return -1;
    }

    /* Check buffer size: if zero, we can save ourselves a thread because
     * we're not going to get any packets on this stream.
     */
    if (dag_get_stream_buffer_size64(nextslot->params.dagfd,
            nextslot->params.streamnum) <= 0) {
        dag_detach_stream(nextslot->params.dagfd, nextslot->params.streamnum);
        return 0;
    }
    nextdagcpu = get_next_thread_cpu(nextslot->params.dagdevname, cpumap,
            nextslot->params.streamnum);
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
    if (!dst->threadstarted) {
        return;
    }
    free(dst->iovs);
    if (destroyfunc) {
        destroyfunc(dst->extra);
    }
}

int run_dag_streams(int dagfd, uint16_t firstport,
        ndag_beacon_params_t *bparams,
        streamparams_t *sparams,
        void *initdata,
        void *(*initfunc)(void *),
        void *(*processfunc)(void *),
        void (*destroyfunc)(void *)) {

    dagstreamthread_t *dagthreads = NULL;
    int maxstreams = 0, errorstate = 0;
    sigset_t sig_before, sig_block_all;
    int ret, i;
    int threadcount = 0;
    beaconthread_t *beaconer = NULL;
    uint8_t *cpumap = NULL;

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

    dagthreads = (dagstreamthread_t *)(
            malloc(sizeof(dagstreamthread_t) * maxstreams));

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
        dst->iovs = (struct iovec *)malloc(sizeof(struct iovec) * 2);
        dst->iov_alloc = 2;
        dst->idletime = 0;
        dst->params.exportport = firstport + (i * DAG_MULTIPLEX_PORT_INCR);
        dst->params.streamnum = i * 2;
        dst->streamstarted = 0;
        memset(&dst->stats, 0, sizeof(streamstats_t));

        assert(dst->params.exportport <= 65534);

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

    beaconer = (beaconthread_t *)malloc(sizeof(beaconthread_t));
    beaconer->params = bparams;
    beaconer->params->numstreams = threadcount;
    beaconer->params->streamports = (uint16_t *)malloc(sizeof(uint16_t) * threadcount);

    for (i = 0; i < threadcount; i++) {
        beaconer->params->streamports[i] =
            firstport + (DAG_MULTIPLEX_PORT_INCR * i);
    }

    /* Create beaconing thread */
    ret = create_multiplex_beaconer(beaconer);
    if (ret < 0) {
        fprintf(stderr, "Failed to create beaconing thread. Exiting.\n");
        errorstate = 1;
        goto halteverything;
    }

    /* Join on all threads */
    for (i = 0; i < threadcount; i++) {
        pthread_join(dagthreads[i].tid, NULL);
    }
    ndag_interrupt_beacon();
    pthread_join(beaconer->tid, NULL);

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

    if (beaconer) {
        free(beaconer->params->streamports);
        free(beaconer);
    }
    fprintf(stderr, "All DAG streams have been halted.\n");

    free(cpumap);
    return errorstate;
}



// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

