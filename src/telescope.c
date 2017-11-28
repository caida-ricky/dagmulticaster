#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
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

#include <numa.h>

#include "dagmultiplexer.h"
#include "ndagmulticaster.h"

#define ENCAP_OVERHEAD (sizeof(ndag_common_t) + sizeof(ndag_encap_t))

int threadcount = 0;
volatile int halted = 0;
volatile int paused = 0;

static void halt_signal(int signal) {
    (void) signal;
    halted = 1;
}

static void toggle_pause_signal(int signal) {
    (void) signal;

    if (paused && threadcount == 0) {
        paused = 0;
    }
    if (!paused) {
        paused = 1;
    }
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

    int i, cpuid;
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

static uint32_t walk_stream_buffer(char *bottom, char *top,
        uint16_t *reccount, uint16_t streamnum, uint16_t maxsize,
        dagstreamthread_t *dst) {

    uint32_t walked = 0;

    while (bottom < top && walked < maxsize) {
        dag_record_t *erfhdr = (dag_record_t *)bottom;
        uint16_t len = ntohs(erfhdr->rlen);
        uint16_t lctr = ntohs(erfhdr->lctr);

        if (top - bottom < len) {
            /* Partial record in the buffer */
            break;
        }

        if (walked > 0 && walked + len > maxsize) {
            /* Current record would push us over the end of our datagram */
            break;
        }

        if (lctr != 0) {
            fprintf(stderr, "Loss counter for stream %u is %u\n", streamnum,
                    lctr);
            halted = 1;
            return 0;
        }

        if (dst->iovs[0].iov_base == NULL) {
            dst->iovs[0].iov_base = bottom;
        }
        dst->iovs[0].iov_len += len;

        walked += len;
        bottom += len;
        (*reccount)++;
    }

    /* walked can be larger than maxsize if the first record is
     * very large. This is intentional; the multicaster will truncate the
     * packet record if it is too big and set the truncation flag.
     */

    return walked;

}

static void *per_dagstream(void *threaddata) {

    dag_size_t mindata;
    struct timeval maxwait, poll;
    ndag_encap_params_t state;
    dagstreamthread_t *dst = (dagstreamthread_t *)threaddata;
    void *bottom, *top;
    uint32_t available = 0;
    int sock = -1;
    int oldcancel, i;
    uint64_t allrecords = 0;
    struct addrinfo *targetinfo = NULL;
    struct timeval timetaken, endtime, starttime;
    uint32_t idletime = 0;

    /* Everything we send should be contiguous in memory, so we should
     * only need one iovec. */
    dst->iovs = (struct iovec *)malloc(sizeof(struct iovec));
    dst->iov_alloc = 1;

    if (pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldcancel) != 0) {
        strerror(errno);
        goto exitthread;
    }

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
        goto detachstream;
    }

    /* Start stream */
    if (dag_start_stream(dst->params.dagfd, dst->params.streamnum) != 0) {
        fprintf(stderr, "Failed to start DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
        goto detachstream;
    }


    /* Create an exporting socket */
    sock = ndag_create_multicaster_socket(dst->params.exportport,
            dst->params.multicastgroup, dst->params.sourceaddr, &targetinfo);
    if (sock == -1) {
        fprintf(stderr, "Failed to create multicaster socket for DAG stream %d\n",
                dst->params.streamnum);
        goto stopstream;
    }

    /* TODO allow compress level to be set, although it will trash performance.
     * Note that this should not be set to non-zero if HAVE_LIBZ != 1, as we
     * won't have any compression libraries available. */

    ndag_init_encap(&state, sock, targetinfo, dst->params.monitorid,
            dst->params.streamnum, dst->params.globalstart, dst->params.mtu,
            0);


    bottom = NULL;
    top = NULL;

    fprintf(stderr, "In main per-thread loop: %d\n", dst->params.streamnum);
    gettimeofday(&starttime, NULL);
    /* DO dag_advance_stream WHILE not interrupted and not error */
    while (!halted && !paused) {
        uint16_t records_walked = 0;
        int savedtosend = 0;

        top = dag_advance_stream(dst->params.dagfd, dst->params.streamnum,
                (uint8_t **)(&bottom));
        if (top == NULL) {
            fprintf(stderr, "Error while advancing DAG stream %d: %s\n",
                    dst->params.streamnum, strerror(errno));
            break;
        }

        if (bottom == top) {
            idletime += DAG_POLL_MAXWAIT;

            if (idletime > 5 * 1000000) {
                if (ndag_send_keepalive(&state) < 0) {
                    break;
                }
                idletime = 0;
            }
            continue;
        }

        ndag_reset_encap_state(&state);
        /* Sadly, we have to walk whatever dag_advance_stream gives us because
         *   a) top is not guaranteed to be on a packet boundary.
         *   b) there is no way to put an upper limit on the amount of bytes
         *      that top is moved forward, so we can't guarantee we won't end
         *      up with too much data to fit in one datagram.
         */
        do {
            dst->iovs[0].iov_base = NULL;
            dst->iovs[0].iov_len = 0;

            available = walk_stream_buffer((char *)bottom, (char *)top,
                &records_walked, dst->params.streamnum,
                dst->params.mtu - ENCAP_OVERHEAD, dst);

            allrecords += records_walked;
            if (available > 0) {
                idletime = 0;
                if (ndag_push_encap_iovecs(&state, dst->iovs,
                        dst->iov_alloc, records_walked, savedtosend) == 0) {
                    halted = 1;
                    break;
                }
                savedtosend ++;

            }
            bottom += available;
        } while (available > 0 && savedtosend < NDAG_BATCH_SIZE);

        if (savedtosend > 0) {
                if (ndag_send_encap_records(&state, savedtosend) == 0) {
                    break;
                }
        }

    }

    gettimeofday(&endtime, NULL);
    ndag_destroy_encap(&state);

    timersub(&endtime, &starttime, &timetaken);
    /* Close socket */
    fprintf(stderr, "Halting stream %d after processing %lu records in %d.%d seconds\n",
            dst->params.streamnum, allrecords, timetaken.tv_sec,
            timetaken.tv_usec);

    /* Stop stream */
stopstream:
    if (dag_stop_stream(dst->params.dagfd, dst->params.streamnum) != 0) {
        fprintf(stderr, "Error while stopping DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
    }
    ndag_close_multicaster_socket(sock, targetinfo);

detachstream:
    /* Detach stream */
    if (dag_detach_stream(dst->params.dagfd, dst->params.streamnum) != 0) {
        fprintf(stderr, "Error while detaching DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
    }
exitthread:
    /* Finished */
    if (dst->iovs) {
        free(dst->iovs);
    }
    fprintf(stderr, "Exiting thread for stream %d\n", dst->params.streamnum);
    pthread_exit(NULL);
}

static int start_dag_thread(streamparams_t *params, int index,
        dagstreamthread_t *nextslot, uint16_t firstport, uint8_t *cpumap) {

    int ret, nextdagcpu;
#ifdef __linux__
    pthread_attr_t attrib;
    cpu_set_t cpus;
    int i;
#endif

    nextslot->params = *params;

    /* Choose destination port for multicast */
    nextslot->params.exportport = firstport + (index * DAG_MULTIPLEX_PORT_INCR);
    nextslot->params.streamnum = index * 2;

    assert(nextslot->params.exportport <= 65534);

    /* Attach to a stream */
    if (dag_attach_stream64(params->dagfd, nextslot->params.streamnum, 0,
            8 * 1024 * 1024) != 0) {
        if (errno == ENOMEM)
            return 0;

        fprintf(stderr, "Failed to attach to DAG stream %d: %s\n",
                nextslot->params.streamnum, strerror(errno));
        return -1;
    }

    /* Check buffer size: if zero, we can save ourselves a thread because
     * we're not going to get any packets on this stream.
     */
    if (dag_get_stream_buffer_size64(params->dagfd,
            nextslot->params.streamnum) <= 0) {
        dag_detach_stream(params->dagfd, nextslot->params.streamnum);
        return 0;
    }

    nextdagcpu = get_next_thread_cpu(params->dagdevname, cpumap,
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
    ret = pthread_create(&nextslot->tid, &attrib, per_dagstream,
            (void *)nextslot);
    pthread_attr_destroy(&attrib);

#else
    ret = pthread_create(&nextslot->tid, NULL, per_dagstream,
            (void *)nextslot);
#endif


    if (ret != 0) {
        return -1;
    }

    return 1;
}


int create_multiplex_beaconer(beaconthread_t *bthread) {

    int ret;

#ifdef __linux__
    pthread_attr_t attrib;
    cpu_set_t cpus;
    int i;
#endif

#ifdef __linux__

	/* This thread is low impact so can be bound to core 0 */
    CPU_ZERO(&cpus);
	CPU_SET(0, &cpus);
    pthread_attr_init(&attrib);
    pthread_attr_setaffinity_np(&attrib, sizeof(cpus), &cpus);
    ret = pthread_create(&(bthread->tid), &attrib, ndag_start_beacon,
            (void *)&(bthread->params));
    pthread_attr_destroy(&attrib);

#else
    ret = pthread_create(&(bthread->tid), NULL, ndag_start_beacon,
            (void *)&(bthread->params));
#endif


    if (ret != 0) {
        return -1;
    }

    return 1;

}

void print_help(char *progname) {

    fprintf(stderr,
        "Usage: %s [ -d dagdevice ] [ -p beaconport ] [ -m monitorid ] [ -c ]\n"
        "          [ -a multicastaddress ] [ -s sourceaddress ]\n"
        "          [ -M exportmtu ]\n", progname);

}

int main(int argc, char **argv) {
    char *dagdev = NULL;
    char *multicastgroup = NULL;
    char *sourceaddr = NULL;
    streamparams_t params;
    int dagfd, maxstreams, ret, i, errorstate;
    dagstreamthread_t *dagthreads = NULL;
    beaconthread_t *beaconer = NULL;
    uint16_t beaconport = 9001;
    uint16_t mtu = 1400;
    time_t t;
    struct sigaction sigact;
    sigset_t sig_before, sig_block_all;
    uint16_t firstport;
    struct timeval starttime;

    uint8_t *cpumap = NULL;

    struct sched_param schedparam;

    srand((unsigned) time(&t));

    cpumap = (uint8_t *)malloc(sizeof(uint8_t) * get_nb_cores());
    memset(cpumap, 0, sizeof(uint8_t) * get_nb_cores());

    /* Process user config options */
    /*  options:
     *      dag device name
     *      monitor id
     *      beaconing port number
     *      multicast address/group
     *      starting port for multicast (if not set, choose at random)
     *      compress output - yes/no?
     *      max streams per core
     *      interfaces to send multicast on
     *      anything else?
     */

    /* For now, I'm going to use getopt for config. If our config becomes
     * more complicated or we have so many options that configuration becomes
     * unwieldy, then we can look at using a config file instead.
     */

    params.compressflag = 0;
    params.monitorid = 1;

    /* This lets us do fast polling on the DAG card. Fast polls (< 2ms) will
     * be implemented as busy-waits so there will be high CPU usage.
     */
    schedparam.sched_priority = sched_get_priority_max(SCHED_RR);
    sched_setscheduler(0, SCHED_RR, &schedparam);

    while (1) {
        int option_index = 0;
        int c;
        static struct option long_options[] = {
            { "device", 1, 0, 'd' },
            { "help", 0, 0, 'h' },
            { "monitorid", 1, 0, 'm' },
            { "beaconport", 1, 0, 'p' },
            { "compress", 0, 0, 'c' },
            { "groupaddr", 1, 0, 'a' },
            { "sourceaddr", 1, 0, 's' },
            { "mtu", 1, 0, 'M' },
            { NULL, 0, 0, 0 }
        };

        c = getopt_long(argc, argv, "a:s:d:hm:M:p:c", long_options,
                &option_index);
        if (c == -1)
            break;

        switch (c) {
            case 'd':
                dagdev = strdup(optarg);
                break;
            case 'm':
                params.monitorid = (uint16_t)(strtoul(optarg, NULL, 0) % 65536);
                break;
            case 'p':
                beaconport = (uint16_t)(strtoul(optarg, NULL, 0) % 65536);
                break;
            case 'c':
                params.compressflag = 1;
                break;
            case 'a':
                multicastgroup = strdup(optarg);
                break;
            case 's':
                sourceaddr = strdup(optarg);
                break;
            case 'M':
                mtu = (uint16_t)(strtoul(optarg, NULL, 0) % 65536);
                break;
            case 'h':
            default:
                print_help(argv[0]);
                exit(1);
        }
    }

    /* Set signal callbacks */
    /* Interrupt for halt, hup to toggle pause */
    sigact.sa_handler = toggle_pause_signal;
    sigemptyset(&sigact.sa_mask);
    sigact.sa_flags = SA_RESTART;
    sigaction(SIGHUP, &sigact, NULL);

    sigact.sa_handler = halt_signal;
    sigemptyset(&sigact.sa_mask);
    sigact.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sigact, NULL);
    sigaction(SIGTERM, &sigact, NULL);


    /* Try to set a sensible default */
    if (dagdev == NULL) {
        dagdev = strdup("/dev/dag0");
    }
    if (multicastgroup == NULL) {
        multicastgroup = strdup("225.0.0.225");
    }
    if (sourceaddr == NULL) {
        fprintf(stderr,
            "Warning: no source address specified. Using default interface.");
        sourceaddr = strdup("0.0.0.0");
    }
    if (params.monitorid == 0) {
        fprintf(stderr,
            "0 is not a valid monitor ID -- choose another number.\n");
        goto finalcleanup;
    }


    /* Open DAG card */
    fprintf(stderr, "Attempting to open DAG device: %s\n", dagdev);

    dagfd = dag_open(dagdev);
    if (dagfd < 0) {
        fprintf(stderr, "Failed to open DAG device: %s\n", strerror(errno));
        goto finalcleanup;
    }
    params.dagdevname = dagdev;
    params.dagfd = dagfd;
    params.multicastgroup = multicastgroup;
    params.sourceaddr = sourceaddr;
    params.mtu = mtu;

    gettimeofday(&starttime, NULL);
    params.globalstart = bswap_host_to_be64(
            (starttime.tv_sec - 1509494400) * 1000) +
            (starttime.tv_usec / 1000.0);
    halted = 0;
    threadcount = 0;
    beaconer = (beaconthread_t *)malloc(sizeof(beaconthread_t));
    firstport = 10000 + (rand() % 50000);

    while (!halted) {
        errorstate = 0;
        dagthreads = NULL;

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

        for (i = 0; i < maxstreams; i++) {
            ret = start_dag_thread(&params, i, &(dagthreads[threadcount]),
                    firstport, cpumap);


            if (ret < 0) {
                fprintf(stderr, "Error creating new thread for DAG processing\n");
                errorstate = 1;
                goto halteverything;
            }

            if (ret == 0)
                continue;

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

        beaconer->params.srcaddr = sourceaddr;
        beaconer->params.groupaddr = multicastgroup;
        beaconer->params.beaconport = beaconport;
        beaconer->params.numstreams = threadcount;
        beaconer->params.streamports = (uint16_t *)malloc(sizeof(uint16_t) * threadcount);
        beaconer->params.frequency = DAG_MULTIPLEX_BEACON_FREQ;
        beaconer->params.monitorid = params.monitorid;

        for (i = 0; i < threadcount; i++) {
            beaconer->params.streamports[i] =
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
        free(dagthreads);
        dagthreads = NULL;
        threadcount = 0;
        fprintf(stderr, "All DAG streams have been halted.\n");

        /* If we are paused, we want to wait here until we get the signal to
         * restart.
         *
         * TODO implement pausing and unpausing.
         */
        while (paused) {
            usleep(10000);
        }
    }

halteverything:
    fprintf(stderr, "Shutting down DAG multiplexer.\n");
    if (errorstate) {
        /* Something went horribly wrong earlier -- force all threads to
         * stop running.
         */
        halted = 1;
        for (i = 0; i < maxstreams; i++) {
            /* XXX hope this will actually complete in an error scenario */
            pthread_join(dagthreads[i].tid, NULL);
        }
    }
    if (dagthreads) {
        free(dagthreads);
    }
    free(beaconer->params.streamports);
    free(beaconer);

    /* Close DAG card */
    dag_close(dagfd);

finalcleanup:
    free(dagdev);
    free(multicastgroup);
    free(sourceaddr);
}


// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
