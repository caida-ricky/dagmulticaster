#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <assert.h>

#include <pthread.h>
#include <dagapi.h>

#include "dagmultiplexer.h"
#include "ndagmulticaster.h"

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

static uint32_t walk_stream_buffer(char *bottom, char *top,
        uint16_t *reccount) {

    uint32_t walked = 0;

    while (bottom < top && walked < NDAG_MAX_DGRAM_SIZE) {
        dag_record_t *erfhdr = (dag_record_t *)bottom;
        uint16_t len = ntohs(erfhdr->rlen);

        if (walked > 0 && walked + len > NDAG_MAX_DGRAM_SIZE) {
            /* Current record would push us over the end of our datagram */
            break;
        }

        walked += len;
        bottom += len;
        (*reccount)++;
    }

    /* walked can be larger than NDAG_MAX_DGRAM_SIZE if the first record is
     * very large. This is intentional; the multicaster will truncate the
     * packet record if it is too big and set the truncation flag.
     */

    return walked;

}

static void *per_dagstream(void *threaddata) {

    dag_size_t mindata;
    struct timeval maxwait, poll;
    dagstreamthread_t *dst = (dagstreamthread_t *)threaddata;
    void *bottom, *top;
    uint32_t available = 0;
    int sock = -1;

    /* Set polling parameters
     * TODO: are these worth making configurable?
     * Currently defined in dagmultiplexer.h.
     */
    mindata = DAG_POLL_MINDATA;
    maxwait.tv_usec = DAG_POLL_MAXWAIT;
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
            dst->params.multicastgroup);
    if (sock == -1) {
        fprintf(stderr, "Failed to create multicaster socket for DAG stream %d\n",
                dst->params.streamnum);
        goto stopstream;
    }


    bottom = NULL;
    top = NULL;

    /* DO dag_advance_stream WHILE not interrupted and not error */
    while (!halted && !paused) {
        uint16_t records_walked = 0;
        top = dag_advance_stream(dst->params.dagfd, dst->params.streamnum,
                (uint8_t **)(&bottom));
        if (top == NULL) {
            fprintf(stderr, "Error while advancing DAG stream %d: %s\n",
                    dst->params.streamnum, strerror(errno));
            break;
        }

        /* Sadly, we have to walk whatever dag_advance_stream gives us because
         *   a) top is not guaranteed to be on a packet boundary.
         *   b) there is no way to put an upper limit on the amount of bytes
         *      that top is moved forward, so we can't guarantee we won't end
         *      up with too much data to fit in one datagram.
         */
        available = walk_stream_buffer((char *)bottom, (char *)top,
                &records_walked);
        if (available > 0) {
            if (ndag_send_encap_records(sock, (char *)bottom, available,
                    records_walked) < 0) {
                break;
            }
        }
        bottom += available;
    }

    /* Close socket */
    close(sock);

    /* Stop stream */
stopstream:
    if (dag_stop_stream(dst->params.dagfd, dst->params.streamnum) != 0) {
        fprintf(stderr, "Error while stopping DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
    }

detachstream:
    /* Detach stream */
    if (dag_detach_stream(dst->params.dagfd, dst->params.streamnum) != 0) {
        fprintf(stderr, "Error while detaching DAG stream %d: %s\n",
                dst->params.streamnum, strerror(errno));
    }
exitthread:
    /* Finished */
    pthread_exit(NULL);
}

static int start_dag_thread(streamparams_t *params, int index,
        dagstreamthread_t *nextslot, uint16_t firstport) {

    int ret;
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
        return -1;
    }


#ifdef __linux__

	/* Allow this thread to appear on any core */
    CPU_ZERO(&cpus);
    for (i = 0; i < get_nb_cores(); i++) {
		CPU_SET(i, &cpus);
	}
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


int create_multiplex_beaconer(beaconthread_t *bthread,  char *groupaddr,
        uint16_t beaconport, uint16_t firststreamport, uint16_t numstreams) {

    /* TODO write this whole thing */
    return 1;

}

void print_help(char *progname) {

    fprintf(stderr,
        "Usage: %s [ -d dagdevice ] [ -p beaconport ] [ -m monitorid ] [ -c ]\n"
        "          [ -a multicastaddress ]\n", progname);

}

int main(int argc, char **argv) {
    char *dagdev = NULL;
    char *multicastgroup = NULL;
    streamparams_t params;
    int dagfd, maxstreams, ret, i, errorstate;
    dagstreamthread_t *dagthreads = NULL;
    beaconthread_t beaconer;
    uint16_t beaconport = 9001;
    time_t t;
    struct sigaction sigact;
    sigset_t sig_before, sig_block_all;
    uint16_t firstport;

    srand((unsigned) time(&t));

    /* Process user config options */
    /*  options:
     *      dag device name
     *      monitor id
     *      beaconing port number
     *      multicast address
     *      compress output - yes/no?
     *      anything else?
     */

    /* For now, I'm going to use getopt for config. If our config becomes
     * more complicated or we have so many options that configuration becomes
     * unwieldy, then we can look at using a config file instead.
     */

    params.compressflag = 0;
    params.monitorid = 1;

    while (1) {
        int option_index = 0;
        int c;
        static struct option long_options[] = {
            { "device", 1, 0, 'd' },
            { "help", 0, 0, 'h' },
            { "monitorid", 1, 0, 'm' },
            { "beaconport", 1, 0, 'p' },
            { "compress", 0, 0, 'c' },
            { "address", 1, 0, 'a' },
            { NULL, 0, 0, 0 }
        };

        c = getopt_long(argc, argv, "d:hm:p:c", long_options, &option_index);
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

    /* Open DAG card */
    fprintf(stderr, "Attempting to open DAG device: %s\n", dagdev);

    dagfd = dag_open(dagdev);
    if (dagfd < 0) {
        fprintf(stderr, "Failed to open DAG device: %s\n", strerror(errno));
        exit(1);
    }
    params.dagfd = dagfd;
    params.multicastgroup = multicastgroup;

    halted = 0;
    threadcount = 0;
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
        firstport = 10000 + (rand() % 50000);

        sigemptyset(&sig_block_all);
        if (pthread_sigmask(SIG_SETMASK, &sig_block_all, &sig_before) < 0) {
            fprintf(stderr, "Unable to disable signals before starting threads.\n");
            errorstate = 1;
            goto halteverything;
        }

        /* Create reading thread for each available stream */
        for (i = 0; i < maxstreams; i++) {
            ret = start_dag_thread(&params, i, &(dagthreads[threadcount]),
                    firstport);

            if (ret < 0) {
                fprintf(stderr, "Error creating new thread for DAG processing\n");
                errorstate = 1;
                goto halteverything;
            }

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

        /* Create beaconing thread */
        ret = create_multiplex_beaconer(&beaconer, multicastgroup, beaconport,
                firstport, threadcount);
        if (ret < 0) {
            fprintf(stderr, "Failed to create beaconing thread. Exiting.\n");
            errorstate = 1;
            goto halteverything;
        }

        /* Join on all threads */
        pthread_join(beaconer.tid, NULL);
        for (i = 0; i < maxstreams; i++) {
            pthread_join(dagthreads[i].tid, NULL);
        }
        free(dagthreads);
        dagthreads = NULL;
        threadcount = 0;
        fprintf(stderr, "All DAG streams have been halted.\n");

        /* If we are paused, we want to wait here until we get the signal to
         * restart.
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

    /* Close DAG card */
    dag_close(dagfd);

    free(dagdev);
    free(multicastgroup);
}


// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
