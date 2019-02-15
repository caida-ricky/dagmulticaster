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

#include "telescope.h"
#include "dagmultiplexer.h"
#include "ndagmulticaster.h"
#include "byteswap.h"
#include "darkfilter.h"

static volatile sig_atomic_t reload = 0;
static pthread_t darkfilter_tid;

static void reload_signal(int signal) {
    (void) signal;
    reload = 1;
}

static char * walk_stream_buffer(char *bottom, char *top,
        uint16_t *reccount, uint16_t *curiov, dagstreamthread_t *dst,
        darkfilter_t *filter) {

    uint32_t walked = 0;
    uint32_t wwalked = 0;
    uint32_t tx_wire = 0;
    uint16_t maxsize = dst->params.mtu - ENCAP_OVERHEAD;
    int ret;

    *curiov = 0;
    dst->iovs[*curiov].iov_base = NULL;
    dst->iovs[*curiov].iov_len = 0;

    while (bottom < top && walked < maxsize) {
        dag_record_t *erfhdr = (dag_record_t *)bottom;
        uint16_t len = ntohs(erfhdr->rlen);
        uint16_t wlen = ntohs(erfhdr->wlen);
        uint16_t lctr = ntohs(erfhdr->lctr);

        if (lctr != 0) {
            dst->stats.dropped_records += lctr;
        }

        if (top - bottom < len) {
            /* Partial record in the buffer */
            break;
        }

        if (filter) {
            ret = apply_darkfilter(filter, bottom);
            if (ret < 0) {
                fprintf(stderr, "Error applying darknet filter to received traffic.\n");
                halt_program();
                return bottom;
            }

            if (ret == 0) {
                /* skip this packet */
                bottom += len;

                /* update stats */
                dst->stats.walked_records++;
                dst->stats.walked_bytes += len;
                dst->stats.walked_wbytes += wlen;
                dst->stats.tx_bytes += dst->iovs[*curiov].iov_len;
                dst->stats.tx_wbytes += tx_wire;

                /* end current iovec if it has something in it already */
                if (dst->iovs[*curiov].iov_len == 0) {
                    continue;
                }
                *curiov = *curiov + 1;
                if (*curiov == dst->iov_alloc) {
                    dst->iovs = (struct iovec *)realloc(dst->iovs,
                            sizeof(struct iovec) * (dst->iov_alloc + 10));
                    dst->iov_alloc += 10;
                }
                dst->iovs[*curiov].iov_base = NULL;
                dst->iovs[*curiov].iov_len = 0;
                tx_wire = 0;
                continue;
            }
        }

        if (walked > 0 && walked + len > maxsize) {
            /* Current record would push us over the end of our datagram */
            break;
        }

        if (dst->iovs[*curiov].iov_base == NULL) {
            dst->iovs[*curiov].iov_base = bottom;
        }
        dst->iovs[*curiov].iov_len += len;
        tx_wire += wlen;

        walked += len;
        wwalked += wlen;
        bottom += len;
        (*reccount)++;
        dst->stats.walked_records++;
    }

    dst->stats.walked_bytes += walked;
    dst->stats.walked_wbytes += wwalked;
    dst->stats.tx_bytes += dst->iovs[*curiov].iov_len;
    dst->stats.tx_wbytes += tx_wire;

    /* walked can be larger than maxsize if the first record is
     * very large. This is intentional; the multicaster will truncate the
     * packet record if it is too big and set the truncation flag.
     */
    if (walked > maxsize) {
        dst->stats.truncated_records++;
    }

    return bottom;

}

uint16_t telescope_walk_records(char **bottom, char *top,
        dagstreamthread_t *dst, uint16_t *savedtosend,
        ndag_encap_params_t *state) {

    uint16_t available = 0;
    uint16_t total_walked = 0;
    uint16_t records_walked = 0;
    darkfilter_t *filter = (darkfilter_t *)dst->extra;

    /* Sadly, we have to walk whatever dag_advance_stream gives us because
     *   a) top is not guaranteed to be on a packet boundary.
     *   b) there is no way to put an upper limit on the amount of bytes
     *      that top is moved forward, so we can't guarantee we won't end
     *      up with too much data to fit in one datagram.
     */

    do {
        records_walked = 0;
        (*bottom) = walk_stream_buffer((*bottom), top,
                &records_walked, &available, dst, filter);

        total_walked += records_walked;
        if (records_walked > 0) {
            dst->idletime = 0;

            if (ndag_push_encap_iovecs(state, dst->iovs, available + 1,
                        records_walked, *savedtosend) == 0) {
                halt_program();
                break;
            }
            (*savedtosend) = (*savedtosend) + 1;

        }
    } while (!is_halted() && records_walked > 0 &&
        *savedtosend < NDAG_BATCH_SIZE);

    return total_walked;
}

static void *per_dagstream(void *threaddata) {

    ndag_encap_params_t state;
    dagstreamthread_t *dst = (dagstreamthread_t *)threaddata;

    if (init_dag_stream(dst, &state) == -1) {
        halt_dag_stream(dst, NULL);
    } else {
        dag_stream_loop(dst, &state, telescope_walk_records);
        ndag_destroy_encap(&state);
        halt_dag_stream(dst, &state);
    }

    fprintf(stderr, "Exiting thread for stream %d\n", dst->params.streamnum);
    pthread_exit(NULL);
}

static void *darkfilter_reloader(void *threaddata) {
    darkfilter_filter_t *darkfilter = (darkfilter_filter_t *)threaddata;

    fprintf(stderr, "Darkfilter reloader thread started\n");

    while (!is_halted()) {
        if (reload) {
            fprintf(stderr, "Starting darkfilter reload\n");
            if (update_darkfilter_exclusions(darkfilter) != 0) {
                /* parsing the file probably failed, so just log the error and
                   move on */
                fprintf(stderr, "Failed to reload darkfilter exclusion file\n");
            }
            reload = 0;
        }
        usleep(1000);
    }

    pthread_exit(NULL);
}

static darkfilter_filter_t *init_darkfilter(int first_octet, char *excl_file) {
    darkfilter_filter_t *darkfilter =
        create_darkfilter_filter(first_octet, excl_file);

    /* create thread to watch for reload events and trigger exclusion updates */
    if (pthread_create(&darkfilter_tid, NULL, darkfilter_reloader,
                       (void *)darkfilter) != 0) {
        fprintf(stderr, "Failed to create darkfilter reloader thread\n");
        destroy_darkfilter_filter(darkfilter);
        return NULL;
    }

    return darkfilter;
}

void print_help(char *progname) {
    fprintf(stderr, "Usage: %s -c configfile.yaml\n", progname);
}

int main(int argc, char **argv) {
    telescope_global_t *glob = NULL;
    char *configfile = NULL;

    streamparams_t params;
    int dagfd, errorstate;
    int beaconcnt = 0;
    int i = 0;
    ndag_beacon_params_t* beaconparams;
    time_t t;
    uint16_t firstport;
    struct timeval starttime;
    struct sigaction sigact;
    darkfilter_filter_t *darkfilter = NULL;

    srand((unsigned) time(&t));

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
     *      file with /24s to exclude
     *      first octet of darknet space
     *      anything else?
     */

    while (1) {
        int option_index = 0;
        int c;
        static struct option long_options[] = {
            { "config", required_argument, 0, 'c' },
            { "help",   no_argument,       0, 'h' },
            { NULL, 0, 0, 0 }
        };

        c = getopt_long(argc, argv, "c:h", long_options,
                &option_index);
        if (c == -1)
            break;

        switch (c) {
            case 'c':
                configfile = strdup(optarg);
                break;
            case 'h':
            default:
                print_help(argv[0]);
                exit(1);
        }
    }

    if (configfile == NULL) {
        print_help(argv[0]);
        return -1;
    }

    if ((glob = telescope_init_global(configfile)) == NULL) {
        goto finalcleanup;
    }
    if (glob->torrentcount == 0 || glob->torrents == NULL) {
        fprintf(stderr, "Please specify at least on torrent.\n");
        goto finalcleanup;
    }

    torrent_t *torr = glob->torrents;

    /* Set signal callbacks */
    /* Interrupt for halt, hup to signal darkfilter reload */
    sigact.sa_handler = reload_signal;
    sigemptyset(&sigact.sa_mask);
    sigact.sa_flags = SA_RESTART;
    sigaction(SIGHUP, &sigact, NULL);

    sigact.sa_handler = halt_signal;
    sigemptyset(&sigact.sa_mask);
    sigact.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sigact, NULL);
    sigaction(SIGTERM, &sigact, NULL);

    /* Open DAG card */
    fprintf(stderr, "Attempting to open DAG device: %s\n", glob->dagdev);

    dagfd = dag_open(glob->dagdev);
    if (dagfd < 0) {
        fprintf(stderr, "Failed to open DAG device: %s\n", strerror(errno));
        goto finalcleanup;
    }
    // TODO: needs to be an array of things, not just one.
    params.monitorid = torr->monitorid;
    params.dagdevname = glob->dagdev;
    params.dagfd = dagfd;
    params.multicastgroup = torr->mcastaddr;
    params.sourceaddr = torr->srcaddr;
    params.mtu = torr->mtu;
    params.statinterval = glob->statinterval;
    params.statdir = glob->statdir;

    gettimeofday(&starttime, NULL);
    params.globalstart = bswap_host_to_be64(
            (starttime.tv_sec - 1509494400) * 1000) +
            (starttime.tv_usec / 1000.0);
    firstport = 10000 + (rand() % 50000);

    /* We have to count since not all torrents require a beacon. */
    for (torrent_t* itr = glob->torrents; itr != NULL; itr = itr->next) {
        if (itr->mcastaddr != NULL) {
            ++beaconcnt;
        }
    }
    
    /* Allocate parameter array for beacons. */
    beaconparams = 
        (ndag_beacon_params_t *) malloc(sizeof(ndag_beacon_params_t) * beaconcnt);
    if (beaconparams == NULL) {
        fprintf(stderr, "Failed to allocate memory for beacon parameters\n");
        goto finalcleanup;
    }

    /* Copy parameters from config. */
    for (torrent_t* itr = glob->torrents; itr != NULL; itr = itr->next) {
        if (itr->mcastaddr != NULL) {
            beaconparams[i].srcaddr = itr->srcaddr;
            beaconparams[i].groupaddr = itr->mcastaddr;
            beaconparams[i].beaconport = itr->mcastport;
            beaconparams[i].frequency = DAG_MULTIPLEX_BEACON_FREQ;
            beaconparams[i].monitorid = itr->monitorid;
            ++i;
        }
    }

    if (torr->filterfile) {
        /* boot up the things needed for managing the darkfilter */
        darkfilter = init_darkfilter(glob->darknetoctet, torr->filterfile);
        if (!darkfilter) {
            fprintf(stderr, "Failed to create darkfilter filter.\n");
            goto finalcleanup;
        }
    }

    while (!is_halted()) {
        if (darkfilter) {
            errorstate = run_dag_streams(dagfd, firstport, beaconcnt,
                    beaconparams, &params, darkfilter, create_darkfilter,
                    per_dagstream, destroy_darkfilter);
        } else {
            errorstate = run_dag_streams(dagfd, firstport, beaconcnt,
                    beaconparams, &params, NULL, NULL, per_dagstream, NULL);
        }

        if (errorstate != 0) {
            break;
        }

        while (is_paused()) {
            usleep(10000);
        }
    }

    fprintf(stderr, "Shutting down DAG multiplexer.\n");

    /* Close DAG card */
    dag_close(dagfd);

finalcleanup:
    if (darkfilter) {
        pthread_join(darkfilter_tid, NULL);
        destroy_darkfilter_filter(darkfilter);
    }
    if (glob) {
        telescope_cleanup_global(glob);
    }
    if (configfile) {
        free(configfile);
    }
    if (beaconparams) {
        free(beaconparams);
    }
}

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
