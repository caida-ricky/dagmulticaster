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

/* Check if bit at position `needle` is set in `haystack`. */
#define IS_SET(haystack, needle) (((haystack >> needle) & 0x1) != 0)

/* Hide the needle at bit `pos` in `haystack`. */
#define SET_IT(haystack, position) (haystack |= (0x1 << position))

static volatile sig_atomic_t reload = 0;
static pthread_t darkfilter_tid;

static int leading_zeros(uint8_t color) {
  assert(color > 0);
  int cnt = 0;
  /* Count by hand. There may be compiler intrinsics for this, but I'm not
   * sure how they compare speed-wise and at what point they pay off. */
  while (((color >> cnt) & 1) == 0) {
    ++cnt;
  }
  return cnt;
}

static void reload_signal(int signal) {
    (void) signal;
    reload = 1;
}

/* Add the packet to the state with color. Note down collected bytes. */
static void append(iov_data_t *iov, uint16_t curiov, int color,
        char *bottom, uint16_t len, uint32_t *collected) {
    if (iov->vec[curiov].iov_base == NULL) {
        iov->vec[curiov].iov_base = bottom;
    }
    iov->vec[curiov].iov_len += len;
    *collected += len;
}

/* End an iov and allocate a new one if necessary. */
static void end(iov_data_t *iov, uint16_t *curiov) {
    if (iov->vec[*curiov].iov_len != 0) {
        *curiov = *curiov + 1;
        /* Allocate more iovs if we don't have enough. */
        if (*curiov == iov->len) {
            iov->vec = (struct iovec *) realloc(iov->vec,
                    sizeof(struct iovec) * (iov->len + 10));
            iov->len += 10;
        }
        iov->vec[*curiov].iov_base = NULL;
        iov->vec[*curiov].iov_len = 0;
    }
}

static char * walk_stream_buffer(char *bottom, char *top,
        uint16_t *total_reccount, uint16_t *curiov,
        dagstreamthread_t *dst, darkfilter_t *filter,
        uint16_t* reccounts) {
    uint32_t collected[DAG_COLOR_SLOTS];
    uint32_t tx[DAG_COLOR_SLOTS];
    uint32_t txw[DAG_COLOR_SLOTS];
    uint32_t walked = 0;
    uint32_t wwalked = 0;
    int i;
    int color = 1;
    int non_default_open = 0; // Track if previous packet had a non-default sink.

    /* Sanity check. */
    if (dst->params.sinkcnt == 0) {
        fprintf(stderr, "Need at least one sink to write to.\n");
    }

    /* Nothing collected atm. */
    memset(collected, 0, sizeof(collected));
    memset(tx, 0, sizeof(tx));
    memset(txw, 0, sizeof(txw));

    for (i = 0; i < dst->inuse; ++i) {
        dst->iovs[i].vec[curiov[i]].iov_base = NULL;
        dst->iovs[i].vec[curiov[i]].iov_len = 0;
    }

    /* Remove this check from the loop: `&& walked < maxsize`. I think that is
     * checked before appending data to an iovec. */
    while (bottom < top) {
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
            color = apply_darkfilter(filter, bottom);
            if (color < 0) {
                fprintf(stderr, "Error applying darknet filter to received "
                        "traffic.\n");
                halt_program();
                return bottom;
            }

            /* No color (i.e. 0) drops packets, see telescope.h */
            if (color == 0) {
                /* Skip this packet. */
                bottom += len;

                /* Update stats. */
                dst->stats.walked_records++;
                dst->stats.walked_bytes += len;
                dst->stats.walked_wbytes += wlen;
                dst->stats.filtered_out.tx_records++;
                dst->stats.filtered_out.tx_bytes += len;
                dst->stats.filtered_out.tx_wbytes += wlen;

                /* Close running iovecs. */
                for (i = 0; i < dst->inuse; ++i) {
                    end(&dst->iovs[i], &curiov[i]);
                }

                /* Next packet. */
                continue;
            }
        }

        /* The default case should be fast. Lot's of indirections here. */
        if (color == 1) {
            i = leading_zeros(color); // Should be 0.
            if (collected[i] > 0 && collected[i] + len > dst->iovs[i].maxsize) {
                /* Current record would push us over the end of our datagram */
                break;
            }
            tx[i] += len;
            txw[i] += wlen;
            reccounts[i] += 1;
            append(&dst->iovs[i], curiov[i], i, bottom, len, &collected[i]);

            /* Close all running non-default iovecs. Technically,
             * this means skipping the first entry. */
            if (non_default_open) {
                for (i = 1; i < dst->inuse; ++i) {
                    end(&dst->iovs[i], &curiov[i]);
                }
            }
            non_default_open = 0;
        } else {
            /* Sadly we have to figure this out before we do anything else.
             * Otherwise we risk appending a packet to the same sink twice.*/
            for (i = 1; i < dst->inuse; ++i) {
                if (IS_SET(color, i) && collected[i] > 0
                        && collected[i] + len > dst->iovs[i].maxsize) {
                    goto stopwalking;
                }
            }

            /* Iterate over all colors, need to close default if it was open. */
            for (i = 0; i < dst->inuse; ++i) {
                if (IS_SET(color, i)) {
                    tx[i] += len;
                    txw[i] += wlen;
                    reccounts[i] += 1;
                    append(&dst->iovs[i], curiov[i], i, bottom, len, &collected[i]);
                } else {
                    end(&dst->iovs[i], &curiov[i]);
                }
            }

            /* Need to close these if next packet is default only. */
            non_default_open = 1;
        }

        /* Record local stats. */
        walked += len;
        wwalked += wlen;

        /* Global stats and progress. */
        bottom += len;
        ++(*total_reccount);
        dst->stats.walked_records++;
    }

stopwalking:
    /* Write local stats to global info. */
    dst->stats.walked_bytes += walked;
    dst->stats.walked_wbytes += wwalked;
    for (i = 0; i < dst->inuse; ++i) {
        dst->stats.sinks[i].tx_bytes += tx[i];
        dst->stats.sinks[i].tx_wbytes += txw[i];
    }

    /* Walked can be larger than maxsize if the first record is
     * very large. This is intentional; the multicaster will truncate the
     * packet record if it is too big and set the truncation flag.
     */
    for (i = 0; i < dst->inuse; ++i) {
        if (collected[i] > dst->iovs[i].maxsize) {
            dst->stats.truncated_records++;
            break;
        }
    }

    return bottom;
}

void telescope_walk_records(char **bottom, char *top,
        dagstreamthread_t *dst, uint16_t *savedtosend,
        uint16_t *records_walked_total, ndag_encap_params_t *state) {

    uint16_t available[DAG_COLOR_SLOTS];
    uint16_t records_walked[DAG_COLOR_SLOTS];
    uint16_t records_walked_loop;
    int i, max;

    /* Get our application-specific state. */
    darkfilter_t *filter = (darkfilter_t *)dst->extra;

    /* Sadly, we have to walk whatever dag_advance_stream gives us because
     *   a) top is not guaranteed to be on a packet boundary.
     *   b) there is no way to put an upper limit on the amount of bytes
     *      that top is moved forward, so we can't guarantee we won't end
     *      up with too much data to fit in one datagram.
     */

    do {
        records_walked_loop = 0;

        /* Iteration data. */
        memset(available, 0, sizeof(available));
        memset(records_walked, 0, sizeof(records_walked));

        (*bottom) = walk_stream_buffer((*bottom), top,
                &records_walked_loop, available, dst, filter, records_walked);

        if (records_walked_loop > 0) {
            dst->idletime = 0;

            /* Append all new iovecs to ndag stream. */
            for (i = 0; i < dst->inuse; ++i) {
                if (records_walked[i] > 0) {
                    if (ndag_push_encap_iovecs(&state[i], dst->iovs[i].vec,
                                available[i] + 1, records_walked[i],
                                    savedtosend[i]) == 0) {
                        halt_program();
                        break;
                    }
                    savedtosend[i] += 1;

                    /* Collect overall processed records. */
                    records_walked_total[i] += records_walked[i];
                }
            }
        }

        /* Find largest current batch size. */
        max = 0;
        for (i = 0; i < dst->inuse; ++i) {
            if (savedtosend[i] > max) {
                max = savedtosend[i];
            }
        }
    } while (!is_halted() && records_walked_loop > 0 && max < NDAG_BATCH_SIZE);
}

static void *per_dagstream(void *threaddata) {
    dagstreamthread_t *dst = (dagstreamthread_t *)threaddata;
    ndag_encap_params_t state[DAG_COLOR_SLOTS];
    int initialized = 0;
    int i, res, idx;

    /* Sanity check. */
    if (dst->params.sinkcnt == 0 || dst->params.sinks == NULL) {
        fprintf(stderr, "At least on sink is required to start a dag stream.\n");
        goto perdagstreamexit;
    }

    /* Initialize dagstream thread.*/
    if (init_dag_stream(dst) == -1) {
        goto perdagstreamexit;
    }

    /* Color bit indices should match the iovec position to make lookup easy. */
    for (initialized = 0; initialized < dst->params.sinkcnt; ++initialized) {
        /* The colors are assigned in incrementing order, which should ensure
         * that leading_zeros(color) returns their own index. The expection is
         * the default route that _always_ has color 1 and might be positioned
         * anywhere in the list. Hence the translation of the color to index.
         */
        /* The color bit position is our index. */
        idx = leading_zeros(dst->params.sinks[initialized].color);
        res = init_dag_sink(&state[idx], &dst->params.sinks[initialized],
            dst->params.streamnum, dst->params.globalstart);
        dst->iovs[idx].maxsize =
            dst->params.sinks[initialized].mtu - ENCAP_OVERHEAD;
        dst->stats.sinks[idx].name = dst->params.sinks[initialized].name;
        if (res == -1) {
            goto perdagstreamexit;
        }
    }
    dst->inuse = initialized;

    dag_stream_loop(dst, state, telescope_walk_records);
    for (i = 0; i < dst->params.sinkcnt; ++i) {
        ndag_destroy_encap(&state[i]);
    }

perdagstreamexit:
    /* Stop sinks and clean up their state. */
    for (i = 0; i < initialized; ++i) {
        halt_dag_sink(&state[i]);
    }

    /* Stop reading new data. Shouldn't this happen before we stop the sinks? */
    halt_dag_stream(dst);

    /* Clean up iovecs and their array. */
    for (i = 0; i < DAG_COLOR_SLOTS; ++i) {
        if (dst->iovs[i].vec != NULL) {
            free(dst->iovs[i].vec);
            dst->iovs[i].len = 0;
        }
    }

    /* Exit. */
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

static darkfilter_filter_t *init_darkfilter(int first_octet, int cnt,
                                            darkfilter_file_t *files) {
    darkfilter_filter_t *darkfilter =
        create_darkfilter_filter(first_octet, cnt, files);

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
    int beaconindex = 0;
    int filecnt = 0;
    int fileindex = 0;
    ndag_beacon_params_t *beaconparams = NULL;
    darkfilter_file_t *darkfilterfiles = NULL;
    darkfilter_filter_t *darkfilter = NULL;
    time_t t;
    uint16_t firstport;
    struct timeval starttime;
    struct sigaction sigact;

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

    /* Fill in global parameters. */
    params.dagdevname = glob->dagdev;
    params.dagfd = dagfd;
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
        if (itr->filterfile != NULL) {
            ++filecnt;
        }
    }

    /* Allocate parameter array for beacons. */
    beaconparams =
        (ndag_beacon_params_t *) malloc(sizeof(ndag_beacon_params_t) * beaconcnt);
    if (beaconparams == NULL) {
        fprintf(stderr, "Failed to allocate memory for beacon parameters.\n");
        goto finalcleanup;
    }

    /* Allocate filter file array. */
    darkfilterfiles =
        (darkfilter_file_t *) malloc(sizeof(darkfilter_file_t) * filecnt);
    if (darkfilterfiles == NULL) {
        fprintf(stderr, "Failed to allocate memory for darkfilter files.\n");
        goto finalcleanup;
    }

    /* Allocate param array for stream sinks. */
    params.sinkcnt = beaconcnt;
    params.sinks = (streamsink_t *) malloc(sizeof(streamsink_t) * beaconcnt);
    if (params.sinks == NULL) {
        fprintf(stderr, "Failed to allocate memory for stream sink parameters.\n");
        goto finalcleanup;
    }

    /* Copy parameters from config. Ownership retained by config,
     * which will clean up the strings.*/
    beaconindex = 0;
    fileindex = 0;
    for (torrent_t* itr = glob->torrents; itr != NULL; itr = itr->next) {
        if (itr->mcastaddr != NULL) {
            /* Data to create beacons for rendezvous. */
            beaconparams[beaconindex].srcaddr = itr->srcaddr;
            beaconparams[beaconindex].groupaddr = itr->mcastaddr;
            beaconparams[beaconindex].beaconport = itr->mcastport;
            beaconparams[beaconindex].frequency = DAG_MULTIPLEX_BEACON_FREQ;
            beaconparams[beaconindex].monitorid = itr->monitorid;
            /* Streamparameters to sort incoming packets into. */
            params.sinks[beaconindex].color = itr->color;
            params.sinks[beaconindex].sourceaddr = itr->srcaddr;
            params.sinks[beaconindex].multicastgroup = itr->mcastaddr;
            params.sinks[beaconindex].monitorid = itr->monitorid;
            params.sinks[beaconindex].mtu = itr->mtu;
            /* The config maintains ownership of the name. */
            params.sinks[beaconindex].name = itr->name;
            /* Got one.*/
            beaconindex += 1;
        }
        if (itr->filterfile != NULL) {
            /* Data to build the filter from exclusion files. */
            darkfilterfiles[fileindex].color = itr->color;
            darkfilterfiles[fileindex].excl_file = itr->filterfile;
            itr->filterfile = NULL; // Transfer ownership.
            /* Got one.*/
            fileindex += 1;
        }
    }

    /* boot up the things needed for managing the darkfilter */
    darkfilter = init_darkfilter(glob->darknetoctet, filecnt, darkfilterfiles);
    if (!darkfilter) {
        fprintf(stderr, "Failed to create darkfilter filter.\n");
        goto finalcleanup;
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
    // TODO: Should this happen in `destroy_darkfilter`?
    if (darkfilterfiles) {
        for (fileindex = 0; fileindex < filecnt; ++fileindex) {
            if (darkfilterfiles[fileindex].excl_file) {
                free(darkfilterfiles[fileindex].excl_file);
            }
        }
        free(darkfilterfiles);
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
    if (params.sinks) {
        free(params.sinks);
    }
}

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
