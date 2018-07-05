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
#include <libtrace.h>
#include <numa.h>
#include <libpacketdump.h>

#include "dagmultiplexer.h"
#include "ndagmulticaster.h"
#include "wdcapsniffer.h"
#include "byteswap.h"

#define ENCAP_OVERHEAD (sizeof(ndag_common_t) + sizeof(ndag_encap_t))

static char *walk_stream_buffer(char *bottom, char *top,
        uint16_t *reccount, uint16_t *curiov,
        dagstreamthread_t *dst, wdcapdata_t *wdcap) {

    uint32_t walked = 0;
    uint16_t maxsize = dst->params.mtu - ENCAP_OVERHEAD;
    uint16_t streamnum = dst->params.streamnum;

    *curiov = 0;

    dst->iovs[*curiov].iov_base = NULL;
    dst->iovs[*curiov].iov_len = 0;

    while (bottom < top && walked < maxsize) {
        dag_record_t *erfhdr = (dag_record_t *)bottom;
        uint16_t len = ntohs(erfhdr->rlen);
        uint16_t lctr = ntohs(erfhdr->lctr);
        uint16_t snapto = len;

        if (lctr != 0) {
            fprintf(stderr, "Loss counter for stream %u is %u\n", streamnum,
                    lctr);
            halt_program();
            return bottom;
        }

        if (top - bottom < len) {
            /* Partial packet in the buffer */
            break;
        }


        if (!wdcap->skipwdcap) {
            if (trace_prepare_packet(wdcap->dummytrace, wdcap->packet, bottom,
                    TRACE_RT_DATA_ERF, TRACE_PREP_DO_NOT_OWN_BUFFER) == -1) {
                fprintf(stderr, "Unable to convert DAG buffer contents to libtrace packet.\n");
                halt_program();
                return bottom;
            }
            snapto = processWdcapPacket(wdcap->wdcapproc, wdcap->packet);
            if (snapto > len) {
                /* Error: something went wrong -- skip this packet */
                bottom += len;
                continue;
            }
            if (snapto < ntohs(erfhdr->rlen)) {
                erfhdr->rlen = htons(snapto);
            }

            /* If first iter or last record was snapped, set ptr to bottom */
            if (dst->iovs[*curiov].iov_base == NULL) {
                dst->iovs[*curiov].iov_base = bottom;
            }

            dst->iovs[*curiov].iov_len += snapto;

            if (snapto < len) {
                /* Packet is truncated, end current iovec because next packet
                 * is not going to be contiguous with this one.
                 */
                *curiov = *curiov + 1;

                if (*curiov == dst->iov_alloc) {
                    dst->iovs = (struct iovec *)realloc(dst->iovs,
                            sizeof(struct iovec) * (dst->iov_alloc + 10));
                    dst->iov_alloc += 10;
                }

                dst->iovs[*curiov].iov_base = NULL;
                dst->iovs[*curiov].iov_len = 0;
            }

        } else {
            /* No processing or was processed last time around */

            /* if first iter, set ptr to bottom */
            if (dst->iovs[*curiov].iov_base == NULL) {
                dst->iovs[*curiov].iov_base = bottom;
            }

            /* Just add len to iov_len */
            dst->iovs[*curiov].iov_len += len;

            if (wdcap->savedlen && wdcap->savedlen > len) {
                /* Packet is truncated, end current iovec because next packet
                 * is not going to be contiguous with this one.
                 */
                *curiov = *curiov + 1;

                if (*curiov == dst->iov_alloc) {
                    dst->iovs = (struct iovec *)realloc(dst->iovs,
                            sizeof(struct iovec) * (dst->iov_alloc + 10));
                    dst->iov_alloc += 10;
                }

                dst->iovs[*curiov].iov_base = NULL;
                dst->iovs[*curiov].iov_len = 0;
            }

        }

        if (wdcap->wdcapproc) {
            wdcap->skipwdcap = 0;
        }

        if (walked > 0 && walked + snapto > maxsize) {
            /* Current record would push us over the end of our datagram */
            if (wdcap->wdcapproc) {
                wdcap->skipwdcap = 1;
                wdcap->savedlen = len;
            }
            if (dst->iovs[*curiov].iov_len > 0) {
                assert(snapto <= dst->iovs[*curiov].iov_len);
                dst->iovs[*curiov].iov_len -= snapto;
            } else {
                assert(*curiov > 0);
                *curiov = (*curiov) - 1;
                assert(snapto <= dst->iovs[(*curiov)].iov_len);
                dst->iovs[(*curiov)].iov_len -= snapto;
            }
            break;
        }

        walked += snapto;
        if (wdcap->savedlen) {
            bottom += wdcap->savedlen;
            wdcap->savedlen = 0;
        } else {
            bottom += len;
        }
        (*reccount)++;
    }

    /* walked can be larger than maxsize if the first record is
     * very large. This is intentional; the multicaster will truncate the
     * packet record if it is too big and set the truncation flag.
     */
    return bottom;

}

uint16_t wdcap_walk_records(char **bottom, char *top, dagstreamthread_t *dst,
        uint16_t *savedtosend, ndag_encap_params_t *state) {

    uint16_t total_walked = 0;
    uint16_t records_walked;
    uint16_t available = 0;
    wdcapdata_t *wdcap = (wdcapdata_t *)dst->extra;

    do {
        records_walked = 0;
        (*bottom) = walk_stream_buffer((*bottom), top,
                &records_walked, &available, dst, wdcap);

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
    } while (!is_halted() && records_walked > 0 && *savedtosend < NDAG_BATCH_SIZE);

    return total_walked;

}

static void *per_dagstream(void *threaddata) {

    ndag_encap_params_t state;
    dagstreamthread_t *dst = (dagstreamthread_t *)threaddata;
    wdcapdata_t *wdcap = (wdcapdata_t *)dst->extra;

    if (init_dag_stream(dst, &state) == -1) {
        halt_dag_stream(dst, NULL);
        goto exitthread;
    }

    if (wdcap->wdcapconf) {
        wdcap->wdcapproc = createWdcapPacketProcessor(wdcap->wdcapconf);
        wdcap->skipwdcap = 0;
        wdcap->savedlen = 0;
        wdcap->packet = trace_create_packet();
        wdcap->dummytrace = trace_create_dead("erf:dummy.erf");
    }

    dag_stream_loop(dst, &state, wdcap_walk_records);
    ndag_destroy_encap(&state);
    halt_dag_stream(dst, &state);

exitthread:
    fprintf(stderr, "Exiting thread for stream %d\n", dst->params.streamnum);
    pthread_exit(NULL);
}

static void destroy_wdcap_data(void *data) {
    wdcapdata_t *wdcap = (wdcapdata_t *)data;

    if (wdcap->wdcapproc) {
        deleteWdcapPacketProcessor(wdcap->wdcapproc);
    }
    if (wdcap->packet) {
        trace_destroy_packet(wdcap->packet);
    }
    if (wdcap->dummytrace) {
        trace_destroy_dead(wdcap->dummytrace);
    }
    free(wdcap);
}

static void *init_wdcap_data(void *conf) {

    WdcapProcessingConfig *wdcapconf = (WdcapProcessingConfig *)conf;
    wdcapdata_t *wdcapdata = (wdcapdata_t *)malloc(sizeof(wdcapdata_t));
    wdcapdata->wdcapconf = wdcapconf;
    wdcapdata->wdcapproc = NULL;
    wdcapdata->skipwdcap = 1;
    wdcapdata->savedlen = 0;
    wdcapdata->packet = NULL;
    wdcapdata->dummytrace = NULL;

    return wdcapdata;
}

void print_help(char *progname) {

    fprintf(stderr,
        "Usage: %s [ -d dagdevice ] [ -p beaconport ] [ -m monitorid ]\n"
        "          [ -a multicastaddress ] [ -s sourceaddress ]\n"
        "          [ -M exportmtu ] [ -w wdcapconfigfile ]\n", progname);

}

int main(int argc, char **argv) {
    char *dagdev = NULL;
    char *multicastgroup = NULL;
    char *sourceaddr = NULL;
    streamparams_t params;
    int dagfd, errorstate;
    dagstreamthread_t *dagthreads = NULL;
    ndag_beacon_params_t beaconparams;
    uint16_t beaconport = 9001;
    uint16_t mtu = 1400;
    time_t t;
    struct sigaction sigact;
    uint16_t firstport;
    struct timeval starttime;
    char *wdcapconffile = NULL;
    WdcapProcessingConfig *wdcapconf = NULL;

    struct sched_param schedparam;

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
     *      anything else?
     */

    /* For now, I'm going to use getopt for config. If our config becomes
     * more complicated or we have so many options that configuration becomes
     * unwieldy, then we can look at using a config file instead.
     */

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
            { "groupaddr", 1, 0, 'a' },
            { "sourceaddr", 1, 0, 's' },
            { "mtu", 1, 0, 'M' },
            { "wdcapconfig", 1, 0, 'w' },
            { NULL, 0, 0, 0 }
        };

        c = getopt_long(argc, argv, "w:a:s:d:hm:M:p:", long_options,
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
            case 'a':
                multicastgroup = strdup(optarg);
                break;
            case 's':
                sourceaddr = strdup(optarg);
                break;
            case 'M':
                mtu = (uint16_t)(strtoul(optarg, NULL, 0) % 65536);
                break;
            case 'w':
                wdcapconffile = optarg;
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

    if (wdcapconffile != NULL) {
        wdcapconf = parseWdcapProcessingConfig(wdcapconffile);
        if (wdcapconf == NULL) {
            fprintf(stderr, "Failed to parse WDCap config file.\n");
            goto finalcleanup;
        }
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
    firstport = 10000 + (rand() % 50000);

    beaconparams.srcaddr = sourceaddr;
    beaconparams.groupaddr = multicastgroup;
    beaconparams.beaconport = beaconport;
    beaconparams.frequency = DAG_MULTIPLEX_BEACON_FREQ;
    beaconparams.monitorid = params.monitorid;

    while (!is_halted()) {
        errorstate = run_dag_streams(dagfd, firstport, &beaconparams,
                &params, wdcapconf, init_wdcap_data, per_dagstream,
                destroy_wdcap_data);

        if (errorstate != 0) {
            break;
        }

        while (is_paused()) {
            usleep(10000);
        }
    }

halteverything:
    fprintf(stderr, "Shutting down DAG multiplexer.\n");

    /* Close DAG card */
    dag_close(dagfd);

finalcleanup:

    if (wdcapconf) {
        deleteWdcapProcessingConfig(wdcapconf);
    }
    free(dagdev);
    free(multicastgroup);
    free(sourceaddr);
}


// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
