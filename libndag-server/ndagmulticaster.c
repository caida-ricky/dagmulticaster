
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <unistd.h>

#include "ndagmulticaster.h"

volatile int halted = 0;

int ndag_interrupt_beacon(void) {
    halted = 1;
    return halted;
}

int ndag_create_multicaster_socket(uint16_t port, char *groupaddr,
        char *srcaddr, struct addrinfo **targetinfo) {

    struct addrinfo hints;
    struct addrinfo *gotten;
    struct addrinfo *source;
    char portstr[16];
    int sock;
    uint32_t ttl = 1;       /* TODO make this configurable */

    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = 0;

    snprintf(portstr, 15, "%u", port);

    if (getaddrinfo(groupaddr, portstr, &hints, &gotten) != 0) {
        fprintf(stderr, "nDAG: Call to getaddrinfo failed for %s:%s -- %s\n",
                groupaddr, portstr, strerror(errno));
        return -1;
    }
    *targetinfo = gotten;

    if (getaddrinfo(srcaddr, NULL, &hints, &source) != 0) {
        fprintf(stderr, "nDAG: Call to getaddrinfo failed for %s:NULL -- %s\n",
                srcaddr, strerror(errno));
        return -1;
    }

    sock = socket(gotten->ai_family, gotten->ai_socktype, 0);
    if (sock < 0) {
        fprintf(stderr,
                "nDAG: Failed to create multicast socket for %s:%s -- %s\n",
                groupaddr, portstr, strerror(errno));
        goto sockcreateover;
    }

    if (setsockopt(sock,
            gotten->ai_family == PF_INET6 ? IPPROTO_IPV6: IPPROTO_IP,
            gotten->ai_family == PF_INET6 ? IPV6_MULTICAST_HOPS :
                    IP_MULTICAST_TTL,
            (char *)&ttl, sizeof(ttl)) != 0) {
        fprintf(stderr,
                "nDAG: Failed to configure multicast TTL for %s:%s -- %s\n",
                groupaddr, portstr, strerror(errno));
        close(sock);
        sock = -1;
        goto sockcreateover;
    }

    if (setsockopt(sock,
            source->ai_family == PF_INET6 ? IPPROTO_IPV6: IPPROTO_IP,
            source->ai_family == PF_INET6 ? IPV6_MULTICAST_IF: IP_MULTICAST_IF,
            source->ai_addr, source->ai_addrlen) != 0) {
        fprintf(stderr,
                "nDAG: Failed to set outgoing multicast interface %s -- %s\n",
                srcaddr, strerror(errno));
        close(sock);
        sock = -1;
        goto sockcreateover;
    }

sockcreateover:
    freeaddrinfo(source);
    return sock;
}

void ndag_close_multicaster_socket(int ndagsock, struct addrinfo *targetinfo) {
    if (targetinfo) {
        freeaddrinfo(targetinfo);
    }

    if (ndagsock >= 0) {
        close(ndagsock);
    }
}

int ndag_send_encap_records(int sock, char *buf, uint32_t tosend,
        uint16_t reccount) {


    /* TODO actually construct and send datagrams to the multicast socket! */


    return reccount;
}

int ndag_send_encap_libtrace(int sock, libtrace_packet_t *packet) {

    /* TODO actually construct and send datagrams to the multicast socket! */
    return 1;
}


static uint32_t construct_beacon(char **buffer, ndag_beacon_params_t *nparams) {

    uint32_t beacsize = sizeof(ndag_common_t) +
            (sizeof(uint16_t) * (nparams->numstreams + 1));

    char *beac = (char *)malloc(beacsize);
    ndag_common_t *hdr;
    int i;
    uint16_t *next;

    if (beacsize > NDAG_MAX_DGRAM_SIZE) {
        fprintf(stderr, "nDAG beacon is too large to fit in a single datagram!\n");
        free(beac);
        return 0;
    }

    hdr = (ndag_common_t *)beac;
    hdr->magic = htonl(NDAG_MAGIC_NUMBER);
    hdr->version = NDAG_EXPORT_VERSION;
    hdr->type = NDAG_PKT_BEACON;
    hdr->monitorid = htons(nparams->monitorid);

    next = (uint16_t *)(beac + sizeof(ndag_common_t));

    *next = htons(nparams->numstreams);
    next ++;

    for (i = 0; i < nparams->numstreams; i++) {
        *next = htons(nparams->streamports[i]);
        next++;
    }

    *buffer = beac;
    return beacsize;

}


void *ndag_start_beacon(void *params) {

    ndag_beacon_params_t *nparams = (ndag_beacon_params_t *)params;
    int beacsock;
    char *beaconrec = NULL;
    uint32_t beacsize = 0;
    struct addrinfo *targetinfo = NULL;

    beacsock = ndag_create_multicaster_socket(nparams->beaconport,
            nparams->groupaddr, nparams->srcaddr, &targetinfo);

    if (beacsock == -1) {
        fprintf(stderr,
                "Failed to create multicast socket for nDAG beacon thread.\n");
        goto endbeaconthread;
    }

    if (targetinfo == NULL) {
        fprintf(stderr, "Failed to get addrinfo for nDAG beacon thread.\n");
        goto endbeaconthread;
    }

    beacsize = construct_beacon(&beaconrec, nparams);

    while (beacsize > 0 && beaconrec != NULL && !halted) {

        if (sendto(beacsock, beaconrec, beacsize, 0, targetinfo->ai_addr,
                    targetinfo->ai_addrlen) != beacsize) {
            fprintf(stderr, "Failed to send the full nDAG beacon: %s\n",
                    strerror(errno));
            break;
        }
        usleep(nparams->frequency * 1000);
    }

    if (beaconrec) {
        free(beaconrec);
    }

endbeaconthread:
    fprintf(stderr, "Halting nDAG beacon thread.\n");
    ndag_close_multicaster_socket(beacsock, targetinfo);
    pthread_exit(NULL);

}


// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
