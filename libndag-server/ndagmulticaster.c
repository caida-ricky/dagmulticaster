
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

static inline char *populate_common_header(char *bufstart,
        uint16_t monitorid, uint8_t pkttype) {

    ndag_common_t *hdr;
    hdr = (ndag_common_t *)bufstart;
    hdr->magic = htonl(NDAG_MAGIC_NUMBER);
    hdr->version = NDAG_EXPORT_VERSION;
    hdr->type = pkttype;
    hdr->monitorid = htons(monitorid);

    return bufstart + sizeof(ndag_common_t);
}

uint16_t ndag_send_encap_records(ndag_encap_params_t *params, char *buf,
        uint32_t tosend, uint16_t reccount) {

    ndag_encap_t *encap;
    uint32_t pktsize = 0;
    struct iovec sendbufs[2];
    struct msghdr mhdr;

    if (params->sendbuf == NULL) {
        params->sendbuf = (char *)malloc(NDAG_MAX_DGRAM_SIZE +
                sizeof(ndag_common_t) + sizeof(ndag_encap_t));
    }

    if (params->sendbuf == NULL) {
        fprintf(stderr, "Failed to allocate memory for nDAG encap. ERF!\n");
        return 0;
    }

    encap = (ndag_encap_t *)(populate_common_header(params->sendbuf,
            params->monitorid, NDAG_PKT_ENCAPERF));
    encap->seqno = htonl(params->seqno);
    encap->streamid = htons(params->streamnum);

    if (tosend > NDAG_MAX_DGRAM_SIZE) {
        /* Use MSB for indicating truncation */
        reccount |= 0x8000;
        pktsize = NDAG_MAX_DGRAM_SIZE + sizeof(ndag_common_t) +
                sizeof(ndag_encap_t);
        tosend = NDAG_MAX_DGRAM_SIZE;
    } else {
        pktsize = sizeof(ndag_common_t) + sizeof(ndag_encap_t) + tosend;
    }

    /* TODO add ability to compress the ERF records.
     * Use 2nd MSB for indicating compression.
     * Note: this may not play nicely with truncation?
     */
    encap->recordcount = ntohs(reccount);

    mhdr.msg_name = params->target->ai_addr;
    mhdr.msg_namelen = params->target->ai_addrlen;
    mhdr.msg_iov = sendbufs;
    mhdr.msg_iovlen = 2;
    mhdr.msg_control = NULL;
    mhdr.msg_controllen = 0;
    mhdr.msg_flags = 0;

    sendbufs[0].iov_base = params->sendbuf;
    sendbufs[0].iov_len = sizeof(ndag_common_t) + sizeof(ndag_encap_t);

    sendbufs[1].iov_base = buf;
    sendbufs[1].iov_len = tosend;

    if (sendmsg(params->sock, &mhdr, 0) != pktsize) {
        fprintf(stderr, "Failed to send the full nDAG encap. record: %s\n",
                strerror(errno));
        reccount = 0;
    }
    params->seqno += 1;

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
    int i;
    uint16_t *next;

    if (beac == NULL) {
        fprintf(stderr, "Failed to allocate memory for nDAG beacon!\n");
        return 0;
    }

    if (beacsize > NDAG_MAX_DGRAM_SIZE) {
        fprintf(stderr, "nDAG beacon is too large to fit in a single datagram!\n");
        free(beac);
        return 0;
    }

    next = (uint16_t *)(populate_common_header(beac, nparams->monitorid,
            NDAG_PKT_BEACON));

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
