#define _GNU_SOURCE

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <unistd.h>
#include <dagapi.h>
#include <pthread.h>

#include "byteswap.h"
#include "ndagmulticaster.h"

#if HAVE_SENDMMSG
#else
#include <sys/syscall.h>
#endif

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
    int bufsize;

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

#if 0
    // AK disables to allow spitzer to consume its own stream
    if (setsockopt(sock,
            source->ai_family == PF_INET6 ? IPPROTO_IPV6: IPPROTO_IP,
            source->ai_family == PF_INET6 ? IPV6_MULTICAST_LOOP: IP_MULTICAST_LOOP,
            &disable, sizeof(disable)) != 0) {
        fprintf(stderr,
                "nDAG: Failed to disable looping on multicast interface %s -- %s\n",
                srcaddr, strerror(errno));
        close(sock);
        sock = -1;
        goto sockcreateover;
    }
#endif

    bufsize = 16 * 1024 * 1024;
    if (setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &bufsize,
                (socklen_t)sizeof(int)) != 0) {
        fprintf(stderr,
                "nDAG: Failed to increase buffer size on multicast interface %s -- %s\n",
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

void ndag_init_encap(ndag_encap_params_t *params, int sock,
        struct addrinfo *targetinfo, uint16_t monitorid, uint16_t streamid,
        uint64_t start, uint16_t mtu, int compress) {

    int i, j;

    params->sock = sock;
    params->target = targetinfo;
    params->streamnum = streamid;
    params->seqno = 1;
    params->starttime = start;
    params->maxdgramsize = mtu;
    params->monitorid = monitorid;
    params->mmsgbufs = (struct mmsghdr *)calloc(sizeof(struct mmsghdr),
            NDAG_BATCH_SIZE);

    for (i = 0; i < NDAG_BATCH_SIZE; i++) {
        params->headerspace[i] = (char *)calloc(1, NDAG_MAX_DGRAM_SIZE);
        params->mmsgbufs[i].msg_hdr.msg_iov = (struct iovec *)malloc(
                sizeof(struct iovec) * 2);
        params->iovec_count[i] = 2;
        for (j = 0; j < params->iovec_count[i]; j++) {
            params->mmsgbufs[i].msg_hdr.msg_iov[j].iov_base = NULL;
            params->mmsgbufs[i].msg_hdr.msg_iov[j].iov_len = 0;
        }
    }

}

void ndag_destroy_encap(ndag_encap_params_t *params) {
    int i;

    for (i = 0; i < NDAG_BATCH_SIZE; i++) {
        free(params->headerspace[i]);
        free(params->mmsgbufs[i].msg_hdr.msg_iov);
    }
    free(params->mmsgbufs);
}

void ndag_reset_encap_state(ndag_encap_params_t *params) {

    int i, j;

    for (i = 0; i < NDAG_BATCH_SIZE; i++) {
        params->mmsgbufs[i].msg_len = 0;
        params->mmsgbufs[i].msg_hdr.msg_name = params->target->ai_addr;
        params->mmsgbufs[i].msg_hdr.msg_namelen = params->target->ai_addrlen;

        for (j = 0; j < params->iovec_count[i]; j++) {
            params->mmsgbufs[i].msg_hdr.msg_iov[j].iov_base = NULL;
            params->mmsgbufs[i].msg_hdr.msg_iov[j].iov_len = 0;
        }
        params->mmsgbufs[i].msg_hdr.msg_iovlen = 0;
        params->mmsgbufs[i].msg_hdr.msg_control = NULL;
        params->mmsgbufs[i].msg_hdr.msg_controllen = 0;
        params->mmsgbufs[i].msg_hdr.msg_flags = 0;
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

uint16_t ndag_push_encap_iovecs(ndag_encap_params_t *params,
        struct iovec *iovecs, uint16_t num_iov, uint16_t reccount, int index) {

    ndag_encap_t *encap;
    int i;

    encap = (ndag_encap_t *)(populate_common_header(params->headerspace[index],
            params->monitorid, NDAG_PKT_ENCAPERF));
    encap->started = params->starttime;
    encap->seqno = htonl(params->seqno);
    encap->streamid = htons(params->streamnum);

    if (params->iovec_count[index] < num_iov + 1) {
        params->mmsgbufs[index].msg_hdr.msg_iov = realloc(
                params->mmsgbufs[index].msg_hdr.msg_iov, (num_iov + 1) *
                sizeof(struct iovec));
        params->iovec_count[index] = num_iov + 1;
    }

    params->mmsgbufs[index].msg_hdr.msg_iov[0].iov_base =
            params->headerspace[index];
    params->mmsgbufs[index].msg_hdr.msg_iov[0].iov_len =
            sizeof(ndag_common_t) + sizeof(ndag_encap_t);

    if (num_iov == 1 && iovecs[0].iov_len > params->maxdgramsize -
                sizeof(ndag_common_t) - sizeof(ndag_encap_t)) {

        /* Just the one record and it is too big to send, so truncate it */
        dag_record_t *erfptr = (dag_record_t *)iovecs[0].iov_base;
        reccount |= 0x8000;

        erfptr->rlen = htons(params->maxdgramsize - sizeof(ndag_common_t) -
                sizeof(ndag_encap_t));
        iovecs[0].iov_len = params->maxdgramsize - sizeof(ndag_common_t) -
                sizeof(ndag_encap_t);
    }

    for (i = 0; i < num_iov; i++) {
        params->mmsgbufs[index].msg_hdr.msg_iov[i+1].iov_base = iovecs[i].iov_base;
        params->mmsgbufs[index].msg_hdr.msg_iov[i+1].iov_len = iovecs[i].iov_len;
    }

    params->mmsgbufs[index].msg_hdr.msg_iovlen = num_iov + 1;

    encap->recordcount = htons(reccount);
    params->seqno += 1;
    if (params->seqno == 0) {
        params->seqno ++;
    }

    return reccount & 0x3fff;
}

uint16_t ndag_send_encap_records(ndag_encap_params_t *params, int msgcount) {
    int reccount = msgcount;

    if (msgcount == 0) {
        return 0;
    }

#if HAVE_SENDMMSG
    if (sendmmsg(params->sock, params->mmsgbufs, msgcount, 0) != msgcount) {
        fprintf(stderr, "Failed to send nDAG encap. records: %s\n",
                strerror(errno));
        reccount = 0;
    }
#else
    if (syscall(__NR_sendmmsg, params->sock, params->mmsgbufs, msgcount,
                0) != msgcount) {
        fprintf(stderr, "Failed to send nDAG encap. records: %s\n",
                strerror(errno));
        reccount = 0;
    }
#endif
    return reccount;
}

int ndag_send_encap_libtrace(int sock, libtrace_packet_t *packet) {

    /* TODO actually construct and send datagrams to the multicast socket! */
    return 1;
}

int ndag_send_keepalive(ndag_encap_params_t *params) {

    char *alive = (char *)malloc(sizeof(ndag_common_t));

    populate_common_header(alive, params->monitorid, NDAG_PKT_KEEPALIVE);

    if (sendto(params->sock, alive, sizeof(ndag_common_t), 0,
            params->target->ai_addr, params->target->ai_addrlen) !=
            sizeof(ndag_common_t)) {
        fprintf(stderr, "Failed to send an nDAG keep alive message: %s\n",
                strerror(errno));
        return -1;
    }
    free(alive);

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
