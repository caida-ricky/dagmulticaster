
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>

#include "ndagmulticaster.h"

int ndag_create_multicaster_socket(uint16_t port, char *groupaddr) {

    struct addrinfo hints;
    struct addrinfo *gotten;
    char portstr[16];
    int sock;
    uint32_t ttl = 1;       /* TODO make this configurable */

    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = 0;

    snprintf(portstr, 15, "%u", port);

    if (getaddrinfo(groupaddr, portstr, &hints, &gotten) != 0) {
        fprintf(stderr, "Call to getaddrinfo failed for %s:%s -- %s\n",
                groupaddr, portstr, strerror(errno));
        return -1;
    }

    sock = socket(gotten->ai_family, gotten->ai_socktype, 0);
    if (sock < 0) {
        fprintf(stderr, "Failed to create multicast socket for %s:%s -- %s\n",
                groupaddr, portstr, strerror(errno));
        freeaddrinfo(gotten);
        return -1;
    }

    if (setsockopt(sock,
            gotten->ai_family = PF_INET6 ? IPPROTO_IPV6: IPPROTO_IP,
            gotten->ai_family = PF_INET6 ? IPV6_MULTICAST_HOPS :
                    IP_MULTICAST_TTL,
            (char *)&ttl, sizeof(ttl)) != 0) {
        fprintf(stderr, "Failed to configure multicast TTL for %s:%s -- %s\n",
                groupaddr, portstr, strerror(errno));
        freeaddrinfo(gotten);
        return -1;
    }

    freeaddrinfo(gotten);
    return sock;
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


// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
