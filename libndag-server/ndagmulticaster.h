#ifndef NDAGMULTICASTER_H_
#define NDAGMULTICASTER_H_

#include <stdlib.h>
#include <sys/types.h>
#include <libtrace.h>

/* Maximum size for non-encapsulated messages (e.g. beacons or
 * keepalives).
 *
 * Maximum size for encapsulated messages (e.g. encap ERF) is set
 * via ndag_encap_params_t, can be a different value (but should be
 * less than your MTU!) and should be configurable by the user.
 */
#define NDAG_MAX_DGRAM_SIZE (1400)

#define NDAG_MAGIC_NUMBER (0x4E444147)
#define NDAG_EXPORT_VERSION 1

/* Note: libtrace.h provides PACKED for us... */

/* Required for multicasting NDAG records */

typedef struct ndagbeaconparams {
    char *srcaddr;
    char *groupaddr;
    uint16_t beaconport;
    uint16_t numstreams;
    uint16_t *streamports;
    uint32_t frequency;
    uint16_t monitorid;
} ndag_beacon_params_t;

typedef struct ndagencapparams {
    int sock;
    uint16_t monitorid;
    uint16_t streamnum;
    uint32_t seqno;
    struct addrinfo *target;
    char *sendbuf;
    int compresslevel;
    uint64_t starttime;
    uint16_t maxdgramsize;
} ndag_encap_params_t;

enum {
    NDAG_PKT_BEACON = 0x01,
    NDAG_PKT_ENCAPERF = 0x02,
    //NDAG_PKT_RESTARTED = 0x03,
    NDAG_PKT_ENCAPRT = 0x04,
    NDAG_PKT_KEEPALIVE = 0x05,
};

/* == Protocol header structures == */

/* Common header -- is prepended to all exported records */
typedef struct ndag_common_header {
    uint32_t magic;
    uint8_t version;
    uint8_t type;
    uint16_t monitorid;
} PACKED ndag_common_t;

/* Beacon -- structure is too simple to be worth defining as a struct */
/*
 * uint16_t numberofstreams;
 * uint16_t firststreamport;
 * uint16_t secondstreamport;
 * ....
 * uint16_t laststreamport;
 */

/* Encapsulation header -- used by both ENCAPERF and ENCAPRT records */
typedef struct ndag_encap {
    uint64_t started;
    uint32_t seqno;
    uint16_t streamid;
    uint16_t recordcount; /* acts as RT type for ENCAPRT records */
} PACKED ndag_encap_t;

int ndag_interrupt_beacon(void);
void *ndag_start_beacon(void *params);
int ndag_create_multicaster_socket(uint16_t port, char *groupaddr,
        char *srcaddr, struct addrinfo **targetinfo);
void ndag_close_multicaster_socket(int ndagsock, struct addrinfo *targetinfo);
uint16_t ndag_send_encap_records(ndag_encap_params_t *params, char *buf,
        uint32_t tosend, uint16_t reccount);
int ndag_send_keepalive(ndag_encap_params_t *params);
int ndag_send_encap_libtrace(int sock, libtrace_packet_t *packet);


#endif
// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
