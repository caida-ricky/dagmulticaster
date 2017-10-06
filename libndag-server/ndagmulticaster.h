#ifndef NDAGMULTICASTER_H_
#define NDAGMULTICASTER_H_

#include <stdlib.h>
#include <sys/types.h>
#include <libtrace.h>

#define NDAG_MAX_DGRAM_SIZE 8900

/* Required for multicasting NDAG records */
int ndag_create_multicaster_socket(uint16_t port, char *groupaddr);
int ndag_send_encap_records(int sock, char *buf, uint32_t tosend,
        uint16_t reccount);
int ndag_send_encap_libtrace(int sock, libtrace_packet_t *packet);


/* TODO define NDAG protocol headers in here */

#endif
// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
