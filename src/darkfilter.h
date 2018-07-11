
#ifndef DARKFILTER_H_
#define DARKFILTER_H_

#include <libtrace.h>
#include <wandio.h>

typedef struct darkfilter {

    uint32_t darknet;
    uint8_t *exclude;
    libtrace_t *dummytrace;
    libtrace_packet_t *packet;
    char *excl_file;
} darkfilter_t;

typedef struct darkfilter_conf {
    int first_octet;
    char *excl_file;
} darkfilter_params_t;

/* The create and destroy functions accept and return void ptrs so they
 * can be integrated with the callback functionality provided by
 * run_dag_streams().
 */
void *create_darkfilter(void *params);
int apply_darkfilter(darkfilter_t *state, libtrace_packet_t *packet);
void destroy_darkfilter(void *data);

#endif

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
