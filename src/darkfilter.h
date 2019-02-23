
#ifndef DARKFILTER_H_
#define DARKFILTER_H_

#include <libtrace.h>
#include <wandio.h>

typedef struct darkfilter_file {
  uint8_t color;
  char *excl_file;
} darkfilter_file_t;

typedef struct filter {
    int filecnt;
    darkfilter_file_t *files;

    uint32_t darknet;

    uint8_t *exclude[2];
    volatile sig_atomic_t current_exclude;

} darkfilter_filter_t;

typedef struct darkfilter {
    darkfilter_filter_t *filter; // shared filter state
    libtrace_t *dummytrace;
    libtrace_packet_t *packet;
} darkfilter_t;

darkfilter_filter_t *create_darkfilter_filter(int first_octet, int cnt,
        darkfilter_file_t* files);
void destroy_darkfilter_filter(darkfilter_filter_t *filter);
int update_darkfilter_exclusions(darkfilter_filter_t *filter);

/* The create and destroy functions accept and return void ptrs so they
 * can be integrated with the callback functionality provided by
 * run_dag_streams().
 */
void *create_darkfilter(void *filter);
int apply_darkfilter(darkfilter_t *df, char *pktbuf);
void destroy_darkfilter(void *df);

#endif

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
