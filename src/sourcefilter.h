#ifndef SOURCEFILTER_H_
#define SOURCEFILTER_H_

#include <wandio.h>
#include <libtrace.h>
#include <assert.h>
#include <signal.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include "dagmultiplexer.h"

#define CURRENT_SRCEXCLUDE(filter) ((filter)->excludefilter[(filter)->current_exclude])
#define HALFIPSPACE (1<<16)
#define IPHIGH(ip) (ip >> 16)
#define IPLO(ip) (ip & 0x0000FFFF)
#define PROTECTSTREAMCOLOR 2

//typedef uint8_t match_t;

typedef struct sourcefilter_file {
    color_t color;
    char *exclsrc;
    uint8_t exclude;
} sourcefilter_file_t;

typedef struct sourcefilterelement {
    color_t color;
    struct sourcefilterelement * loweroctet;
} sourcefilter_element_t;

typedef struct sourcefilter {
    //int filecnt;
    //only one stream can use source exclude filter
    sourcefilter_file_t *srcexcludefile;

    sourcefilter_element_t * excludefilter[2];
    volatile sig_atomic_t current_exclude;

} sourcefilter_filter_t;

sourcefilter_filter_t * create_sourcefilter_filter(sourcefilter_file_t * file);
int update_sourcefilter(sourcefilter_filter_t *filter);
int apply_sourcefilter(sourcefilter_filter_t * filter, struct in_addr ipsrc);
void destroy_sourcefilter_filter(sourcefilter_filter_t *filter);

#endif