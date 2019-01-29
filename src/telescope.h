
#ifndef TELESCOPE_H_
#define TELESCOPE_H_

#include <stdint.h>

typedef struct telescope_glob {
    char *dagdev;
    char *statdir;
    char *filterfile;
    char *srcaddr;
    char *mcastaddr;
    uint16_t mcastport;
    uint16_t monitorid;
    uint16_t mtu;
    int darknetoctet;
    int statinterval;
} telescope_global_t;

telescope_global_t *telescope_init_global(char *filename);

void telescope_cleanup_global(telescope_global_t *conf);

#endif // TELESCOPE_H_

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
