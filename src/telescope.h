
#ifndef TELESCOPE_H_
#define TELESCOPE_H_

#include <stdint.h>

#include "dagmultiplexer.h"

typedef struct torrent {
    /*
     * The color is a bitmask. This allows filters to overlap. Since the default
     * should be forwarding to the catch all multicast group we should optimze
     * for that specific case. In general we have to factors that decide where
     * to forward packets, the filterfile and mcastaddr.
     *
     *  filterfile | mcastaddr | action
     * ------------+-----------+-------
     *  set        | set       | send matchting packets to the group
     *  set        | NULL      | drop these packets
     *  NULL       | set       | default route (should be unique)
     *  NULL       | NULL      | error: print msg and exit
     *
     * Reserved bitmasks
     *  0x0 --> drop packets
     *  0x1 --> default route
     */
    color_t color;
    char *mcastaddr;
    char *srcaddr;
    char *filterfile;
    char *name;
    uint16_t mcastport;
    uint16_t mtu;
    uint16_t monitorid;
    struct torrent *next;
} torrent_t;

typedef struct telescope_glob {
    char *dagdev;
    char *statdir;
    int darknetoctet;
    int statinterval;
    int torrentcount;
    torrent_t *torrents;
} telescope_global_t;

telescope_global_t *telescope_init_global(char *filename);

void telescope_cleanup_torrent(torrent_t *torr);
void telescope_cleanup_global(telescope_global_t *conf);

#endif // TELESCOPE_H_

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
