
#ifndef TELESCOPE_H_
#define TELESCOPE_H_

#include <stdint.h>

typedef struct torrent {
    char *mcastaddr;
    char *srcaddr;
    char *filterfile;
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
