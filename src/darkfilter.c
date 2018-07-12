/* Original author: Alistair King, CAIDA    <alistair@caida.org>
 *
 * Adapted to the dagmulticaster by Shane Alcock, University of Waikato
 *      <salcock@waikato.ac.nz>
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <wandio.h>

#include "darkfilter.h"

/* max number of /24s in a /8 darknet */
#define EXCLUDE_LEN (1<<16)

/* semi-hax to ignore the darknet network itself in the exclusion list */
#define MIN_PFX_LEN 15

#define CURRENT_EXCLUDE(filter) ((filter)->exclude[(filter)->current_exclude])

/* TODO port to libwandio?? */
static off_t wandio_fgets(io_t *file, void *buffer, off_t len, int chomp)
{
    char cbuf;
    int rval;
    int i;
    int done = 0;

    if (file == NULL) {
        return 0;
    }

    if(buffer == NULL || len <= 0)
    {
        return 0;
    }

    for(i=0; !done && i < len-1; i++)
    {
        if((rval = wandio_read(file, &cbuf, 1)) < 0)
        {
            return rval;
        }
        if(rval == 0)
        {
            done = 1;
            i--;
        }
        else
        {
            ((char*)buffer)[i] = cbuf;
            if(cbuf == '\n')
            {
                if(chomp != 0)
                {
                    ((char*)buffer)[i] = '\0';
                }
                done = 1;
            }
        }
    }

    ((char*)buffer)[i] = '\0';
    return i;
}

static int parse_excl_file(uint8_t *exclude, const char *excl_file) {
    io_t *file;
    char buf[1024];
    char *mask_str;
    int mask;

    uint32_t addr;

    uint32_t first_addr;
    uint32_t last_addr;

    uint32_t first_slash24;
    uint32_t last_slash24;

    uint64_t x;

    int cnt = 0;
    int overlaps = 0;
    int idx;

    if ((file = wandio_create(excl_file)) == NULL) {
        fprintf(stderr, "Failed to open exclusion file %s\n", excl_file);
        return -1;
    }

    while (wandio_fgets(file, buf, 1024, 1) != 0) {
        // split the line to get ip and len
        if ((mask_str = strchr(buf, '/')) == NULL) {
            fprintf(stderr, "ERROR: Malformed prefix for darkfilter: %s\n",
                    buf);
            goto err;
        }
        *mask_str = '\0';
        mask_str++;

        // convert the ip and mask to a number
        addr = inet_addr(buf);
        addr = ntohl(addr);
        mask = atoi(mask_str);
        if (mask < 0 || mask > 32) {
            fprintf(stderr, "ERROR: Malformed prefix for darkfilter: %s/%s\n",
                    buf, mask_str);
            goto err;
        }
        if (mask < MIN_PFX_LEN) {
          fprintf(stderr, "[darkfilter] WARN: Ignoring short prefix: %s/%s\n",
                  buf, mask_str);
          continue;
        }
        // compute the /24s that this prefix covers
        // perhaps not the most efficient way to do this, but i've borrowed it
        // from other code that I'm sure actually works, and this only happens
        // once at startup, so whatevs ;)
        first_addr = addr & (~0 << (32-mask));
        last_addr = first_addr + (1<<(32-mask))-1;

        first_slash24 = (first_addr/256)*256;
        last_slash24 = (last_addr/256)*256;

        for(x = first_slash24; x <= last_slash24; x += 256) {
            idx = (x&0x00FFFF00)>>8;
            if (exclude[idx] == 0) {
                exclude[idx] = 1;
                cnt++;
            } else {
                overlaps++;
            }
        }
    }

    fprintf(stderr, "[darkfilter] INFO: Excluding %d /24s\n", cnt);
    fprintf(stderr, "[darkfilter] INFO: Overlaps %d /24s\n", overlaps);

    wandio_destroy(file);

    return 0;

err:
    wandio_destroy(file);
    return -1;
}

darkfilter_filter_t *create_darkfilter_filter(int first_octet, char *excl_file) {
    darkfilter_filter_t *filter;
    int i;

    filter = malloc(sizeof(darkfilter_filter_t));
    if (!filter) {
        goto err;
    }

    if (first_octet < 0 || first_octet > 255) {
        fprintf(stderr, "ERROR: Invalid first octet for darkfilter: %d\n",
                first_octet);
        fprintf(stderr,
                "Check that you have set the darknet octet option correctly\n");
        goto err;
    }

    filter->excl_file = excl_file;
    filter->darknet = first_octet << 24;

    for (i=0; i<2; i++) {
        if ((filter->exclude[i] =
             calloc(EXCLUDE_LEN, sizeof(uint8_t))) == NULL) {
            goto err;
        }
    }
    filter->current_exclude = 0;

    if (parse_excl_file(CURRENT_EXCLUDE(filter), filter->excl_file) != 0) {
      goto err;
    }

    return filter;

 err:
    destroy_darkfilter_filter(filter);
    return NULL;
}

void destroy_darkfilter_filter(darkfilter_filter_t *filter) {
    int i;

    if (!filter) {
        return;
    }
    for (i=0; i<2; i++) {
        free(filter->exclude[i]);
    }
    free(filter);
}

int update_darkfilter_exclusions(darkfilter_filter_t *filter) {
    uint8_t *excl = filter->exclude[!filter->current_exclude];
    memset(excl, 0, EXCLUDE_LEN);
    if (parse_excl_file(excl, filter->excl_file) != 0) {
        return -1;
    }
    filter->current_exclude = !filter->current_exclude;
    return 0;
}

int apply_darkfilter(darkfilter_t *state, char *pktbuf) {
    /* Return 1 if packet should NOT be discarded, i.e. it doesn't match
     * any of our exclusion /24s */

    /* Return 0 to exclude the packet. */

    libtrace_ip_t  *ip_hdr  = NULL;
    uint32_t ip_addr;

    /* prepare a libtrace packet */
    if (trace_prepare_packet(state->dummytrace, state->packet, pktbuf,
                             TRACE_RT_DATA_ERF,
                             TRACE_PREP_DO_NOT_OWN_BUFFER) == -1) {
        fprintf(stderr,
                "Unable to convert DAG buffer contents to libtrace packet.\n");
        return -1;
    }

    /* check for ipv4 */
    if((ip_hdr = trace_get_ip(state->packet)) == NULL) {
        /* not an ip packet */
        goto skip;
    }
    ip_addr = htonl(ip_hdr->ip_dst.s_addr);

    if(((ip_addr & 0xFF000000) != state->filter->darknet) ||
       (CURRENT_EXCLUDE(state->filter)[(ip_addr & 0x00FFFF00) >> 8] != 0)) {
        goto skip;
    }

    return 1;

skip:
    return 0;
}

void *create_darkfilter(void *params) {
    darkfilter_filter_t *filter = (darkfilter_filter_t *)params;
    darkfilter_t *state = NULL;

    state = (darkfilter_t *)malloc(sizeof(darkfilter_t));
    if (!state) {
        goto err;
    }

    state->filter = filter;
    state->dummytrace = trace_create_dead("erf:dummy.erf");
    state->packet = trace_create_packet();

    return (void *)state;

 err:
    destroy_darkfilter(state);
    return NULL;
}

void destroy_darkfilter(void *data) {
    darkfilter_t *state = (darkfilter_t *)data;

    if (!state) {
      return;
    }

    if (state->packet) {
      trace_destroy_packet(state->packet);
    }
    if (state->dummytrace) {
        trace_destroy_dead(state->dummytrace);
    }

    free(state);
}

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
