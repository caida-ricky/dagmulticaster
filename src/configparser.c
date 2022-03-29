
#include <errno.h>
#include <yaml.h>
#include <assert.h>

#include "telescope.h"
#include "sourcefilter.h"

static int parse_onoff_option(char *value, uint8_t *opt) {
    if (strcmp(value, "yes") == 0 || strcmp(value, "true") == 0 ||
            strcmp(value, "on") == 0 || strcmp(value, "enabled") == 0) {
        *opt = 1;
    } else if (strcmp(value, "no") == 0 || strcmp(value, "false") == 0 ||
            strcmp(value, "off") == 0 || strcmp(value, "disabled") == 0) {
        *opt = 0;
    } else {
        return -1;
    }
    return 0;
}

static int parse_torrents(telescope_global_t *glob,
        yaml_document_t *doc, yaml_node_t *torrentlist) {

    yaml_node_item_t *item;
    int needsdefaults;
    int torrentcount = 0;
    int nostreamcount = 0;
    int nofiltercount = 0;
    int srcfiltercount = 0;
    torrent_t *current = NULL;
    torrent_t *new = NULL;

    /* Assign colors incrementally, starting at 0x1 << 1. We could keep a list
     * of colors in use to check if we have multiple filters that send to the
     * same multicast group. Seems a bit overkill though. */
    int nextcolorshift = 3;

    if (glob->torrents != NULL) {
        fprintf(stderr, "Config not empty.");
        return -1;
    }

    for (item = torrentlist->data.sequence.items.start;
            item != torrentlist->data.sequence.items.top;
                ++item) {
        yaml_node_t *node;
        yaml_node_pair_t *pair;
        needsdefaults = 0;

        /* Get next torrent entry. */
        node = yaml_document_get_node(doc, *item);
        if (node == NULL) {
            fprintf(stderr, "YAML parsing error.\n");
            return 0;
        }

        /* Create a new entry for this sequence item. */
        new = (torrent_t *)malloc(sizeof(torrent_t));
        if (new == NULL) {
            fprintf(stderr, "Failed to allocate memory for torrent config.\n");
            torrentcount = 0;
            goto torrentparseerror;
        }

        /* Initialize everything. */
        new->color = 0;
        new->mcastaddr = NULL;
        new->srcaddr = NULL;
        new->filterfile = NULL;
        new->sourcefilterfile = NULL;
        new->mcastport = 0;
        new->mtu = 0;
        new->monitorid = 0;
        new->next = NULL;
        new->name = NULL;
        new->ttl = 1;
        new->exclude = 1; // true

        /* Make sure save the list in the global state. */
        if (glob->torrents == NULL) {
            glob->torrents = new;
        } else {
            current->next = new;
        }
        current = new;

        /* Parse entries for this item. */
        for (pair = node->data.mapping.pairs.start;
                pair < node->data.mapping.pairs.top;
                    ++pair) {
            yaml_node_t *key, *value;

            key = yaml_document_get_node(doc, pair->key);
            value = yaml_document_get_node(doc, pair->value);

            if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                    && !strcmp((char *)key->data.scalar.value, "monitorid")) {
                current->monitorid =
                    (uint16_t) strtoul((char *)value->data.scalar.value, NULL, 10);
                needsdefaults = 1;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "mcastport")) {
                current->mcastport =
                    (uint16_t) strtoul((char *)value->data.scalar.value, NULL, 10);
                needsdefaults = 1;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "mcastaddr")) {
                current->mcastaddr = strdup((char *)value->data.scalar.value);
                needsdefaults = 1;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "srcaddr")) {
                current->srcaddr = strdup((char *)value->data.scalar.value);
                needsdefaults = 1;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "mtu")) {
                current->mtu =
                    (uint16_t) strtoul((char *)value->data.scalar.value, NULL, 10);
                needsdefaults = 1;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "ttl")) {
                current->ttl =
                    (uint8_t) strtoul((char *)value->data.scalar.value, NULL, 10);
                needsdefaults = 1;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "sourcefilterfile")) {
                current->sourcefilterfile = strdup((char *)value->data.scalar.value);
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "filterfile")) {
                current->filterfile = strdup((char *)value->data.scalar.value);
            }


            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "name")) {
                current->name = strdup((char *)value->data.scalar.value);
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "exclude")) {
                if (parse_onoff_option((char *)value->data.scalar.value,
                                       &current->exclude) != 0) {
                    fprintf(stderr, "Not a viable option 'exclude': %s.\n",
                        (char *)value->data.scalar.value);
                    goto torrentparseerror;
                }
            }
        }

        /* Set defaults if any entry besides the filterfile is set. */
        if (needsdefaults) {
            if (current->mcastaddr == NULL) {
                current->mcastaddr = strdup("225.0.0.225");
            }

            if (current->srcaddr == NULL) {
                fprintf(stderr, "Warning: no source address specified. Using "
                    "default interface.\n");
                current->srcaddr = strdup("0.0.0.0");
            }

            if (current->monitorid == 0) {
                fprintf(stderr,
                    "0 is not a valid monitor ID -- choose another number.\n");
                goto torrentparseerror;
            }

            if (current->name == NULL) {
                fprintf(stderr,
                    "Please specify a name for each multicast sink.\n");
                goto torrentparseerror;
            }
        }

        /* Assign color to filter, see telescope.h for rules. */

        if (current->sourcefilterfile == NULL){
            //source filter is NULL
            if (current->filterfile != NULL && current->mcastaddr !=  NULL) {
                //if (nextcolorshift >= 8) {
                if (nextcolorshift >= 8) {
                    //reserve 7 for source filter
                    fprintf(stderr,
                        "Too many streams. Cannot handle more than eight multicast "
                        "groups.\n");
                    goto torrentparseerror;
                }
                current->color = 0x1 << nextcolorshift;
                ++nextcolorshift;
            }else if (current->filterfile != NULL && current->mcastaddr == NULL) {
                current->color = 0x0;
                ++nostreamcount;
                if (current->exclude == 0) {
                    fprintf(stderr,
                        "WARNING: Filters that drop packets must be excluded from "
                        "the default sink, enabling flag.\n");
                    current->exclude = 1;
                }
            }else if (current->filterfile == NULL && current->mcastaddr != NULL) {
                current->color = 0x1;
                ++nofiltercount;
                if (current->exclude == 0) {
                    fprintf(stderr,
                        "WARNING: Exclude flag has no effect on the default sink.\n");
                }
            }else {
                /* All NULL. */
                fprintf(stderr,
                    "Found empty entry. Specify a sourcefilter, filterfile, or mcastaddr.\n");
                goto torrentparseerror;
            }
        }else{
            /* Source filter is not NULL*/
            if (current->filterfile == NULL && current->mcastaddr != NULL){
                current->color = 0x1 << PROTECTSTREAMCOLOR;
                ++srcfiltercount;    
            }else if (current->filterfile != NULL){
                fprintf(stderr,
                    "Source filter is not compatiable with exclude filter.\n");
                goto torrentparseerror;
            }else{
                /* mcastaddr is NULL*/
                fprintf(stderr,
                    "Specify a mcastaddr for sourcefilter.\n");
                goto torrentparseerror;
            }
        }

        /* Got one. */
        ++torrentcount;
    }

    /* There should be exactly one default sink. */
    if (nofiltercount != 1) {
        fprintf(stderr, "Please specify exactly one default sink, i.e., "
            "an entry without a filterfile.\n");
        goto torrentparseerror;
    }
    /* Only allow one source IP filter */
    if (srcfiltercount >1 ) {
        fprintf(stderr, "Only support at most one source filter\n");
        goto torrentparseerror;
    }

    /* Found more than one entry with only a filterfile. */
    if (nostreamcount > 1) {
        fprintf(stderr, "Warning: More than one filter drops packets.\n");
    }

    /* Move default route to the front of the list. */
    new = glob->torrents;
    current = NULL;
    /* Find entry. */
    while (new->color != 1) {
        /* We checked above that the default sink exists. */
        assert(new != NULL);
        current = new;
        new = new->next;
    }
    if (current != NULL) {
      /* Move it. */
      current->next = new->next;
      new->next = glob->torrents;
      glob->torrents = new;
    }

    glob->torrentcount = torrentcount;
    return torrentcount;

torrentparseerror:
    current = glob->torrents;
    while (current != NULL) {
        glob->torrents = current->next;
        telescope_cleanup_torrent(current);
        current = glob->torrents;
    }
    glob->torrentcount = 0;
    return -1;
}

static int parse_option(telescope_global_t *glob, yaml_document_t *doc,
        yaml_node_t *key, yaml_node_t *value) {
    int torrentcount = 0;

    if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
            && !strcmp((char *)key->data.scalar.value, "dagdev")) {
        glob->dagdev = strdup((char *)value->data.scalar.value);
    }

    else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                 && !strcmp((char *)key->data.scalar.value, "darknetoctet")) {
        glob->darknetoctet= atoi((char *)value->data.scalar.value);
    }

    else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                 && !strcmp((char *)key->data.scalar.value, "statinterval")) {
        glob->statinterval = atoi((char *)value->data.scalar.value);
    }

    else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                 && !strcmp((char *)key->data.scalar.value, "statdir")) {
        glob->statdir = strdup((char *)value->data.scalar.value);
    }

    if (key->type == YAML_SCALAR_NODE && value->type == YAML_SEQUENCE_NODE
            && !strcmp((char *)key->data.scalar.value, "outputs")) {
        torrentcount = parse_torrents(glob, doc, value);
        if (torrentcount < 0) {
            return -1;
        }
        glob->torrentcount = torrentcount;
    }

    return 1;
}

static int parse_yaml(telescope_global_t* glob,
                      char *configfile,
                      int (*parsefunc)(telescope_global_t* glob,
                          yaml_document_t *doc, yaml_node_t *,
                          yaml_node_t *)) {
    FILE *in = NULL;
    int ret = 0;

    /* YAML config parser. */
    yaml_parser_t parser;
    yaml_document_t document;
    yaml_node_t *root, *key, *value;
    yaml_node_pair_t *pair;

    if ((in = fopen(configfile, "r")) == NULL) {
        fprintf(stderr, "Failed to open config file: %s.\n", strerror(errno));
        return -1;
    }

    yaml_parser_initialize(&parser);
    yaml_parser_set_input_file(&parser, in);

    if (!yaml_parser_load(&parser, &document)) {
        fprintf(stderr, "Malformed config file.\n");
        ret = -1;
        goto yamlfail;
    }

    root = yaml_document_get_root_node(&document);
    if (!root) {
        fprintf(stderr, "Config file is empty!\n");
        ret = -1;
        goto endconfig;
    }

    if (root->type != YAML_MAPPING_NODE) {
        fprintf(stderr, "Top level of config should be a map.\n");
        ret = -1;
        goto endconfig;
    }

    /* Parse values */
    for (pair = root->data.mapping.pairs.start;
            pair < root->data.mapping.pairs.top;
                ++pair) {

        key = yaml_document_get_node(&document, pair->key);
        value = yaml_document_get_node(&document, pair->value);

        if ((ret = parsefunc(glob, &document, key, value)) <= 0) {
            break;
        }
        ret = 0;
    }

endconfig:
    yaml_document_delete(&document);
    yaml_parser_delete(&parser);

yamlfail:
    fclose(in);
    return ret;
}

telescope_global_t *telescope_init_global(char *configfile) {
    telescope_global_t *glob = NULL;

    if (configfile == NULL) {
        return NULL;
    }

    glob = (telescope_global_t *)malloc(sizeof(telescope_global_t));
    if (glob == NULL) {
        fprintf(stderr, "Failed to allocate memory for global variables\n");
        return NULL;
    }

    /* Initialization. */
    glob->dagdev = NULL;
    glob->statdir = NULL;
    glob->darknetoctet = -1;
    glob->statinterval = 0;
    glob->torrentcount = 0;
    glob->torrents = NULL;

    /* Parse config file. */
    if (parse_yaml(glob, configfile, parse_option) == -1) {
        telescope_cleanup_global(glob);
        return NULL;
    }

    /* Try to set a sensible defaults. */
    if (glob->dagdev == NULL) {
        glob->dagdev = strdup("/dev/dag0");
    }

    /* All done. */
    return glob;
}

void telescope_cleanup_torrent(torrent_t *torr) {
    if (torr == NULL) {
        return;
    }

    if (torr->mcastaddr) {
        free(torr->mcastaddr);
    }

    if (torr->srcaddr) {
        free(torr->srcaddr);
    }

    if (torr->filterfile) {
        free(torr->filterfile);
    }

    if (torr->sourcefilterfile) {
        free(torr->sourcefilterfile);
    }

    if (torr->name) {
        free(torr->name);
    }

    free(torr);
}

void telescope_cleanup_global(telescope_global_t *glob) {
    /* Clean up torrent list. */
    torrent_t *itr = glob->torrents;
    while (itr != NULL) {
        glob->torrents = itr->next;
        telescope_cleanup_torrent(itr);
        itr = glob->torrents;
    }

    /* Clean up other members. */
    if (glob == NULL) {
      return;
    }

    if (glob->dagdev) {
        free(glob->dagdev);
    }

    if (glob->statdir) {
        free(glob->statdir);
    }

    free(glob);
}

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
