
#include <errno.h>
#include <yaml.h>

#include "telescope.h"

static int parse_torrents(telescope_global_t *glob,
        yaml_document_t *doc, yaml_node_t *pluginlist) {

    yaml_node_item_t *item;
    int torrentcount = 0;
    torrent_t *current = NULL;

    if (glob->torrents != NULL) {
        fprintf(stderr, "Config not empty.");
        return -1;
    }

    printf("starting sequnce\n");
    for (item = pluginlist->data.sequence.items.start;
            item != pluginlist->data.sequence.items.top;
                ++item) {
        printf(" next item\n");
        yaml_node_t *node;
        yaml_node_pair_t *pair;
        int non_filter_entires = 0;

        node = yaml_document_get_node(doc, *item);
        if (node == NULL) {
            fprintf(stderr, "YAML parsing error.\n");
            return 0;
        }

        /* Create a new entry for this sequence item. */
        current = (torrent_t *)malloc(sizeof(torrent_t));
        if (current == NULL) {
            fprintf(stderr, "Failed to allocate memory for torrent config.\n");
            torrentcount = 0;
            goto cleanuptorrents;
        }

        /* Initialize everything. */
        current->mcastaddr = NULL;
        current->srcaddr = NULL;
        current->filterfile = NULL;
        current->mcastport = 0;
        current->mtu = 0;
        current->monitorid = 0;
        current->next = NULL;

        /* Make sure save the list in the global state. */
        if (glob->torrents == NULL) {
            glob->torrents = current;
        }

        /* Parse entries for this item. */
        for (pair = node->data.mapping.pairs.start;
                pair < node->data.mapping.pairs.top;
                    ++pair) {
            yaml_node_t *key, *value;

            key = yaml_document_get_node(doc, pair->key);
            value = yaml_document_get_node(doc, pair->value);
            printf("  key: %s, value: %s\n", (char *)key->data.scalar.value, 
                    (char *)value->data.scalar.value);

            if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                    && !strcmp((char *)key->data.scalar.value, "monitorid")) {
                current->monitorid = (uint16_t) strtoul((char *)value->data.scalar.value, NULL, 10);
                ++non_filter_entires;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "mcastport")) {
                current->mcastport = (uint16_t) strtoul((char *)value->data.scalar.value, NULL, 10);
                ++non_filter_entires;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "mcastaddr")) {
                current->mcastaddr = strdup((char *)value->data.scalar.value);
                ++non_filter_entires;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "srcaddr")) {
                current->srcaddr = strdup((char *)value->data.scalar.value);
                ++non_filter_entires;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "mtu")) {
                current->mtu = (uint16_t) strtoul((char *)value->data.scalar.value, NULL, 10);
                ++non_filter_entires;
            }

            else if (key->type == YAML_SCALAR_NODE && value->type == YAML_SCALAR_NODE
                         && !strcmp((char *)key->data.scalar.value, "filterfile")) {
                current->filterfile= strdup((char *)value->data.scalar.value);
            }
        }
        printf(" end item\n");

        /* TODO: Not sure this makes sense in this case. */
        if (non_filter_entires > 0) {
            if (current->mcastaddr == NULL) {
                current->mcastaddr = strdup("225.0.0.225");
            }

            if (current->srcaddr == NULL) {
                fprintf(stderr,
                    "Warning: no source address specified. Using default interface.\n");
                current->srcaddr = strdup("0.0.0.0");
            }

            if (current->monitorid == 0) {
                fprintf(stderr,
                    "0 is not a valid monitor ID -- choose another number.\n");
                goto cleanuptorrents;
            }
        }

        /* Next torrent. */
        current = current->next;
        ++torrentcount;
    }
    printf("end sequence (%d items)\n", torrentcount);

    glob->torrentcount = torrentcount;
    return torrentcount;

cleanuptorrents:
    current = glob->torrents;
    while (current != NULL) {
        glob->torrents = current->next;
        telescope_cleanup_torrent(current);
        current = glob->torrents;
    }
    glob->torrentcount = 0;
    return 0;
}

static int parse_option(telescope_global_t *glob, yaml_document_t *doc,
        yaml_node_t *key, yaml_node_t *value) {

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
        glob->torrentcount = parse_torrents(glob, doc, value);
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
    if (torr->mcastaddr) {
        free(torr->mcastaddr);
    }

    if (torr->srcaddr) {
        free(torr->srcaddr);
    }

    if (torr->filterfile) {
        free(torr->filterfile);
    }
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
