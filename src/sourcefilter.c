#include "sourcefilter.h"

static int parse_excl_source_file(sourcefilter_element_t *exclude, const sourcefilter_file_t *filter_file) {
    io_t *file;
    char buf[1024];
    uint32_t addr, addrhi, addrlo;
    int i;

    //only allow source filter in "protect" stream
    assert(filter_file->color == PROTECTSTREAMCOLOR);
    if ((file = wandio_create(filter_file->exclsrc)) == NULL) {
        fprintf(stderr, "Failed to open exclusion file %s\n", filter_file->exclsrc);
        return -1;
    }

    while (wandio_fgets(file, buf, 1024, 1) != 0) {
        addr = inet_addr(buf);
        addr = ntohl(addr);
        addrhi = IPHIGH(addr) ; //higher octet of IP address
        addrlo = IPLO(addr) ;
        if (exclude[addrhi].loweroctet==NULL){
            exclude[addrhi].loweroctet = calloc(HALFIPSPACE, sizeof(sourcefilter_element_t));
            if (exclude[addrhi].loweroctet == NULL) {
                wandio_destroy(file);
                return -1;
            }
            memset(exclude[addrhi].loweroctet, '\0', HALFIPSPACE * sizeof(sourcefilter_element_t));
            exclude[addrhi].loweroctet[addrlo].color = filter_file->color;
        }else{
            exclude[addrhi].loweroctet[addrlo].color = filter_file->color;
        }
    }
    wandio_destroy(file);
    return 0;
}

void reset_sourcefilter_filter(sourcefilter_element_t *filter){
    //free all the leaves. reset values
    int i;
    printf("reset_sourcefilter_filter\n");
    for (i = 0; i < HALFIPSPACE; i++)
    {
        if (filter[i].loweroctet != NULL)
        {
            free(filter[i].loweroctet);
            filter[i].loweroctet = NULL;
            filter[i].color = 0;
        }
    }
}

void destroy_sourcefilter_filter(sourcefilter_filter_t *filter){
    int i,j;
    if (!filter){
        return;
    }
    for (j=0; j<2; j++){
        if (filter->excludefilter[j]!=NULL){
            for (i=0; i<HALFIPSPACE; i++){
                if (filter->excludefilter[j][i].loweroctet != NULL){
                    free(filter->excludefilter[j][i].loweroctet);
                }
            }
            free(filter->excludefilter[j]);
        }
    }
    free(filter);
    return;
}

sourcefilter_filter_t * create_sourcefilter_filter(sourcefilter_file_t * file){
    sourcefilter_filter_t * filter;
    int i;
    filter = malloc(sizeof(sourcefilter_filter_t));
    if (!filter){
        goto err;
    }
    for (i=0; i<2; i++){
        //create two sets of first layer filters
        if ((filter->excludefilter[i]=calloc(HALFIPSPACE, sizeof(sourcefilter_element_t)))==NULL){
            goto err;
        }
        //zero the memory block
        memset(filter->excludefilter[i], '\0', HALFIPSPACE* sizeof(sourcefilter_element_t));
    }
    filter->srcexcludefile = file;
    filter->current_exclude = 0;
    if (parse_excl_source_file(CURRENT_SRCEXCLUDE(filter), file)!=0){
        goto err;
    }  
    return filter;
    err:
        destroy_sourcefilter_filter(filter);
        return NULL;
}

int update_sourcefilter(sourcefilter_filter_t *filter){
    sourcefilter_element_t * nextfilter = filter->excludefilter[!filter->current_exclude];
    reset_sourcefilter_filter(nextfilter);
    printf("update_sourcefilter\n");
    if (parse_excl_source_file(nextfilter, filter->srcexcludefile)!=0){
        return -1;
    } 
    //new filter is ready.
    filter->current_exclude = !filter->current_exclude;
    return 0;
}




int apply_sourcefilter(sourcefilter_filter_t * filter, struct in_addr ipsrc){
    uint32_t ip_srcaddr;
    
    ip_srcaddr = htonl(ipsrc.s_addr);
    /*check if the high 2 octets match.*/
    if (CURRENT_SRCEXCLUDE(filter)[IPHIGH(ip_srcaddr)].loweroctet==NULL){
        return 0;
    }

    /* Return matching color(s). */
    return (int) CURRENT_SRCEXCLUDE(filter)[IPHIGH(ip_srcaddr)].loweroctet[IPLO(ip_srcaddr)].color;
}