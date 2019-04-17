#ifndef WDCAP_SNIFFER_H_
#define WDCAP_SNIFFER_H_

#include <libtrace.h>
#include <WdcapPacketProcessor.h>

typedef struct wdcapdata {

    WdcapPacketProcessor *wdcapproc;
    WdcapProcessingConfig *wdcapconf;
    libtrace_t *dummytrace;
    libtrace_packet_t *packet;
    uint8_t skipwdcap;
    uint16_t savedlen;

} wdcapdata_t;

#endif

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
