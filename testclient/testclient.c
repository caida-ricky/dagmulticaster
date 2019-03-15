#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <getopt.h>
#include <net/if.h>
#include <netdb.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "ndagmulticaster.h"
#include "testclient.h"
#include <pthread.h>

#define MAXBUFSIZE 10000

volatile int halted = 0;

static void halt_signal(int signal)
{
  (void)signal;
  halted = 1;
}

static inline int get_nb_cores()
{
  int numCPU;
#ifdef _SC_NPROCESSORS_ONLN
  /* Most systems do this now */
  numCPU = sysconf(_SC_NPROCESSORS_ONLN);

#else
  int mib[] = {CTL_HW, HW_AVAILCPU};
  size_t len = sizeof(numCPU);

  /* get the number of CPUs from the system */
  sysctl(mib, 2, &numCPU, &len, NULL, 0);
#endif
  return numCPU <= 0 ? 1 : numCPU;
}

static inline int generic_thread_start(pthread_t *tid,
                                       void *(*startfunc)(void *), void *tdata)
{

  int ret;
#ifdef __linux__
  pthread_attr_t attrib;
  cpu_set_t cpus;
  int i;

  /* Allow this thread to appear on any core */
  CPU_ZERO(&cpus);
  for (i = 0; i < get_nb_cores(); i++) {
    CPU_SET(i, &cpus);
  }
  pthread_attr_init(&attrib);
  pthread_attr_setaffinity_np(&attrib, sizeof(cpus), &cpus);
  ret = pthread_create(tid, &attrib, startfunc, tdata);
  pthread_attr_destroy(&attrib);

#else
  ret = pthread_create(tid, NULL, startfunc, tdata);
#endif

  if (ret != 0) {
    return -1;
  }

  return 1;
}

static int join_multicast_group(char *groupaddr, char *localiface,
                                char *portstr, uint16_t portnum,
                                struct addrinfo **srcinfo)
{

  struct addrinfo hints;
  struct addrinfo *gotten;
  struct addrinfo *group;
  unsigned int interface;
  char pstr[16];
  struct group_req greq;

  int sock;

  if (portstr == NULL) {
    snprintf(pstr, 15, "%u", portnum);
    portstr = pstr;
  }

  interface = if_nametoindex(localiface);
  if (interface == 0) {
    fprintf(stderr, "Failed to lookup interface %s -- %s\n", localiface,
            strerror(errno));
    return -1;
  }

  hints.ai_family = PF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_PASSIVE;
  hints.ai_protocol = 0;

  if (getaddrinfo(NULL, portstr, &hints, &gotten) != 0) {
    fprintf(stderr, "Call to getaddrinfo failed for NULL:%s -- %s\n", portstr,
            strerror(errno));
    return -1;
  }

  if (getaddrinfo(groupaddr, NULL, &hints, &group) != 0) {
    fprintf(stderr, "Call to getaddrinfo failed for %s -- %s\n", groupaddr,
            strerror(errno));
    return -1;
  }

  *srcinfo = gotten;

  sock = socket(gotten->ai_family, gotten->ai_socktype, 0);
  if (sock < 0) {
    fprintf(stderr, "Failed to create multicast socket for %s:%s -- %s\n",
            groupaddr, portstr, strerror(errno));
    goto sockcreateover;
  }

  if (bind(sock, (struct sockaddr *)gotten->ai_addr, gotten->ai_addrlen) < 0) {
    fprintf(stderr, "Failed to bind to multicast socket %s:%s -- %s\n",
            groupaddr, portstr, strerror(errno));
    close(sock);
    sock = -1;
    goto sockcreateover;
  }

  greq.gr_interface = interface;
  memcpy(&(greq.gr_group), group->ai_addr, sizeof(struct sockaddr_storage));

  if (setsockopt(sock, IPPROTO_IP, MCAST_JOIN_GROUP, &greq, sizeof(greq)) < 0) {
    fprintf(stderr, "Failed to join multicast group %s:%s -- %s\n", groupaddr,
            portstr, strerror(errno));
    close(sock);
    sock = -1;
    goto sockcreateover;
  }

sockcreateover:
  freeaddrinfo(group);
  return sock;
}

static uint8_t check_ndag_header(char *msgbuf, int msgsize)
{

  ndag_common_t *header = (ndag_common_t *)msgbuf;

  if (msgsize < sizeof(ndag_common_t)) {
    fprintf(stderr, "nDAG message does not have a complete nDAG header.\n");
    return 0;
  }

  if (ntohl(header->magic) != NDAG_MAGIC_NUMBER) {
    fprintf(stderr, "nDAG message does not have a valid magic number.\n");
    return 0;
  }

  if (header->version > NDAG_EXPORT_VERSION || header->version == 0) {
    fprintf(stderr, "nDAG message has an invalid header version: %u\n",
            header->version);
    return 0;
  }

  return header->type;
}

static int add_new_streamsock(recvthread_t *rt, streamsource_t src)
{

  streamsock_t *ssock = NULL;

  if (rt->sourcecount == 0) {
    rt->sources = (streamsock_t *)malloc(sizeof(streamsock_t) * 10);
  } else if ((rt->sourcecount % 10) == 0) {
    rt->sources = (streamsock_t *)realloc(
      rt->sources, sizeof(streamsock_t) * (rt->sourcecount + 10));
  }

  ssock = &(rt->sources[rt->sourcecount]);

  ssock->sock = join_multicast_group(src.groupaddr, src.localiface, NULL,
                                     src.port, &(ssock->srcaddr));

  if (ssock->sock < 0) {
    return -1;
  }

  ssock->port = src.port;
  ssock->groupaddr = src.groupaddr;
  ssock->expectedseq = 0;
  ssock->monitorid = src.monitor;
  rt->sourcecount += 1;

  fprintf(stderr, "Added new stream %s:%u to thread %d\n", ssock->groupaddr,
          ssock->port, rt->threadindex);

  return ssock->port;
}

static uint16_t parse_streamed_record(streamsock_t *ssock, int threadindex,
                                      char *buf, int recordsize)
{
  uint8_t type;
  uint16_t reccount = 0;
  ndag_encap_t *encaphdr;
  ndag_common_t *commonhdr = (ndag_common_t *)buf;

  type = check_ndag_header(buf, recordsize);
  recordsize -= sizeof(ndag_common_t);

  if (type == NDAG_PKT_ENCAPERF) {
    encaphdr = (ndag_encap_t *)(buf + sizeof(ndag_common_t));

    /* TODO deal with seqno wrap, reordering... */
    if (ssock->expectedseq != 0 &&
        ntohl(encaphdr->seqno) != ssock->expectedseq) {
      fprintf(stderr, "Missing %u records from %s:%u  (%u:%u)\n",
              ntohl(encaphdr->seqno) - ssock->expectedseq, ssock->groupaddr,
              ssock->port, ntohs(commonhdr->monitorid),
              ntohs(encaphdr->streamid));
    }
    ssock->expectedseq = ntohl(encaphdr->seqno) + 1;

    reccount = ntohs(encaphdr->recordcount);
    if ((reccount & 0x8000) != 0) {
      /* Not really worth a warning, but useful for testing right now */
      fprintf(stderr, "Warning: received truncated record on %s:%u\n",
              ssock->groupaddr, ssock->port);
      reccount &= (0x7fff);
    }
    return reccount;
  }

  return 0;
}

static int receive_encap_records(recvthread_t *rt)
{
  fd_set allgroups;
  int maxfd, i, ret;
  struct timeval timeout;
  char buf[MAXBUFSIZE];

  FD_ZERO(&allgroups);
  maxfd = 0;
  timeout.tv_sec = 0;
  timeout.tv_usec = 10000;

  /* TODO maybe need a way to tidy up "dead" sockets and signal back
   * to the control thread that we've killed the socket for a particular
   * port.
   */

  for (i = 0; i < rt->sourcecount; i++) {
    if (rt->sources[i].sock != -1) {
      FD_SET(rt->sources[i].sock, &allgroups);
      if (rt->sources[i].sock > maxfd) {
        maxfd = rt->sources[i].sock;
      }
    }
  }

  if (select(maxfd + 1, &allgroups, NULL, NULL, &timeout) < 0) {
    fprintf(stderr, "Error waiting to receive records: %s\n", strerror(errno));
    return -1;
  }

  for (i = 0; i < rt->sourcecount; i++) {
    if (rt->sources[i].sock == -1) {
      continue;
    }

    if (!FD_ISSET(rt->sources[i].sock, &allgroups)) {
      continue;
    }

    ret = recvfrom(rt->sources[i].sock, buf, MAXBUFSIZE, 0,
                   rt->sources[i].srcaddr->ai_addr,
                   &(rt->sources[i].srcaddr->ai_addrlen));
    if (ret < 0) {
      fprintf(stderr, "Error receiving encapsulated records from %s:%u -- %s\n",
              rt->sources[i].groupaddr, rt->sources[i].port, strerror(errno));
      close(rt->sources[i].sock);
      rt->sources[i].sock = -1;
      continue;
    }

    if (ret == 0) {
      fprintf(stderr, "Received zero bytes on the channel for %s:%u.\n",
              rt->sources[i].groupaddr, rt->sources[i].port);
      close(rt->sources[i].sock);
      rt->sources[i].sock = -1;
      continue;
    }

    if ((ret = parse_streamed_record(&(rt->sources[i]), rt->threadindex, buf,
                                     ret)) == 0) {
      fprintf(stderr, "Received bogus records on the channel: %s:%u.\n",
              rt->sources[i].groupaddr, rt->sources[i].port);
      /* Still try to keep going... */
      continue;
    }
    rt->records_received += ret;
  }
  return 1;
}

static inline int any_restarted_sources(uint16_t monid, recvthread_t *rt)
{

  int i;
  for (i = 0; i < rt->sourcecount; i++) {
    if (rt->sources[i].monitorid == monid)
      return 1;
  }
  return 0;
}

static void *receiver_thread_run(void *threaddata)
{

  recvthread_t *rt = (recvthread_t *)threaddata;
  mymessage_t msg;
  int threadhalted = 0;

  while (!threadhalted) {

    if (libtrace_message_queue_try_get(&(rt->mqueue), (void *)&msg) !=
        LIBTRACE_MQ_FAILED) {

      switch (msg.type) {
      case NDAG_CLIENT_HALT:
        threadhalted = 1;
        break;
      /*
      case NDAG_CLIENT_RESTARTED:
          if (any_restarted_sources(msg.contents.monitor, rt)) {
              fprintf(stderr,
                      "Upstream monitor %u indicates that it has restarted.
      Packets may have been lost.\n", msg.contents.monitor);
          }
          break;
      */
      case NDAG_CLIENT_NEWGROUP:
        if (add_new_streamsock(rt, msg.contents) < 0) {
          fprintf(stderr, "Error adding new stream to receiver thread.\n");
          threadhalted = 1;
        }
        break;
      }
      continue;
    }

    /* No streams currently joined, sleep for a bit to avoid CPU burn */
    if (rt->sourcecount == 0) {
      usleep(10000);
      continue;
    }

    if (receive_encap_records(rt) < 0) {
      break;
    }
  }

  fprintf(stderr, "Exiting receiver thread %d after parsing %lu records.\n",
          rt->threadindex, rt->records_received);
  pthread_exit(NULL);
}

static int create_receiver_thread(recvthread_t *tdata)
{

  return generic_thread_start(&(tdata->tid), receiver_thread_run,
                              (void *)tdata);
}

static int parse_control_message(char *msgbuf, int msgsize,
                                 recvthread_t *rthreads, uint16_t *nextthread,
                                 controlparams_t *params, uint16_t *ptmap)
{

  int i;
  mymessage_t alert;
  uint8_t msgtype = check_ndag_header(msgbuf, msgsize);
  ndag_common_t *ndaghdr = (ndag_common_t *)msgbuf;

  if (msgtype == 0) {
    return -1;
  }

  msgsize -= sizeof(ndag_common_t);

  if (msgtype == NDAG_PKT_BEACON) {
    /* If message is a beacon, make sure every port included in the
     * beacon is assigned to a receive thread.
     */
    uint16_t *ptr, numstreams;

    if (msgsize < sizeof(uint16_t)) {
      fprintf(stderr, "Malformed beacon (missing number of streams).\n");
      return -1;
    }

    ptr = (uint16_t *)(msgbuf + sizeof(ndag_common_t));
    numstreams = ntohs(*ptr);
    ptr++;

    if (msgsize != ((numstreams + 1) * sizeof(uint16_t))) {
      fprintf(stderr,
              "Malformed beacon (length doesn't match number of streams).\n");
      return -1;
    }

    for (i = 0; i < numstreams; i++) {
      uint16_t streamport = ntohs(*ptr);

      if (ptmap[streamport] == 0xffff) {
        alert.type = NDAG_CLIENT_NEWGROUP;
        alert.contents.groupaddr = params->groupaddr;
        alert.contents.localiface = params->localiface;
        alert.contents.port = streamport;
        alert.contents.monitor = ntohs(ndaghdr->monitorid);

        ptmap[streamport] = *nextthread;

        libtrace_message_queue_put(&(rthreads[(*nextthread)].mqueue),
                                   (void *)&alert);
        *nextthread = ((*nextthread + 1) % params->maxthreads);
      }

      ptr++;
    }
    /*
    } else if (msgtype == NDAG_PKT_RESTARTED) {
        alert.type = NDAG_CLIENT_RESTARTED;
        alert.contents.monitor = ntohs(ndaghdr->monitorid);
        alert.contents.groupaddr = NULL;
        alert.contents.localiface = NULL;
        alert.contents.port = 0;
        for (i = 0; i < params->maxthreads; i++) {
            libtrace_message_queue_put(&(rthreads[i].mqueue),
                    (void *)&alert);
        }

    */
  } else {
    fprintf(stderr, "Unexpected message type on control channel: %u\n",
            msgtype);
    return -1;
  }

  return 0;
}

static void *control_thread_run(void *threaddata)
{
  controlparams_t *cparams = (controlparams_t *)threaddata;
  recvthread_t *rthreads = NULL;
  uint16_t ptmap[65536];
  uint16_t nextthread = 0;
  int sock = -1;
  struct addrinfo *receiveaddr = NULL;
  fd_set listening;
  struct timeval timeout;
  int i;

  /* ptmap is a dirty hack to allow us to quickly check if we've already
   * assigned a stream to a thread.
   */
  memset(ptmap, 0xff, 65536 * sizeof(uint16_t));

  /* Prepare and start the stream receiver threads. Note that we have
   * a separate set of stream receivers for each control channel -- this
   * is intentional. Sources may have different steering configurations
   * and should probably be kept apart for now.
   *
   * If the end user wants to combine packets from multiple sources, they'll
   * need to go through an additional aggregation step to ensure everything
   * ends up in the right place.
   */
  rthreads = (recvthread_t *)malloc(sizeof(recvthread_t) * cparams->maxthreads);

  for (i = 0; i < cparams->maxthreads; i++) {
    rthreads[i].sources = NULL;
    rthreads[i].sourcecount = 0;
    rthreads[i].threadindex = i;
    rthreads[i].records_received = 0;
    libtrace_message_queue_init(&(rthreads[i].mqueue), sizeof(mymessage_t));

    if (create_receiver_thread(&(rthreads[i])) < 0) {
      fprintf(stderr, "Failed to create receiver thread %d: %s\n.", i,
              strerror(errno));
      halted = true;
      break;
    }
  }

  /* Join the multicast group for the control channel */
  sock = join_multicast_group(cparams->groupaddr, cparams->localiface,
                              cparams->portstr, 0, &receiveaddr);
  if (sock == -1) {
    fprintf(stderr, "Failed to join multicast group for control channel.\n");
    halted = true;
  }

  while (!halted) {
    int ret;
    char buf[MAXBUFSIZE];

    FD_ZERO(&listening);
    FD_SET(sock, &listening);

    timeout.tv_sec = 0;
    timeout.tv_usec = 500000;

    /* Receive the next message from the control channel */
    ret = select(sock + 1, &listening, NULL, NULL, &timeout);
    if (ret < 0) {
      fprintf(stderr, "Error while waiting for control messages.\n");
      break;
    }

    if (!FD_ISSET(sock, &listening)) {
      continue;
    }

    ret = recvfrom(sock, buf, MAXBUFSIZE, 0, receiveaddr->ai_addr,
                   &(receiveaddr->ai_addrlen));
    if (ret < 0) {
      fprintf(stderr, "Error while receiving a control message.\n");
      break;
    }

    if (ret == 0) {
      fprintf(stderr, "Received zero bytes over control channel.\n");
      break;
    }

    if (parse_control_message(buf, ret, rthreads, &nextthread, cparams, ptmap) <
        0) {
      fprintf(stderr, "Error while parsing control channel message.\n");
      continue;
    }
  }

  // exitcontrol:
  if (sock >= 0) {
    close(sock);
  }

  if (rthreads != NULL) {
    mymessage_t haltmsg;
    haltmsg.type = NDAG_CLIENT_HALT;

    for (i = 0; i < cparams->maxthreads; i++) {
      /* Send a halt thread message */
      libtrace_message_queue_put(&(rthreads[i].mqueue), (void *)&haltmsg);

      /* Join on the thread */
      pthread_join(rthreads[i].tid, NULL);

      /* Free the message queue and ports */
      libtrace_message_queue_destroy(&(rthreads[i].mqueue));
      if (rthreads[i].sources) {
        free(rthreads[i].sources);
      }
    }
  }

  pthread_exit(NULL);
}

static int create_control_thread(controlparams_t *cparams)
{
  return generic_thread_start(&(cparams->tid), control_thread_run,
                              (void *)cparams);
}

static void print_help(char *progname)
{
  fprintf(stderr,
          "Usage: %s [ -a multicastaddr ] [ -p beaconport ] [ -t maxthreads ]\n"
          "          [ -s localiface ]\n",
          progname);
}

int main(int argc, char **argv)
{

  char *multicastgroup = NULL;
  char *portstr = NULL;
  char *localiface = NULL;
  uint32_t maxthreads = 1;
  struct sigaction sigact;
  sigset_t sig_before, sig_block_all;
  controlparams_t cparams;
  // uint16_t nextthreadid = 0;

  while (1) {
    int option_index = 0;
    int c;
    static struct option long_options[] = {{"groupaddr", 1, 0, 'a'},
                                           {"controlport", 1, 0, 'p'},
                                           {"maxthreads", 1, 0, 't'},
                                           {"localiface", 1, 0, 's'},
                                           {NULL, 0, 0, 0}};

    c = getopt_long(argc, argv, "s:a:p:t:", long_options, &option_index);

    if (c == -1) {
      break;
    }

    switch (c) {
    case 'a':
      multicastgroup = strdup(optarg);
      break;
    case 'p':
      portstr = strdup(optarg);
      break;
    case 's':
      localiface = strdup(optarg);
      break;
    case 't':
      maxthreads = strtoul(optarg, NULL, 0);
      break;
    default:
      print_help(argv[0]);
      exit(1);
    }
  }

  if (multicastgroup == NULL) {
    multicastgroup = strdup("225.0.0.225");
  }
  if (portstr == NULL) {
    portstr = strdup("9001");
  }
  if (localiface == NULL) {
    localiface = strdup("eth0");
  }

  if (maxthreads <= 0) {
    fprintf(stderr, "Stupid value set for maxthreads, setting to 1 instead.\n");
    maxthreads = 1;
  }

  sigact.sa_handler = halt_signal;
  sigemptyset(&sigact.sa_mask);
  sigact.sa_flags = SA_RESTART;
  sigaction(SIGINT, &sigact, NULL);
  sigaction(SIGTERM, &sigact, NULL);

  sigemptyset(&sig_block_all);
  if (pthread_sigmask(SIG_SETMASK, &sig_block_all, &sig_before) < 0) {
    fprintf(stderr, "Unable to disable signals before starting threads.\n");
    goto tidyup;
  }

  /* Just put a cap on this, just to be safe */
  if (maxthreads > 1000) {
    maxthreads = 1000;
  }

  /* Create a thread for the control channel */
  cparams.groupaddr = multicastgroup;
  cparams.portstr = portstr;
  cparams.localiface = localiface;
  cparams.maxthreads = maxthreads;

  if (create_control_thread(&cparams) < 0) {
    fprintf(stderr,
            "Failed to create thread for receiving control messages,\n");
    goto tidyup;
  }

  if (pthread_sigmask(SIG_SETMASK, &sig_before, NULL)) {
    fprintf(stderr, "Unable to re-enable signals after thread creation.\n");
    goto tidyup;
  }

  pthread_join(cparams.tid, NULL);

tidyup:

  free(portstr);
  free(multicastgroup);
  free(localiface);
}

// vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
