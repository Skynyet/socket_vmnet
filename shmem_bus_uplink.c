#include "shmem_bus_uplink.h"

#include "bus_client.h"
#include "log.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BUS_JOIN_TIMEOUT_MS 5000
#define BUS_CLAIM_RETRIES 8
#define BUS_VMNET_WRITE_BATCH 32

struct membership_cache {
  int valid;
  struct bus_membership value;
};

struct shmem_bus_uplink {
  struct bus_client client;
  interface_ref iface;
  pthread_t consumer_thread;
  int thread_started;
  int stop_pipe[2];
  pthread_mutex_t control_lock;
  atomic_bool active;

  // Each cache has one thread-owner. The vmnet callback publishes on
  // host_queue; consumer_thread drains guest rings. Membership changes are
  // rare, so steady state is one epoch load rather than 64 directory entries.
  struct membership_cache producer_membership;
  struct membership_cache consumer_membership;

  _Atomic(uint64_t) published_slots;
  _Atomic(uint64_t) published_frames;
  _Atomic(uint64_t) reserve_drops;
  _Atomic(uint64_t) doorbells;
  _Atomic(uint64_t) consumed_slots;
  _Atomic(uint64_t) consumed_frames;
  _Atomic(uint64_t) vmnet_write_drops;
};

struct pin_context {
  struct shmem_bus_uplink *uplink;
  struct membership_cache *cache;
};

struct identity_context {
  uint32_t generation;
  struct bus_port_view self;
};

static int set_nonblock(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int membership(struct shmem_bus_uplink *u, struct membership_cache *cache,
                      struct bus_membership *out) {
  uint64_t epoch = atomic_load_explicit(&u->client.dir.header->membership_epoch,
                                        memory_order_acquire);
  if (cache->valid && epoch != 0 && (epoch & 1) == 0 &&
      cache->value.epoch == epoch) {
    *out = cache->value;
    return 0;
  }
  if (bus_directory_read(&u->client.dir, out, 8) < 0) {
    return -1;
  }
  cache->value = *out;
  cache->valid = 1;
  return 0;
}

static int pin_mask(void *opaque, uint64_t publish_epoch, uint64_t old_targets,
                    uint64_t *pins_out) {
  struct pin_context *ctx = opaque;
  struct bus_membership snap;
  if (membership(ctx->uplink, ctx->cache, &snap) < 0) {
    return -1;
  }
  uint64_t pins = old_targets;
  for (uint64_t mask = old_targets; mask != 0; mask &= mask - 1) {
    uint32_t port = (uint32_t)__builtin_ctzll(mask);
    const struct bus_port_view *p = &snap.ports[port];
    if (p->state == BUS_PORT_DRAINING ||
        (p->state == BUS_PORT_LIVE && p->changed_at_epoch != 0 &&
         p->changed_at_epoch <= publish_epoch)) {
      continue;
    }
    pins &= ~((uint64_t)1 << port);
  }
  *pins_out = pins;
  return 0;
}

static int identity_valid(void *opaque, uint64_t publish_epoch) {
  const struct identity_context *ctx = opaque;
  return ctx->self.generation == ctx->generation &&
         ctx->self.changed_at_epoch != 0 &&
         ctx->self.changed_at_epoch <= publish_epoch &&
         (ctx->self.state == BUS_PORT_LIVE ||
          ctx->self.state == BUS_PORT_DRAINING);
}

static int copy_target_states(struct shmem_bus_uplink *u,
                              const struct bus_membership *snap,
                              uint64_t targets,
                              struct bus_consumer_view views[BUS_MAX_PORTS],
                              struct bus_consumer_view *states[BUS_MAX_PORTS],
                              int doorbells[BUS_MAX_PORTS]) {
  memset(states, 0, sizeof(*states) * BUS_MAX_PORTS);
  for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) {
    doorbells[i] = -1;
  }

  int rc = 0;
  pthread_mutex_lock(&u->control_lock);
  for (uint64_t mask = targets; mask != 0; mask &= mask - 1) {
    uint32_t port = (uint32_t)__builtin_ctzll(mask);
    struct bus_client_peer *peer = &u->client.peers[port];
    if (!peer->installed || peer->generation != snap->ports[port].generation ||
        peer->state == NULL || peer->doorbell_tx < 0 ||
        bus_consumer_open(&views[port], peer->state,
                          (size_t)peer->state_bytes) < 0) {
      rc = -1;
      break;
    }
    states[port] = &views[port];
    doorbells[port] = peer->doorbell_tx;
  }
  pthread_mutex_unlock(&u->control_lock);
  return rc;
}

static void ring_targets(struct shmem_bus_uplink *u, uint64_t targets,
                         const int doorbells[BUS_MAX_PORTS]) {
  uint64_t rung = 0;
  unsigned char hint = 1;
  for (uint64_t mask = targets; mask != 0; mask &= mask - 1) {
    uint32_t port = (uint32_t)__builtin_ctzll(mask);
    if (doorbells[port] < 0) {
      continue;
    }
    ssize_t n = send(doorbells[port], &hint, sizeof(hint), 0);
    if (n == (ssize_t)sizeof(hint) ||
        (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS))) {
      rung++;
    }
  }
  if (rung != 0) {
    bus_ring_record_doorbells(&u->client.ring, rung);
    atomic_fetch_add_explicit(&u->doorbells, rung, memory_order_relaxed);
  }
}

static int publish_reservation(struct shmem_bus_uplink *u,
                               struct bus_reservation *reservation,
                               uint64_t epoch, uint64_t targets,
                               struct bus_consumer_view *states[BUS_MAX_PORTS],
                               const int doorbells[BUS_MAX_PORTS]) {
  uint64_t sequence = reservation->sequence;
  if (bus_reservation_publish(reservation, epoch) < 0) {
    return -1;
  }
  atomic_fetch_add_explicit(&u->published_slots, 1, memory_order_relaxed);

  uint64_t caught_up = 0;
  for (uint64_t mask = targets; mask != 0; mask &= mask - 1) {
    uint32_t port = (uint32_t)__builtin_ctzll(mask);
    if (states[port] != NULL &&
        bus_consumer_cursor(states[port], u->client.id) == sequence) {
      caught_up |= (uint64_t)1 << port;
    }
  }
  ring_targets(u, caught_up, doorbells);
  return 0;
}

int shmem_bus_uplink_publish(struct shmem_bus_uplink *u,
                             const struct vmpktdesc *packets, int count) {
  if (u == NULL || packets == NULL || count <= 0 ||
      !atomic_load_explicit(&u->active, memory_order_acquire)) {
    return 0;
  }

  struct bus_membership snap;
  if (membership(u, &u->producer_membership, &snap) < 0) {
    return 0; // membership mutation: leave this vmnet batch on its old path
  }
  uint64_t targets = bus_membership_targets(&snap, u->client.id);
  if (targets == 0) {
    return 0;
  }

  struct bus_consumer_view views[BUS_MAX_PORTS];
  struct bus_consumer_view *states[BUS_MAX_PORTS];
  int doorbells[BUS_MAX_PORTS];
  if (copy_target_states(u, &snap, targets, views, states, doorbells) < 0) {
    return 0; // a control bundle is still in flight; never publish unsafely
  }

  struct pin_context pin = {.uplink = u, .cache = &u->producer_membership};
  struct bus_reservation reservation;
  int reserved = 0;
  int committed = 0;

  for (int i = 0; i < count; i++) {
    if (packets[i].vm_pkt_iovcnt != 1 || packets[i].vm_pkt_iov == NULL ||
        packets[i].vm_pkt_size > UINT32_MAX) {
      continue;
    }
    uint32_t frame_len = (uint32_t)packets[i].vm_pkt_size;
    const void *frame = packets[i].vm_pkt_iov[0].iov_base;
    size_t record_space =
        (u->client.ring.slot_bytes - BUS_SLOT_HEADER_BYTES) & ~(size_t)7;
    if (record_space < BUS_FRAME_HEADER_BYTES ||
        frame_len > record_space - BUS_FRAME_HEADER_BYTES) {
      ERRORF("shmem bus: vmnet frame %u does not fit a %u-byte slot",
             frame_len, u->client.ring.slot_bytes);
      return -1;
    }

    for (;;) {
      if (!reserved) {
        int r = bus_ring_try_reserve(&u->client.ring, states, pin_mask, &pin,
                                     &reservation);
        if (r < 0) {
          return -1;
        }
        if (r == 0) {
          atomic_fetch_add_explicit(&u->reserve_drops, 1, memory_order_relaxed);
          ring_targets(u, targets, doorbells);
          return 0;
        }
        reserved = 1;
        committed = 0;
      }

      uint8_t *dst = NULL;
      size_t capacity = 0;
      int b = bus_reservation_frame_buffer(&reservation, &dst, &capacity);
      if (b < 0) {
        return -1;
      }
      if (b == 0 || capacity < frame_len) {
        // The preflight above proves an empty slot fits, so this means the
        // current packed slot is full. Publishing is mandatory after exposing
        // its mutable bytes; restoring the old guard would resurrect data we
        // were allowed to overwrite.
        if (committed == 0) return -1;
        if (publish_reservation(u, &reservation, snap.epoch, targets, states,
                                doorbells) < 0) {
          return -1;
        }
        reserved = 0;
        continue;
      }

      memcpy(dst, frame, frame_len);
      uint64_t frame_targets = frame_len >= 14 ? targets : 0;
      if (bus_reservation_commit_frame(&reservation, frame_len, 0,
                                       frame_targets) < 0) {
        return -1;
      }
      committed++;
      atomic_fetch_add_explicit(&u->published_frames, 1, memory_order_relaxed);
      break;
    }
  }

  if (reserved &&
      publish_reservation(u, &reservation, snap.epoch, targets, states,
                          doorbells) < 0) {
    return -1;
  }
  return 0;
}

static int vmnet_flush(struct shmem_bus_uplink *u, struct vmpktdesc *pdv,
                       int count) {
  if (count == 0) {
    return 0;
  }
  int written = count;
  vmnet_return_t status = vmnet_write(u->iface, pdv, &written);
  if (status != VMNET_SUCCESS || written != count) {
    int dropped = status == VMNET_SUCCESS && written > 0 ? count - written : count;
    atomic_fetch_add_explicit(&u->vmnet_write_drops,
                              (uint64_t)dropped,
                              memory_order_relaxed);
    if (status != VMNET_SUCCESS) {
      ERRORF("shmem bus: vmnet_write failed with status %d; dropped %d frames",
             status, dropped);
    }
    // A vmnet failure drops this Ethernet batch; it does not corrupt the bus
    // or justify abandoning every later slot. This matches socket_vmnet's
    // existing datagram path and, crucially, lets us release the current claim
    // so one external failure cannot pin a guest's producer ring forever.
    return 0;
  }
  return 0;
}

static int consume_claim(struct shmem_bus_uplink *u, struct bus_claim *claim) {
  struct vmpktdesc pdv[BUS_VMNET_WRITE_BATCH];
  struct iovec iov[BUS_VMNET_WRITE_BATCH];
  int count = 0;
  for (;;) {
    struct bus_frame_view frame;
    int r = bus_claim_next(claim, &frame);
    if (r < 0) {
      return -1;
    }
    if (r == 0) {
      break;
    }
    iov[count] = (struct iovec){.iov_base = (void *)frame.data,
                                .iov_len = frame.length};
    pdv[count] = (struct vmpktdesc){.vm_pkt_size = frame.length,
                                    .vm_pkt_iov = &iov[count],
                                    .vm_pkt_iovcnt = 1,
                                    .vm_flags = 0};
    count++;
    atomic_fetch_add_explicit(&u->consumed_frames, 1, memory_order_relaxed);
    if (count == BUS_VMNET_WRITE_BATCH) {
      if (vmnet_flush(u, pdv, count) < 0) {
        return -1;
      }
      count = 0;
    }
  }
  return vmnet_flush(u, pdv, count);
}

static int consume_source(struct shmem_bus_uplink *u,
                          struct bus_ring_view *ring,
                          const struct bus_membership *snap) {
  struct identity_context identity = {
      .generation = u->client.generation,
      .self = snap->ports[u->client.id],
  };

  for (;;) {
    uint64_t cursor = bus_consumer_cursor(&u->client.consumer,
                                          ring->owner_port);
    if (cursor >= bus_ring_head(ring)) {
      return 0;
    }

    struct bus_claim claim;
    enum bus_claim_status status = BUS_CLAIM_ERROR;
    for (int retry = 0; retry <= BUS_CLAIM_RETRIES; retry++) {
      status = bus_ring_try_claim(ring, &u->client.consumer, u->client.id,
                                  cursor, identity_valid, &identity, &claim);
      if (status != BUS_CLAIM_RECLAIMING) {
        break;
      }
      sched_yield();
    }

    switch (status) {
    case BUS_CLAIM_READY: {
      int rc = consume_claim(u, &claim);
      int release_rc = bus_claim_release(&claim, 1);
      if (rc < 0 || release_rc < 0) {
        return -1;
      }
      atomic_fetch_add_explicit(&u->consumed_slots, 1, memory_order_relaxed);
      break;
    }
    case BUS_CLAIM_RECLAIMING:
      // Do not spin forever. A later hint retries; after coordinator-driven
      // retirement the producer can reclaim without this stale claim.
      bus_consumer_record_retry_exhausted(&u->client.consumer,
                                          ring->owner_port);
      return 0;
    case BUS_CLAIM_NOT_TARGETED:
    case BUS_CLAIM_STALE_TARGET:
    case BUS_CLAIM_NOT_PUBLISHED:
    case BUS_CLAIM_LAPPED:
      break; // try_claim advanced the cursor
    case BUS_CLAIM_NOT_INSTALLED:
    case BUS_CLAIM_SOURCE_CHANGED:
      return 0;
    default:
      return -1;
    }
  }
}

static int consume_all(struct shmem_bus_uplink *u) {
  struct bus_membership snap;
  if (membership(u, &u->consumer_membership, &snap) < 0) {
    return 0;
  }

  struct bus_ring_view rings[BUS_MAX_PORTS];
  uint32_t generations[BUS_MAX_PORTS] = {0};
  int open[BUS_MAX_PORTS] = {0};
  pthread_mutex_lock(&u->control_lock);
  for (uint32_t port = 0; port < BUS_MAX_PORTS; port++) {
    struct bus_client_peer *peer = &u->client.peers[port];
    if (peer->installed && peer->ring_open) {
      rings[port] = peer->ring;
      generations[port] = peer->generation;
      open[port] = 1;
    }
  }
  pthread_mutex_unlock(&u->control_lock);

  for (uint32_t port = 0; port < BUS_MAX_PORTS; port++) {
    const struct bus_port_view *identity = &snap.ports[port];
    if (!open[port] || generations[port] != identity->generation ||
        (identity->state != BUS_PORT_LIVE &&
         identity->state != BUS_PORT_DRAINING)) {
      continue;
    }
    if (consume_source(u, &rings[port], &snap) < 0) {
      return -1;
    }
  }
  return 0;
}

static void drain_doorbell(int fd) {
  unsigned char hints[256];
  while (recv(fd, hints, sizeof(hints), 0) > 0) {
  }
}

static void *consumer_main(void *opaque) {
  struct shmem_bus_uplink *u = opaque;
  for (;;) {
    if (consume_all(u) < 0) {
      ERROR("shmem bus: consumer failed");
      break;
    }

    struct pollfd fds[3] = {
        {.fd = u->client.lease, .events = POLLIN},
        {.fd = u->client.doorbell_rx, .events = POLLIN},
        {.fd = u->stop_pipe[0], .events = POLLIN},
    };
    int n = poll(fds, 3, -1);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      ERRORN("shmem bus: poll");
      break;
    }
    if (fds[2].revents != 0) {
      break;
    }
    if (fds[0].revents != 0) {
      int closed = 0;
      pthread_mutex_lock(&u->control_lock);
      int rc = bus_client_poll(&u->client, &closed);
      pthread_mutex_unlock(&u->control_lock);
      if (rc < 0 || closed) {
        ERROR("shmem bus: coordinator lease ended");
        break;
      }
    }
    if (fds[1].revents != 0) {
      drain_doorbell(u->client.doorbell_rx);
    }
  }
  atomic_store_explicit(&u->active, false, memory_order_release);
  return NULL;
}

int shmem_bus_uplink_open(struct shmem_bus_uplink **out,
                          const char *control_path, interface_ref iface) {
  if (out == NULL || control_path == NULL || iface == NULL) {
    errno = EINVAL;
    return -1;
  }
  *out = NULL;
  struct shmem_bus_uplink *u = calloc(1, sizeof(*u));
  if (u == NULL) {
    return -1;
  }
  u->iface = iface;
  u->stop_pipe[0] = -1;
  u->stop_pipe[1] = -1;
  u->client.lease = -1;
  u->client.doorbell_rx = -1;
  bus_msg_reader_init(&u->client.reader);
  for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) {
    u->client.peers[i].doorbell_tx = -1;
  }
  int mutex_error = pthread_mutex_init(&u->control_lock, NULL);
  if (mutex_error != 0) {
    free(u);
    errno = mutex_error;
    return -1;
  }

  if (bus_control_check_cache_line() < 0 ||
      bus_client_join(&u->client, control_path, 0, BUS_PORT_FLAG_UPLINK,
                      BUS_JOIN_TIMEOUT_MS) < 0 ||
      bus_ring_open(&u->client.ring, u->client.own_data,
                    (size_t)u->client.own_data_bytes) < 0 ||
      pipe(u->stop_pipe) < 0 || set_nonblock(u->stop_pipe[0]) < 0 ||
      set_nonblock(u->stop_pipe[1]) < 0) {
    int saved = errno;
    shmem_bus_uplink_close(u);
    errno = saved;
    return -1;
  }
  atomic_store_explicit(&u->active, true, memory_order_release);
  int thread_error = pthread_create(&u->consumer_thread, NULL, consumer_main, u);
  if (thread_error != 0) {
    atomic_store_explicit(&u->active, false, memory_order_release);
    shmem_bus_uplink_close(u);
    errno = thread_error;
    return -1;
  }
  u->thread_started = 1;
  INFOF("shmem bus: joined as uplink port %u generation %u", u->client.id,
        u->client.generation);
  *out = u;
  return 0;
}

void shmem_bus_uplink_dump_metrics(const struct shmem_bus_uplink *u) {
  if (u == NULL) return;
  INFOF("shmem bus: %s; published %llu slots/%llu frames, consumed %llu slots/%llu "
        "frames, reserve drops %llu, doorbells %llu, vmnet write drops %llu",
        atomic_load_explicit(&u->active, memory_order_relaxed) ? "active" : "inactive",
        (unsigned long long)atomic_load(&u->published_slots),
        (unsigned long long)atomic_load(&u->published_frames),
        (unsigned long long)atomic_load(&u->consumed_slots),
        (unsigned long long)atomic_load(&u->consumed_frames),
        (unsigned long long)atomic_load(&u->reserve_drops),
        (unsigned long long)atomic_load(&u->doorbells),
        (unsigned long long)atomic_load(&u->vmnet_write_drops));
}

void shmem_bus_uplink_close(struct shmem_bus_uplink *u) {
  if (u == NULL) {
    return;
  }
  if (u->thread_started) {
    unsigned char stop = 1;
    (void)write(u->stop_pipe[1], &stop, sizeof(stop));
    pthread_join(u->consumer_thread, NULL);
  }
  shmem_bus_uplink_dump_metrics(u);
  if (u->stop_pipe[0] >= 0) {
    close(u->stop_pipe[0]);
  }
  if (u->stop_pipe[1] >= 0) {
    close(u->stop_pipe[1]);
  }
  bus_client_close(&u->client);
  pthread_mutex_destroy(&u->control_lock);
  free(u);
}
