// C implementation of the shared-memory bus participant control client.
#include "../include/bus_client.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

static int64_t now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void *map_ro(int fd, uint64_t bytes) {
  void *m = mmap(NULL, (size_t)bytes, PROT_READ, MAP_SHARED, fd, 0);
  return (m == MAP_FAILED) ? NULL : m;
}

static void *map_rw(int fd, uint64_t bytes) {
  void *m = mmap(NULL, (size_t)bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  return (m == MAP_FAILED) ? NULL : m;
}

// Verify that a read-only capability cannot be remapped writable. Refuse the
// join if the kernel permits that mapping.

static int refuses_write(int fd, uint64_t bytes, const char *what) {
  void *m = mmap(NULL, (size_t)bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (m == MAP_FAILED) {
    return 0;
  }
  munmap(m, (size_t)bytes);
  fprintf(stderr, "bus: %s could be mapped writable; this is not a capability\n", what);
  return -1;
}

static int check(const void *map, uint64_t bytes, uint32_t kind, const char *what) {
  if (map == NULL) {
    fprintf(stderr, "bus: could not map %s\n", what);
    return -1;
  }
  if (bus_region_check_prefix(map, bytes, kind) < 0) {
    fprintf(stderr, "bus: %s does not carry the ABI prefix it should\n", what);
    return -1;
  }
  return 0;
}

// A superseded generation is retained rather than released: a retirement hint
// may have been lost, so a new generation arriving is not evidence that nobody
// is still inside the old mapping. The doorbell descriptor goes with it,
// because a closed fd number is immediately reusable and a thread still ringing
// the old one would write its hint into whatever opened next.
static int retain(struct bus_client *c, struct bus_client_peer *q) {
  if (c->retained_count == c->retained_cap) {
    unsigned want = (c->retained_cap == 0) ? 16 : c->retained_cap * 2;
    struct bus_client_peer *grown = realloc(c->retained, want * sizeof(*grown));
    if (grown == NULL) {
      // Refuse rather than drop: overwriting the record here would leave the
      // mapping and the descriptor unreachable, which is worse than failing
      // loudly on a control path that runs once per membership change.
      fprintf(stderr, "bus: cannot retain a superseded generation, out of memory\n");
      return -1;
    }
    c->retained = grown;
    c->retained_cap = want;
  }
  c->retained[c->retained_count++] = *q;
  memset(q, 0, sizeof(*q));
  q->doorbell_tx = -1;
  return 0;
}

// Every per-port index in this file comes from a control message, and this
// library is linked into a process holding vmnet capabilities. "The coordinator
// would not send that" is not a property of the library, which is what the
// boundary is for.
static int port_in_range(uint32_t port) {
  if (port >= BUS_MAX_PORTS) {
    fprintf(stderr, "bus: control message names port %u, out of %u\n", port, BUS_MAX_PORTS);
    return 0;
  }
  return 1;
}

static void release_peer(struct bus_client_peer *q) {
  if (q->data != NULL) { munmap(q->data, (size_t)q->data_bytes); }
  if (q->state != NULL) { munmap(q->state, (size_t)q->state_bytes); }
  if (q->doorbell_tx >= 0) { close(q->doorbell_tx); }
  memset(q, 0, sizeof(*q));
  q->doorbell_tx = -1;
}

// A word is stored only once the thing it claims is true, and only for the
// concrete generation it is about. Installing a source belongs to the bundle
// rather than being a separate act: the cursor starts at that source's current
// head, which is what stops an old slot from acquiring a new participant's bit
// retroactively.
static int ledger_mark(struct bus_client *c, uint32_t port, uint32_t generation) {
  if (c->own_state == NULL || generation == 0) {
    return -1;
  }
  if (port == c->id) {
    if (generation != c->generation) {
      fprintf(stderr, "bus: told to prepare generation %u, we are %u\n", generation,
              c->generation);
      return -1;
    }
    return bus_consumer_mark_prepared(&c->consumer, generation);
  }

  struct bus_client_peer *q = &c->peers[port];
  if (!q->installed || q->generation != generation || q->data == NULL || q->state == NULL ||
      q->doorbell_tx < 0) {
    fprintf(stderr, "bus: told port %u generation %u is complete, and it is not\n", port,
            generation);
    return -1;
  }
  // Open the peer's ring here rather than at PEER_DATA: the coordinator creates
  // the region before its owner initialises it, so "the peer exists" and "its
  // ring can be read" are different facts and only the second one is safe to
  // act on.
  if (bus_ring_open(&q->ring, q->data, (size_t)q->data_bytes) < 0) {
    fprintf(stderr, "bus: peer %u ring is not readable yet\n", port);
    return -1;
  }
  q->ring_open = 1;
  uint64_t head = bus_ring_head(&q->ring);
  if (head == 0) {
    fprintf(stderr, "bus: peer %u ring head is zero\n", port);
    return -1;
  }
  if (bus_consumer_install_source(&c->consumer, port, generation, head) < 0) {
    return -1;
  }
  return bus_consumer_mark_source_installed(&c->consumer, port, generation);
}

// One capability. This runs during the join and long afterwards: a late joiner
// arrives as exactly the same messages, which is why there is no separate
// late-join path to get wrong.
static int install(struct bus_client *c, const struct bus_msg *m, int fd) {
  int owned = fd;
  int keep = 0;
  int rc = 0;

  switch ((enum bus_msg_type)m->type) {
    case BUS_MSG_WELCOME: {
      if (!port_in_range(m->port)) { rc = -1; break; }
      if (c->dir.header != NULL) {
        // A record may be replayed, and a second directory mapping would leak
        // the first. Keep the first and check the duplicate agrees about who we
        // are, because a WELCOME naming a different port is not a repeat.
        if (m->port != c->id || m->generation != c->generation) {
          fprintf(stderr, "bus: second WELCOME says port %u generation %u, we are %u/%u\n",
                  m->port, m->generation, c->id, c->generation);
          rc = -1;
        }
        break;
      }
      c->id = m->port;
      c->generation = m->generation;
      if (refuses_write(fd, m->bytes, "the directory") < 0) { rc = -1; break; }
      void *map = map_ro(fd, m->bytes);
      if (check(map, m->bytes, BUS_REGION_DIRECTORY, "the directory") < 0) {
        rc = -1;
        break;
      }
      rc = bus_directory_open(&c->dir, map, m->bytes);
      break;
    }

    case BUS_MSG_OWN_DATA:
      if (c->own_data != NULL) { break; }   // idempotent: a record may repeat
      c->own_data = map_rw(fd, m->bytes);
      c->own_data_bytes = m->bytes;
      rc = check(c->own_data, m->bytes, BUS_REGION_PRODUCER, "my own ring");
      break;

    case BUS_MSG_OWN_STATE:
      if (c->own_state != NULL) { break; }
      c->own_state = map_rw(fd, m->bytes);
      c->own_state_bytes = m->bytes;
      rc = check(c->own_state, m->bytes, BUS_REGION_CONSUMER, "my own state region");
      if (rc == 0) { rc = bus_consumer_open(&c->consumer, c->own_state, m->bytes); }
      break;

    case BUS_MSG_DOORBELL_RX:
      if (c->doorbell_rx >= 0) { break; }
      c->doorbell_rx = fd;
      keep = 1;
      break;

    case BUS_MSG_PEER_DATA: {
      if (!port_in_range(m->port)) { rc = -1; break; }
      struct bus_client_peer *q = &c->peers[m->port];
      if (q->installed && q->generation != m->generation && retain(c, q) < 0) { rc = -1; break; }
      if (q->data != NULL) { break; }
      if (refuses_write(fd, m->bytes, "a peer ring") < 0) { rc = -1; break; }
      q->data = map_ro(fd, m->bytes);
      q->data_bytes = m->bytes;
      q->generation = m->generation;
      q->installed = 1;
      rc = check(q->data, m->bytes, BUS_REGION_PRODUCER, "a peer ring");
      break;
    }

    case BUS_MSG_PEER_STATE: {
      if (!port_in_range(m->port)) { rc = -1; break; }
      struct bus_client_peer *q = &c->peers[m->port];
      if (q->installed && q->generation != m->generation && retain(c, q) < 0) { rc = -1; break; }
      if (q->state != NULL) { break; }
      if (refuses_write(fd, m->bytes, "a peer state region") < 0) { rc = -1; break; }
      q->state = map_ro(fd, m->bytes);
      q->state_bytes = m->bytes;
      q->generation = m->generation;
      q->installed = 1;
      rc = check(q->state, m->bytes, BUS_REGION_CONSUMER, "a peer state region");
      break;
    }

    case BUS_MSG_DOORBELL_TX: {
      if (!port_in_range(m->port)) { rc = -1; break; }
      struct bus_client_peer *q = &c->peers[m->port];
      if (q->doorbell_tx >= 0) { break; }
      q->doorbell_tx = fd;
      q->generation = m->generation;
      keep = 1;
      break;
    }

    case BUS_MSG_END_CAPABILITIES: {
      if (!port_in_range(m->port)) { rc = -1; break; }
      rc = ledger_mark(c, m->port, m->generation);
      if (rc == 0) {
        // A hint, and only that: the coordinator's poll finds the word anyway.
        struct bus_msg wake;
        memset(&wake, 0, sizeof(wake));
        wake.type = BUS_MSG_WAKE;
        wake.version = BUS_CONTROL_VERSION;
        wake.port = m->port;
        wake.generation = m->generation;
        bus_msg_send(c->lease, &wake, -1);
      }
      break;
    }

    case BUS_MSG_ABORTED: {
      if (!port_in_range(m->port)) { rc = -1; break; }
		// Retain the mapping: a consumer may have installed it before the join
		// was aborted and may still be using it.
      struct bus_client_peer *q = &c->peers[m->port];
      if (q->generation == m->generation && retain(c, q) < 0) { rc = -1; }
      break;
    }

    case BUS_MSG_RETIRED: {
      if (!port_in_range(m->port)) { rc = -1; break; }
      struct bus_client_peer *q = &c->peers[m->port];
      if (q->generation == m->generation && retain(c, q) < 0) { rc = -1; }
      break;
    }

    default:
      break;
  }

  if (!keep && owned >= 0) { close(owned); }
  return rc;
}

int bus_client_join(struct bus_client *c, const char *control_socket, uint64_t mac,
                    uint32_t flags, int64_t timeout_ms) {
  memset(c, 0, sizeof(*c));
  c->lease = -1;
  c->doorbell_rx = -1;
  c->flags = flags;
  for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) { c->peers[i].doorbell_tx = -1; }
  bus_msg_reader_init(&c->reader);

  if ((flags & ~(uint32_t)BUS_PORT_FLAGS_MASK) != 0) {
    fprintf(stderr, "bus: unknown port flags 0x%x\n", flags);
    return -1;
  }
  struct sockaddr_un a;
  memset(&a, 0, sizeof(a));
  a.sun_family = AF_UNIX;
  // Truncation here would connect to a different socket than the caller named,
  // which is a failure that looks like the coordinator being absent.
  if (strlen(control_socket) >= sizeof(a.sun_path)) {
    fprintf(stderr, "bus: control socket path is %zu bytes, the limit is %zu\n",
            strlen(control_socket), sizeof(a.sun_path) - 1);
    return -1;
  }
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) { return -1; }
  snprintf(a.sun_path, sizeof(a.sun_path), "%s", control_socket);
  if (connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0) {
    int e = errno;
    close(fd);
    errno = e;
    return -1;
  }
  c->lease = fd;
  int fl = fcntl(fd, F_GETFL, 0);
  if (fl >= 0) { fcntl(fd, F_SETFL, fl | O_NONBLOCK); }

  struct bus_msg hello;
  memset(&hello, 0, sizeof(hello));
  hello.type = BUS_MSG_HELLO;
  hello.version = BUS_CONTROL_VERSION;
  hello.port = BUS_PORT_NONE;
  hello.bytes = mac;
  hello.epoch = flags;   // the contract: HELLO carries port flags here
  if (bus_msg_send(c->lease, &hello, -1) < 0) {
    bus_client_close(c);
    return -1;
  }

  int64_t deadline = now_ms() + timeout_ms;
  for (;;) {
    struct bus_msg m;
    int fdin = -1;
    int r = bus_msg_read_by(c->lease, &c->reader, &m, &fdin, deadline - now_ms());
    if (r <= 0) {
      if (fdin >= 0) { close(fdin); }
      bus_client_close(c);
      return -1;
    }
    if (m.type == BUS_MSG_READY) {
      if (fdin >= 0) { close(fdin); }
      return 0;
    }
    if (install(c, &m, fdin) < 0) {
      bus_client_close(c);
      return -1;
    }
  }
}

int bus_client_poll(struct bus_client *c, int *closed) {
  *closed = 0;
  int handled = 0;
  for (;;) {
    struct bus_msg m;
    int fd = -1;
    int r = bus_msg_read(c->lease, &c->reader, &m, &fd);
    if (r == BUS_MSG_INCOMPLETE) { return handled; }
    if (r == 0) {
      // The coordinator went away. Not a failure of this process: the bus is
      // over, and every mapping it holds is now a fossil.
      *closed = 1;
      return handled;
    }
    if (r < 0) {
      if (fd >= 0) { close(fd); }
      return -1;
    }
    if (install(c, &m, fd) < 0) { return -1; }
    handled = 1;
  }
}

int bus_client_targets(const struct bus_client *c, uint64_t *mask, uint64_t *epoch) {
  struct bus_membership snap;
  if (bus_directory_read(&c->dir, &snap, 8) < 0) {
    return -1;
  }
  *mask = bus_membership_targets(&snap, c->id);
  if (epoch != NULL) { *epoch = snap.epoch; }
  return 0;
}

void bus_client_close(struct bus_client *c) {
  if (c->lease >= 0) { close(c->lease); c->lease = -1; }
  bus_msg_reader_close(&c->reader);
  if (c->doorbell_rx >= 0) { close(c->doorbell_rx); c->doorbell_rx = -1; }
  for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) { release_peer(&c->peers[i]); }
  for (unsigned i = 0; i < c->retained_count; i++) { release_peer(&c->retained[i]); }
  free(c->retained);
  c->retained = NULL;
  c->retained_count = 0;
  c->retained_cap = 0;
  if (c->dir.header != NULL) { munmap(c->dir.header, (size_t)c->dir.bytes); c->dir.header = NULL; }
  if (c->own_data != NULL) { munmap(c->own_data, (size_t)c->own_data_bytes); c->own_data = NULL; }
  if (c->own_state != NULL) { munmap(c->own_state, (size_t)c->own_state_bytes); c->own_state = NULL; }
}
