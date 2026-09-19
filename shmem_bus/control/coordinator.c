// Coordinates bus membership and participant liveness. Payload stays on the
// participant data path.
#include "../include/bus_control.h"
#include "../include/bus_coordinator.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

struct bus_port {
  int in_use;
  int lease;                 // control stream; its EOF is the death signal
  struct bus_msg_reader reader;
  uint32_t generation;
  uint32_t flags;             // BUS_PORT_FLAG_* role metadata
  int live;                  // published LIVE in the directory
  int draining;              // excluded from new target masks, claims still honoured
  struct bus_region data;    // this port's producer ring
  struct bus_region state;   // this port's cursors and active claims
  int doorbell_tx;           // retained; duplicated to every producer
};

// A join in flight. Capability messages have been sent but nobody is live yet,
// because sending a descriptor is not the same as installing it.
struct bus_join {
  int active;
  uint32_t port;
  uint64_t mac;
  uint32_t generation;
  int64_t deadline_ms;
};

// Tracks an active source claim and when the coordinator first observed it.
struct claim_watch {
  uint64_t seq;
  int64_t since_ms;
};

struct bus_coordinator {
  int listen_fd;
  char *socket_path;
  struct bus_region dir_region;
  struct bus_directory_view dir;
  struct bus_port ports[BUS_MAX_PORTS];
  struct bus_join join;
	// Region geometry is carried in each ABI prefix.
  uint32_t capacity;
  uint32_t slot_bytes;
  uint32_t mtu;
  uint64_t data_bytes;
  uint64_t state_bytes;
  int64_t join_deadline_ms;
  int64_t claim_deadline_ms;
  int64_t claim_poll_ms;
  struct claim_watch watch[BUS_MAX_PORTS][BUS_MAX_PORTS];
  int verbose;
};

static int64_t now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int set_nonblock(int fd) {
  int fl = fcntl(fd, F_GETFL, 0);
  return (fl < 0) ? -1 : fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static int send_msg(int sock, enum bus_msg_type type, uint32_t port, uint32_t generation,
                    uint64_t bytes, uint64_t epoch, int fd) {
  struct bus_msg m;
  memset(&m, 0, sizeof(m));
  m.type = (uint32_t)type;
  m.version = BUS_CONTROL_VERSION;
  m.port = port;
  m.generation = generation;
  m.bytes = bytes;
  m.epoch = epoch;
  return bus_msg_send(sock, &m, fd);
}

static void coordinator_retire(struct bus_coordinator *c, uint32_t id);
static void coordinator_drain(struct bus_coordinator *c, uint32_t id, const char *why);

// Join order is specified in bus_control.h. The coordinator publishes LIVE only
// after the newcomer and existing participants acknowledge their capabilities.

static int port_alloc(struct bus_coordinator *c) {
  for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) {
    if (!c->ports[i].in_use) {
      return (int)i;
    }
  }
  return -1;
}

static void port_release(struct bus_coordinator *c, uint32_t id) {
  struct bus_port *p = &c->ports[id];
  bus_msg_reader_close(&p->reader);
  if (p->lease >= 0) { close(p->lease); p->lease = -1; }
  if (p->doorbell_tx >= 0) { close(p->doorbell_tx); p->doorbell_tx = -1; }
  bus_region_destroy(&p->data);
  bus_region_destroy(&p->state);
  p->in_use = 0;
  p->flags = 0;
  p->live = 0;
  p->draining = 0;
  // generation is deliberately kept: reusing this id later must not reuse its
  // identity. The bump on the next join plus changed_at_epoch is what makes a
  // stale bit in an old slot detectable rather than plausible.
}

static void port_init(struct bus_port *p) {
  p->in_use = 0;
  p->lease = -1;
  bus_msg_reader_init(&p->reader);
  p->live = 0;
  p->draining = 0;
  p->doorbell_tx = -1;
  p->data.rw = p->data.ro = -1;
  p->state.rw = p->state.ro = -1;
  p->data.map = p->state.map = NULL;
}

// A port that is live and not draining. Only these are targetable, and only
// these owe an acknowledgement when someone new arrives.
static int port_targetable(const struct bus_port *p) {
  return p->in_use && p->live && !p->draining;
}

// The ledger. Written by participants into the state regions the coordinator
// created and kept mapped, so nobody has to report anything and a participant
// too wedged to report is exactly the one worth noticing.
static uint64_t ledger_installed(const struct bus_port *p, uint32_t source) {
  const struct bus_consumer_source *e =
      (const struct bus_consumer_source *)(const void *)((const unsigned char *)p->state.map +
                                                         BUS_REGION_HEADER_BYTES +
                                                         (size_t)source *
                                                             BUS_CONSUMER_SOURCE_BYTES);
  return atomic_load_explicit(&e->installed_generation, memory_order_acquire);
}

static uint64_t ledger_prepared(const struct bus_port *p) {
  const struct bus_consumer_header *h =
      (const struct bus_consumer_header *)(const void *)p->state.map;
  return atomic_load_explicit(&h->prepared_generation, memory_order_acquire);
}

static uint64_t dir_epoch(const struct bus_coordinator *c) {
  return atomic_load_explicit(&c->dir.header->membership_epoch, memory_order_acquire);
}

// Publishes LIVE and answers the newcomer. Everyone who had to install
// something has said so, or has been excluded for failing to.
// Every condition is a comparison of a ledger word against a directory
// generation. A word alone is never a permission: it is a fact about one pair
// of concrete generations, and a late writer can only ever name the bundle it
// actually installed.
static int join_ready(struct bus_coordinator *c) {
  uint32_t id = c->join.port;
  const struct bus_port *p = &c->ports[id];
  if (ledger_prepared(p) != (uint64_t)p->generation) {
    return 0;
  }
  for (uint32_t q = 0; q < BUS_MAX_PORTS; q++) {
    if (q == id || !port_targetable(&c->ports[q])) { continue; }
    // q can read p's claims and ring its doorbell: p is safe to target.
    if (ledger_installed(&c->ports[q], id) != (uint64_t)p->generation) {
      return 0;
    }
    // p can read q's ring: q is safe to consume. Not the same statement, and
    // they stop being true at different moments.
    if (ledger_installed(p, q) != (uint64_t)c->ports[q].generation) {
      return 0;
    }
  }
  return 1;
}

static void join_commit(struct bus_coordinator *c) {
  uint32_t id = c->join.port;
  struct bus_port *p = &c->ports[id];

  bus_directory_begin(&c->dir);
  bus_port_publish(&c->dir, id, BUS_PORT_LIVE, p->generation, c->join.mac, p->data.bytes,
                   p->state.bytes, c->mtu, p->flags);
  uint64_t now = bus_directory_commit(&c->dir);
  p->live = 1;
  c->join.active = 0;

  if (send_msg(p->lease, BUS_MSG_READY, id, p->generation, 0, now, -1) < 0) {
    coordinator_retire(c, id);
    return;
  }
  if (c->verbose) {
    fprintf(stderr, "bus: port %u generation %u live at epoch %llu\n", id, p->generation,
            (unsigned long long)now);
  }
}

static void join_fail(struct bus_coordinator *c, const char *why) {
  uint32_t id = c->join.port;
  uint32_t gen = c->join.generation;
  // Clear the join before anything else: cancelling can retire a peer, and a
  // retirement must not find a half-dead join to satisfy.
  c->join.active = 0;

  if (c->verbose) {
    fprintf(stderr, "bus: join of port %u abandoned: %s\n", id, why);
  }
  // Peers may already hold its ring, its state region and its doorbell. Nothing
  // made those usable -- no directory epoch ever named this port live -- but
  // they are still mappings and descriptors, and reusing the id later would
  // overwrite whatever the peer filed them under. Say so explicitly rather than
  // leaving them to be discovered.
  // A hint, sent to everyone still here. It is keyed by (port, generation) and
  // the receiver ignores it if it names something it does not hold, so there is
  // no need to remember who was told what -- which is one more piece of
  // reconstructed history the ledger lets us delete.
  for (uint32_t q = 0; q < BUS_MAX_PORTS; q++) {
    if (!c->ports[q].in_use || q == id) { continue; }
    if (send_msg(c->ports[q].lease, BUS_MSG_ABORTED, id, gen, 0, dir_epoch(c), -1) < 0) {
      coordinator_drain(c, q, "could not be told a join was abandoned");
    }
  }
  port_release(c, id);
}

// Called on every poll tick while a join is outstanding, and on a wake hint.
// Reading the ledger is cheap and the hint is only latency.
static void join_poll(struct bus_coordinator *c) {
  if (c->join.active && join_ready(c)) {
    join_commit(c);
  }
}

static void join_deadline(struct bus_coordinator *c) {
  uint32_t id = c->join.port;
  struct bus_port *p = &c->ports[id];

  if (ledger_prepared(p) != (uint64_t)p->generation) {
    // The newcomer never finished installing what it was given. Nothing has
    // been published about it beyond PREPARING and nobody can be targeting it,
    // so the join simply did not happen.
    join_fail(c, "the joining port never finished installing its capabilities");
    return;
  }
  for (uint32_t q = 0; q < BUS_MAX_PORTS; q++) {
    if (q == id || !port_targetable(&c->ports[q])) { continue; }
    if (ledger_installed(&c->ports[q], id) == (uint64_t)p->generation &&
        ledger_installed(p, q) == (uint64_t)c->ports[q].generation) {
      continue;
    }
    // A live peer that has not installed in time is not a dead process, and
    // releasing its claims on that basis would be exactly the mistake that
    // corrupts a reader mid-send. It stops being targetable; it keeps
    // everything it holds.
    coordinator_drain(c, q, "did not install a new port in time");
  }
  join_commit(c);
}

static int coordinator_join(struct bus_coordinator *c, int lease) {
  struct bus_msg hello;
  int junk = -1;
  set_nonblock(lease);
  struct bus_msg_reader hello_reader;
  bus_msg_reader_init(&hello_reader);
  int r = bus_msg_read_by(lease, &hello_reader, &hello, &junk, c->join_deadline_ms);
  if (junk >= 0) { close(junk); }
  bus_msg_reader_close(&hello_reader);
  if (r <= 0 || hello.type != BUS_MSG_HELLO) {
    return -1;
  }
  if (hello.version != BUS_CONTROL_VERSION) {
    fprintf(stderr, "bus: rejecting version %u, this coordinator speaks %u\n",
            hello.version, BUS_CONTROL_VERSION);
    return -1;
  }
  if ((hello.epoch & ~(uint64_t)BUS_PORT_FLAGS_MASK) != 0) {
    fprintf(stderr, "bus: rejecting unknown port flags 0x%llx\n",
            (unsigned long long)hello.epoch);
    return -1;
  }

  int id = port_alloc(c);
  if (id < 0) {
    fprintf(stderr, "bus: no free port, %u in use\n", BUS_MAX_PORTS);
    return -1;
  }
  struct bus_port *p = &c->ports[id];
  uint32_t gen = p->generation + 1;

  // Step 1: regions, their ABI prefixes, and the doorbell pair. The prefix is
  // written here and not by the owner, so no descriptor for a region ever names
  // something a peer could catch half-built.
  if (bus_region_create(&p->data, "data", (unsigned)id, c->data_bytes) < 0) {
    perror("bus: producer region");
    return -1;
  }
  bus_region_init_prefix(&p->data, BUS_REGION_PRODUCER, (uint32_t)id, gen, c->mtu,
                         c->capacity, c->slot_bytes);
  if (bus_region_create(&p->state, "state", (unsigned)id, c->state_bytes) < 0) {
    perror("bus: consumer-state region");
    bus_region_destroy(&p->data);
    return -1;
  }
  bus_region_init_prefix(&p->state, BUS_REGION_CONSUMER, (uint32_t)id, gen, 0,
                         BUS_MAX_PORTS, BUS_CONSUMER_SOURCE_BYTES);
  int db[2];
  if (socketpair(AF_UNIX, SOCK_DGRAM, 0, db) < 0) {
    perror("bus: doorbell");
    bus_region_destroy(&p->data);
    bus_region_destroy(&p->state);
    return -1;
  }
  // Both ends nonblocking: a full doorbell buffer means the consumer already
  // has more work queued than it has drained, which is exactly the case where
  // another hint carries no information. EAGAIN here is success.
  set_nonblock(db[0]);
  set_nonblock(db[1]);

  p->in_use = 1;
  p->lease = lease;
  bus_msg_reader_init(&p->reader);
  p->generation = gen;
  p->flags = (uint32_t)hello.epoch;
  p->doorbell_tx = db[0];

  // Step 2: in the directory, holding its id, targetable by nobody. A producer
  // only ever targets LIVE, so PREPARING costs nothing and makes the port's
  // existence and generation readable before a single capability moves.
  bus_directory_begin(&c->dir);
  bus_port_publish(&c->dir, (uint32_t)id, BUS_PORT_PREPARING, gen, hello.bytes,
                   p->data.bytes, p->state.bytes, c->mtu, p->flags);
  uint64_t epoch = bus_directory_commit(&c->dir);

  // Step 3: its own capabilities. The directory is read-only to it, always.
  int ok = 0;
  ok |= send_msg(lease, BUS_MSG_WELCOME, (uint32_t)id, gen, c->dir_region.bytes, epoch,
                 c->dir_region.ro);
  ok |= send_msg(lease, BUS_MSG_OWN_DATA, (uint32_t)id, gen, p->data.bytes, epoch, p->data.rw);
  ok |= send_msg(lease, BUS_MSG_OWN_STATE, (uint32_t)id, gen, p->state.bytes, epoch, p->state.rw);
  ok |= send_msg(lease, BUS_MSG_DOORBELL_RX, (uint32_t)id, gen, 0, epoch, db[1]);
  close(db[1]);  // the consumer owns the receive end now; we must not keep one

  // Step 4: every targetable peer, in both roles. It reads their rings and it
  // rings their doorbells, so it needs data, state and a send end for each --
  // one bundle per peer, each closed by its own delimiter, because late join is
  // one bundle and there is no reason for a second batching rule.
  for (uint32_t q = 0; q < BUS_MAX_PORTS && ok == 0; q++) {
    if (!port_targetable(&c->ports[q]) || q == (uint32_t)id) { continue; }
    struct bus_port *peer = &c->ports[q];
    ok |= send_msg(lease, BUS_MSG_PEER_DATA, q, peer->generation, peer->data.bytes, epoch,
                   peer->data.ro);
    ok |= send_msg(lease, BUS_MSG_PEER_STATE, q, peer->generation, peer->state.bytes, epoch,
                   peer->state.ro);
    ok |= send_msg(lease, BUS_MSG_DOORBELL_TX, q, peer->generation, 0, epoch, peer->doorbell_tx);
    ok |= send_msg(lease, BUS_MSG_END_CAPABILITIES, q, peer->generation, 0, epoch, -1);
  }
  if (ok != 0) {
    port_release(c, (uint32_t)id);
    return -1;
  }

  c->join.active = 1;
  c->join.port = (uint32_t)id;
  c->join.generation = gen;
  c->join.mac = hello.bytes;
  c->join.deadline_ms = now_ms() + c->join_deadline_ms;

  // Step 5: the newcomer, to everyone already here -- before it is live.
  //
  // A peer whose lease breaks here is a peer that has died, and the newcomer
  // has done nothing wrong: retire that peer and carry on, rather than failing
  // the join of a healthy process because an unrelated one went away.
  for (uint32_t q = 0; q < BUS_MAX_PORTS; q++) {
    if (!port_targetable(&c->ports[q]) || q == (uint32_t)id) { continue; }
    int peer_lease = c->ports[q].lease;
    int bad = 0;
    bad |= send_msg(peer_lease, BUS_MSG_PEER_DATA, (uint32_t)id, gen, p->data.bytes, epoch,
                    p->data.ro);
    bad |= send_msg(peer_lease, BUS_MSG_PEER_STATE, (uint32_t)id, gen, p->state.bytes, epoch,
                    p->state.ro);
    bad |= send_msg(peer_lease, BUS_MSG_DOORBELL_TX, (uint32_t)id, gen, 0, epoch, p->doorbell_tx);
    bad |= send_msg(peer_lease, BUS_MSG_END_CAPABILITIES, (uint32_t)id, gen, 0, epoch, -1);
    if (bad != 0) {
      // EPIPE is the peer's end already gone, which is the closure the state
      // machine treats as death. Anything else -- a full buffer that never
      // drained, an interrupted send -- proves nothing about whether it is
      // still reading a slot, so it drains and we keep waiting for the EOF.
      if (errno == EPIPE) {
        coordinator_retire(c, q);
      } else {
        coordinator_drain(c, q, "could not be given a new port's capabilities");
      }
    }
  }

  // Step 6: the newcomer's own delimiter, last of all, so that the
  // prepared_generation it then writes means everything it was handed rather
  // than only its own regions.
  if (send_msg(lease, BUS_MSG_END_CAPABILITIES, (uint32_t)id, gen, 0, epoch, -1) < 0) {
    join_fail(c, "the joining port's lease broke before it was told its bundle ended");
    return -1;
  }

  // Step 7 happens when the ledger says so, not here.
  join_poll(c);
  return id;
}

// ---------------------------------------------------------------------------
// Retirement and draining
//
// These are different claims about the world and must not be confused.
//
// RETIRED says the process cannot execute another read. Only a lease EOF proves
// that, and only then may a producer stop honouring the port's active claims
// and may its id be reused.
//
// DRAINING says we no longer want to send it anything. A deadline, a protocol
// error, a peer that stopped answering -- none of those prove a process has
// stopped reading, and treating them as proof is what turns a stalled consumer
// into a torn frame on its guest link. A draining port is excluded from every
// new target mask, keeps its id, and keeps every claim it holds.
// ---------------------------------------------------------------------------

static void coordinator_drain(struct bus_coordinator *c, uint32_t id, const char *why) {
  struct bus_port *p = &c->ports[id];
  if (!p->in_use || p->draining) { return; }
  p->draining = 1;
  // Publishing it is what makes it mean anything: enforcing exclusion inside
  // this process only stops *new* capability distribution, while producers keep
  // targeting whatever the directory calls live. The port keeps its regions,
  // its id and every claim it holds -- DRAINING says we stopped sending, not
  // that anyone may overwrite what it is reading.
  if (p->live) {
    bus_directory_begin(&c->dir);
    bus_port_publish(&c->dir, id, BUS_PORT_DRAINING, p->generation, 0, p->data.bytes,
                     p->state.bytes, c->mtu, p->flags);
    bus_directory_commit(&c->dir);
  }
  if (c->verbose) {
    fprintf(stderr, "bus: port %u generation %u draining: %s\n", id, p->generation, why);
  }
}

static void coordinator_retire(struct bus_coordinator *c, uint32_t id) {
  struct bus_port *p = &c->ports[id];
  if (!p->in_use) { return; }
  // The port being joined is not a member yet. Retiring it the ordinary way
  // would release it and then satisfy its own barrier with the released slot,
  // publishing a port that no longer exists. A join that loses its subject has
  // not ended early -- it has failed.
  if (c->join.active && id == c->join.port && !p->live) {
    join_fail(c, "the joining port went away before it was published");
    return;
  }
  uint32_t gen = p->generation;
  int was_live = p->live;

  uint64_t now = dir_epoch(c);
  if (was_live) {
    bus_directory_begin(&c->dir);
    bus_port_publish(&c->dir, id, BUS_PORT_RETIRED, gen, 0, 0, 0, 0, 0);
    now = bus_directory_commit(&c->dir);
  }

  // The directory is the authority; this message is a courtesy that saves peers
  // a poll interval. Nothing in the protocol may depend on receiving it.
  for (uint32_t q = 0; q < BUS_MAX_PORTS; q++) {
    if (!c->ports[q].in_use || q == id || !c->ports[q].live) { continue; }
    send_msg(c->ports[q].lease, BUS_MSG_RETIRED, id, gen, 0, now, -1);
  }
  port_release(c, id);
  if (c->verbose) {
    fprintf(stderr, "bus: port %u generation %u retired at epoch %llu\n", id, gen,
            (unsigned long long)now);
  }
  // A port that has gone away cannot install anything, and readiness only
  // counts targetable ports -- so its departure may be exactly what completes
  // the join now waiting on it.
  join_poll(c);
}

// ---------------------------------------------------------------------------

static void on_lease(struct bus_coordinator *c, uint32_t id) {
  struct bus_msg m;
  int fd = -1;
  int r = bus_msg_read(c->ports[id].lease, &c->ports[id].reader, &m, &fd);
  if (fd >= 0) { close(fd); }   // no participant sends us a capability

  if (r == BUS_MSG_INCOMPLETE) {
    return;  // part of a message; the rest arrives when it arrives
  }
  if (r == 0) {
    coordinator_retire(c, id);   // EOF: the process is gone, claims release
    return;
  }
  if (r < 0) {
    coordinator_drain(c, id, "its lease failed mid-message");
    return;
  }
  if (m.type == BUS_MSG_WAKE) {
    // Pure latency. The ledger is already readable and the poll tick would find
    // it; this only means finding it sooner. A wake for a join that has already
    // ended, or for one that never existed, costs one read.
    join_poll(c);
    return;
  }
  if (c->ports[id].draining) {
    return;  // already given up on; read only to keep the EOF coming
  }
  // A participant saying something this protocol does not contain is not a dead
  // participant. It may still be reading a slot right now.
  coordinator_drain(c, id, "sent a message this protocol does not define");
}

// ---------------------------------------------------------------------------
// The liveness signal
//
// Not cursor distance. `head - cursor > capacity` says a consumer was lapped
// and must fast-forward; it says nothing about why, and skipped sequences,
// overload, and a consumer that is simply slower than its producer all produce
// it. Draining a port on that basis would drain healthy ports under load.
//
// The signal is one claim that has not moved: the same (source identity,
// active_seq) past a locally measured deadline, while the lease is still open.
// The coordinator can read it because it created every consumer-state region
// and kept the mapping -- no participant has to report anything, and a
// participant too stuck to answer is exactly the one we need to detect.
//
// Detection drains; it never retires. The claim goes on pinning its physical
// slot, and the producer goes on past it by skipping the sequence, until either
// the claim clears or the lease ends.
// ---------------------------------------------------------------------------

static void scan_claims(struct bus_coordinator *c, int64_t now) {
  for (uint32_t id = 0; id < BUS_MAX_PORTS; id++) {
    if (!port_targetable(&c->ports[id])) { continue; }
    const unsigned char *base = (const unsigned char *)c->ports[id].state.map;
    for (uint32_t src = 0; src < BUS_MAX_PORTS; src++) {
      if (!c->ports[src].in_use) { continue; }
      const struct bus_consumer_source *e =
          (const struct bus_consumer_source *)(const void *)(base + BUS_REGION_HEADER_BYTES +
                                                             (size_t)src *
                                                                 BUS_CONSUMER_SOURCE_BYTES);
      uint64_t gen = atomic_load_explicit(&e->source_generation, memory_order_acquire);
      uint64_t active = atomic_load_explicit(&e->active_seq, memory_order_acquire);
      struct claim_watch *w = &c->watch[id][src];

      // No claim, or a claim against an identity this source no longer has:
      // nothing is pinned and there is nothing to time.
      if (active == BUS_SEQUENCE_NONE || gen != (uint64_t)c->ports[src].generation) {
        w->seq = BUS_SEQUENCE_NONE;
        continue;
      }
      if (active != w->seq) {
        w->seq = active;
        w->since_ms = now;
        continue;
      }
      if (now - w->since_ms >= c->claim_deadline_ms) {
        coordinator_drain(c, id, "held one claim past the deadline");
        break;
      }
    }
  }
}

static int listen_on(const char *path) {
  struct sockaddr_un a;
  if (strlen(path) >= sizeof(a.sun_path)) {
    errno = ENAMETOOLONG;
    return -1;
  }
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) { return -1; }
  memset(&a, 0, sizeof(a));
  a.sun_family = AF_UNIX;
  snprintf(a.sun_path, sizeof(a.sun_path), "%s", path);
  unlink(path);
  if (bind(fd, (struct sockaddr *)&a, sizeof(a)) < 0 || listen(fd, 16) < 0) {
    int e = errno;
    close(fd);
    errno = e;
    return -1;
  }
  return fd;
}

int bus_coordinator_open(struct bus_coordinator **out, const char *path, uint32_t mtu) {
  if (out == NULL || path == NULL || path[0] == '\0' || mtu < 68 || mtu > 65535) {
    errno = EINVAL;
    return -1;
  }
  *out = NULL;
  struct bus_coordinator *c = calloc(1, sizeof(*c));
  if (c == NULL) { return -1; }
  c->listen_fd = -1;
  c->dir_region.rw = -1;
  c->dir_region.ro = -1;
  for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) {
    port_init(&c->ports[i]);
  }
  c->socket_path = strdup(path);
  c->capacity = BUS_DEFAULT_CAPACITY;
  c->slot_bytes = BUS_DEFAULT_SLOT_BYTES;
  c->mtu = mtu;
  c->join_deadline_ms = 2000;
  c->claim_deadline_ms = 2000;
  c->claim_poll_ms = 250;
  c->verbose = 1;
  if (c->socket_path == NULL) {
    int saved = errno;
    bus_coordinator_close(c);
    errno = saved;
    return -1;
  }
  if (bus_control_check_cache_line() < 0) {
    bus_coordinator_close(c);
    errno = EINVAL;
    return -1;
  }
  c->data_bytes = bus_ring_region_bytes(c->capacity, c->slot_bytes);
  c->state_bytes = bus_consumer_region_bytes();
  if (bus_region_create(&c->dir_region, "dir", 0, bus_directory_region_bytes()) < 0) {
    int saved = errno;
    bus_coordinator_close(c);
    errno = saved;
    return -1;
  }
  bus_region_init_prefix(&c->dir_region, BUS_REGION_DIRECTORY, BUS_PORT_NONE, 0, 0,
                         BUS_MAX_PORTS, BUS_DIRECTORY_ENTRY_BYTES);
  if (bus_directory_open(&c->dir, c->dir_region.map, c->dir_region.bytes) < 0) {
    int saved = EINVAL;
    bus_coordinator_close(c);
    errno = saved;
    return -1;
  }
  bus_directory_init(&c->dir);
  c->listen_fd = listen_on(path);
  if (c->listen_fd < 0) {
    int saved = errno;
    bus_coordinator_close(c);
    errno = saved;
    return -1;
  }
  fprintf(stderr, "bus: coordinator on %s, %u x %u-byte slots = %llu-byte rings, "
                  "%llu-byte state, mtu %u\n",
          path, c->capacity, c->slot_bytes, (unsigned long long)c->data_bytes,
          (unsigned long long)c->state_bytes, c->mtu);
  *out = c;
  return 0;
}

int bus_coordinator_poll(struct bus_coordinator *c, int max_wait_ms) {
  if (c == NULL || max_wait_ms < 0) {
    errno = EINVAL;
    return -1;
  }
  struct pollfd pf[BUS_MAX_PORTS + 1];
  uint32_t who[BUS_MAX_PORTS + 1];
  nfds_t n = 0;
  // No new arrivals while a barrier is outstanding. One join at a time keeps
  // the acknowledgement set unambiguous; queueing a connection costs a
  // newcomer milliseconds and costs the bus nothing.
  if (!c->join.active) {
    pf[n].fd = c->listen_fd;
    pf[n].events = POLLIN;
    pf[n].revents = 0;
    who[n] = BUS_PORT_NONE;
    n++;
  }
  for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) {
    if (!c->ports[i].in_use) { continue; }
    pf[n].fd = c->ports[i].lease;
    pf[n].events = POLLIN;
    pf[n].revents = 0;
    who[n] = i;
    n++;
  }
  int64_t timeout = c->claim_poll_ms;
  if (max_wait_ms < timeout) { timeout = max_wait_ms; }
  if (c->join.active) {
    int64_t left = c->join.deadline_ms - now_ms();
    if (left < timeout) { timeout = (left > 0) ? left : 0; }
  }
  int r = poll(pf, n, (int)timeout);
  if (r < 0) {
    if (errno == EINTR) { return 0; }
    return -1;
  }
  int64_t now = now_ms();
  scan_claims(c, now);
  join_poll(c);
  if (r == 0) {
    if (c->join.active && now >= c->join.deadline_ms) { join_deadline(c); }
    return 0;
  }
  for (nfds_t i = 0; i < n; i++) {
    if (pf[i].revents == 0) { continue; }
    if (who[i] == BUS_PORT_NONE) {
      int lease = accept(c->listen_fd, NULL, NULL);
      if (lease < 0) { continue; }
      if (coordinator_join(c, lease) < 0) {
        close(lease);
      }
      continue;
    }
    if (c->ports[who[i]].in_use) {
      on_lease(c, who[i]);
    }
  }
  return 0;
}

void bus_coordinator_close(struct bus_coordinator *c) {
  if (c == NULL) { return; }
  for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) {
    if (c->ports[i].in_use) { coordinator_retire(c, i); }
  }
  if (c->listen_fd >= 0) { close(c->listen_fd); }
  if (c->socket_path != NULL) { unlink(c->socket_path); }
  bus_region_destroy(&c->dir_region);
  free(c->socket_path);
  free(c);
}
