// Shared control protocol, region capabilities, and directory access. The
// shared-memory layout is defined in bus_abi.h; this header does not depend on
// slot internals.

#ifndef BUS_CONTROL_H
#define BUS_CONTROL_H

#include "bus_abi.h"

#include <stddef.h>
#include <stdint.h>

#define BUS_CONTROL_VERSION 1u

// Generations use uint32 values stored in the ABI's 64-bit owner/source fields.

// ---------------------------------------------------------------------------
// Membership directory
//
// The coordinator writes the directory; participants map it read-only. It
// lets producers validate target identities without a control-plane round trip.
// The region contains a header followed by BUS_MAX_PORTS entries.
// ---------------------------------------------------------------------------

struct bus_directory_view {
  struct bus_directory_header *header;
  struct bus_directory_entry *entries;
  uint64_t bytes;
};

// These pointers address a read-only mapping; the MMU rejects writes.
int bus_directory_open(struct bus_directory_view *v, void *region, uint64_t bytes);

// ---------------------------------------------------------------------------
// Epoch protocol
//
// The coordinator brackets directory updates with odd/even epochs. Readers
// accept a snapshot only when the same even epoch is observed before and after
// copying entries. Per-entry reads use one atomic identity word.
// ---------------------------------------------------------------------------

// A validated private copy of one directory snapshot.
struct bus_port_view {
  uint32_t state;
  uint32_t generation;
  uint64_t changed_at_epoch;
  uint64_t mac;
  uint64_t data_bytes;
  uint64_t state_bytes;
  uint64_t mtu_flags;
};

struct bus_membership {
  uint64_t epoch;                   // even; the membership is exact at this epoch
  uint32_t max_ports;
  uint64_t live_mask;               // bit per LIVE port, the flood set at `epoch`
  uint64_t uplink_mask;             // LIVE ports carrying BUS_PORT_FLAG_UPLINK
  struct bus_port_view ports[BUS_MAX_PORTS];
};

// Coordinator-side directory updates; begin and commit must be paired.
void bus_directory_init(struct bus_directory_view *v);
uint64_t bus_directory_begin(struct bus_directory_view *v);   // returns the odd epoch
uint64_t bus_directory_commit(struct bus_directory_view *v);  // returns the new even epoch
void bus_port_publish(struct bus_directory_view *v, uint32_t port, uint32_t state,
                      uint32_t generation, uint64_t mac, uint64_t data_bytes,
                      uint64_t state_bytes, uint32_t mtu, uint32_t flags);

// Returns 0 for a consistent snapshot, or -1 if all retries raced an update.
int bus_directory_read(const struct bus_directory_view *v, struct bus_membership *out,
                       unsigned retries);

// Guests target every other live port. Uplinks target guests only, preventing
// forwarding loops between uplinks.
static inline uint64_t bus_membership_targets(const struct bus_membership *m, uint32_t source) {
  uint64_t others = m->live_mask & ~((uint64_t)1 << source);
  int source_is_uplink = (m->uplink_mask & ((uint64_t)1 << source)) != 0;
  return source_is_uplink ? (others & ~m->uplink_mask) : others;
}

// Read the identity before changed_at_epoch to avoid pairing a new identity
// with an old change epoch.
void bus_port_read(const struct bus_directory_view *v, uint32_t port,
                   struct bus_port_view *out);

int bus_control_check_cache_line(void);

// ---------------------------------------------------------------------------
// Regions
//
// Initialize each region's ABI prefix before passing its descriptor to
// participants.
// ---------------------------------------------------------------------------

struct bus_region {
  int rw;            // coordinator/owner writes through this
  int ro;            // retained for every participant that joins later
  uint64_t bytes;
  void *map;         // coordinator's own mapping, RW
};

int bus_region_create(struct bus_region *r, const char *tag, unsigned id, uint64_t bytes);
void bus_region_destroy(struct bus_region *r);

// Fills struct bus_region_prefix. Call before the descriptor is sent anywhere.
void bus_region_init_prefix(struct bus_region *r, uint32_t kind, uint32_t owner_port,
                            uint32_t owner_generation, uint32_t mtu, uint32_t capacity,
                            uint32_t slot_bytes);
// What a participant checks on arrival. Returns 0 if the region is what the
// message said it was.
int bus_region_check_prefix(const void *map, uint64_t bytes, uint32_t kind);

// ---------------------------------------------------------------------------
// Wire messages
//
// Messages are fixed-width little-endian structs. Each capability message
// carries one descriptor in its SCM_RIGHTS payload.
// ---------------------------------------------------------------------------

enum bus_msg_type {
  BUS_MSG_HELLO = 1,        // participant -> coordinator
  BUS_MSG_WELCOME = 2,      // coordinator -> participant, + directory fd (RO)
  BUS_MSG_OWN_DATA = 3,     // + this port's producer region (RW)
  BUS_MSG_OWN_STATE = 4,    // + this port's consumer-state region (RW)
  BUS_MSG_PEER_DATA = 5,    // + a peer's producer region (RO)
  BUS_MSG_PEER_STATE = 6,   // + a peer's consumer-state region (RO)
  BUS_MSG_DOORBELL_RX = 7,  // + this port's doorbell receive end
  BUS_MSG_DOORBELL_TX = 8,  // + a peer's doorbell send end
  BUS_MSG_READY = 9,        // coordinator -> participant: install complete
  BUS_MSG_RETIRED = 10,     // coordinator -> participants: a port went away, a hint
  BUS_MSG_END_CAPABILITIES = 11,  // coordinator -> participant: that is the whole bundle for `port`
  BUS_MSG_WAKE = 12,        // participant -> coordinator: the ledger changed, a hint
  BUS_MSG_ABORTED = 13,     // coordinator -> participants: a join failed, undo `port`, a hint
};

// Capability records are idempotent by (kind, port, generation). RETIRED,
// ABORTED, and WAKE are hints; the directory and ledger are authoritative.

struct bus_msg {
  uint32_t type;            // enum bus_msg_type
  uint32_t version;         // BUS_CONTROL_VERSION, checked on HELLO
  uint32_t port;            // subject of the message; BUS_PORT_NONE in HELLO
  uint32_t generation;      // of `port`, so a stale message is detectable
  uint64_t bytes;           // size of the region in the attached descriptor
  uint64_t epoch;           // directory epoch; BUS_PORT_FLAG_* in HELLO
};

// The control lease uses SOCK_STREAM, so readers assemble each message across
// reads and retain attached descriptors until the full message is available.
#define BUS_MSG_INCOMPLETE 2
#define BUS_MSG_SEND_TIMEOUT_MS 2000

struct bus_msg_reader {
  struct bus_msg partial;
  size_t got;
  int fd;                   // held until the message it belongs to is whole
};

void bus_msg_reader_init(struct bus_msg_reader *r);
void bus_msg_reader_close(struct bus_msg_reader *r);

int bus_msg_send(int sock, const struct bus_msg *m, int fd);
int bus_msg_read(int sock, struct bus_msg_reader *r, struct bus_msg *m, int *fd);
int bus_msg_read_by(int sock, struct bus_msg_reader *r, struct bus_msg *m, int *fd,
                    int64_t timeout_ms);

// The control stream is the lease; EOF signals participant disconnect.
// Join order is normative: prepare regions, distribute capabilities, wait for
// ledger acknowledgements, then publish LIVE.
#endif // BUS_CONTROL_H
