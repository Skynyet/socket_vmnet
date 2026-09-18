// Shared-memory ABI for the Lima shared Ethernet bus.

#ifndef BUS_ABI_H
#define BUS_ABI_H

#include <stdalign.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#define BUS_ABI_VERSION 2u
#define BUS_ABI_MAGIC UINT64_C(0x315355424d48534c) // little-endian "LSHMBUS1"

// Use 128-byte spacing to isolate frequently updated fields.
#define BUS_CACHELINE_BYTES 128u
#define BUS_MAX_PORTS 64u
#define BUS_PORT_NONE UINT32_MAX

#define BUS_REGION_PREFIX_BYTES 128u
#define BUS_REGION_HEADER_BYTES 256u
#define BUS_SLOT_HEADER_BYTES 128u
#define BUS_FRAME_HEADER_BYTES 16u
#define BUS_CONSUMER_SOURCE_BYTES 128u
#define BUS_DIRECTORY_ENTRY_BYTES 128u

#define BUS_DEFAULT_CAPACITY 256u
#define BUS_DEFAULT_SLOT_PAYLOAD_BYTES (128u * 1024u)
#define BUS_DEFAULT_SLOT_BYTES (BUS_SLOT_HEADER_BYTES + BUS_DEFAULT_SLOT_PAYLOAD_BYTES)
#define BUS_DEFAULT_MTU 1500u

// Port role metadata shares one directory word with the MTU: the low 32 bits
// are the MTU and the high 32 bits are flags. Ordinary guest ports have no
// flags. An uplink is an external network participant, not a magic port id.
#define BUS_PORT_FLAG_UPLINK UINT32_C(1)
#define BUS_PORT_FLAGS_MASK BUS_PORT_FLAG_UPLINK

#define BUS_SEQUENCE_NONE UINT64_C(0)
#define BUS_GUARD_RECLAIMING_BIT UINT64_C(1)
#define BUS_GUARD_SEQUENCE_MAX (UINT64_MAX >> 1)

enum bus_region_kind {
  BUS_REGION_PRODUCER = 1,
  BUS_REGION_CONSUMER = 2,
  BUS_REGION_DIRECTORY = 3,
};

enum bus_port_state {
  BUS_PORT_FREE = 0,
  BUS_PORT_PREPARING = 1,
  BUS_PORT_LIVE = 2,
  BUS_PORT_DRAINING = 3,
  BUS_PORT_RETIRED = 4,
};

// Generations are uint32 values. The 64-bit owner/source storage in this ABI
// has its upper 32 bits reserved and zero.
static inline uint64_t bus_identity_state(uint32_t state, uint32_t generation) {
  return (uint64_t)state | ((uint64_t)generation << 32);
}
static inline uint32_t bus_identity_port_state(uint64_t identity) {
  return (uint32_t)identity;
}
static inline uint32_t bus_identity_generation(uint64_t identity) {
  return (uint32_t)(identity >> 32);
}
static inline uint64_t bus_pack_mtu_flags(uint32_t mtu, uint32_t flags) {
  return (uint64_t)mtu | ((uint64_t)flags << 32);
}
static inline uint32_t bus_port_mtu(uint64_t mtu_flags) {
  return (uint32_t)mtu_flags;
}
static inline uint32_t bus_port_flags(uint64_t mtu_flags) {
  return (uint32_t)(mtu_flags >> 32);
}

// First cache line of every region. Fields are native-endian: all mappings are
// local to one Darwin host, never persisted and never transmitted over a wire.
struct bus_region_prefix {
  _Alignas(BUS_CACHELINE_BYTES) uint64_t magic;
  uint32_t abi_version;
  uint32_t region_kind;
  uint32_t header_bytes;
  uint32_t max_ports;
  uint64_t region_bytes;
  uint64_t owner_generation;
  uint32_t owner_port;
  uint32_t mtu;
  uint32_t capacity;
  uint32_t slot_bytes;
  uint64_t flags;
  uint64_t reserved[8];
};

// One producer owns this header and all following slots. Consumers map the
// entire region read-only. `head` is the next sequence to publish and starts at
// 1, so published work is [consumer.cursor, head).
struct bus_ring_header {
  struct bus_region_prefix prefix;
  _Alignas(BUS_CACHELINE_BYTES) _Atomic(uint64_t) head;
  _Atomic(uint64_t) published_slots;
  _Atomic(uint64_t) drop_slot_actively_claimed;
  _Atomic(uint64_t) doorbells;
  uint64_t reserved[12];
};

// A slot is immutable while guard is PUBLISHED(seq). Only the producer writes
// it. RECLAIMING(seq) closes the gate to new readers while the producer checks
// consumer-owned active claims.
struct bus_slot_header {
  _Alignas(BUS_CACHELINE_BYTES) _Atomic(uint64_t) guard;
  // Routing metadata is atomic because an uninterested consumer must be able
  // to decide that it can skip the slot without claiming the payload. It reads
  // guard, these two words, then guard again and accepts only a stable snapshot.
  _Atomic(uint64_t) publish_epoch;
  _Atomic(uint64_t) target_union;
  uint32_t payload_bytes;
  uint32_t record_count;
  uint64_t reserved[12];
};

// Records are packed in a slot payload and padded to an 8-byte boundary. The
// frame bytes immediately follow this header.
struct bus_frame_header {
  uint32_t length;
  uint32_t flags;
  uint64_t destination_mask;
};

// One entry for every installed source identity. This region is RW only for
// its consumer; producers and the coordinator receive RO capabilities.
struct bus_consumer_source {
  _Alignas(BUS_CACHELINE_BYTES) _Atomic(uint64_t) source_generation;
  _Atomic(uint64_t) cursor;
  _Atomic(uint64_t) active_seq;
  // Reserved in ABI v2 and required to remain zero. It has no clock semantics;
  // the coordinator keeps claim-liveness timing in private process memory.
  uint64_t reserved_clock;
  _Atomic(uint64_t) claim_conflicts;
  _Atomic(uint64_t) drop_claim_conflict_retry_exhausted;
  // Written by this consumer after it has installed the complete capability
  // bundle for this concrete source generation.
  _Atomic(uint64_t) installed_generation;
  uint64_t reserved[9];
};

struct bus_consumer_header {
  struct bus_region_prefix prefix;
  // Reserved in ABI v2 and required to remain zero. Consumer ledger words are
  // individually atomic and do not use a region-wide seqlock.
  _Alignas(BUS_CACHELINE_BYTES) _Atomic(uint64_t) reserved_state_epoch;
  // Written by this consumer after every retained source in the directory has
  // been installed. It makes the consumer itself eligible to become LIVE.
  _Atomic(uint64_t) prepared_generation;
  uint64_t reserved[14];
};

// Directory updates use a global odd/even seqlock. The coordinator changes
// membership_epoch from even to odd, updates atomic entry words, then stores the
// next even epoch. Readers accept a snapshot only when both epoch loads match
// and are even. changed_at_epoch stores that final even value.
struct bus_directory_entry {
  _Alignas(BUS_CACHELINE_BYTES) _Atomic(uint64_t) identity_state;
  _Atomic(uint64_t) changed_at_epoch;
  _Atomic(uint64_t) mac;
  _Atomic(uint64_t) data_bytes;
  _Atomic(uint64_t) state_bytes;
  _Atomic(uint64_t) mtu_flags;
  uint64_t reserved[10];
};

struct bus_directory_header {
  struct bus_region_prefix prefix;
  _Alignas(BUS_CACHELINE_BYTES) _Atomic(uint64_t) membership_epoch;
  uint64_t reserved[15];
};

static inline uint64_t bus_guard_published(uint64_t seq) { return seq << 1; }
static inline uint64_t bus_guard_reclaiming(uint64_t seq) {
  return (seq << 1) | BUS_GUARD_RECLAIMING_BIT;
}
static inline uint64_t bus_guard_sequence(uint64_t guard) { return guard >> 1; }
static inline int bus_guard_is_reclaiming(uint64_t guard) {
  return (guard & BUS_GUARD_RECLAIMING_BIT) != 0;
}
static inline size_t bus_align8(size_t n) { return (n + 7u) & ~(size_t)7u; }
static inline size_t bus_ring_region_bytes(uint32_t capacity, uint32_t slot_bytes) {
  return BUS_REGION_HEADER_BYTES + (size_t)capacity * slot_bytes;
}
static inline size_t bus_consumer_region_bytes(void) {
  return BUS_REGION_HEADER_BYTES + (size_t)BUS_MAX_PORTS * BUS_CONSUMER_SOURCE_BYTES;
}
static inline size_t bus_directory_region_bytes(void) {
  return BUS_REGION_HEADER_BYTES + (size_t)BUS_MAX_PORTS * BUS_DIRECTORY_ENTRY_BYTES;
}

_Static_assert(sizeof(_Atomic(uint64_t)) == sizeof(uint64_t),
               "shared uint64 atomics must not carry hidden storage");
_Static_assert(_Alignof(_Atomic(uint64_t)) <= sizeof(uint64_t),
               "shared uint64 atomics require unexpected alignment");
_Static_assert(sizeof(struct bus_region_prefix) == BUS_REGION_PREFIX_BYTES,
               "region prefix ABI drift");
_Static_assert(sizeof(struct bus_ring_header) == BUS_REGION_HEADER_BYTES,
               "ring header ABI drift");
_Static_assert(sizeof(struct bus_consumer_header) == BUS_REGION_HEADER_BYTES,
               "consumer header ABI drift");
_Static_assert(sizeof(struct bus_directory_header) == BUS_REGION_HEADER_BYTES,
               "directory header ABI drift");
_Static_assert(sizeof(struct bus_slot_header) == BUS_SLOT_HEADER_BYTES,
               "slot header ABI drift");
_Static_assert(sizeof(struct bus_frame_header) == BUS_FRAME_HEADER_BYTES,
               "frame header ABI drift");
_Static_assert(sizeof(struct bus_consumer_source) == BUS_CONSUMER_SOURCE_BYTES,
               "consumer source ABI drift");
_Static_assert(sizeof(struct bus_directory_entry) == BUS_DIRECTORY_ENTRY_BYTES,
               "directory entry ABI drift");
_Static_assert(offsetof(struct bus_ring_header, head) == BUS_CACHELINE_BYTES,
               "ring head must start a cache line");
_Static_assert(offsetof(struct bus_slot_header, guard) == 0,
               "slot guard offset drift");
_Static_assert(offsetof(struct bus_consumer_source, installed_generation) == 48,
               "installed generation offset drift");
_Static_assert(offsetof(struct bus_consumer_header, prepared_generation) ==
                   BUS_CACHELINE_BYTES + sizeof(uint64_t),
               "prepared generation offset drift");
_Static_assert(BUS_DEFAULT_SLOT_BYTES % BUS_CACHELINE_BYTES == 0,
               "default slot stride must be cache-line integral");

#endif // BUS_ABI_H
