#ifndef BUS_RING_H
#define BUS_RING_H

#include "../include/bus_abi.h"

#include <stddef.h>
#include <stdint.h>

struct bus_ring_view {
  uint8_t *mem;
  size_t bytes;
  struct bus_ring_header *header;
  uint32_t capacity;
  uint32_t slot_bytes;
  uint32_t owner_port;
  uint32_t owner_generation;
};

struct bus_consumer_view {
  uint8_t *mem;
  size_t bytes;
  struct bus_consumer_header *header;
};

enum bus_claim_status {
  BUS_CLAIM_READY = 0,
  BUS_CLAIM_NOT_TARGETED,
  BUS_CLAIM_STALE_TARGET,
  BUS_CLAIM_RECLAIMING,
  BUS_CLAIM_NOT_PUBLISHED,
  BUS_CLAIM_LAPPED,
  BUS_CLAIM_NOT_INSTALLED,
  BUS_CLAIM_SOURCE_CHANGED,
  BUS_CLAIM_ERROR,
};

struct bus_claim {
  struct bus_ring_view *ring;
  struct bus_consumer_view *consumer;
  struct bus_slot_header *slot;
  uint64_t sequence;
  uint64_t target_bit;
  size_t next_offset;
  size_t payload_end;
  uint32_t records_left;
  int released;
};

struct bus_frame_view {
  const uint8_t *data;
  uint32_t length;
  uint32_t flags;
  uint64_t destination_mask;
};

struct bus_reservation {
  struct bus_ring_view *ring;
  struct bus_slot_header *slot;
  uint64_t sequence;
  uint64_t old_guard;
  size_t next_offset;
  uint32_t record_count;
  uint64_t target_union;
  int buffer_exposed;
  int frame_buffer_outstanding;
  int done;
};

typedef int (*bus_identity_valid_fn)(void *opaque, uint64_t publish_epoch);
typedef int (*bus_pin_mask_fn)(void *opaque, uint64_t publish_epoch,
                               uint64_t old_targets, uint64_t *pin_mask);

int bus_ring_init(struct bus_ring_view *out, void *mem, size_t bytes,
                  uint32_t owner_port, uint32_t owner_generation, uint32_t mtu,
                  uint32_t capacity, uint32_t slot_bytes);
int bus_ring_open(struct bus_ring_view *out, void *mem, size_t bytes);
int bus_consumer_init(struct bus_consumer_view *out, void *mem, size_t bytes,
                      uint32_t owner_port, uint32_t owner_generation);
int bus_consumer_open(struct bus_consumer_view *out, void *mem, size_t bytes);
// Reinstalling an already marked generation is an idempotent no-op and does
// not reset its cursor. A different generation starts at head.
int bus_consumer_install_source(struct bus_consumer_view *consumer,
                                uint32_t source_port,
                                uint32_t source_generation, uint64_t head);
int bus_consumer_mark_source_installed(struct bus_consumer_view *consumer,
                                       uint32_t source_port,
                                       uint32_t source_generation);
uint64_t bus_consumer_installed_generation(
    const struct bus_consumer_view *consumer, uint32_t source_port);
int bus_consumer_mark_prepared(struct bus_consumer_view *consumer,
                               uint32_t consumer_generation);
uint64_t bus_consumer_prepared_generation(
    const struct bus_consumer_view *consumer);

uint64_t bus_ring_head(const struct bus_ring_view *ring);
uint64_t bus_consumer_cursor(const struct bus_consumer_view *consumer,
                             uint32_t source_port);

enum bus_claim_status bus_ring_try_claim(
    struct bus_ring_view *ring, struct bus_consumer_view *consumer,
    uint32_t consumer_port, uint64_t sequence,
    bus_identity_valid_fn identity_valid, void *identity_opaque,
    struct bus_claim *out);
// Returns 1 with a targeted frame, 0 at end, -1 for malformed slot contents.
int bus_claim_next(struct bus_claim *claim, struct bus_frame_view *out);
int bus_claim_release(struct bus_claim *claim, int advance);

// pin_mask contains only old targets whose identity is still live for the
// slot's publish epoch. states is indexed by consumer port.
// Returns 1 with a reservation, 0 when an active claim forced this sequence to
// be skipped, and -1 on error.
int bus_ring_try_reserve(struct bus_ring_view *ring,
                         struct bus_consumer_view *states[BUS_MAX_PORTS],
                         bus_pin_mask_fn pin_mask, void *pin_opaque,
                         struct bus_reservation *out);
// Returns a receive destination that writes frame bytes directly into the slot.
// Once this succeeds, abort is forbidden: publish the reservation even when a
// subsequent nonblocking receive reports EAGAIN.
int bus_reservation_frame_buffer(struct bus_reservation *reservation,
                                 uint8_t **data, size_t *capacity);
int bus_reservation_commit_frame(struct bus_reservation *reservation,
                                 uint32_t length, uint32_t flags,
                                 uint64_t destination_mask);
int bus_reservation_publish(struct bus_reservation *reservation,
                            uint64_t publish_epoch);
int bus_reservation_abort(struct bus_reservation *reservation);
void bus_consumer_record_retry_exhausted(struct bus_consumer_view *consumer,
                                         uint32_t source_port);
void bus_ring_record_doorbells(struct bus_ring_view *ring, uint64_t count);

#endif // BUS_RING_H
