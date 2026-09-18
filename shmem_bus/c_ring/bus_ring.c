#include "bus_ring.h"

#include <limits.h>
#include <stdatomic.h>
#include <string.h>

static int prefix_valid(const struct bus_region_prefix *p, size_t bytes,
                        uint32_t kind) {
  return p->magic == BUS_ABI_MAGIC && p->abi_version == BUS_ABI_VERSION &&
         p->region_kind == kind && p->header_bytes == BUS_REGION_HEADER_BYTES &&
         p->max_ports == BUS_MAX_PORTS && p->region_bytes == bytes;
}

static void prefix_init(struct bus_region_prefix *p, uint32_t kind,
                        size_t bytes, uint32_t owner_port,
                        uint32_t owner_generation, uint32_t mtu,
                        uint32_t capacity, uint32_t slot_bytes) {
  p->magic = BUS_ABI_MAGIC;
  p->abi_version = BUS_ABI_VERSION;
  p->region_kind = kind;
  p->header_bytes = BUS_REGION_HEADER_BYTES;
  p->max_ports = BUS_MAX_PORTS;
  p->region_bytes = bytes;
  p->owner_generation = owner_generation;
  p->owner_port = owner_port;
  p->mtu = mtu;
  p->capacity = capacity;
  p->slot_bytes = slot_bytes;
}

static struct bus_slot_header *slot_at(const struct bus_ring_view *ring,
                                       uint64_t sequence) {
  size_t index = (size_t)((sequence - 1) % ring->capacity);
  return (struct bus_slot_header *)(ring->mem + BUS_REGION_HEADER_BYTES +
                                    index * ring->slot_bytes);
}

static struct bus_consumer_source *source_at(
    const struct bus_consumer_view *consumer, uint32_t source_port) {
  return (struct bus_consumer_source *)(consumer->mem +
      BUS_REGION_HEADER_BYTES + (size_t)source_port * BUS_CONSUMER_SOURCE_BYTES);
}

int bus_ring_open(struct bus_ring_view *out, void *mem, size_t bytes) {
  if (out == NULL || mem == NULL || bytes < BUS_REGION_HEADER_BYTES) return -1;
  struct bus_ring_header *h = mem;
  uint32_t capacity = h->prefix.capacity;
  uint32_t slot_bytes = h->prefix.slot_bytes;
  if (!prefix_valid(&h->prefix, bytes, BUS_REGION_PRODUCER) || capacity == 0 ||
      h->prefix.owner_port >= BUS_MAX_PORTS ||
      h->prefix.owner_generation > UINT32_MAX ||
      slot_bytes < BUS_SLOT_HEADER_BYTES + BUS_FRAME_HEADER_BYTES ||
      slot_bytes % BUS_CACHELINE_BYTES != 0 ||
      bus_ring_region_bytes(capacity, slot_bytes) != bytes ||
      !atomic_is_lock_free(&h->head)) {
    return -1;
  }
  uint64_t head = atomic_load_explicit(&h->head, memory_order_seq_cst);
  if (head == 0 || head > BUS_GUARD_SEQUENCE_MAX) return -1;
  struct bus_slot_header *first =
      (struct bus_slot_header *)((uint8_t *)mem + BUS_REGION_HEADER_BYTES);
  if (!atomic_is_lock_free(&first->guard)) return -1;
  *out = (struct bus_ring_view){
      .mem = mem, .bytes = bytes, .header = h, .capacity = capacity,
      .slot_bytes = slot_bytes, .owner_port = h->prefix.owner_port,
      .owner_generation = (uint32_t)h->prefix.owner_generation};
  return 0;
}

int bus_ring_init(struct bus_ring_view *out, void *mem, size_t bytes,
                  uint32_t owner_port, uint32_t owner_generation, uint32_t mtu,
                  uint32_t capacity, uint32_t slot_bytes) {
  if (mem == NULL || owner_port >= BUS_MAX_PORTS || capacity == 0 ||
      slot_bytes < BUS_SLOT_HEADER_BYTES + BUS_FRAME_HEADER_BYTES ||
      slot_bytes % BUS_CACHELINE_BYTES != 0 ||
      bus_ring_region_bytes(capacity, slot_bytes) != bytes) return -1;
  memset(mem, 0, bytes);
  struct bus_ring_header *h = mem;
  prefix_init(&h->prefix, BUS_REGION_PRODUCER, bytes, owner_port,
              owner_generation, mtu, capacity, slot_bytes);
  atomic_store_explicit(&h->head, 1, memory_order_seq_cst);
  return bus_ring_open(out, mem, bytes);
}

int bus_consumer_open(struct bus_consumer_view *out, void *mem, size_t bytes) {
  if (out == NULL || mem == NULL || bytes != bus_consumer_region_bytes()) return -1;
  struct bus_consumer_header *h = mem;
  if (!prefix_valid(&h->prefix, bytes, BUS_REGION_CONSUMER) ||
      h->prefix.owner_port >= BUS_MAX_PORTS ||
      h->prefix.owner_generation > UINT32_MAX) return -1;
  struct bus_consumer_source *first = source_at(
      &(struct bus_consumer_view){.mem = mem}, 0);
  if (!atomic_is_lock_free(&first->active_seq)) return -1;
  *out = (struct bus_consumer_view){.mem = mem, .bytes = bytes, .header = h};
  return 0;
}

int bus_consumer_init(struct bus_consumer_view *out, void *mem, size_t bytes,
                      uint32_t owner_port, uint32_t owner_generation) {
  if (mem == NULL || owner_port >= BUS_MAX_PORTS ||
      bytes != bus_consumer_region_bytes()) return -1;
  memset(mem, 0, bytes);
  struct bus_consumer_header *h = mem;
  prefix_init(&h->prefix, BUS_REGION_CONSUMER, bytes, owner_port,
              owner_generation, 0, BUS_MAX_PORTS, BUS_CONSUMER_SOURCE_BYTES);
  return bus_consumer_open(out, mem, bytes);
}

int bus_consumer_install_source(struct bus_consumer_view *consumer,
                                uint32_t source_port,
                                uint32_t source_generation, uint64_t head) {
  if (consumer == NULL || source_port >= BUS_MAX_PORTS || head == 0) return -1;
  struct bus_consumer_source *s = source_at(consumer, source_port);
  uint64_t installed = atomic_load_explicit(&s->installed_generation,
                                            memory_order_seq_cst);
  if (installed == source_generation && source_generation != 0) {
    return atomic_load_explicit(&s->source_generation, memory_order_seq_cst) ==
                   installed
               ? 0
               : -1;
  }
  atomic_store_explicit(&s->active_seq, BUS_SEQUENCE_NONE, memory_order_seq_cst);
  atomic_store_explicit(&s->cursor, head, memory_order_seq_cst);
  atomic_store_explicit(&s->source_generation, source_generation,
                        memory_order_seq_cst);
  return 0;
}

int bus_consumer_mark_source_installed(struct bus_consumer_view *consumer,
                                       uint32_t source_port,
                                       uint32_t source_generation) {
  if (consumer == NULL || source_port >= BUS_MAX_PORTS ||
      source_generation == 0) return -1;
  struct bus_consumer_source *s = source_at(consumer, source_port);
  if (atomic_load_explicit(&s->source_generation, memory_order_seq_cst) !=
      source_generation) return -1;
  atomic_store_explicit(&s->installed_generation, source_generation,
                        memory_order_seq_cst);
  return 0;
}

uint64_t bus_consumer_installed_generation(
    const struct bus_consumer_view *consumer, uint32_t source_port) {
  if (consumer == NULL || source_port >= BUS_MAX_PORTS) return 0;
  return atomic_load_explicit(&source_at(consumer, source_port)->installed_generation,
                              memory_order_seq_cst);
}

int bus_consumer_mark_prepared(struct bus_consumer_view *consumer,
                               uint32_t consumer_generation) {
  if (consumer == NULL || consumer_generation == 0 ||
      consumer->header->prefix.owner_generation != consumer_generation)
    return -1;
  atomic_store_explicit(&consumer->header->prepared_generation,
                        consumer_generation, memory_order_seq_cst);
  return 0;
}

uint64_t bus_consumer_prepared_generation(
    const struct bus_consumer_view *consumer) {
  if (consumer == NULL) return 0;
  return atomic_load_explicit(&consumer->header->prepared_generation,
                              memory_order_seq_cst);
}

uint64_t bus_ring_head(const struct bus_ring_view *ring) {
  return atomic_load_explicit(&ring->header->head, memory_order_seq_cst);
}

uint64_t bus_consumer_cursor(const struct bus_consumer_view *consumer,
                             uint32_t source_port) {
  if (consumer == NULL || source_port >= BUS_MAX_PORTS) return 0;
  return atomic_load_explicit(&source_at(consumer, source_port)->cursor,
                              memory_order_seq_cst);
}

static enum bus_claim_status classify_miss(
    struct bus_ring_view *ring, struct bus_consumer_source *state,
    uint64_t sequence, uint64_t guard) {
  uint64_t observed = bus_guard_sequence(guard);
  if (observed == sequence && bus_guard_is_reclaiming(guard)) {
    atomic_fetch_add_explicit(&state->claim_conflicts, 1, memory_order_relaxed);
    return BUS_CLAIM_RECLAIMING;
  }
  if (observed < sequence) {
    atomic_store_explicit(&state->cursor, sequence + 1, memory_order_seq_cst);
    return BUS_CLAIM_NOT_PUBLISHED;
  }
  uint64_t head = bus_ring_head(ring);
  uint64_t oldest = 1;
  if (head > ring->capacity) oldest = head - ring->capacity;
  if (oldest <= sequence) oldest = sequence + 1;
  atomic_store_explicit(&state->cursor, oldest, memory_order_seq_cst);
  return BUS_CLAIM_LAPPED;
}

enum bus_claim_status bus_ring_try_claim(
    struct bus_ring_view *ring, struct bus_consumer_view *consumer,
    uint32_t consumer_port, uint64_t sequence,
    bus_identity_valid_fn identity_valid, void *identity_opaque,
    struct bus_claim *out) {
  if (ring == NULL || consumer == NULL || out == NULL || identity_valid == NULL ||
      consumer_port >= BUS_MAX_PORTS || sequence == 0) return BUS_CLAIM_ERROR;
  struct bus_consumer_source *state = source_at(consumer, ring->owner_port);
  uint64_t source_generation =
      atomic_load_explicit(&state->source_generation, memory_order_seq_cst);
  if (source_generation == 0) return BUS_CLAIM_NOT_INSTALLED;
  if (source_generation != ring->owner_generation)
    return BUS_CLAIM_SOURCE_CHANGED;
  struct bus_slot_header *slot = slot_at(ring, sequence);
  uint64_t want = bus_guard_published(sequence);
  uint64_t guard = atomic_load_explicit(&slot->guard, memory_order_seq_cst);
  if (guard != want) return classify_miss(ring, state, sequence, guard);
  uint64_t targets = atomic_load_explicit(&slot->target_union, memory_order_seq_cst);
  uint64_t epoch = atomic_load_explicit(&slot->publish_epoch, memory_order_seq_cst);
  uint64_t again = atomic_load_explicit(&slot->guard, memory_order_seq_cst);
  if (again != guard) return classify_miss(ring, state, sequence, again);
  uint64_t bit = UINT64_C(1) << consumer_port;
  if ((targets & bit) == 0) {
    atomic_store_explicit(&state->cursor, sequence + 1, memory_order_seq_cst);
    return BUS_CLAIM_NOT_TARGETED;
  }
  if (!identity_valid(identity_opaque, epoch)) {
    atomic_store_explicit(&state->cursor, sequence + 1, memory_order_seq_cst);
    return BUS_CLAIM_STALE_TARGET;
  }

  atomic_store_explicit(&state->active_seq, sequence, memory_order_seq_cst);
  again = atomic_load_explicit(&slot->guard, memory_order_seq_cst);
  if (again != want) {
    atomic_store_explicit(&state->active_seq, BUS_SEQUENCE_NONE,
                          memory_order_seq_cst);
    return classify_miss(ring, state, sequence, again);
  }

  uint32_t payload = slot->payload_bytes;
  if (payload > ring->slot_bytes - BUS_SLOT_HEADER_BYTES) {
    atomic_store_explicit(&state->active_seq, BUS_SEQUENCE_NONE,
                          memory_order_seq_cst);
    return BUS_CLAIM_ERROR;
  }
  *out = (struct bus_claim){
      .ring = ring, .consumer = consumer, .slot = slot, .sequence = sequence,
      .target_bit = bit, .next_offset = BUS_SLOT_HEADER_BYTES,
      .payload_end = BUS_SLOT_HEADER_BYTES + payload,
      .records_left = slot->record_count, .released = 0};
  return BUS_CLAIM_READY;
}

int bus_claim_next(struct bus_claim *claim, struct bus_frame_view *out) {
  if (claim == NULL || out == NULL || claim->released) return -1;
  uint8_t *base = (uint8_t *)claim->slot;
  while (claim->records_left != 0) {
    if (claim->next_offset > claim->payload_end ||
        claim->payload_end - claim->next_offset < BUS_FRAME_HEADER_BYTES) return -1;
    struct bus_frame_header *f =
        (struct bus_frame_header *)(base + claim->next_offset);
    size_t frame_end = claim->next_offset + BUS_FRAME_HEADER_BYTES + f->length;
    if (frame_end > claim->payload_end) return -1;
    size_t next = bus_align8(frame_end);
    if (next > claim->payload_end) return -1;
    claim->next_offset = next;
    claim->records_left--;
    if ((f->destination_mask & claim->target_bit) == 0) continue;
    *out = (struct bus_frame_view){
        .data = (const uint8_t *)f + BUS_FRAME_HEADER_BYTES,
        .length = f->length, .flags = f->flags,
        .destination_mask = f->destination_mask};
    return 1;
  }
  return claim->next_offset == claim->payload_end ? 0 : -1;
}

int bus_claim_release(struct bus_claim *claim, int advance) {
  if (claim == NULL || claim->released) return -1;
  struct bus_consumer_source *state =
      source_at(claim->consumer, claim->ring->owner_port);
  atomic_store_explicit(&state->active_seq, BUS_SEQUENCE_NONE,
                        memory_order_seq_cst);
  if (advance) atomic_store_explicit(&state->cursor, claim->sequence + 1,
                                     memory_order_seq_cst);
  claim->released = 1;
  return 0;
}

int bus_ring_try_reserve(struct bus_ring_view *ring,
                         struct bus_consumer_view *states[BUS_MAX_PORTS],
                         bus_pin_mask_fn pin_mask, void *pin_opaque,
                         struct bus_reservation *out) {
  if (ring == NULL || states == NULL || pin_mask == NULL || out == NULL) return -1;
  uint64_t seq = bus_ring_head(ring);
  if (seq == 0 || seq > BUS_GUARD_SEQUENCE_MAX) return -1;
  struct bus_slot_header *slot = slot_at(ring, seq);
  uint64_t old_guard = atomic_load_explicit(&slot->guard, memory_order_seq_cst);
  if (bus_guard_is_reclaiming(old_guard)) return -1;
  uint64_t old_seq = bus_guard_sequence(old_guard);
  atomic_store_explicit(&slot->guard, bus_guard_reclaiming(old_seq),
                        memory_order_seq_cst);
  uint64_t old_targets = atomic_load_explicit(&slot->target_union,
                                              memory_order_seq_cst);
  uint64_t old_epoch = atomic_load_explicit(&slot->publish_epoch,
                                            memory_order_seq_cst);
  uint64_t pins = 0;
  if (pin_mask(pin_opaque, old_epoch, old_targets, &pins) < 0)
    goto error_restore;
  uint64_t targets = old_targets & pins;
  while (targets != 0) {
    unsigned port = (unsigned)__builtin_ctzll(targets);
    targets &= targets - 1;
    if (states[port] == NULL) goto error_restore;
    struct bus_consumer_source *state = source_at(states[port], ring->owner_port);
    uint64_t generation = atomic_load_explicit(&state->source_generation,
                                               memory_order_seq_cst);
    uint64_t active = atomic_load_explicit(&state->active_seq,
                                           memory_order_seq_cst);
    if (old_seq != BUS_SEQUENCE_NONE && generation == ring->owner_generation &&
        active == old_seq) {
      atomic_store_explicit(&slot->guard, old_guard, memory_order_seq_cst);
      atomic_store_explicit(&ring->header->head, seq + 1, memory_order_seq_cst);
      atomic_fetch_add_explicit(&ring->header->drop_slot_actively_claimed, 1,
                                memory_order_relaxed);
      return 0;
    }
  }
  *out = (struct bus_reservation){
      .ring = ring, .slot = slot, .sequence = seq, .old_guard = old_guard,
      .next_offset = BUS_SLOT_HEADER_BYTES};
  return 1;

error_restore:
  atomic_store_explicit(&slot->guard, old_guard, memory_order_seq_cst);
  return -1;
}

int bus_reservation_frame_buffer(struct bus_reservation *r,
                                 uint8_t **data, size_t *capacity) {
  if (r == NULL || data == NULL || capacity == NULL || r->done) return -1;
  if (r->next_offset > r->ring->slot_bytes ||
      r->ring->slot_bytes - r->next_offset < BUS_FRAME_HEADER_BYTES) return 0;
  // recv(2) writes before commit_frame can validate.  Round the available
  // record down here so every length the kernel can return, including the
  // advertised maximum, still leaves room for mandatory 8-byte padding.
  size_t record_capacity =
      (r->ring->slot_bytes - r->next_offset) & ~(size_t)7;
  *data = (uint8_t *)r->slot + r->next_offset + BUS_FRAME_HEADER_BYTES;
  *capacity = record_capacity - BUS_FRAME_HEADER_BYTES;
  if (*capacity > UINT32_MAX) *capacity = UINT32_MAX;
  r->buffer_exposed = 1;
  r->frame_buffer_outstanding = 1;
  return 1;
}

int bus_reservation_commit_frame(struct bus_reservation *r,
                                 uint32_t length, uint32_t flags,
                                 uint64_t destination_mask) {
  if (r == NULL || r->done || !r->frame_buffer_outstanding) return -1;
  size_t record_bytes = bus_align8(BUS_FRAME_HEADER_BYTES + (size_t)length);
  if (r->next_offset > r->ring->slot_bytes ||
      record_bytes > r->ring->slot_bytes - r->next_offset) return -1;
  struct bus_frame_header *f =
      (struct bus_frame_header *)((uint8_t *)r->slot + r->next_offset);
  f->length = length;
  f->flags = flags;
  f->destination_mask = destination_mask;
  size_t unpadded = BUS_FRAME_HEADER_BYTES + (size_t)length;
  if (record_bytes != unpadded)
    memset((uint8_t *)f + unpadded, 0, record_bytes - unpadded);
  r->next_offset += record_bytes;
  r->record_count++;
  r->target_union |= destination_mask;
  r->frame_buffer_outstanding = 0;
  return 0;
}

int bus_reservation_publish(struct bus_reservation *r,
                            uint64_t publish_epoch) {
  if (r == NULL || r->done || publish_epoch == 0 || (publish_epoch & 1) != 0)
    return -1;
  size_t payload = r->next_offset - BUS_SLOT_HEADER_BYTES;
  if (payload > UINT32_MAX) return -1;
  r->slot->payload_bytes = (uint32_t)payload;
  r->slot->record_count = r->record_count;
  atomic_store_explicit(&r->slot->publish_epoch, publish_epoch,
                        memory_order_seq_cst);
  atomic_store_explicit(&r->slot->target_union, r->target_union,
                        memory_order_seq_cst);
  atomic_store_explicit(&r->slot->guard, bus_guard_published(r->sequence),
                        memory_order_seq_cst);
  atomic_store_explicit(&r->ring->header->head, r->sequence + 1,
                        memory_order_seq_cst);
  atomic_fetch_add_explicit(&r->ring->header->published_slots, 1,
                            memory_order_relaxed);
  r->done = 1;
  return 0;
}

int bus_reservation_abort(struct bus_reservation *r) {
  if (r == NULL || r->done || r->buffer_exposed) return -1;
  atomic_store_explicit(&r->slot->guard, r->old_guard, memory_order_seq_cst);
  r->done = 1;
  return 0;
}

void bus_consumer_record_retry_exhausted(struct bus_consumer_view *consumer,
                                         uint32_t source_port) {
  if (consumer == NULL || source_port >= BUS_MAX_PORTS) return;
  atomic_fetch_add_explicit(
      &source_at(consumer, source_port)->drop_claim_conflict_retry_exhausted,
      1, memory_order_relaxed);
}

void bus_ring_record_doorbells(struct bus_ring_view *ring, uint64_t count) {
  if (ring == NULL) return;
  atomic_fetch_add_explicit(&ring->header->doorbells, count,
                            memory_order_relaxed);
}
