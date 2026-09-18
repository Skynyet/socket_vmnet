// The membership directory: the coordinator writes it, everyone maps it
// read-only, and it answers liveness without a control round trip.
//
// Two access paths on purpose. A producer's hot question -- is this port still
// the participant whose claim I installed -- is one aligned 64-bit load and
// pays nothing, because state and generation share a word. Enumeration, where a
// reader wants the membership of one moment rather than 64 entries read one at
// a time, goes through the epoch seqlock.
#include "../include/bus_control.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <sys/sysctl.h>

int bus_directory_open(struct bus_directory_view *v, void *region, uint64_t bytes) {
  if (region == NULL || bytes != bus_directory_region_bytes()) {
    return -1;
  }
  v->header = (struct bus_directory_header *)region;
  v->entries = (struct bus_directory_entry *)((unsigned char *)region + BUS_REGION_HEADER_BYTES);
  v->bytes = bytes;
  return 0;
}

void bus_directory_init(struct bus_directory_view *v) {
  memset(v->entries, 0, (size_t)BUS_MAX_PORTS * sizeof(struct bus_directory_entry));
  // Starts at 2, not 0: even means stable, and leaving 0 unused means a slot
  // still carrying a zeroed stamp is recognisably unstamped rather than
  // plausibly from the first epoch.
  atomic_store_explicit(&v->header->membership_epoch, 2, memory_order_release);
}

uint64_t bus_directory_begin(struct bus_directory_view *v) {
  uint64_t e = atomic_load_explicit(&v->header->membership_epoch, memory_order_relaxed) + 1;
  atomic_store_explicit(&v->header->membership_epoch, e, memory_order_relaxed);
  // The odd epoch has to be visible before the mutations it covers, or a reader
  // could copy half-changed entries and validate them against an even epoch.
  atomic_thread_fence(memory_order_release);
  return e;
}

uint64_t bus_directory_commit(struct bus_directory_view *v) {
  uint64_t e = atomic_load_explicit(&v->header->membership_epoch, memory_order_relaxed) + 1;
  atomic_thread_fence(memory_order_release);
  atomic_store_explicit(&v->header->membership_epoch, e, memory_order_relaxed);
  return e;
}

// Call between begin() and commit(). The identity store is last and is a
// release, so a reader that sees LIVE by the single-entry path has necessarily
// also seen the sizes, the mac and the change epoch that go with it.
void bus_port_publish(struct bus_directory_view *v, uint32_t port, uint32_t state,
                      uint32_t generation, uint64_t mac, uint64_t data_bytes,
                      uint64_t state_bytes, uint32_t mtu, uint32_t flags) {
  if (port >= BUS_MAX_PORTS) {
    return;
  }
  struct bus_directory_entry *e = &v->entries[port];
  // The epoch in flight is odd; what this entry will be current at is the even
  // one the commit installs. Stamping the odd value would name no membership.
  uint64_t at = atomic_load_explicit(&v->header->membership_epoch, memory_order_relaxed) + 1;
  atomic_store_explicit(&e->mac, mac, memory_order_relaxed);
  atomic_store_explicit(&e->data_bytes, data_bytes, memory_order_relaxed);
  atomic_store_explicit(&e->state_bytes, state_bytes, memory_order_relaxed);
  atomic_store_explicit(&e->mtu_flags, bus_pack_mtu_flags(mtu, flags), memory_order_relaxed);
  atomic_store_explicit(&e->changed_at_epoch, at, memory_order_relaxed);
  atomic_store_explicit(&e->identity_state, bus_identity_state(state, generation),
                        memory_order_release);
}

void bus_port_read(const struct bus_directory_view *v, uint32_t port,
                   struct bus_port_view *out) {
  memset(out, 0, sizeof(*out));
  if (port >= BUS_MAX_PORTS) {
    return;
  }
  const struct bus_directory_entry *e = &v->entries[port];
  // Identity first, and the acquire is what makes the rest meaningful: it
  // forbids seeing a new identity beside the old change epoch. The opposite
  // order -- old identity, newer epoch -- costs only a slot released early.
  uint64_t ident = atomic_load_explicit(&e->identity_state, memory_order_acquire);
  out->state = bus_identity_port_state(ident);
  out->generation = bus_identity_generation(ident);
  out->changed_at_epoch = atomic_load_explicit(&e->changed_at_epoch, memory_order_relaxed);
  out->mac = atomic_load_explicit(&e->mac, memory_order_relaxed);
  out->data_bytes = atomic_load_explicit(&e->data_bytes, memory_order_relaxed);
  out->state_bytes = atomic_load_explicit(&e->state_bytes, memory_order_relaxed);
  out->mtu_flags = atomic_load_explicit(&e->mtu_flags, memory_order_relaxed);
}

int bus_directory_read(const struct bus_directory_view *v, struct bus_membership *out,
                       unsigned retries) {
  for (unsigned attempt = 0; attempt <= retries; attempt++) {
    uint64_t before = atomic_load_explicit(&v->header->membership_epoch, memory_order_acquire);
    if ((before & 1) != 0) {
      continue;  // a mutation is in flight; nothing read now would name an epoch
    }
    memset(out, 0, sizeof(*out));
    out->epoch = before;
    out->max_ports = BUS_MAX_PORTS;
    for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) {
      const struct bus_directory_entry *e = &v->entries[i];
      uint64_t ident = atomic_load_explicit(&e->identity_state, memory_order_relaxed);
      struct bus_port_view *p = &out->ports[i];
      p->state = bus_identity_port_state(ident);
      p->generation = bus_identity_generation(ident);
      p->changed_at_epoch = atomic_load_explicit(&e->changed_at_epoch, memory_order_relaxed);
      p->mac = atomic_load_explicit(&e->mac, memory_order_relaxed);
      p->data_bytes = atomic_load_explicit(&e->data_bytes, memory_order_relaxed);
      p->state_bytes = atomic_load_explicit(&e->state_bytes, memory_order_relaxed);
      p->mtu_flags = atomic_load_explicit(&e->mtu_flags, memory_order_relaxed);
      if (p->state == BUS_PORT_LIVE) {
        out->live_mask |= (uint64_t)1 << i;
        if ((bus_port_flags(p->mtu_flags) & BUS_PORT_FLAG_UPLINK) != 0) {
          out->uplink_mask |= (uint64_t)1 << i;
        }
      }
    }
    // Validate after the copy, exactly as the daemon's ring snapshot does: the
    // copy is what makes it legal to check afterwards.
    atomic_thread_fence(memory_order_acquire);
    if (atomic_load_explicit(&v->header->membership_epoch, memory_order_relaxed) == before) {
      return 0;
    }
  }
  return -1;
}

// BUS_CACHELINE_BYTES is a build-time guess about the machine. This is the
// machine answering. Note the type: hw.cachelinesize is a 64-bit sysctl, and
// asking for it into an int returns ENOMEM -- which is how this check silently
// disabled itself in the daemon until someone ran the binary and noticed the
// warning that never appeared.
int bus_control_check_cache_line(void) {
  long long line = 0;
  size_t len = sizeof(line);
  if (sysctlbyname("hw.cachelinesize", &line, &len, NULL, 0) < 0) {
    fprintf(stderr, "bus: cannot read hw.cachelinesize, assuming %u\n", BUS_CACHELINE_BYTES);
    return 0;
  }
  if (line != (long long)BUS_CACHELINE_BYTES) {
    fprintf(stderr, "bus: built for a %u-byte cache line, host reports %lld\n",
            BUS_CACHELINE_BYTES, line);
    return -1;
  }
  return 0;
}
