// Region creation and capability distribution. Retain a read-only descriptor
// before shm_unlink so the coordinator can grant access to later joiners.
#include "../include/bus_control.h"

#include <errno.h>
#include <stdatomic.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

// shm_open then immediate shm_unlink: the region has no name in the filesystem,
// so it is reachable only by descriptor holders. A capability, not a shared
// resource that anything with the path can open.
int bus_region_create(struct bus_region *r, const char *tag, unsigned id, uint64_t bytes) {
  char name[64];
  snprintf(name, sizeof(name), "/shmbus-%d-%s-%u", (int)getpid(), tag, id);

  r->rw = shm_open(name, O_CREAT | O_RDWR | O_EXCL, 0600);
  if (r->rw < 0) {
    return -1;
  }
  // Before the unlink, or there is no second chance at this.
  r->ro = shm_open(name, O_RDONLY, 0600);
  if (r->ro < 0) {
    int e = errno;
    close(r->rw);
    shm_unlink(name);
    errno = e;
    return -1;
  }
  // Separate from the ftruncate below on purpose. Folding them into one
  // condition means a failed unlink leaves a named object behind that nothing
  // ever removes -- a capability with a filesystem name is not a capability,
  // and it survives until the host reboots.
  if (shm_unlink(name) < 0) {
    int e = errno;
    close(r->rw);
    close(r->ro);
    errno = e;
    return -1;
  }
  if (ftruncate(r->rw, (off_t)bytes) < 0) {
    int e = errno;
    close(r->rw);
    close(r->ro);
    errno = e;
    return -1;
  }
  r->bytes = bytes;
  r->map = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, r->rw, 0);
  if (r->map == MAP_FAILED) {
    int e = errno;
    close(r->rw);
    close(r->ro);
    r->map = NULL;
    errno = e;
    return -1;
  }
  return 0;
}

void bus_region_destroy(struct bus_region *r) {
  if (r->map != NULL) {
    munmap(r->map, r->bytes);
    r->map = NULL;
  }
  if (r->rw >= 0) { close(r->rw); r->rw = -1; }
  if (r->ro >= 0) { close(r->ro); r->ro = -1; }
  r->bytes = 0;
}

// Initialize the complete region before publishing its descriptor. Store the
// magic last so readers cannot accept a partially initialized prefix.

void bus_region_init_prefix(struct bus_region *r, uint32_t kind, uint32_t owner_port,
                            uint32_t owner_generation, uint32_t mtu, uint32_t capacity,
                            uint32_t slot_bytes) {
  struct bus_region_prefix *p = (struct bus_region_prefix *)r->map;
  memset(p, 0, sizeof(*p));
  p->abi_version = BUS_ABI_VERSION;
  p->region_kind = kind;
  p->header_bytes = BUS_REGION_HEADER_BYTES;
  p->max_ports = BUS_MAX_PORTS;
  p->region_bytes = r->bytes;
  p->owner_generation = owner_generation;
  p->owner_port = owner_port;
  p->mtu = mtu;
  p->capacity = capacity;
  p->slot_bytes = slot_bytes;
  if (kind == BUS_REGION_PRODUCER) {
    // A ring whose head is zero is not a ring: sequence 0 is never published,
    // and a consumer installing a cursor from it would index the slot before
    // the first one. Arming it is part of making the region whole.
    struct bus_ring_header *h = (struct bus_ring_header *)r->map;
    atomic_store_explicit(&h->head, 1, memory_order_relaxed);
  }
  atomic_thread_fence(memory_order_release);
  p->magic = BUS_ABI_MAGIC;
}

int bus_region_check_prefix(const void *map, uint64_t bytes, uint32_t kind) {
  if (map == NULL || bytes < BUS_REGION_HEADER_BYTES) {
    return -1;
  }
  const struct bus_region_prefix *p = (const struct bus_region_prefix *)map;
  if (p->magic != BUS_ABI_MAGIC || p->abi_version != BUS_ABI_VERSION) {
    return -1;
  }
  if (p->region_kind != kind || p->region_bytes != bytes) {
    return -1;
  }
  return 0;
}
