// C participant client for joining the bus and installing its capabilities.

#ifndef BUS_CLIENT_H
#define BUS_CLIENT_H

#include "bus_control.h"
#include "../c_ring/bus_ring.h"

struct bus_client_peer {
  int installed;
  uint32_t generation;
  void *data;              // that peer's ring, read-only
  uint64_t data_bytes;
  void *state;             // that peer's cursors and claims, read-only
  uint64_t state_bytes;
  int doorbell_tx;         // a hint channel; its failures are not errors
  struct bus_ring_view ring;
  int ring_open;           // its owner had initialised the ring when we opened it
};

struct bus_client {
  int lease;               // the control stream; its EOF is how we learn we are gone
  struct bus_msg_reader reader;
  uint32_t id;
  uint32_t generation;
  uint32_t flags;          // BUS_PORT_FLAG_UPLINK, or none

  struct bus_directory_view dir;

  void *own_data;
  uint64_t own_data_bytes;
  struct bus_ring_view ring;          // this port's producer ring

  void *own_state;
  uint64_t own_state_bytes;
  struct bus_consumer_view consumer;  // this port's cursors and active claims

  int doorbell_rx;
  struct bus_client_peer peers[BUS_MAX_PORTS];

	// Superseded mappings and doorbells remain open until close. The number of
	// retired generations may exceed BUS_MAX_PORTS over a client's lifetime.
  struct bus_client_peer *retained;
  unsigned retained_count;
  unsigned retained_cap;
};

// Joins and returns once the coordinator says READY: every capability installed,
// the ledger written, the port live in the directory. Blocking, with a deadline,
// because a process that cannot join has nothing else to do.
int bus_client_join(struct bus_client *c, const char *control_socket, uint64_t mac,
                    uint32_t flags, int64_t timeout_ms);

// One non-blocking pass over the control stream: late joins, retirements,
// abandoned joins, and the END_CAPABILITIES that ledger writes answer. Returns
// 1 if anything was handled, 0 if there was nothing, -1 on a broken lease, and
// 0 with *closed set when the coordinator went away.
int bus_client_poll(struct bus_client *c, int *closed);

// Who this port may target, from a validated membership snapshot: a guest
// targets every other live port, an uplink targets live ports minus every live
// uplink. Returns 0 and leaves *mask untouched if the directory could not be
// read consistently.
int bus_client_targets(const struct bus_client *c, uint64_t *mask, uint64_t *epoch);

void bus_client_close(struct bus_client *c);

#endif // BUS_CLIENT_H
