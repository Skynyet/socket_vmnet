// In-process owner of the shared-memory bus control plane.
//
// The coordinator has no payload role.  It owns the directory, capabilities
// and membership state; callers drive its event loop from a thread they own.
#ifndef BUS_COORDINATOR_H
#define BUS_COORDINATOR_H

struct bus_coordinator;

// Creates all coordinator-owned regions and binds/listens on `socket_path`
// before returning.  Thus success is the listener readiness boundary: a
// participant may connect as soon as this call succeeds.  The caller owns the
// returned object and must not call poll concurrently from more than one
// thread.
int bus_coordinator_open(struct bus_coordinator **out, const char *socket_path);

// Runs one control-plane poll step. `max_wait_ms` bounds this call even when
// the coordinator's own liveness cadence is longer. Returns 0 on a completed
// step and -1 on an unrecoverable poll error.
int bus_coordinator_poll(struct bus_coordinator *coordinator, int max_wait_ms);

// Retires any remaining ports, removes the control socket, releases all
// coordinator-owned mappings and frees `coordinator`. Call only after its
// polling thread has stopped.
void bus_coordinator_close(struct bus_coordinator *coordinator);

#endif // BUS_COORDINATOR_H
