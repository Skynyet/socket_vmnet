#ifndef SOCKET_VMNET_SHMEM_BUS_COORDINATOR_H
#define SOCKET_VMNET_SHMEM_BUS_COORDINATOR_H

struct shmem_bus_coordinator;

// Create the in-process control plane, grant the daemon's socket group access
// to its listener, and start its bounded polling thread. On return the socket
// is ready for the daemon uplink and Lima participants to join.
int shmem_bus_coordinator_open(struct shmem_bus_coordinator **out, const char *path,
                               const char *socket_group);

// Stop polling, retire every participant, unlink the listener and release all
// coordinator-owned shared regions.
void shmem_bus_coordinator_close(struct shmem_bus_coordinator *coordinator);

#endif
