#ifndef SOCKET_VMNET_SHMEM_BUS_UPLINK_H
#define SOCKET_VMNET_SHMEM_BUS_UPLINK_H

#include <vmnet/vmnet.h>

struct shmem_bus_uplink;

// Join an already-running bus coordinator as the single vmnet uplink and
// start the consumer that drains guest rings into vmnet.framework.
int shmem_bus_uplink_open(struct shmem_bus_uplink **out, const char *control_path,
                          interface_ref iface);

// Publish one vmnet_read() batch into the uplink's producer ring. The frame
// bytes are copied once into shared memory; the caller retains its buffers.
int shmem_bus_uplink_publish(struct shmem_bus_uplink *uplink,
                             const struct vmpktdesc *packets, int count);

void shmem_bus_uplink_dump_metrics(const struct shmem_bus_uplink *uplink);

// Stop the consumer, leave the bus, and release all mappings.
void shmem_bus_uplink_close(struct shmem_bus_uplink *uplink);

#endif
