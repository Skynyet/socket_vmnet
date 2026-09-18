#include "bus_client.h"
#include "shmem_bus_coordinator.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
  if (argc != 2) { return 2; }
  struct shmem_bus_coordinator *coordinator = NULL;
  if (shmem_bus_coordinator_open(&coordinator, argv[1], NULL) < 0) {
    perror("coordinator open");
    return 1;
  }

  struct bus_client client;
  memset(&client, 0, sizeof(client));
  client.lease = -1;
  client.doorbell_rx = -1;
  bus_msg_reader_init(&client.reader);
  for (uint32_t i = 0; i < BUS_MAX_PORTS; i++) {
    client.peers[i].doorbell_tx = -1;
  }
  if (bus_client_join(&client, argv[1], 0, BUS_PORT_FLAG_UPLINK, 5000) < 0) {
    perror("client join");
    shmem_bus_coordinator_close(coordinator);
    return 1;
  }
  printf("joined port %u generation %u\n", client.id, client.generation);
  bus_client_close(&client);
  shmem_bus_coordinator_close(coordinator);
  return 0;
}
