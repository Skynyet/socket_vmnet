#include "shmem_bus_coordinator.h"

#include "bus_coordinator.h"

#include <errno.h>
#include <grp.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "log.h"

struct shmem_bus_coordinator {
  struct bus_coordinator *core;
  pthread_t thread;
  int thread_started;
  atomic_bool stopping;
  atomic_bool failed;
};

static void *coordinator_thread_main(void *opaque) {
  struct shmem_bus_coordinator *coordinator = opaque;
  while (!atomic_load_explicit(&coordinator->stopping, memory_order_acquire)) {
    if (bus_coordinator_poll(coordinator->core, 100) < 0) {
      atomic_store_explicit(&coordinator->failed, true, memory_order_release);
      ERRORF("shmem bus: coordinator poll failed: %s", strerror(errno));
      break;
    }
  }
  return NULL;
}

int shmem_bus_coordinator_open(struct shmem_bus_coordinator **out, const char *path,
                               const char *socket_group, uint32_t mtu) {
  if (out == NULL || path == NULL || path[0] == '\0') {
    errno = EINVAL;
    return -1;
  }
  *out = NULL;
  struct shmem_bus_coordinator *coordinator = calloc(1, sizeof(*coordinator));
  if (coordinator == NULL) { return -1; }

  if (bus_coordinator_open(&coordinator->core, path, mtu) < 0) {
    free(coordinator);
    return -1;
  }
  if (socket_group != NULL) {
    errno = 0;
    struct group *group = getgrnam(socket_group);
    if (group == NULL || chown(path, -1, group->gr_gid) < 0 || chmod(path, 0770) < 0) {
      int saved = (group == NULL && errno == 0) ? EINVAL : errno;
      bus_coordinator_close(coordinator->core);
      free(coordinator);
      errno = saved;
      return -1;
    }
  }

  int thread_error = pthread_create(&coordinator->thread, NULL,
                                    coordinator_thread_main, coordinator);
  if (thread_error != 0) {
    bus_coordinator_close(coordinator->core);
    free(coordinator);
    errno = thread_error;
    return -1;
  }
  coordinator->thread_started = 1;
  INFOF("shmem bus: daemon owns coordinator at %s", path);
  *out = coordinator;
  return 0;
}

void shmem_bus_coordinator_close(struct shmem_bus_coordinator *coordinator) {
  if (coordinator == NULL) { return; }
  atomic_store_explicit(&coordinator->stopping, true, memory_order_release);
  if (coordinator->thread_started) {
    pthread_join(coordinator->thread, NULL);
  }
  if (atomic_load_explicit(&coordinator->failed, memory_order_acquire)) {
    WARN("shmem bus: coordinator stopped after a polling failure");
  }
  bus_coordinator_close(coordinator->core);
  free(coordinator);
}
