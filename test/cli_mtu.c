#include <stdbool.h>
#include <stdio.h>

#include "cli.h"

bool debug = false;

int main(void) {
  char *argv[] = {
      "socket_vmnet",
      "--vmnet-mtu=9000",
      "--shmem-bus-listen=/unused/control.sock",
      "/unused/data.sock",
      NULL,
  };
  struct cli_options *options = cli_options_parse(4, argv);
  if (options->vmnet_mtu != 9000 || options->shmem_bus_mtu != 9000) {
    fprintf(stderr, "parsed vmnet/bus MTU %d/%d, want 9000/9000\n",
            options->vmnet_mtu, options->shmem_bus_mtu);
    cli_options_destroy(options);
    return 1;
  }
  cli_options_destroy(options);
  return 0;
}
