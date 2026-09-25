#include <getopt.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cli.h"

bool debug = false;

static struct cli_options *parse(int argc, char *argv[]) {
  optind = 1;
  optreset = 1;
  return cli_options_parse(argc, argv);
}

static int expect_path(const char *label, const char *actual, const char *want) {
  if (actual == NULL || strcmp(actual, want) != 0) {
    fprintf(stderr, "%s: path %s, want %s\n", label,
            actual == NULL ? "(none)" : actual, want);
    return 1;
  }
  return 0;
}

static int expect_rejected(const char *label, int argc, char *argv[]) {
  pid_t child = fork();
  if (child < 0) {
    perror("fork");
    return 1;
  }
  if (child == 0) {
    if (freopen("/dev/null", "w", stderr) == NULL ||
        freopen("/dev/null", "w", stdout) == NULL) {
      _exit(2);
    }
    struct cli_options *options = parse(argc, argv);
    cli_options_destroy(options);
    _exit(0);
  }
  int status = 0;
  if (waitpid(child, &status, 0) != child || !WIFEXITED(status) ||
      WEXITSTATUS(status) != EXIT_FAILURE) {
    fprintf(stderr, "%s: invalid options were not rejected\n", label);
    return 1;
  }
  return 0;
}

int main(void) {
  char *managed[] = {"socket_vmnet", "--vmnet-gateway=192.168.105.1",
                     "--vmnet-mtu=9000", "/private/var/run/lima/socket_vmnet.shared", NULL};
  struct cli_options *options = parse(4, managed);
  int failed = expect_path("managed", options->shmem_bus_listen_path,
                           "/private/var/run/lima/socket_vmnet_shm.shared");
  if (options->shmem_bus_mtu != 9000) {
    fprintf(stderr, "managed: bus MTU %d, want 9000\n", options->shmem_bus_mtu);
    failed = 1;
  }
  cli_options_destroy(options);

  char *same_path[] = {"socket_vmnet", "--vmnet-gateway=192.168.105.1",
                       "--shmem-bus-listen=/private/var/run/lima/socket_vmnet.shared",
                       "/private/var/run/lima/socket_vmnet.shared", NULL};
  failed |= expect_rejected("same socket path", 4, same_path);

  char long_path[120];
  memset(long_path, 'a', sizeof(long_path) - 1);
  long_path[0] = '/';
  long_path[sizeof(long_path) - 1] = '\0';
  char *too_long[] = {"socket_vmnet", "--vmnet-gateway=192.168.105.1", long_path, NULL};
  failed |= expect_rejected("long derived socket path", 3, too_long);

  char *service[] = {"socket_vmnet", "--vmnet-gateway=192.168.105.1",
                     "/opt/homebrew/var/run/socket_vmnet", NULL};
  options = parse(3, service);
  failed |= expect_path("service", options->shmem_bus_listen_path,
                        "/opt/homebrew/var/run/socket_vmnet_shm");
  if (options->shmem_bus_mtu != 1500) {
    fprintf(stderr, "service: bus MTU %d, want 1500\n", options->shmem_bus_mtu);
    failed = 1;
  }
  cli_options_destroy(options);

  char *disabled[] = {"socket_vmnet", "--vmnet-gateway=192.168.105.1",
                      "--no-shmem-bus", "/private/var/run/lima/socket_vmnet.shared", NULL};
  options = parse(4, disabled);
  if (options->shmem_bus_listen_path != NULL || options->shmem_bus_mtu != 0) {
    fprintf(stderr, "disabled: unexpected bus listener or MTU\n");
    failed = 1;
  }
  cli_options_destroy(options);

  char *external[] = {"socket_vmnet", "--vmnet-gateway=192.168.105.1",
                      "--shmem-bus-control=/unused/external.sock",
                      "/private/var/run/lima/socket_vmnet.shared", NULL};
  options = parse(4, external);
  if (options->shmem_bus_listen_path != NULL) {
    fprintf(stderr, "external: unexpectedly owns a bus listener\n");
    failed = 1;
  }
  cli_options_destroy(options);

  return failed;
}
