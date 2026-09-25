#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <arpa/inet.h>
#include <getopt.h>
#include <sys/un.h>

#include <Availability.h>
#include <uuid/uuid.h>

#include "cli.h"
#include "log.h"

#ifndef VERSION
#define VERSION "UNKNOWN"
#endif

#if __MAC_OS_X_VERSION_MAX_ALLOWED < 101500
#error "Requires macOS 10.15 or later"
#endif

#define CLI_DEFAULT_SOCKET_GROUP "staff"

static void print_usage(const char *argv0) {
  printf("Usage: %s [OPTION]... SOCKET\n", argv0);
  printf("vmnet.framework support for rootless QEMU.\n");
  printf("socket_vmnet does not require QEMU to run as the root user, but "
         "socket_vmnet itself has to run as the root, in most cases.\n");
  printf("\n");
  printf("--socket-group=GROUP                socket group name (default: "
         "\"" CLI_DEFAULT_SOCKET_GROUP "\")\n");
  printf("--vmnet-mode=(host|shared|bridged)  vmnet mode (default: \"shared\")\n");
  printf("--vmnet-interface=INTERFACE         interface used for "
         "--vmnet=bridged, e.g., \"en0\"\n");
  printf("--vmnet-gateway=IP                  gateway used for "
         "--vmnet=(host|shared), e.g., \"192.168.105.1\" (default: decided by "
         "macOS)\n");
  printf("                                    the next IP (e.g., "
         "\"192.168.105.2\") is used as the first DHCP address\n");
  printf("--vmnet-dhcp-end=IP                 end of the DHCP range (default: "
         "XXX.XXX.XXX.254)\n");
  printf("                                    requires --vmnet-gateway to be "
         "specified\n");
  printf("--vmnet-mask=MASK                   subnet mask (default: "
         "\"255.255.255.0\")\n");
  printf("                                    requires --vmnet-gateway to be "
         "specified\n");
  printf("--vmnet-interface-id=UUID           vmnet interface ID (default: "
         "random)\n");
  printf(
      "--vmnet-network-identifier=UUID     The identifier(uuid) to uniquely identify the network. "
      "\n"
      "                                    This property is only applicable to a vmnet_interface\n"
      "                                    in VMNET_HOST_MODE.\n"
      "                                    If this property is set, the vmnet_interface is added "
      "to \n"
      "                                    an isolated network with the specified\n"
      "                                    identifier. No DHCP service is provided on this "
      "network.\n");
  printf("--vmnet-nat66-prefix=PREFIX::       The IPv6 prefix to use with "
         "shared mode.\n");
  printf("                                    The prefix must be a ULA i.e. "
         "start with fd00::/8.\n");
  printf("                                    (default: random)\n");
  printf("--vmnet-mtu=BYTES                   Ask vmnet for this MTU. Shared and "
         "host modes\n");
  printf("                                    only; vmnet rejects it in bridged "
         "mode.\n");
  printf("                                    Raises the vmnet interface and its "
         "bridge\n");
  printf("                                    together. 68-65535.\n");
  printf("                                    (default: unset, vmnet uses 1500)\n");
  printf("--shmem-bus-control=PATH            join an external shared-memory bus\n"
         "                                    coordinator as the vmnet uplink\n");
  printf("--shmem-bus-listen=PATH             own the shared-memory bus coordinator\n"
         "                                    at PATH and join it as vmnet uplink\n");
  printf("--no-shmem-bus                      disable the default daemon-owned bus\n"
         "                                    (default path: SOCKET with _shm before its first dot)\n");
  printf("--shmem-bus-mtu=BYTES               coordinator MTU; must match vmnet MTU\n"
         "                                    (default: effective vmnet MTU)\n");
  printf("-p, --pidfile=PIDFILE               save pid to PIDFILE\n");
  printf("-h, --help                          display this help and exit\n");
  printf("-v, --version                       display version information and "
         "exit\n");
  printf("\n");
  printf("version: " VERSION "\n");
}

static void print_version(void) { puts(VERSION); }

enum {
  CLI_OPT_SOCKET_GROUP = CHAR_MAX + 1,
  CLI_OPT_VMNET_MODE,
  CLI_OPT_VMNET_INTERFACE,
  CLI_OPT_VMNET_GATEWAY,
  CLI_OPT_VMNET_DHCP_END,
  CLI_OPT_VMNET_MASK,
  CLI_OPT_VMNET_INTERFACE_ID,
  CLI_OPT_VMNET_NAT66_PREFIX,
  CLI_OPT_VMNET_MTU,
  CLI_OPT_VMNET_NETWORK_IDENTIFIER,
  CLI_OPT_SHMEM_BUS_CONTROL,
  CLI_OPT_SHMEM_BUS_LISTEN,
  CLI_OPT_NO_SHMEM_BUS,
  CLI_OPT_SHMEM_BUS_MTU,
};

// The old framed listener remains at SOCKET. The bus gets a distinct Unix
// socket beside it, with a per-network name when SOCKET has one: for example,
// socket_vmnet.shared -> socket_vmnet_shm.shared. Lima builds that name from
// its network configuration rather than parsing this string.
static char *derived_bus_path(const char *socket_path) {
  const char *base = strrchr(socket_path, '/');
  base = base == NULL ? socket_path : base + 1;
  if (*base == '\0') {
    ERROR("SOCKET must end in a filename");
    return NULL;
  }
  const char *dot = strchr(base, '.');
  size_t original_len = strlen(socket_path);
  size_t derived_len = original_len + sizeof("_shm") - 1;
  if (derived_len >= sizeof(((struct sockaddr_un *)0)->sun_path)) {
    ERRORF("derived shared-memory bus socket path is too long: %zu bytes", derived_len);
    return NULL;
  }
  size_t head_len = dot == NULL ? original_len : (size_t)(dot - socket_path);
  char *path = malloc(derived_len + 1);
  if (path == NULL) {
    ERRORN("malloc");
    return NULL;
  }
  memcpy(path, socket_path, head_len);
  memcpy(path + head_len, "_shm", sizeof("_shm") - 1);
  memcpy(path + head_len + sizeof("_shm") - 1, socket_path + head_len,
         original_len - head_len + 1);
  return path;
}

static int parse_mtu(const char *name, const char *text, int *out) {
  errno = 0;
  char *end = NULL;
  long value = strtol(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || value < 68 || value > 65535) {
    ERRORF("%s must be an integer between 68 and 65535", name);
    return -1;
  }
  *out = (int)value;
  return 0;
}

struct cli_options *cli_options_parse(int argc, char *argv[]) {
  struct cli_options *res = calloc(1, sizeof(*res));
  if (res == NULL) {
    ERRORN("calloc");
    exit(EXIT_FAILURE);
  }

  const struct option longopts[] = {
      {"socket-group",             required_argument, NULL, CLI_OPT_SOCKET_GROUP            },
      {"vmnet-mode",               required_argument, NULL, CLI_OPT_VMNET_MODE              },
      {"vmnet-interface",          required_argument, NULL, CLI_OPT_VMNET_INTERFACE         },
      {"vmnet-gateway",            required_argument, NULL, CLI_OPT_VMNET_GATEWAY           },
      {"vmnet-dhcp-end",           required_argument, NULL, CLI_OPT_VMNET_DHCP_END          },
      {"vmnet-mask",               required_argument, NULL, CLI_OPT_VMNET_MASK              },
      {"vmnet-interface-id",       required_argument, NULL, CLI_OPT_VMNET_INTERFACE_ID      },
      {"vmnet-nat66-prefix",       required_argument, NULL, CLI_OPT_VMNET_NAT66_PREFIX      },
      {"vmnet-mtu",                required_argument, NULL, CLI_OPT_VMNET_MTU               },
      {"vmnet-network-identifier", required_argument, NULL, CLI_OPT_VMNET_NETWORK_IDENTIFIER},
      {"shmem-bus-control",        required_argument, NULL, CLI_OPT_SHMEM_BUS_CONTROL       },
      {"shmem-bus-listen",         required_argument, NULL, CLI_OPT_SHMEM_BUS_LISTEN        },
      {"no-shmem-bus",             no_argument,       NULL, CLI_OPT_NO_SHMEM_BUS            },
      {"shmem-bus-mtu",            required_argument, NULL, CLI_OPT_SHMEM_BUS_MTU           },
      {"pidfile",                  required_argument, NULL, 'p'                             },
      {"help",                     no_argument,       NULL, 'h'                             },
      {"version",                  no_argument,       NULL, 'v'                             },
      {0,                          0,                 0,    0                               },
  };
  int opt = 0;
  while ((opt = getopt_long(argc, argv, "hvp:", longopts, NULL)) != -1) {
    switch (opt) {
    case CLI_OPT_SOCKET_GROUP:
      res->socket_group = strdup(optarg);
      break;
    case CLI_OPT_VMNET_MODE:
      if (strcmp(optarg, "host") == 0) {
        res->vmnet_mode = VMNET_HOST_MODE;
      } else if (strcmp(optarg, "shared") == 0) {
        res->vmnet_mode = VMNET_SHARED_MODE;
      } else if (strcmp(optarg, "bridged") == 0) {
        res->vmnet_mode = VMNET_BRIDGED_MODE;
      } else {
        ERRORF("Unknown vmnet mode \"%s\"", optarg);
        goto error;
      }
      break;
    case CLI_OPT_VMNET_INTERFACE:
      res->vmnet_interface = strdup(optarg);
      break;
    case CLI_OPT_VMNET_GATEWAY:
      res->vmnet_gateway = strdup(optarg);
      break;
    case CLI_OPT_VMNET_DHCP_END:
      res->vmnet_dhcp_end = strdup(optarg);
      break;
    case CLI_OPT_VMNET_MASK:
      res->vmnet_mask = strdup(optarg);
      break;
    case CLI_OPT_VMNET_INTERFACE_ID:
      if (uuid_parse(optarg, res->vmnet_interface_id) < 0) {
        ERRORF("Failed to parse UUID \"%s\"", optarg);
        goto error;
      }
      break;
    case CLI_OPT_VMNET_MTU: {
      // 68 is IPv4's minimum reassembly buffer and what a virtio-net guest
      // reports as its own minimum; 65535 is where an Ethernet MTU stops
      // meaning anything. Between those, do not guess: vmnet.h states no range
      // for vmnet_mtu_key, a smaller MTU is a legitimate thing to ask for when
      // testing path-MTU discovery or a constrained link, and a value vmnet
      // dislikes is refused by vmnet.
      //
      // strtol stops at the first character it cannot use and says where in
      // endptr. Without looking, "--vmnet-mtu=9000nonsense" is accepted as
      // 9000: a typo that silently configures a segment instead of failing.
      errno = 0;
      char *endptr = NULL;
      long mtu = strtol(optarg, &endptr, 10);
      if (errno != 0 || endptr == optarg || *endptr != '\0' || mtu < 68 ||
          mtu > 65535) {
        fprintf(stderr, "--vmnet-mtu must be an integer between 68 and 65535\n");
        goto error;
      }
      res->vmnet_mtu = (int)mtu;
      break;
    }
    case CLI_OPT_VMNET_NAT66_PREFIX:
      res->vmnet_nat66_prefix = strdup(optarg);
      break;
    case CLI_OPT_VMNET_NETWORK_IDENTIFIER:
      if (uuid_parse(optarg, res->vmnet_network_identifier) < 0) {
        ERRORF("Failed to parse network identifier UUID \"%s\"", optarg);
        goto error;
      }
      break;
    case CLI_OPT_SHMEM_BUS_CONTROL:
      res->shmem_bus_control_path = strdup(optarg);
      break;
    case CLI_OPT_SHMEM_BUS_LISTEN:
      res->shmem_bus_listen_path = strdup(optarg);
      break;
    case CLI_OPT_NO_SHMEM_BUS:
      res->no_shmem_bus = 1;
      break;
    case CLI_OPT_SHMEM_BUS_MTU:
      if (parse_mtu("--shmem-bus-mtu", optarg, &res->shmem_bus_mtu) < 0) { goto error; }
      break;
    case 'p':
      res->pidfile = strdup(optarg);
      break;
    case 'h':
      print_usage(argv[0]);
      exit(EXIT_SUCCESS);
      break;
    case 'v':
      print_version();
      exit(EXIT_SUCCESS);
      break;
    default:
      goto error;
      break;
    }
  }
  if (argc - optind != 1) {
    goto error;
  }
  res->socket_path = strdup(argv[optind]);

  if (res->shmem_bus_control_path != NULL && res->shmem_bus_listen_path != NULL) {
    ERROR("--shmem-bus-control and --shmem-bus-listen are mutually exclusive");
    goto error;
  }
  if (res->no_shmem_bus &&
      (res->shmem_bus_control_path != NULL || res->shmem_bus_listen_path != NULL)) {
    ERROR("--no-shmem-bus conflicts with --shmem-bus-control/--shmem-bus-listen");
    goto error;
  }
  if (!res->no_shmem_bus && res->shmem_bus_control_path == NULL &&
      res->shmem_bus_listen_path == NULL) {
    res->shmem_bus_listen_path = derived_bus_path(res->socket_path);
    if (res->shmem_bus_listen_path == NULL) { goto error; }
  }
  if (res->shmem_bus_listen_path != NULL &&
      strcmp(res->shmem_bus_listen_path, res->socket_path) == 0) {
    ERROR("the bus control socket must differ from the legacy framed socket");
    goto error;
  }
  if (res->shmem_bus_mtu != 0 && res->shmem_bus_listen_path == NULL) {
    ERROR("--shmem-bus-mtu requires --shmem-bus-listen");
    goto error;
  }

  /* fill default */
  if (res->socket_group == NULL)
    res->socket_group = strdup(CLI_DEFAULT_SOCKET_GROUP); /* use strdup to make it freeable */
  if (res->vmnet_mode == 0)
    res->vmnet_mode = VMNET_SHARED_MODE;
  if (res->shmem_bus_listen_path != NULL) {
    int effective_vmnet_mtu = res->vmnet_mtu != 0 ? res->vmnet_mtu : 1500;
    if (res->shmem_bus_mtu == 0) {
      res->shmem_bus_mtu = effective_vmnet_mtu;
    } else if (res->shmem_bus_mtu != effective_vmnet_mtu) {
      ERROR("--shmem-bus-mtu must match the effective --vmnet-mtu");
      goto error;
    }
  }
  if (res->vmnet_gateway != NULL && res->vmnet_dhcp_end == NULL) {
    /* Set default vmnet_dhcp_end to XXX.XXX.XXX.254 (only when --vmnet-gateway
     * is specified) */
    struct in_addr sin;
    if (!inet_aton(res->vmnet_gateway, &sin)) {
      ERRORN("inet_aton(res->vmnet_gateway)");
      goto error;
    }
    uint32_t h = ntohl(sin.s_addr);
    h &= 0xFFFFFF00;
    h |= 0x000000FE;
    sin.s_addr = htonl(h);
    const char *end_static = inet_ntoa(sin); /* static storage, do not free */
    if (end_static == NULL) {
      ERRORN("inet_ntoa");
      goto error;
    }
    res->vmnet_dhcp_end = strdup(end_static);
  }
  if (res->vmnet_gateway != NULL && res->vmnet_mask == NULL)
    res->vmnet_mask = strdup("255.255.255.0"); /* use strdup to make it freeable */
  if (uuid_is_null(res->vmnet_interface_id)) {
    uuid_generate_random(res->vmnet_interface_id);
  }

  /* validate */
  if (res->vmnet_mode == VMNET_BRIDGED_MODE && res->vmnet_interface == NULL) {
    ERROR("vmnet mode \"bridged\" require --vmnet-interface to be specified");
    goto error;
  }
  if (res->vmnet_mode == VMNET_BRIDGED_MODE && res->vmnet_mtu != 0) {
    ERROR("--vmnet-mtu is not supported in bridged mode");
    goto error;
  }
  if (res->vmnet_gateway == NULL) {
    if (res->vmnet_mode != VMNET_BRIDGED_MODE && res->vmnet_mode != VMNET_HOST_MODE) {
      WARN("--vmnet-gateway=IP should be explicitly specified to "
           "avoid conflicting with other applications");
    }
    if (res->vmnet_dhcp_end != NULL) {
      ERROR("--vmnet-dhcp-end=IP requires --vmnet-gateway=IP");
      goto error;
    }
    if (res->vmnet_mask != NULL) {
      ERROR("--vmnet-mask=MASK requires --vmnet-gateway=IP");
      goto error;
    }
  } else {
    if (res->vmnet_mode == VMNET_BRIDGED_MODE) {
      ERROR("vmnet mode \"bridged\" conflicts with --vmnet-gateway");
      goto error;
    }
    struct in_addr dummy;
    if (!inet_aton(res->vmnet_gateway, &dummy)) {
      ERRORF("invalid address \"%s\" was specified for --vmnet-gateway", res->vmnet_gateway);
      goto error;
    }
  }
  return res;
error:
  print_usage(argv[0]);
  exit(EXIT_FAILURE);
}

void cli_options_destroy(struct cli_options *x) {
  if (x == NULL)
    return;
  free(x->socket_group);
  free(x->socket_path);
  free(x->vmnet_interface);
  free(x->vmnet_gateway);
  free(x->vmnet_dhcp_end);
  free(x->vmnet_mask);
  free(x->vmnet_nat66_prefix);
  free(x->pidfile);
  free(x->shmem_bus_control_path);
  free(x->shmem_bus_listen_path);
  free(x);
}
