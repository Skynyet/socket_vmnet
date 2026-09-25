#ifndef SOCKET_VMNET_CLI_H
#define SOCKET_VMNET_CLI_H

#include <uuid/uuid.h>

#include <vmnet/vmnet.h>

struct cli_options {
  // --socket-group
  char *socket_group;
  // --vmnet-mode, corresponds to vmnet_operation_mode_key
  operating_modes_t vmnet_mode;
  // --vmnet-interface, corresponds to vmnet_shared_interface_name_key
  char *vmnet_interface;
  // --vmnet-gateway, corresponds to vmnet_start_address_key
  char *vmnet_gateway;
  // --vmnet-dhcp-end, corresponds to vmnet_end_address_key
  char *vmnet_dhcp_end;
  // --vmnet-mask, corresponds to vmnet_subnet_mask_key
  char *vmnet_mask;
  // --vmnet-interface-id, corresponds to vmnet_interface_id_key
  uuid_t vmnet_interface_id;
  // --vmnet-network-identifier, corresponds to vmnet_network_identifier_key
  uuid_t vmnet_network_identifier;
  // --vmnet-nat66-prefix, corresponds to vmnet_nat66_prefix_key
  char *vmnet_nat66_prefix;
  // --vmnet-mtu=BYTES, corresponds to vmnet_mtu_key. Shared and host modes
  // only; vmnet rejects it in bridged mode. 0 means do not ask, which leaves
  // the interface and its bridge at vmnet's default of 1500. vmnet.h states no
  // range, so neither do we beyond what an Ethernet MTU can mean at all.
  int vmnet_mtu;
  // -p, --pidfile; writes pidfile using permissions of socket_vmnet
  char *pidfile;
  // Join an external coordinator as its vmnet uplink (compatibility).
  char *shmem_bus_control_path;
  // Own the coordinator at this path and join it as the vmnet uplink.
  char *shmem_bus_listen_path;
  // Keep only the legacy framed socket; otherwise this product daemon owns
  // a bus coordinator at a path derived from socket_path by default.
  int no_shmem_bus;
  // Published bus MTU. With daemon ownership this must equal the effective
  // vmnet MTU; 0 during parsing means derive it from --vmnet-mtu/default 1500.
  int shmem_bus_mtu;
  // arg
  char *socket_path;
};

struct cli_options *cli_options_parse(int argc, char *argv[]);
void cli_options_destroy(struct cli_options *);

#endif /* SOCKET_VMNET_CLI_H */
