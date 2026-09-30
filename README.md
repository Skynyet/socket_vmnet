# SkyNyet socket_vmnet: shared-memory networking for Lima VZ

[Русская версия](README.ru.md)

This proof of concept (PoC) is a pair of forks based on [socket_vmnet v1.2.2](https://github.com/lima-vm/socket_vmnet/blob/v1.2.2/README.md) and [Lima v2.2.0](https://github.com/lima-vm/lima/blob/v2.2.0/README.md). They are not official releases.

## Why

The main motivation is to speed up vanilla shared networking. Relaying every Ethernet frame through a stream socket adds process switches, copies, and syscalls to traffic that does not need vmnet at all.

The first version used a separate shared-memory coordinator; this fork puts the coordinator *inside* `socket_vmnet`. The hostagents of two or more Lima VMs exchange frames directly through shared-memory rings. The daemon joins the bus as the vmnet uplink, so guests can still reach the host and external networks. An **additional Unix socket** handles bus discovery and joining: for a Lima network named `shared`, it is `socket_vmnet_shm.shared`, beside the original `socket_vmnet.shared`.

## Features and fixes

- **Shared-memory bus on by default; legacy service preserved.**
- **Nonblocking fanout among bus participants.** Upstream's framed relay can stall every client if one VM stops reading. This fork uses independent cursors in shared-memory rings instead of blocking socket writes; a slot held by a recipient may cause a frame to be dropped, but does not make the other clients wait.
- **Safe participant lifecycle.** The control protocol distinguishes connection generations and retains old memory mappings while they may still be in use, including interrupted VM joins and coordinator restarts. Vanilla does not support this and requires restarting all VMs on the affected shared network.
- **vmnet and bus MTU.** `--vmnet-mtu=BYTES` sets the MTU in shared or host vmnet mode; the daemon-owned bus inherits the effective value. A VM's MTU is deliberately allowed to differ from the bus MTU, which can be useful for testing PMTU discovery. Jumbo frames (for example, MTU 9000) can substantially improve throughput.
- **Safer fallback.** The framed reader now assembles headers and payloads across short reads, retries interrupted reads, and rejects truncated or malformed frames. This matters when the bus is disabled and for clients that only speak the framed protocol.

## Performance

- At MTU 1500, vanilla Lima with upstream `socket_vmnet` delivered 2.38 Gbps between two VMs; the shared-memory bus reached 9.88 Gbps (23.4 Gbps at MTU 9000).
- With the vmnet uplink present, two VMs sustained about 5.67 Gbps **in each direction (bidir)** at MTU 1500, with four TCP streams per direction.
- Stopping and rejoining an idle third VM left a concurrent host-to-VM transfer at 7.91, 7.84, and 7.72 Gbps. The two guests also kept exchanging traffic after the vmnet uplink was retired.
- VZ-edge batching in the paired Lima fork cut hostagent CPU per GB by 30.2%.

QEMU and other framed-only clients do not use the shared-memory fast path.

## Fork authorship

Igor Podlesny with AI agents (OpenAI GPT/Codex, Anthropic Claude, xAI Grok, Qwen via OpenCode, and others).

Original Lima and socket_vmnet authorship and licenses are preserved.
