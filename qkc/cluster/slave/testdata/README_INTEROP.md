# Python Interop Tests

These tests verify wire / opcode / serializer / bootstrap compatibility between
the real Go Slave and the real Python Master (pyquarkchain). They are driven by
a single Python harness (`master_harness.py`) that reuses the *real* pyquarkchain
wire stack (`ClusterConnection`, the cluster OP serializer map, and the real RPC
request classes) to drive concrete interactions against Go Slaves.

## Prerequisites

1. Python 3.8+
2. A checkout of pyquarkchain:
   ```bash
   git clone https://github.com/QuarkChain/pyquarkchain.git
   ```
3. Set the PYQUARKCHAIN environment variable:
   ```bash
   export PYQUARKCHAIN=/path/to/pyquarkchain
   ```

## Running

All interop tests:
```bash
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop ./qkc/cluster/slave/
```

Specific tests:
```bash
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop -run TestInteropBootstrap ./qkc/cluster/slave/
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop -run TestInteropMasterRpcRoundTrip ./qkc/cluster/slave/
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop -run TestInteropPeerCreateOrDestroy ./qkc/cluster/slave/
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop -run TestInteropMasterDisconnectShutdown ./qkc/cluster/slave/
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop -run TestInteropPeerMessageRouting ./qkc/cluster/slave/
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop -run TestInteropAddMinorBlockHeaderToMaster ./qkc/cluster/slave/
```

With race detector:
```bash
PYQUARKCHAIN=/path/to/pyquarkchain go test -race -tags interop ./qkc/cluster/slave/
```

If `PYQUARKCHAIN` is not set or its directory doesn't exist, tests are skipped.

## What is tested

- **Bootstrap handshake** (`TestInteropBootstrap`) — `master_harness.py bootstrap`
  runs the full `master.main()` against multiple Go Slaves: PING/PONG, root-tip
  shard initialization, and `CONNECT_TO_SLAVES_REQUEST` establishing
  slave-to-slave (xshard) connections.
- **Master→slave RPC round-trip** (`TestInteropMasterRpcRoundTrip`) —
  `master_harness.py rpc` issues `GET_ACCOUNT_DATA_REQUEST` and confirms a
  successful response.
- **Peer lifecycle isolation** (`TestInteropPeerCreateOrDestroy`) —
  `master_harness.py peer <id1> <id2>` drives two
  `CREATE_CLUSTER_PEER_CONNECTION_REQUEST`s then destroys only the first via
  `DESTROY_CLUSTER_PEER_CONNECTION_COMMAND`. Confirms both peers appear in the
  slave's registry, peer 1 is removed on destroy, and peer 2 survives untouched —
  i.e. `destroy(peer1)` does not affect peer 2.
- **Peer message routing** (`TestInteropPeerMessageRouting`) —
  `master_harness.py peermsg` creates a cluster peer connection, then sends a P2P
  `NEW_TRANSACTION_LIST` command addressed to a target `ClusterMetadata{branch,
  cluster_peer_id}`. Confirms the frame travels the real path
  `MasterConn.routeFrame → PeerConn.HandleFrame → PeerHandler` and that the command
  reaches the handler for the exact `(cluster_peer_id, branch)` target.
- **Master disconnect → slave shutdown** (`TestInteropMasterDisconnectShutdown`) —
  `master_harness.py disconnect` closes its connection and confirms the slave shuts down.
- **Slave → Master AddMinorBlockHeader** (`TestInteropAddMinorBlockHeaderToMaster`) —
  `master_harness.py addminorblock` awaits the Go slave's AddMinorBlockHeader request,
  replies with a real AddMinorBlockHeaderResponse. This verifies the only Go → Python
  outbound RPC direction: Go wire serialization → Python deserialization → Python
  response encoding → Go response decoding.

## Scope

Interop verifies only the real Python Master ↔ real Go Slave boundary:
wire, opcode, serializer and bootstrap compatibility. It deliberately does not
verify lock ordering, races, timeouts, pending RPCs or internal state — those are
covered by the Go unit tests in `head`/`master_conn_test.go`, `peer_conn_test.go`,
`xshard_test.go` and `slave_test.go`.

## Architecture

```
slave/
├── interop_harness.go    # Single Go harness: env guards, backend, slave/process harness, cluster config
├── interop_test.go       # All interop tests (Bootstrap / RPC / Peer / Routing / Disconnect / AddMinor)
└── testdata/
    ├── master_harness.py    # Single Python driver: bootstrap / rpc / peer / disconnect / addminorblock
    └── README_INTEROP.md    # This file
```

### Go harness

- `interopBackend` serves every business handler with a trivial-success response
  while recording lifecycle events (shard creation), so tests exercise the real
  communication layer without a business runtime.
- `startTestSlave` starts a single Go Slave with a communication-only backend.
- `startScenarioMaster` runs a `master_harness.py` sub-command in the background
  and lets tests poll its output (`WaitLine`) and exit status (`WaitExit`).
- `InteropCluster` starts N Go Slaves, writes a `cluster_config.json`, launches
  the full `master.main()` via `master_harness.py bootstrap`, and `WaitBootstrap`
  polls the slaves' `ShardsCreated` events and xshard registrations.

### Python harness

`master_harness.py` is a single entry point; it reuses the real `ClusterConnection`
wire stack and cluster OP serializer map and performs only test-scripted
orchestration on top. Sub-commands:

- `bootstrap` — patches the environment (hostname, qkchash native stubs, SimpleNetwork)
  then runs the real `master.main()`; this heavy path is loaded lazily.
- `rpc <host> <port> <master_shards> <address_hex>`
- `peer <host> <port> <master_shards> <cluster_peer_id...> [--hold]` — creates multiple
  cluster peers, then destroys only the first. Used for `TestInteropPeerCreateOrDestroy`.
- `peermsg <host> <port> <master_shards> <cluster_peer_id> <branch> [--wait]` — creates
  the cluster peer, waits for it to be registered by Go, then sends a P2P
  `NEW_TRANSACTION_LIST` command addressed to the `(cluster_peer_id, branch)` via
  `ClusterMetadata`. Used for `TestInteropPeerMessageRouting`.
- `disconnect <host> <port> <master_shards>`
- `addminorblock <host> <port> <master_shards> [--wait]` — connects, PING/PONG, prints
  `READY`, then answers the Go slave's AddMinorBlockHeader request and prints
  `ADD_MINOR_BLOCK_OK error_code=0`. `--wait` bounds how long it waits for the request.

It does *not* reimplement the wire protocol. The `addminorblock` handler uses the
real `AddMinorBlockHeaderRequest` serializer for decode, the real
`AddMinorBlockHeaderResponse` class for the reply, and the real `ClusterConnection`
RPC framing; it never touches MasterServer block-processing state.

> **Note:** `TestInteropAddMinorBlockHeaderToMaster` depends on the `MinorBlockHeader`
> / `TokenBalanceMap` wire types ported in the companion PR. Until that lands
> (`wire.AddMinorBlockHeaderRequest.MinorBlockHeader` / `CoinbaseAmountMap` are still
> `*RawBytes` placeholders), the test file does not compile under `-tags interop`. It
> is intentionally authored against the merged API and will build that way.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PYQUARKCHAIN` | Yes | Path to pyquarkchain checkout |

## CI Integration

Example CI script:

```bash
#!/bin/bash
set -e

# Clone pyquarkchain if not present
if [ ! -d "../pyquarkchain" ]; then
    git clone --depth 1 https://github.com/QuarkChain/pyquarkchain.git ../pyquarkchain
fi

# Run interop tests
export PYQUARKCHAIN=../pyquarkchain
go test -tags interop ./qkc/cluster/slave/
```