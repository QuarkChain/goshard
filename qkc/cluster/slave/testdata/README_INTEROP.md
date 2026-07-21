# Python Interop Tests

These tests verify protocol compatibility between the Go Slave and the
real Python Master (pyquarkchain).

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
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop -run TestRealMasterBootstrap ./qkc/cluster/slave/
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop -run TestRealMasterPeerLifecycle ./qkc/cluster/slave/
```

With race detector:
```bash
PYQUARKCHAIN=/path/to/pyquarkchain go test -race -tags interop ./qkc/cluster/slave/
```

## What is tested

- **Master connection and PING/PONG handshake** — Real `master.py` main entry point
- **Slave-to-slave (xshard) connectivity** — Real `CONNECT_TO_SLAVES_REQUEST` from master
- **External peer lifecycle** — Real `CREATE_CLUSTER_PEER_CONNECTION_REQUEST` and `DESTROY_CLUSTER_PEER_CONNECTION_COMMAND` from master

## Architecture

```
slave/
├── interop_cluster.go          # InteropCluster framework (start/stop/wait)
├── interop_helpers.go          # Shared helpers (freePort, runPythonScript, etc.)
├── interop_bootstrap_test.go   # TestRealMasterBootstrap
├── interop_peer_test.go        # TestRealMasterPeerLifecycle
└── testdata/
    ├── README_INTEROP.md           # This file
    ├── bootstrap_master_wrapper.py # Launches real master.py with patches
    └── genesis_helper.py           # Computes genesis hash/header from config
```

### Test Files

- `interop_cluster.go` — Reusable `InteropCluster` struct with `startInteropCluster`, `Stop`, `WaitBootstrap`, `Slave`, `MasterOutput`, `P2PPort`, `ConfigPath`
- `interop_bootstrap_test.go` — `TestRealMasterBootstrap`: starts real Python Master + Go Slaves, verifies MasterConn and xshard connections
- `interop_peer_test.go` — `TestRealMasterPeerLifecycle`: connects a minimal external peer via `net.Dial`, verifies PeerConn create/destroy

### Python Scripts

`bootstrap_master_wrapper.py`:
- Patches `qkchash` native library with Python stubs
- Patches `socket.gethostname`/`gethostbyname` for macOS compatibility
- Forces `SimpleNetwork` mode (not `P2PManager/RLPx`) for simple peer testing
- Calls `master.main()` to run the real master

`genesis_helper.py`:
- Computes the genesis block hash and serialized header from `cluster_config.json`
- Used by the peer test to construct a valid HELLO frame

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PYQUARKCHAIN` | Yes | Path to pyquarkchain checkout |

If `PYQUARKCHAIN` is not set or the directory doesn't exist, tests are automatically skipped.

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