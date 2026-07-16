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

```bash
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop ./qkc/cluster/slave/
```

Or run specific tests:

```bash
PYQUARKCHAIN=/path/to/pyquarkchain go test -tags interop -run TestSlaveServer_PythonMasterPing ./qkc/cluster/slave/
```

## What is tested

- **Master connection and PING/PONG handshake** — Uses real pyquarkchain protocol classes
- **Peer virtual connection lifecycle** — CREATE / ROUTE / DESTROY commands
- **Slave-to-slave (xshard) connectivity** — CONNECT_TO_SLAVES command

## Architecture

```
testdata/
├── README_INTEROP.md          # This file
└── py_master_helper.py        # Python helper using real pyquarkchain
```

### Go Test Files

- `interop_test.go` — Test entry points (build tag: `interop`)
- `interop_helpers.go` — Shared infrastructure (Python launch, env detection)

### Python Helper

`py_master_helper.py` imports real pyquarkchain protocol classes:
- `ClusterConnection`, `ClusterMetadata`
- `Ping`, `Pong`, `ConnectToSlavesRequest`
- `CreateClusterPeerConnectionRequest`, `DestroyClusterPeerConnectionCommand`

It acts as a lightweight master-side endpoint to exercise the Go Slave runtime.

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
