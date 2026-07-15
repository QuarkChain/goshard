# slave

The goshard slave node. It boots from a pyquarkchain-compatible
`cluster_config.json` and hosts the shards assigned to one slave identity. It
performs no network I/O — it is the node, not the protocol (tracking issue
[#17](https://github.com/QuarkChain/goshard/issues/17)).

## Build

```
make slave
```

This installs the binary to `./build/bin/slave` (the same convention as `geth`).
The commands below are run from the repo root so the relative config paths resolve.

## Running a slave

The default action boots every shard assigned to `--node_id` and runs until
interrupted — a drop-in for how pyquarkchain's `cluster.py` starts a slave:

```
./build/bin/slave --cluster_config ./qkc/config/singularity/devnet.json --node_id S0
```

Each owned shard gets an isolated chaindb under
`{DB_PATH_ROOT}/shard-0x{full_shard_id}/` (relative to the working directory) and
logs `shard started` with its genesis hash and head height. An empty
`DB_PATH_ROOT` is pyquarkchain's mem-db mode (`use_mem_db`): every shard runs on
an ephemeral in-memory database and nothing is written to disk. `^C` (or SIGTERM)
shuts every shard down cleanly and exits 0; a second signal force-quits. The
handler is installed before any resource opens, so a signal that lands mid-boot
is honored as soon as boot settles. Rerunning against the same datadir validates
the stored genesis metadata against the config (`existing genesis validated`)
and refuses to start if the config changed since initialization.

Only geth's logging and file-based profiling debug flags are exposed. The debug
flags that would open a socket — the `--pprof` HTTP server and `--pyroscope.*`
push — are deliberately not registered, keeping the process free of network I/O.

## Inspecting a datadir

`slave inspect` is read-only and needs no config: it scans `--datadir` for shard
chaindb directories (`shard-0x{full_shard_id}/`), opens each in read-only mode,
and prints the stored genesis metadata record and chain head. A shard that
cannot be opened or read is reported without aborting the others, and the exit
status is non-zero if any shard failed. A running slave holds its chaindb locks
(each shard then reports `resource temporarily unavailable`), so inspect a
stopped node. The report goes to stdout; log lines go to stderr.

```
./build/bin/slave inspect --datadir ./qkc-data/devnet
```

```
shard 0x00000001 (qkc-data/devnet/shard-0x00000001):
  meta version:          1
  chain genesis:         0xea741742184975635c2eb1ba468e7b7f58156025517eee3d7583f4ca0ad2dbca
  root genesis:          0x5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d
  hash_prev_root_block:  0x5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d
  xshard cursor:         root=0 minor=0 deposit=0
  head block:            none recorded (stub chain persists no head)
shard 0x00040001 (qkc-data/devnet/shard-0x00040001):
  ...
2 shard(s) inspected, 0 failed
```

A chaindb whose bootstrap was interrupted before the metadata record committed
prints `genesis metadata: none (bootstrap never completed; next boot
re-initializes)` — the next `slave` run re-runs the fresh initialization path.

## Subcommands

### `slave config`

Parse, validate, and print a normalized summary of a cluster config for one slave.
Exits non-zero on an invalid config or an unknown node id.

```
./build/bin/slave config --cluster_config ./qkc/config/singularity/mainnet.json --node_id S0
```

```
slave S0 @ 127.0.0.1:38000
db path root: ./qkc-data/mainnet
network id:   1
owns 2 shard(s):
  FULL_SHARD_ID  CHAIN  SHARD  CONSENSUS   BLOCK_TIME  GENESIS_TIME  DIFFICULTY  GAS_LIMIT  ALLOC
  0x00000001     0      0/1    POW_ETHASH  10s         1556639999    5000000000  12000000   3
  0x00040001     4      0/1    POW_ETHASH  10s         1556639999    5000000000  12000000   3
config OK
```

`--node_id S9` (an id the config does not define) prints
`unknown node id "S9" (config defines: S0)` and exits non-zero.

### `slave genesis`

Derive and print the cluster's root genesis block. The printed hash is
byte-identical to pyquarkchain's
`GenesisManager.create_root_block().header.get_hash()`.

```
./build/bin/slave genesis --cluster_config ./qkc/config/singularity/mainnet.json
```

```
root genesis block:
  version:           0
  height:            0
  timestamp:         1556639999
  difficulty:        10000000000000
  total_difficulty:  10000000000000
  nonce:             0
  hash_prev_block:   0x0000000000000000000000000000000000000000000000000000000000000000
  hash_merkle_root:  0x0000000000000000000000000000000000000000000000000000000000000000
  seal_hash:         0xe7dcdecc09e724ad81e493d70dedcd6d9ea0ee830d7ab2528a5648f2a0cf8178
  hash:              0x4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51
```

The running devnet config works the same way:

```
./build/bin/slave genesis --cluster_config ./qkc/config/singularity/devnet.json
```

prints `hash: 0x5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d`.

## Fixtures and pyquarkchain cross-validation

Both real (singularity) cluster configs are checked in under
[`qkc/config/singularity/`](../../qkc/config/singularity/) — `mainnet.json` and
`devnet.json`. They are copied verbatim from pyquarkchain; provenance and the
regeneration steps live in [that directory's README](../../qkc/config/singularity/README.md).

To cross-validate a `slave genesis` run against pyquarkchain, derive the same
header there (swap the path for devnet) and compare with the `hash:` line:

```
# from the root of a pyquarkchain checkout, inside a virtualenv with its
# requirements installed (bare system python lacks e.g. aiohttp):
python -c "
import json
from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.genesis import GenesisManager
raw = json.load(open('mainnet/singularity/cluster_config_template.json'))
h = GenesisManager(ClusterConfig.from_dict(raw).QUARKCHAIN).create_root_block().header
print('hash', h.get_hash().hex())
"
```

The shard-level `chain genesis` printed by `slave inspect` is the config
descriptor's fingerprint, not a pyquarkchain minor-block hash; it becomes the
real shard genesis block hash when the QKC block format (#1) lands.

## Follow-up integration checklist

The slave currently provides the process, per-shard database ownership, and
lifecycle around a stub chain. The following replacement points are deliberate:

- **Real shard chain:** when the `qkc/core` shard chain, QKC block format (#1),
  and genesis state materialization are ready, inject its `ChainService` from
  `cmd/slave` instead of relying on `StubChainService`. Adapt `GenesisHash`,
  `Head`, and `Stop`; `Stop` must wait for every chain-owned goroutine before the
  shard database closes.
- **Genesis persistence and inspection:** at the same integration point, delete
  the temporary `GenesisMeta`, descriptor `Fingerprint`, and metadata
  reconciliation path rather than migrating them. Re-bootstrap the genesis-only
  databases, make the real chain reject both genesis and chain-rule changes, and
  update `slave inspect` to read the canonical QKC minor genesis/head through
  `qkc/core/rawdb`, including the branch, previous root block, and x-shard cursor.
- **Integration tests:** switch the boot/reopen, inspect, mismatch, and goleak
  coverage to the real chain. Keep the goleak allowlist empty for slave-owned
  goroutines; fix their `Stop` path instead of ignoring them.
- **Master-driven creation:** when the cluster protocol (#5) lands, replace eager
  shard creation with the pyquarkchain-compatible `PING(root_tip)` trigger while
  preserving partial-boot rollback, idempotent blocking shutdown, and the
  `DBDirName` datadir convention.
