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

```
INFO [..] slave booting               node_id=S0
INFO [..] genesis committed           shard=0x00000001 genesis=661b12..792667
INFO [..] shard started               shard=0x00000001 genesis=661b12..792667 head=0
INFO [..] genesis committed           shard=0x00040001 genesis=c80ea9..5496d2
INFO [..] shard started               shard=0x00040001 genesis=c80ea9..5496d2 head=0
INFO [..] slave running               node_id=S0 shards=2
```

Each owned shard gets an isolated chaindb under
`{DB_PATH_ROOT}/shard-0x{full_shard_id}/` (relative to the working directory),
stands on its QuarkChain minor genesis block, and reports that block's hash and
its head height. An empty `DB_PATH_ROOT` is pyquarkchain's mem-db mode
(`use_mem_db`): every shard runs on an ephemeral in-memory database and nothing
is written to disk.

`^C` (or SIGTERM) shuts every shard down cleanly and exits 0; a second signal
force-quits. The handler is installed before any resource opens, so a signal that
lands mid-boot is honored as soon as boot settles.

Rerunning against the same datadir revalidates each shard's stored genesis block
against the config and logs `existing genesis validated` instead of
`genesis committed`. A datadir initialized from a different config is refused
rather than reused, and the process exits non-zero:

```
slave S0: shard 0x00000001: stored genesis 0x661b12…792667 does not match config
genesis 0x04493a…8a7a0f (db ./qkc-data/devnet/shard-0x00000001) — cluster config
changed since initialization
```

Only geth's logging and file-based profiling debug flags are exposed. The debug
flags that would open a socket — the `--pprof` HTTP server and `--pyroscope.*`
push — are deliberately not registered, keeping the process free of network I/O.

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

### `slave inspect`

Read-only and config-free: scan `--datadir` for shard chaindb directories
(`shard-0x{full_shard_id}/`), open each in read-only mode, and print the stored
minor genesis block and chain head. A shard that cannot be opened, read, or
validated is reported without aborting the others, and the exit status is
non-zero if any shard failed. A running slave holds its chaindb locks (each shard
then reports `resource temporarily unavailable`), so inspect a stopped node. The
report goes to stdout; log lines go to stderr.

```
./build/bin/slave inspect --datadir ./qkc-data/devnet
```

```
shard 0x00000001 (qkc-data/devnet/shard-0x00000001):
  genesis block:         0x661b12d25851f510519f8b157b2b76c95ea8ba4faf2a78f047c12c0bec792667
  height:                0
  state root:            0x76b7e413ee8a10d27ad5158ce91b8b8e61d6af8805b965a4ce11b93db0286ed1
  coinbase:              0x000000000000000000000000000000000000000000000001
  coinbase amount:       token 35760 = 3250000000000000000
  evm_gas_limit:         12000000
  evm_xshard_gas_limit:  6000000
  hash_prev_root_block:  0x5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d
  xshard cursor:         root=0 minor=0 deposit=0
  chain id:              110001
  fork schedule:         byzantium=0 constantinople=0 eip150=0 eip155=0 eip158=0 homestead=0 petersburg=0
  head block:            none recorded (stub chain persists no head)
shard 0x00040001 (qkc-data/devnet/shard-0x00040001):
  ...
2 shard(s) inspected, 0 failed
```

Nothing is printed for a shard until everything the report would assert has been
checked, so a database that fails is described by its error alone rather than
having its fields presented as that shard's genesis:

- **The block must hold together.** A minor block's hash is its header's hash
  alone, and without the cluster config there is no derived encoding to compare
  against, so the meta is rehashed and checked against the `hash_meta` the header
  commits to. Otherwise a database whose meta was replaced would report the
  original, authentic-looking block hash next to a substituted state root. Block 0
  must also be block 0, with an empty body.
- **The block must belong here.** The stored block names its own shard through its
  branch; a chaindb sitting in another shard's directory is reported as a misplaced
  chaindb.
- **A missing block must be the only thing missing.** The genesis block is written
  last, once the chain stands, so its absence is an interrupted bootstrap —
  `genesis block: none (bootstrap never completed; next boot re-initializes)`, and
  the next `slave` run re-runs the fresh path. A head pointer with no genesis under
  it is not a state this lifecycle produces, and is reported rather than described
  as safely re-initializable.

The EVM rule set is stored apart from the genesis block, keyed by its hash, and is the
other half of what a reopen is checked against — so it is reported too: the shard's
chain id (`BASE_ETH_CHAIN_ID + CHAIN_ID + 1`) and its fork schedule, which for every
QuarkChain shard sits entirely at block 0. A datadir initialized before the rule set was
written prints `rule set: none stored`; that one is recoverable, and the next `slave`
run warns and writes it rather than refusing to boot.

A report that cannot be written fails the command instead of being summarized as a
success.

## Fixtures and pyquarkchain cross-validation

Both real (singularity) cluster configs are checked in under
[`qkc/config/singularity/`](../../qkc/config/singularity/) — `mainnet.json` and
`devnet.json`. They are copied verbatim from pyquarkchain; provenance and the
regeneration steps live in [that directory's README](../../qkc/config/singularity/README.md).

To cross-validate a `slave genesis` run against pyquarkchain, derive the same
header there with the command in that README's
[Pinned root-genesis values](../../qkc/config/singularity/README.md#pinned-root-genesis-values)
section and compare its `hash` output with the `hash:` line printed here.

The shard-level genesis hash printed by `slave inspect` is the QuarkChain minor
genesis block hash, byte-identical to pyquarkchain's
`GenesisManager.create_minor_block().header.get_hash()` — the genesis block and
its allocated state are derived by [`qkc.CreateMinorBlock`](../../qkc/genesis.go)
and can be cross-validated the same way as the root genesis (see that README's
[Pinned minor-genesis values](../../qkc/config/singularity/README.md#pinned-minor-genesis-values)).

## Follow-up integration checklist

The slave provides the process, per-shard database ownership, and the lifecycle
around a stub chain. The following replacement points are deliberate:

- **Real shard chain:** when the geth-core shard chain
  ([#1](https://github.com/QuarkChain/goshard/issues/1)) exists, inject its
  `ChainService` from `cmd/slave` instead of the stub. The seam already takes the
  arguments `NewBlockChain` needs; `Stop` must drain every chain-owned goroutine
  before the shard database closes, and the chain must refuse to open on a
  missing head state.
- **Genesis storage:** the same task retires the single `QKC-genesis-block` key —
  it is scaffolding, the block under it is not. Block 0 moves into the chain's own
  block storage (canonical-hash mapping and head pointers), and the key is dropped
  rather than migrated: the databases hold genesis-level data only, so a clean
  re-bootstrap is the whole migration. `slave inspect` then reads block 0 and the
  real head through those accessors.
- **Compatibility check placement:** geth checks rule-set compatibility against
  the persisted head header *before* constructing the chain, and answers an
  incompatibility by rewinding rather than refusing to start. Both become reachable
  once the chain persists a head; move `ReconcileChainConfig` ahead of construction
  then.
- **Integration tests:** move the boot/reopen, inspect, mismatch, and goleak
  coverage onto the real chain, and add a case where the rule set changes without
  changing block 0. Keep the goleak allowlist empty for slave-owned goroutines and
  fix their `Stop` path instead of ignoring them.
- **Master-driven creation:** when the cluster protocol
  ([#5](https://github.com/QuarkChain/goshard/issues/5)) lands, replace eager shard
  creation with the pyquarkchain-compatible `PING(root_tip)` trigger while keeping
  partial-boot rollback, idempotent blocking shutdown, and the `DBDirName` datadir
  convention.
