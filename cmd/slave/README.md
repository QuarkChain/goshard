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
header there with the command in that README's
[Pinned root-genesis values](../../qkc/config/singularity/README.md#pinned-root-genesis-values)
section and compare its `hash` output with the `hash:` line printed here.

The shard-level `chain genesis` printed by `slave inspect` is the config
descriptor's fingerprint, not a pyquarkchain minor-block hash; it becomes the
real shard genesis block hash when the QKC block format (#1) lands.
