# qkcshard

`qkcshard` is a minimal standalone runner for QuarkChain **shardchains**
(slavechains) ported under `qkc/`. It plays the role of a single goquarkchain
*slave* process: it hosts one or more shards (a `FULL_SHARD_ID_LIST`), each with
its own database, genesis and `MinorBlockChain`, and optionally produces blocks
on all of them in parallel.

The cluster layer (master/slave networking, p2p, RPC) is **not** part of this
repository — it keeps running on the original goquarkchain code. Transaction
execution is also not wired up yet; it plugs into the `TODO(execution-issue)`
seams (blocks produced by this runner carry no transactions and the empty state
root).

## Quick start

```bash
# One shard (full shard id 0x2), in-memory database, simulated consensus,
# produce 3 blocks and exit:
go run ./cmd/qkcshard --mine --blocks 3

# Two shards mining in parallel:
go run ./cmd/qkcshard --fullshardid 2,3 --mine --blocks 3

# Persistent databases under /data/slave0/shard-0x2 and /data/slave0/shard-0x3.
# Run until Ctrl-C; restarting resumes from the stored heads:
go run ./cmd/qkcshard --datadir /data/slave0 --fullshardid 2,3 --mine

# Print chain status (heads) without mining:
go run ./cmd/qkcshard --datadir /data/slave0 --fullshardid 2,3

# Use a custom cluster config (e.g. with real proof-of-work, see below):
go run ./cmd/qkcshard --config cluster.json --fullshardid 0x10002 --mine
```

Or build a binary first:

```bash
go build -o qkcshard ./cmd/qkcshard
./qkcshard --datadir /data/slave0 --fullshardid 2,3 --mine
```

## Flags

| Flag | Default | Description |
|---|---|---|
| `--fullshardid` | `2` | Comma-separated full shard ids to host (decimal or `0x` hex), e.g. `2,3,0x10002`. |
| `--datadir` | *(empty)* | Data directory. Each shard stores its chain in `<datadir>/shard-0xN` (pebble). Empty = in-memory, lost on exit. |
| `--config` | *(empty)* | Cluster config JSON file (goquarkchain format). Empty = built-in default config (3 chains × 2 shards, `POW_SIMULATE`). |
| `--mine` | `false` | Produce blocks on all hosted shards. Without it the runner prints the chain status and exits. |
| `--blocks` | `0` | Stop after producing this many blocks **per shard** (`0` = run until SIGINT/SIGTERM). |
| `--verbosity` | `3` | Log level (`0`=crit … `5`=trace). |

## Full shard ids

A full shard id encodes `chainID<<16 | shardSize | shardID`. With the built-in
default config (3 chains, shard size 2 per chain) the valid values are:

```
0x00002 0x00003   (chain 0, shards 0/1)
0x10002 0x10003   (chain 1, shards 0/1)
0x20002 0x20003   (chain 2, shards 0/1)
```

## Consensus

Block production runs in the `qkc/miner` module (the internal commit→seal→result
loop plus the external `GetWork`/`SubmitWork` RPC path), not in this entry point;
the runner only implements `miner.MinerAPI` and forwards chain-head events. The
engine is selected **per shard** from the shard's `CONSENSUS_TYPE`:

| `CONSENSUS_TYPE` | Engine | Behaviour |
|---|---|---|
| `POW_DOUBLESHA256` | `qkc/consensus/doublesha256` | Real proof-of-work: nonces are mined and seals are verified on import. |
| anything else (`POW_SIMULATE`, `NONE`, …) | `qkc/consensus/simulate` | Paced production: the seal "search" sleeps ~`TARGET_BLOCK_TIME` seconds, then emits. |

Block difficulty follows the shard's `EthDifficultyCalculator` parameters
(`DIFFICULTY_ADJUSTMENT_CUTOFF_TIME` / `DIFFICULTY_ADJUSTMENT_FACTOR`) in both
modes; the produced difficulty is validated on import.

To try real PoW, generate a config with `POW_DOUBLESHA256`:

```go
// go run this once to produce cluster.json
package main

import (
	"encoding/json"
	"os"

	"github.com/ethereum/go-ethereum/qkc/config"
)

func main() {
	c := config.NewClusterConfig()
	for _, ch := range c.Quarkchain.Chains {
		ch.ConsensusType = config.PoWDoubleSha256
	}
	data, _ := json.Marshal(c)
	os.WriteFile("cluster.json", data, 0644)
}
```

```bash
go run ./cmd/qkcshard --config cluster.json --mine --blocks 3
```

## Data layout

Every shard gets its **own** database (`<datadir>/shard-0xN`). This is
required, not cosmetic: the qkc rawdb schema stores per-chain singletons
(`LastBlock`, canonical height→hash mappings, …) under fixed keys, so two
shards sharing one key space would overwrite each other's canonical state.
This mirrors goquarkchain, where each slave shard owns a separate database.

Genesis setup is idempotent: on restart the stored genesis is checked against
the configured one (`SetupGenesisMinorBlock`) and mining resumes from the
persisted head.

## Root chain anchoring

Every minor block header carries `PrevRootBlockHash` and the validator requires
that root block to exist in the shard database. In production the **external
root chain** (the original goquarkchain code) supplies root blocks via
`qkc/core/rawdb.WriteRootBlock`. This runner writes the configured root genesis
block into each shard database at startup so the shard genesis has an anchor —
replace that step with real root-chain data when integrating.

## Current limitations

- **No transaction execution**: blocks are structurally validated (header
  rules, PoW, tx/receipt/meta roots, root-chain anchoring) and persisted, but
  carry no transactions and use the empty state root. The execution layer
  (geth `core/state` + `core/vm` via the `Processor`/`ValidateState` seams) is
  a separate work item — search for `TODO(execution-issue)`.
- **No p2p / RPC / txpool / cluster services**: out of scope here; the cluster
  layer stays on the original goquarkchain code.
- **No root chain**: only the shard side lives in this repository.
