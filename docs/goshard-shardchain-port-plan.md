# Port QuarkChain "slavechain" (MinorBlock) block & header into goshard — chain plumbing, execution deferred

## Context

`goshard/base` is currently vanilla-ish go-ethereum v1.17.3 (module `github.com/ethereum/go-ethereum`). We want this branch to **support the shard-chain ("slavechain") block and header as defined in QuarkChain** (`/Users/xuzhiqiang/Documents/github/goquarkchain`). In QuarkChain terms the slavechain block is the **`MinorBlock`**, its header is **`MinorBlockHeader`**, with side-car **`MinorBlockMeta`**.

Decisions locked in through Q&A:
- **Faithful byte-compatible port** — header/block hashes must match QuarkChain byte-for-byte → port QuarkChain's custom reflection-based `serialize` package (headers are NOT RLP) and replicate the exact field layouts & hashing.
- **Shardchain only** — the **root chain (`RootBlockChain`) is NOT implemented here**; it keeps running on the original code.
- **Keep the shardchain code identical to goquarkchain** — do not redesign or strip. Where the shard code references root *types*, port those types as **passive dependencies**. The only unavoidable changes are adapting geth 1.8.20 → 1.17.3 API differences (compile-level), never logic/structure/format.
- **Execution is OUT OF SCOPE (separate issue)** — transaction execution / state transition / state processing is handled in a separate issue. This branch does the block/header + chain plumbing around them, not the running of transactions.
- **Architecture = parallel tree, minimal sharing (confirmed)** — `qkc/` reimplements the chain/engine/rawdb layer; only low-level utilities are shared with geth (see inventory). **No invasive / interface refactor** of goshard's `core`/`consensus`/`rawdb`; goshard's mainnet core stays untouched and upstream-mergeable. The Engine API (`beacon/engine`, `eth/catalyst`) is not shared or touched.

### Decisive structural findings
1. geth's `core/types` already defines `Transaction`/`Receipt`/`Bloom`/`Transactions`/`DeriveSha` with **different** meaning, and geth's `BlockChain`/`HeaderChain`/`consensus.Engine` are hard-typed to `*types.Header`/`*types.Block`. → The port must be a **parallel package tree** (not inside `core/types`), reusing only geth's type-agnostic layers (`ethdb`, `triedb`, `params`, `crypto`, `rlp`, `trie`).
2. `MinorBlockHeader.Hash()` = `keccak256(serialize(header))`; `SealHash` excludes `MixDigest`+`Nonce`. `crypto.NewKeccakState()` is a **byte-exact** replacement for QuarkChain's `sha3.NewKeccak256()`.
3. `MinorBlockMeta.TxHash` is a **custom binary Merkle tree** (`CalculateMerkleRoot`) — **not** a trie; port verbatim. `MinorBlockMeta.ReceiptHash` is a trie root → `types.DeriveSha(list, trie.NewStackTrie(nil))` after porting `Receipt.EncodeIndex` to emit QuarkChain's `Bytes(i)`. (geth's `DeriveSha` now takes an explicit `ListHasher`.)
4. goshard is not stock geth (multidimensional gas, "Amsterdam" fork, UBT trie, `core.Message` on uint256) — irrelevant to this branch since execution is deferred, but relevant to the later execution issue.

## Goal

A self-contained, compiling, tested **block/header + chain-plumbing** layer for the shardchain, under a new `qkc/` package tree, that:
- Defines `MinorBlock`/`MinorBlockHeader`/`MinorBlockMeta` and all their dependencies **byte-identically** to QuarkChain (same serialization, same hashes), keeping every field (incl. the deferred-feature fields) for byte-compat.
- Persists & loads blocks/headers/bodies/receipts via dedicated rawdb accessors.
- Assembles the **genesis** MinorBlock structurally and anchors the canonical chain on it.
- Provides a **`MinorBlockChain`** that imports blocks (store + **structural** validation), maintains the canonical head, serves `GetMinorBlock`/`GetHeader`/`GetBlockByNumber`/`CurrentBlock`, and round-trips through the DB on reopen.
- Ports the root **types** (`RootBlock`/`RootBlockHeader`/`RootConfig`) as passive deps so the shard code is identical to goquarkchain — without any root chain logic.

## Explicitly deferred / out of scope

- **Transaction execution & state (SEPARATE ISSUE):** `Process`/`ApplyTransaction`, state transition, the multi-token / `fromFullShardKey`-`toFullShardKey` / `gasToken` / `refundRate` state semantics, `ValidateState` (re-execution & state-root check), reward `Finalize` into state, `State()`/`StateAt()`, and **genesis state-trie population from alloc**. The fields these touch are still ported (for byte-compat); they're just not *driven*. `MinorBlockMeta.Root` is taken as a provided/known value (from config or the imported block) rather than recomputed.
- **Root chain implementation:** no `RootBlockChain`, no root processing/validation/mining/consensus — stays on original code. Root *types* are ported as passive deps only; `PrevRootBlockHash` is carried as goquarkchain does (set from the referenced root block / config, faithfully).
- **Also deferred:** cross-shard deposit *application*, PoSW, shard TxPool, cluster/master/slave networking, RPC, mining loop, p2p.

## Package layout (new tree; root `qkc/`, adjustable to `shard/`)

```
qkc/serialize/        # verbatim port of goquarkchain/serialize (reflection serializer; NOT rlp)
qkc/account/          # Branch, Address, Recipient(=common.Address), Identity, FullShardKey
qkc/config/           # ShardConfig, ChainConfig, QuarkChainConfig + RootConfig (passive) — faithful, no logic stripped
qkc/types/            # MinorBlock(Header/Meta), Transaction, EvmTransaction, Receipt(s), Bloom, TokenBalances,
                      #   Uint256/Uint32, XShardTxCursorInfo, DeriveSha, CalculateMerkleRoot, Copy*,
                      #   RootBlock/RootBlockHeader (passive deps the shard references)
qkc/core/rawdb/       # Read/Write MinorBlock(Header/Body/Receipts), canonical hash, head pointers; passive root-block read
qkc/core/            # MinorBlockChain (+ minimal engine, structural validator, structural genesis)
```
Cycle discipline (mirrors geth): `qkc/types` never imports `qkc/core*`; engine imports only `qkc/types`+`qkc/config`; the chain-reader interface lives in the consumer package `qkc/core`. `qkc/core/rawdb` → `qkc/types` (same shape as geth `core/rawdb` → `core/types`).

## Code shared with geth (goshard) — inventory

**Key fact:** goquarkchain forks its own `core/state`, `core/vm`, `params`, `consensus`, `common` (under `QuarkChain/goquarkchain/...` — the multi-token execution layer). Its genuine go-ethereum sharing is only the type-agnostic utilities.

### Core-module view (which high-level subsystems are shared)
| geth core module | shared? | note |
|---|---|---|
| **Engine API** (`beacon/engine`, `eth/catalyst`) | ❌ untouched | hard-bound to mainnet `types.Block`/`ExecutableData`; shard chain never uses the CL↔EL Engine API |
| `consensus` (Engine iface + ethash/clique) | ❌ | hard-typed to `*types.Header`; shard uses an engine **ported from QuarkChain** (over `types.IHeader`/`MinorBlockHeader`) |
| `core/types` (Block/Header/Tx/Receipt/Bloom) | ❌ | qkc defines its own MinorBlock set; only borrows `BytesToBloom`/`EncodeNonce`/`DeriveSha` helpers |
| `core` (BlockChain/StateProcessor/Validator/genesis) | ❌ | qkc has its own MinorBlockChain/validator/genesis |
| `core/state` (StateDB) | ⏸ deferred | execution issue only |
| `core/vm` (EVM) | ⏸ deferred | execution issue only |
| `core/rawdb` (accessors) | ❌ | qkc has its own accessors; borrows only `NewMemoryDatabase` |
| `eth` (protocol/handler/downloader/txpool), `miner`, `rpc`, `internal/ethapi` | ❌ untouched | out of scope (no p2p/sync/txpool/mining/RPC) |
| `trie` / `triedb` | ✅ | receipt root + token trie (utility) |
| `crypto` | ✅ | Keccak (header hash) |
| `rlp` | ✅ | EvmTx inner encoding / rawdb |
| `common` (+ `hexutil`) | ✅ | Hash/Address/hex |
| `ethdb` | ✅ | KV interfaces |
| `log` / `event` / `common/mclock` / `common/prque` | ✅ | logging/events/timing/reorg queue |
| `params` | ◐ type only | `ChainConfig` kept as a field; fork-rule use deferred |

**Bottom line:** only low-level utility/infrastructure modules are shared. **No high-level core module** (`core/*` chain machinery, `consensus`, the Engine API, `eth`, `miner`) is shared — each is re-implemented in `qkc/`, deferred (execution), or out of scope.

### Package-level detail — our `qkc/` tree shares with goshard **only**:

**Shared now (imported, not copied):**
- `common` (+ `common/hexutil`) — `Hash`/`Address`/`StorageSize`/`Big`, gencodec JSON hex.
- `crypto` — `NewKeccakState`/`Keccak256` (replaces `crypto/sha3.NewKeccak256`) → header hash + `CalculateMerkleRoot`.
- `rlp` — `EvmTransaction` inner encoding, `Uint32` special RLP, rawdb value encoding.
- `trie` — receipt-root (`DeriveSha`) + `TokenBalances` token trie.
- `core/types` (a few symbols only) — `BytesToBloom`, `EncodeNonce`, `Header` ref; plus `DeriveSha`+`ListHasher`/`DerivableList` (adaptation).
- `ethdb` — KV interfaces for qkc rawdb accessors + chain.
- `triedb` + `core/rawdb` (`NewMemoryDatabase` only) — backing for the ephemeral receipt trie (adaptation; old `new(trie.Trie)` needed none).
- `log`; `event` / `common/mclock` / `common/prque` (chain bookkeeping: head feed, timing, reorg queue).
- `params` — **only** `ChainConfig` held as a struct field on `MinorBlockChain` (faithfully kept; its fork-rule use is execution → deferred).

**Shared only by the deferred execution issue:** `core/state`, `core/vm`, `core` (Message/ApplyMessage/GasPool), `params` (heavy, EVM rules), `consensus` (if geth engine). All forked in goquarkchain; untouched here.

**NOT shared — ported fresh into `qkc/`:** `serialize`, `account`, `qkc/config`, `qkc/core/rawdb`, `qkc/core`, and all of `qkc/types` — including QuarkChain's **own** `Log`/`LogForStorage`, `Bloom`+`CreateBloom`, and `Receipt`/`Receipts`. (Correction to an earlier assumption: do **not** reuse geth's `types.Log`; QuarkChain has its own — `receipt.go:40/84` — required for byte-compat.)

## Faithful-port rules (apply throughout)

- **Copy structure, fields, tags, method names, and signatures verbatim** from goquarkchain (including the `rootBlock *RootBlock` param on `CreateMinorBlock`, `ShardConfig` root references, etc.). No renames, no field drops, no "improvements".
- **Only adapt what won't compile** under geth 1.17.3: imports (`common`, `rlp`, `ethdb`, `trie`, `crypto`), `sha3.NewKeccak256()` → `crypto.NewKeccakState()`, `DeriveSha(list)` → `types.DeriveSha(list, trie.NewStackTrie(nil))`, and the trie constructor (`new(trie.Trie)` → `trie.NewEmpty(triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil))` for the ephemeral receipt trie). Hash bytes must stay identical (verify with golden vectors).
- **Stub deferred call-sites minimally:** where ported shard code calls into execution/state (e.g. `MinorBlockChain.InsertChain` would call `Process`), replace that one call with a clearly-commented `// TODO(execution-issue): run state processing` seam, keeping the surrounding structural logic (store, canonical head, validation) intact.

## Implementation phases (each must `go build` + `go test` green before the next)

**Phase 1 — `qkc/serialize`** (port first; load-bearing). `bytebuffer.go`, `serializer.go`, `deserializer.go`, `typecache.go`, `utils.go` (incl. `Uint128`/`Uint256`, `Serializable`, `SerializeStructWithout`). Port `serializer_test.go`/`deserializer_test.go` to lock byte parity.

**Phase 2 — `qkc/account`**. `Branch{Value uint32}`, `Address{Recipient common.Address; FullShardKey uint32}`, `Recipient = common.Address`, `Identity`, serialize hooks. Port `branch_test.go`/`address_test.go`.

**Phase 3 — `qkc/config`**. Port `ShardConfig`/`ChainConfig`/`QuarkChainConfig`/`RootConfig`/`ShardGenesis`/`Allocation` **faithfully** (root refs kept as passive). `GetFullShardId()` etc. unchanged.

**Phase 4 — `qkc/types`** (the core of "block & header support"; byte-parity is the bar):
- Primitives: `Uint32` (special RLP), `Bloom` (`[256]byte` + `CreateBloom`/`LogsBloom`), `XShardTxCursorInfo`, `TokenBalances` (serialize form: sorted (tokenID,balance) pairs).
- `EvmTransaction` (rlp, with shard keys + token ids) + `Transaction` wrapper (type byte + len-prefixed rlp) + `Transactions`.
- `Receipt`/`Receipts` (+ `EncodeIndex`/`Bytes`).
- `DeriveSha` (via `trie.NewStackTrie`) + `CalculateMerkleRoot` (custom keccak Merkle, verbatim).
- `MinorBlockHeader`, `MinorBlockMeta` (+ `Hash`/`SealHash` via `serHash`, `Copy*`, accessors, gencodec JSON), `MinorBlock` (`NewMinorBlock`, `NewMinorBlockWithHeader`, `extminorblock` Serialize/Deserialize, `Body`, accessors, hash/size caches).
- Passive root types: `RootBlockHeader`, `RootBlock` (ported so shard code referencing them compiles; no root chain logic). `RootBlock` references `MinorBlockHeader` per goquarkchain.

**Phase 5 — `qkc/core/rawdb`**. Accessors over `ethdb`: `Read/WriteMinorBlockHeader`, `Read/WriteMinorBlock`, `Read/WriteBody`, `Read/WriteReceipts`, `Read/WriteCanonicalHash`, `Read/WriteHeadBlockHash`+`HeadHeaderHash`, `Read/WriteHeaderNumber`, plus passive `ReadRootBlock` (read-only). Mirror goquarkchain's key-prefix scheme verbatim (`h`/`mn`/`b`/`r`/`LastBlock`/`LastHeader`, …) for on-disk compat. Round-trip tests.

**Phase 6 — `qkc/core` engine (minimal, header-level)**. Faithful `Engine` interface (over `MinorBlockHeader`) + default impl for the structural pieces: `VerifyHeader` (parent linkage, `number==parent+1`, time monotonic, gaslimit/difficulty sanity), `SealHash`. `Finalize`'s reward-into-state path is a deferred seam.

**Phase 7 — `qkc/core` genesis (structural)**. `CreateMinorBlock(rootBlock *RootBlock, fullShardId, db)` / `CommitMinorBlock` / `SetupGenesisMinorBlock` with goquarkchain signatures kept. Assemble genesis `MinorBlockHeader`+`Meta` from `ShardConfig`; `Meta.Root` taken from config (state population is the deferred issue); write via Phase-5 rawdb. Test: deterministic genesis hash.

**Phase 8 — `qkc/core` validator (structural)**. Faithful `MinorBlockValidator.ValidateBlock`/`ValidateSeal` for everything recomputable **without execution**: parent existence/height, engine header verify, `TxHash==CalculateMerkleRoot(txs)`, `ReceiptHash==DeriveSha(receipts)`, `MetaHash==Meta.Hash()`, `Bloom`/`GasLimit` sanity. `ValidateState` (state-root via re-execution) is a deferred seam.

**Phase 9 — `qkc/core` MinorBlockChain**. Faithful struct & `NewMinorBlockChain` signature (db, caches, `currentBlock atomic`, engine, validator, branch, shardConfig). Loads genesis. `InsertChain` = structural-validate → `WriteBlockWithoutState`-style store (header+body+receipts) → canonical head + head pointers; the `Process`/`WriteBlockWithState` execution call is the deferred seam. Getters: `GetMinorBlock`/`GetHeader`/`GetBlockByNumber`/`CurrentBlock`/`HasBlock`/`Genesis`/`Stop`. (`State()`/`StateAt()` deferred.)

**Phase 10 — tests + tree-wide build**. Byte-compat golden vectors (see Verification); rawdb round-trip; end-to-end: build genesis → import 1–2 prebuilt minor blocks (with given txs/receipts/roots) → structural validation passes → canonical head advances → reopen `MinorBlockChain` on same DB reads identical head/blocks. Then `go build ./...` and `go test ./qkc/...`.

## Verification

- **Byte-compat (critical):** reuse QuarkChain's own test vectors. Port `goquarkchain/serialize/*_test.go`; for the header, assert goshard's `serialize(MinorBlockHeader)` and `MinorBlockHeader.Hash()` equal QuarkChain's for an identical field-by-field construction. Source golden hashes from goquarkchain's `core/types` tests; if none expose a literal, extract one via a tiny throwaway program in the goquarkchain repo and hard-code the hex as the golden value in `qkc/types` tests.
- **Round-trip:** `Serialize`→`Deserialize` equality for header/meta/block (incl. txs & receipts); rawdb write→read equality.
- **End-to-end (structural):** the Phase-10 flow (genesis → import → structural validation → reopen), no execution.
- **Build/test:** `go build ./...`; `go test ./qkc/...`. (Unset proxy env vars first if network errors appear, per global notes.)

## Critical files

**Port sources (goquarkchain):** `serialize/*.go`; `account/{branch,address,identity,common,types}.go`; `core/types/{minorblock,rootblock,transaction,token,bloom9,derive_sha,utils,receipt}.go`; `core/{minorblockchain,minorblock_validator,genesis}.go`; `core/rawdb/{schema,accessors_chain}.go`; `cluster/config/{shard_config,chain_config,cluster_config,root_config}.go`.

**Adaptation references (goshard):** `core/types/hashing.go` (DeriveSha + ListHasher/DerivableList); `crypto/keccak.go` (NewKeccakState/Keccak256); `trie/{trie,stacktrie}.go` + `triedb/database.go` (ephemeral trie for receipt root); `core/rawdb/{schema,accessors_chain}.go` (key-scheme conventions); `core/types/block.go` (Block/Header caching idioms); `consensus/consensus.go` (Engine/ChainHeaderReader shape).

**New files:** the `qkc/...` tree above (serializer port + ~12–16 type/chain files + tests).

## Note on the deferred execution issue (for continuity)

When the separate execution issue is taken up, the seams to fill are: `MinorBlockChain.InsertChain`→state processing, a `qkc/core` state processor (`EvmTransaction`→`core.Message`, EVM run, receipts), `ValidateState`, reward `Finalize`, genesis state-from-alloc, and `State()/StateAt()`. That issue must also decide the multi-token / full-shard-key state model (faithful multi-token would require invasively extending goshard's `core/state`; single native token would reuse it). Block/header hashing is unaffected by that choice and stays byte-compatible.
