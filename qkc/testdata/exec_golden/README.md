# exec_golden

Execution golden vectors generated from pyquarkchain by
[`gen_exec_golden.py`](../gen_exec_golden.py), which drives pyquarkchain's
`quarkchain.evm.state.State` and `ShardState`. The generator imports that state
class as `EvmState` only to keep it distinct from shard-level execution. It reads
the two configs in
[`qkc/config/singularity`](../../config/singularity), so the vectors are bound to
the configs goshard ships rather than to whatever a pyquarkchain checkout happens
to carry.

Three granularities are emitted, each with its own file and its own consumer. The first two are for unit tests, the last is for integration tests.

| file | input | pinned output |
| --- | --- | --- |
| `state_level.json` | direct pyquarkchain `State` mutations | post state root, per-account reads |
| `message_level.json` | one signed transaction or one cross-shard deposit | post state root, receipts, gas counters, produced deposits, coinbase fees |
| `block_level.json` | whole minor blocks against a shard built from its genesis, with a root chain alongside | the seven values a block commits to, plus the deposits it consumed |

A block-level case carries the shard's genesis allocation, the serialized root
blocks it saw, the deposit lists its neighbours sent, and each block in order.
The allocation is the shard's `GENESIS.ALLOC`, so a consumer reaches the genesis
state root by applying it and nothing else — which is the same self-check the
genesis cases make, one level up.

## Regenerating

```
# from the root of a pyquarkchain checkout, inside a virtualenv with its
# requirements installed:
python <path-to-goshard>/qkc/testdata/gen_exec_golden.py
```

The checkout is taken from `$PYQUARKCHAIN`, defaulting to the current directory.

Two things guard the result. The script's first two cases are the genesis
allocations themselves, and it fails unless their state roots match the pinned
[minor-genesis values](../../config/singularity/README.md#pinned-minor-genesis-values)
— a mismatch elsewhere is then a real disagreement, not a case description that
never reached pyquarkchain `State`. And because that self-check says nothing about
execution — changing `messages.py` leaves the genesis root untouched — every
vector file records the oracle it came from: the pyquarkchain commit and a digest
of each module that decides execution. The script refuses to run when one of
those modules has uncommitted changes; `--allow-dirty` proceeds and names the
edited modules in the output instead.

## State-level ops

A case is an allocation, a list of ops, and the state root the ops commit to.
Every op names a method the generator calls on pyquarkchain's
`quarkchain.evm.state.State`. To
test the Go implementation against the same case, an op has to reach the call
in the third column.

| op | pyquarkchain `State` | Go |
| --- | --- | --- |
| `set_full_shard_key` | `full_shard_key = v` | `StateDB.SetFullShardKey` |
| `delta_token_balance` | `delta_token_balance` | `StateDB.DeltaTokenBalance` |
| `set_token_balance` | `set_token_balance` | `StateDB.SetBalanceByTokenID` |
| `read_account` | `get_balance` | `StateDB.GetBalanceByTokenID` |
| `set_nonce` | `set_nonce` | `StateDB.SetNonce` |
| `increment_nonce` | `increment_nonce` | `StateDB.GetNonce` + `StateDB.SetNonce` |
| `set_code` | `set_code` | `StateDB.SetCode` |
| `set_storage` | `set_storage_data` | `StateDB.SetState` |
| `snapshot` | `snapshot` | `StateDB.Snapshot` |
| `revert` | `revert` | `StateDB.RevertToSnapshot` |
| `commit` | `commit` | `StateDB.Commit`, then reopen at the returned root |

The Go consumer operates directly on geth's `core/state.StateDB`, which this
fork has taught QuarkChain's account rules (`core/state/statedb_qkc.go`). The
test adapts pyquarkchain's empty-account reads and explicitly reopens a committed
root instead of giving `StateDB` a second lifecycle.

## Mutable-state policy families (S1)

`core/state.TestStateGolden` consumes all 23 state vectors without a VM or a
transaction executor. The following nine supplement the 14 retained S0 cases.
Each checks the committed state root and account read-back against the pinned oracle.
The `_qkc` and `_qeth` variants exercise the two balance dispatch paths.

| case | policy pinned in the committed state |
| --- | --- |
| `full_shard_key_first_read_survives_revert` | The first-read key survives revert; a different account uses the restored context key. |
| `full_shard_key_first_write_survives_revert` | The frozen key survives removal of a newly created Go state object. |
| `full_shard_key_blank_read_expires_at_commit` | Commit ends the blank account's cached shard-key lifetime. |
| `set_token_balance_zero_keeps_token_absent_qkc` / `_qeth` | Setting zero does not create a token entry in a surviving account. |
| `delta_token_balance_zero_keeps_token_absent_qkc` / `_qeth` | Adding zero does not create a token entry either. |
| `sixteen_tokens_stay_list_encoded` | Sixteen nonzero balances use list encoding across commit. |
| `seventeenth_zero_token_does_not_enable_trie` | The threshold counts nonzero balances, not cached token entries. |

Seventeen **nonzero** tokens require the unsupported token-trie representation;
they are outside this S1 success corpus. Execution gates, PoSW transfer checks,
precompile activation and transaction rejection belong to the message/block
layers, where their effects can reach receipts or transaction acceptance.

Storage reset and account deletion are deliberately absent from the state-op
vocabulary. They are internal steps of CREATE and SELFDESTRUCT in pyquarkchain,
not standalone execution-layer operations. The reachable lifecycle cases pin
their consensus-visible effects instead:

| case | lifecycle behavior pinned |
| --- | --- |
| `contract_creation` | CREATE preserves balances already sent to the destination. |
| `contract_selfdestruct` | SELFDESTRUCT removes code, balances, and old storage. |
| `contract_selfdestruct_reverted_with_parent` | Reverting a parent frame also reverts a child's SELFDESTRUCT. |
| `devnet_selfdestruct_then_paid_again` | A later transaction in the same block can revive the account without reviving its old storage. |
| `devnet_selfdestruct_then_create2` | CREATE2 can recreate the destroyed address with a fresh storage trie. |

## The other two files

`message_level.json` and `block_level.json` are not op lists. Each case is a
whole input — one transaction or deposit, or a sequence of minor blocks — and
the pinned values are listed in the table at the top of this file.


## What earns a vector

A golden vector pins the answer to a question where geth's native behaviour and
QuarkChain's policy disagree, and where that disagreement reaches consensus bytes. 
A candidate earns a vector when both hold:

- **Divergence.** geth's corresponding primitive, left alone, produces something
   different from what pyquarkchain does — a different value, a different journal
   behaviour, a different account lifetime.

- **Consensus-visible.** That difference lands in bytes the network agrees on:
   the state root, the receipt root, or whether a transaction is accepted at all.

### How the candidate set is enumerated

Two axes, swept mechanically.

- **The op vocabulary.** The ops in the table above are the state-mutating
entry points. For each one, ask what geth's primitive does by default; where it
diverges, emit a small family — the op alone, the op combined with an adjacent op,
the op across a revert, the op across a commit.

- **Configuration.** The same op under a different switch is a different
question. Every switch that gates execution needs both of its sides covered
somewhere, by whichever layer can reach it. Shipping configs do not span this axis
on their own: where mainnet and devnet agree on a setting, a synthetic config is
the only way to reach the other side.
