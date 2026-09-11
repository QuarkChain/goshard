# exec_golden

Execution golden vectors generated from pyquarkchain by
[`gen_exec_golden.py`](../gen_exec_golden.py), which drives pyquarkchain's own
`EvmState` and `ShardState`. It reads the two configs in
[`qkc/config/singularity`](../../config/singularity), so the vectors are bound to
the configs goshard ships rather than to whatever a pyquarkchain checkout happens
to carry.

Three granularities are emitted, each with its own file and its own consumer in
`qkc/core`:

| file | input | pinned output |
| --- | --- | --- |
| `state_level.json` | direct `EvmState` mutations | post state root, per-account reads |
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
never reached `EvmState`. And because that self-check says nothing about
execution — changing `messages.py` leaves the genesis root untouched — every
vector file records the oracle it came from: the pyquarkchain commit and a digest
of each module that decides execution. The script refuses to run when one of
those modules has uncommitted changes; `--allow-dirty` proceeds and names the
edited modules in the output instead.

## State-level ops

A case is an allocation, a list of ops, and the state root the ops commit to.
Every op names a method the generator calls on pyquarkchain's `EvmState`. To
test the Go implementation against the same case, an op has to reach the call
in the third column.

| op | pyquarkchain `EvmState` | Go |
| --- | --- | --- |
| `set_full_shard_key` | `full_shard_key = v` | `EvmState.SetFullShardKey` |
| `delta_token_balance` | `delta_token_balance` | `EvmState.DeltaTokenBalance` |
| `set_token_balance` | `set_token_balance` | `EvmState.SetTokenBalance` |
| `read_account` | `get_balance` | `EvmState.GetBalance` |
| `set_nonce` | `set_nonce` | `EvmState.SetNonce` |
| `increment_nonce` | `increment_nonce` | `EvmState.IncrementNonce` |
| `set_code` | `set_code` | `EvmState.SetCode` |
| `set_storage` | `set_storage_data` | `StateDB.SetState` |
| `reset_balances` | `reset_balances` | `StateDB.ResetBalances` |
| `reset_storage` | `reset_storage` | `StateDB.ResetStorage` |
| `del_account` | `del_account` | `StateDB.DelAccount` |
| `snapshot` | `snapshot` | `EvmState.Snapshot` |
| `revert` | `revert` | `EvmState.RevertToSnapshot` |
| `commit` | `commit` | `EvmState.Commit` |

The two Go receivers are one object. `EvmState` is QuarkChain's, in `qkc/state`;
`StateDB` is geth's, in `core/state`, which this fork has taught QuarkChain's
account rules (`core/state/statedb_qkc.go`). `EvmState` embeds a `*state.StateDB`,
and Go makes an embedded type's methods callable on the outer one, so a row
naming `StateDB` is that method reached through `EvmState` unchanged — no
forwarding code exists for it. `qkc/state` writes its own method only where
QuarkChain's semantics differ from geth's.

## Mutable-state policy families (S1)

`qkc/state.TestStateGolden` consumes all 42 state vectors without a VM or a
transaction executor. The following 25 supplement the original 17. Each checks
the committed state root and account read-back against the pinned oracle.
The `_qkc` and `_qeth` variants exercise the two balance dispatch paths.

| case | policy pinned in the committed state |
| --- | --- |
| `reset_storage_alone_does_not_touch` | Reset alone leaves the stored slots intact. |
| `reset_storage_equal_write_touches` | An equal-value storage write publishes the reset. |
| `reset_storage_revert_restores_dirty_slots` | Revert restores the storage root and writes made before the snapshot. |
| `reset_storage_revert_restores_clean_account` | A later touch publishes the restored storage of an account that was clean at the snapshot. |
| `untouched_storage_reset_lost_across_commit` | A later touch cannot publish a reset discarded at commit. |
| `reset_storage_write_survives_commit` | New storage survives reopening; abandoned slots stay absent. |
| `reset_storage_zero_delta_touches_qkc` / `_qeth` | A zero balance delta touches a nonblank account and publishes its storage reset. |
| `reset_storage_equal_balance_touches_qkc` / `_qeth` | An unchanged balance write also publishes the storage reset. |
| `reset_balances_alone_does_not_touch` | Reset alone leaves the stored balances intact. |
| `untouched_balance_reset_lost_across_commit` | A later touch uses balances read from the trie, not the discarded reset. |
| `set_code_revert_restores_unloaded_code` | Revert restores code even when it had not been loaded before replacement. |
| `del_account_revert_on_touched_account` | A pre-snapshot touch publishes the unrestored balance reset, with nonce, code and storage restored. |
| `del_account_revert_then_touch` | A post-revert touch exposes the same unrestored balance reset. |
| `full_shard_key_first_read_survives_revert` | The first-read key survives revert; a different account uses the restored context key. |
| `full_shard_key_first_write_survives_revert` | The frozen key survives removal of a newly created Go state object. |
| `full_shard_key_blank_read_expires_at_commit` | Commit ends the blank account's cached shard-key lifetime. |
| `set_token_balance_zero_keeps_token_absent_qkc` / `_qeth` | Setting zero does not create a token entry in a surviving account. |
| `delta_token_balance_zero_keeps_token_absent_qkc` / `_qeth` | Adding zero does not create a token entry either. |
| `ripemd_touch_reverts_after_balance_reset` | Address 3 has no geth-style persistent dirty mark after revert. |
| `sixteen_tokens_stay_list_encoded` | Sixteen nonzero balances use list encoding across commit. |
| `seventeenth_zero_token_does_not_enable_trie` | The threshold counts nonzero balances, not cached token entries. |

Seventeen **nonzero** tokens require the unsupported token-trie representation;
they are outside this S1 success corpus. Execution gates, PoSW transfer checks,
precompile activation and transaction rejection belong to the message/block
layers, where their effects can reach receipts or transaction acceptance.

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
