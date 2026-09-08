# exec_golden

Execution golden vectors generated from pyquarkchain by
[`gen_exec_golden.py`](../gen_exec_golden.py). What the three files hold, how
they are regenerated, and how the result is guarded is documented in
[`qkc/config/singularity/README.md`](../../config/singularity/README.md), under
"Execution golden vectors".

This file records the one thing a consumer cannot read off the vectors: which
call each `state_level.json` op stands for.

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

## The other two files

`message_level.json` and `block_level.json` are not op lists. Each case is a
whole input — one transaction or deposit, or a sequence of minor blocks — and
the pinned values are listed in the table in the singularity README.
