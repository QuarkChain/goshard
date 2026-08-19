#!/usr/bin/env python3
"""Generate execution golden vectors by driving pyquarkchain's own EvmState.

The vectors pin what goshard's execution layer must reproduce byte for byte:
a pre-allocation, a sequence of inputs, and the resulting state root plus every
consensus-visible side effect. Three granularities are emitted:

  state    direct EvmState mutations (balances, nonce, code, storage, deletion)
           -> post state root. Covers the account and storage encoding rules.
  message  a signed transaction or a cross-shard deposit run through
           apply_transaction / apply_xshard_deposit -> post state root,
           receipts, gas counters, produced deposits, coinbase fees.
  block    whole minor blocks run through ShardState.run_block, on a shard
           built from its genesis with a root chain alongside it -> the seven
           values a block commits to, plus the deposits it consumed.

The first state case of each network is the genesis allocation itself, and its
post root is asserted against qkc/testdata/minor_genesis_golden.json. Without
that self-check a mismatch cannot be told apart from a broken case description
(allocation never applied, shard config never set).

Usage (from a pyquarkchain checkout, inside a virtualenv with its requirements
installed; see qkc/config/singularity/README.md):

    python <path-to-goshard>/qkc/testdata/gen_exec_golden.py

The pyquarkchain checkout is taken from $PYQUARKCHAIN, defaulting to the
current directory.
"""

import asyncio
import hashlib
import json
import os
import subprocess
import sys

_TESTDATA_DIR = os.path.dirname(os.path.abspath(__file__))
_QKC_DIR = os.path.dirname(_TESTDATA_DIR)
_OUT_DIR = os.path.join(_TESTDATA_DIR, "exec_golden")

_PYQUARKCHAIN = os.path.abspath(os.environ.get("PYQUARKCHAIN", os.getcwd()))
_ALLOW_DIRTY = "--allow-dirty" in sys.argv

# The modules that decide what these vectors say. Their digests go into the
# output so a vector can be traced back to the exact source that produced it:
# the genesis self-check cannot do that job, since changing execution semantics
# leaves the genesis state root untouched.
_ORACLE_MODULES = (
    "quarkchain/evm/messages.py",
    "quarkchain/evm/state.py",
    "quarkchain/evm/transactions.py",
    "quarkchain/evm/specials.py",
    "quarkchain/evm/opcodes.py",
    "quarkchain/cluster/shard_state.py",
    "quarkchain/core.py",
    "quarkchain/genesis.py",
    "quarkchain/config.py",
    "quarkchain/utils.py",
)

sys.path.insert(0, _PYQUARKCHAIN)

# cluster_config resolves the local hostname at import time to pick a default
# bind address (cluster_config.py:15). That lookup fails on a machine whose
# hostname has no A record, and the address it computes has no bearing on
# execution: nothing here starts a cluster. Pinning it to loopback keeps the
# import a pure code load, and leaves the oracle modules untouched.
import socket as _socket

_socket.gethostbyname = lambda _host: "127.0.0.1"

try:
    from quarkchain.cluster.cluster_config import ClusterConfig
    from quarkchain.cluster.shard_state import ShardState
    from quarkchain.constants import GENERAL_NATIVE_TOKEN_CONTRACT_BYTECODE
    from quarkchain.core import (
        Address,
        Branch,
        CrossShardTransactionDeposit,
        CrossShardTransactionList,
        MinorBlockHeader,
        SerializedEvmTransaction,
        TypedTransaction,
    )
    from quarkchain.db import InMemoryDb
    from quarkchain.env import Env
    from quarkchain.evm.messages import apply_transaction, apply_xshard_deposit
    from quarkchain.evm.state import State as EvmState
    from quarkchain.evm.transactions import Transaction as EvmTransaction
    from quarkchain.evm.utils import privtoaddr
    from quarkchain.genesis import GenesisManager
    from quarkchain.utils import token_id_encode
except ImportError as exc:  # pragma: no cover - operator feedback only
    sys.exit(
        "cannot import quarkchain ({}); run from a pyquarkchain checkout or set "
        "PYQUARKCHAIN to one, inside a virtualenv with its requirements".format(exc)
    )


# ---------------------------------------------------------------------------
# helpers


def oracle_provenance():
    """Identify the pyquarkchain the vectors were taken from.

    A golden vector is only as auditable as its oracle. Recording the commit,
    refusing a dirty checkout and digesting the modules that decide execution
    is what makes a regenerated file's differences attributable.
    """
    def git(*args):
        return subprocess.check_output(
            ("git", "-C", _PYQUARKCHAIN) + args, text=True
        ).strip()

    try:
        commit = git("rev-parse", "HEAD")
        # Only the modules above are checked for local edits. A dirty script or
        # an untracked note has no bearing on what the vectors say, and failing
        # on those would only teach the operator to pass --allow-dirty by habit.
        status = git("status", "--porcelain", "--", *_ORACLE_MODULES)
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        sys.exit("cannot read the pyquarkchain checkout at {}: {}".format(_PYQUARKCHAIN, exc))

    # Porcelain lines are two status characters, a space, then the path.
    dirty = sorted(line.split(maxsplit=1)[-1] for line in status.splitlines() if line.strip())
    if dirty and not _ALLOW_DIRTY:
        sys.exit(
            "these pyquarkchain modules decide what the vectors say and have "
            "uncommitted changes:\n  {}\ncommit or stash them so the vectors "
            "name a reproducible oracle, or pass --allow-dirty to record the "
            "edits in the output instead".format("\n  ".join(dirty))
        )

    digests = {}
    for relative in _ORACLE_MODULES:
        with open(os.path.join(_PYQUARKCHAIN, relative), "rb") as f:
            digests[relative] = hashlib.sha256(f.read()).hexdigest()[:16]
    return {
        "pyquarkchain_commit": commit,
        "dirty_modules": dirty,
        "module_digests": digests,
    }


def _hex(data):
    return "0x" + data.hex()


def _addr(recipient_hex, full_shard_key):
    """A 24-byte QuarkChain address from a 20-byte recipient."""
    return Address(bytes.fromhex(recipient_hex), full_shard_key)


def _recipient(recipient_hex):
    return bytes.fromhex(recipient_hex)


# Recipients used by the hand-written cases. Values are arbitrary but fixed, so
# a regenerated file diffs cleanly against the previous one.
A = "aaaa0000000000000000000000000000000000aa"
B = "bbbb0000000000000000000000000000000000bb"
C = "cccc0000000000000000000000000000000000cc"

# Deterministic signing keys; the vectors carry the resulting signature, so the
# Go side never has to reproduce pyquarkchain's signing. Message-level cases
# fund the key's own recipient, since the sender is recovered from the
# signature and not taken from the case description.
KEY_A = bytes.fromhex("1" * 64)
KEY_B = bytes.fromhex("2" * 64)
SENDER_A = privtoaddr(KEY_A).hex()
SENDER_B = privtoaddr(KEY_B).hex()


def load_networks():
    """The singularity configs goshard ships, loaded through pyquarkchain."""
    networks = {}
    for name in ("mainnet", "devnet"):
        path = os.path.join(_QKC_DIR, "config", "singularity", name + ".json")
        with open(path) as f:
            raw = json.load(f)
        networks[name] = ClusterConfig.from_dict(raw)
    return networks


def make_evm_state(cluster_config, full_shard_id, **overrides):
    """EvmState set up the way ShardState.__create_evm_state does.

    Env.cluster_config is assigned rather than passed to the constructor: its
    setter is what configures the precompiled contracts' enable timestamps from
    the hard fork config (env.py:45-53).
    """
    env = Env(db=InMemoryDb())
    env.cluster_config = cluster_config
    qkc_config = env.quark_chain_config
    state = EvmState(env=env.evm_env, db=env.db, qkc_config=qkc_config)
    state.shard_config = qkc_config.shards[full_shard_id]
    state.sender_disallow_map = {}
    for key, value in overrides.items():
        setattr(state, key, value)
    return state


def apply_alloc(state, alloc):
    """Apply a pre-allocation exactly as GenesisManager.create_minor_block does.

    Sharing this path with the genesis case is what makes the genesis
    self-check meaningful (quarkchain/genesis.py:55-86).
    """
    for address_hex, entry in alloc.items():
        address = Address.create_from(bytes.fromhex(address_hex))
        state.full_shard_key = address.full_shard_key
        recipient = address.recipient
        code = entry.get("code")
        if code is not None:
            state.set_code(recipient, bytes.fromhex(code[2:]))
            state.set_nonce(recipient, 1)
        for slot, value in entry.get("storage", {}).items():
            state.set_storage_data(recipient, int(slot, 16), int(value, 16))
        for token, value in entry.get("balances", {}).items():
            state.delta_token_balance(recipient, token_id_encode(token), int(value))


def config_alloc(cluster_config, full_shard_id):
    """A shard's genesis ALLOC in the vectors' own allocation shape."""
    genesis = cluster_config.QUARKCHAIN.shards[full_shard_id].GENESIS
    alloc = {}
    for address_hex, entry in genesis.ALLOC.items():
        balances = entry.get("balances", entry)
        out = {
            "balances": {
                k: str(v) for k, v in balances.items() if k not in ("code", "storage")
            }
        }
        if "code" in entry:
            out["code"] = entry["code"]
        if "storage" in entry:
            out["storage"] = entry["storage"]
        alloc[address_hex] = out
    return alloc


# ---------------------------------------------------------------------------
# state-level vectors


def run_state_ops(state, ops):
    """Interpret one case's op list against a live EvmState."""
    snapshots = []
    for op in ops:
        kind = op["op"]
        if kind == "set_full_shard_key":
            state.full_shard_key = op["value"]
        elif kind == "delta_token_balance":
            state.delta_token_balance(
                _recipient(op["address"]), token_id_encode(op["token"]), int(op["value"])
            )
        elif kind == "set_token_balance":
            state.set_token_balance(
                _recipient(op["address"]), token_id_encode(op["token"]), int(op["value"])
            )
        elif kind == "read_account":
            # A plain read, which caches a blank account stamped with the shard
            # key current right now (state.py:387). The cache outlives the
            # transaction, so this is where the key freezes.
            state.get_balance(_recipient(op["address"]))
        elif kind == "set_nonce":
            state.set_nonce(_recipient(op["address"]), op["value"])
        elif kind == "increment_nonce":
            state.increment_nonce(_recipient(op["address"]))
        elif kind == "set_code":
            state.set_code(_recipient(op["address"]), bytes.fromhex(op["code"][2:]))
        elif kind == "set_storage":
            state.set_storage_data(
                _recipient(op["address"]), int(op["key"], 16), int(op["value"], 16)
            )
        elif kind == "reset_balances":
            state.reset_balances(_recipient(op["address"]))
        elif kind == "reset_storage":
            state.reset_storage(_recipient(op["address"]))
        elif kind == "del_account":
            state.del_account(_recipient(op["address"]))
        elif kind == "snapshot":
            snapshots.append(state.snapshot())
        elif kind == "revert":
            state.revert(snapshots.pop())
        elif kind == "commit":
            state.commit()
        else:
            raise ValueError("unknown state op {}".format(kind))


def observe(state, recipients, slots):
    """Post-commit reads, so a mismatched root can be localized to an account."""
    out = {}
    for recipient_hex in sorted(recipients):
        recipient = _recipient(recipient_hex)
        account = state.get_and_cache_account(recipient, should_cache=False)
        entry = {
            "nonce": account.nonce,
            "code_hash": _hex(account.code_hash),
            "full_shard_key": account.full_shard_key,
            "exists": not account.is_blank(),
            "balances": {
                str(token): str(value)
                for token, value in sorted(account.token_balances.to_dict().items())
            },
        }
        case_slots = slots.get(recipient_hex)
        if case_slots:
            entry["storage"] = {
                slot: "0x{:x}".format(account.get_storage_data(int(slot, 16)))
                for slot in sorted(case_slots)
            }
        out[recipient_hex] = entry
    return out


def build_state_case(networks, case):
    cluster_config = networks[case["network"]]
    full_shard_id = case.get("full_shard_id", 1)
    state = make_evm_state(cluster_config, full_shard_id)
    alloc = case["pre_alloc"]

    # The allocation is committed before the operations run, so they start from
    # a state that has been through the trie -- accounts read back from leaves,
    # nothing left touched from having been created. Executing against a still
    # dirty cache is a shape that only occurs while genesis is being built, and
    # it changes the answers: an account nothing touches is skipped by commit
    # entirely, so a case run against uncommitted allocations can pin a result
    # that no block would ever produce.
    apply_alloc(state, alloc)
    state.commit()
    pre_state_root = _hex(state.trie.root_hash)

    run_state_ops(state, case["ops"])
    state.commit()

    recipients = {a[:40] for a in alloc}
    slots = {a[:40]: set(alloc[a].get("storage", {})) for a in alloc}
    for op in case["ops"]:
        if "address" in op:
            recipients.add(op["address"])
            if op["op"] == "set_storage":
                slots.setdefault(op["address"], set()).add(op["key"])

    return {
        "name": case["name"],
        "comment": case["comment"],
        "network": case["network"],
        "full_shard_id": full_shard_id,
        "pre_alloc": alloc,
        # The root the allocation commits to, before any operation runs. A
        # consumer that reaches a different one has a problem in its allocation
        # handling, not in the operations under test.
        "pre_state_root": pre_state_root,
        "ops": case["ops"],
        "post_state_root": _hex(state.trie.root_hash),
        "accounts": observe(state, recipients, slots),
    }


def state_cases(networks):
    """Hand-written state cases, genesis self-check first."""
    cases = []
    for network in ("mainnet", "devnet"):
        cases.append(
            {
                "name": "genesis_alloc_" + network,
                "comment": "self-check: the shipped genesis ALLOC must reproduce the "
                "pinned minor genesis state root",
                "network": network,
                "pre_alloc": config_alloc(networks[network], 1),
                "ops": [],
            }
        )

    qkc_only = {"balances": {"QKC": "1000000000000000000"}}
    cases += [
        {
            "name": "touched_blank_account_pruned",
            "comment": "a zero-value delta only marks the account touched; commit "
            "deletes it rather than writing a blank leaf (state.py:562-586)",
            "network": "devnet",
            "pre_alloc": {},
            "ops": [
                {"op": "delta_token_balance", "address": A, "token": "QKC", "value": "0"}
            ],
        },
        {
            "name": "drained_account_pruned",
            "comment": "an account whose only balance goes back to zero is blank "
            "again and leaves the trie",
            "network": "devnet",
            "pre_alloc": {A + "00000001": qkc_only},
            "ops": [
                {
                    "op": "delta_token_balance",
                    "address": A,
                    "token": "QKC",
                    "value": "-1000000000000000000",
                }
            ],
        },
        {
            "name": "nonce_keeps_account",
            "comment": "nonce alone defeats is_blank, so a balance-less account "
            "stays in the trie",
            "network": "devnet",
            "pre_alloc": {},
            "ops": [
                {"op": "set_full_shard_key", "value": 1},
                {"op": "increment_nonce", "address": A},
            ],
        },
        {
            "name": "full_shard_key_freezes_at_first_read",
            "comment": "the shard key stamped into a new account is the one "
            "current when the address was first looked up, not when it was "
            "first written: get_and_cache_account builds the blank account "
            "with state.full_shard_key and caches it (state.py:387), and the "
            "cache lives until commit. A is read under key 1 and written under "
            "key 2, so its leaf carries 1; B is only ever written, so it "
            "carries 2 and shows what the other would have looked like",
            "network": "devnet",
            "pre_alloc": {},
            "ops": [
                {"op": "set_full_shard_key", "value": 1},
                {"op": "read_account", "address": A},
                {"op": "set_full_shard_key", "value": 2},
                {"op": "delta_token_balance", "address": A, "token": "QKC", "value": "5"},
                {"op": "delta_token_balance", "address": B, "token": "QKC", "value": "5"},
            ],
        },
        {
            "name": "storage_alone_prunes_account",
            "comment": "storage does not defeat is_blank: the account is dropped "
            "and its storage trie becomes unreachable",
            "network": "devnet",
            "pre_alloc": {},
            "ops": [
                {"op": "set_full_shard_key", "value": 1},
                {"op": "set_storage", "address": A, "key": "0x01", "value": "0x2a"},
            ],
        },
        {
            "name": "storage_zero_deletes_slot",
            "comment": "writing zero deletes the slot instead of storing it "
            "(state.py:236-241)",
            "network": "devnet",
            "pre_alloc": {
                A
                + "00000001": {
                    "balances": {"QKC": "5"},
                    "code": "0x6000",
                    "storage": {"0x01": "0x2a", "0x02": "0x2b"},
                }
            },
            "ops": [{"op": "set_storage", "address": A, "key": "0x01", "value": "0x0"}],
        },
        {
            "name": "token_blob_sorted_and_zero_free",
            "comment": "the balance blob is ordered by token id and carries no "
            "zero entries, whatever order they were written in",
            "network": "devnet",
            "pre_alloc": {},
            "ops": [
                {"op": "set_full_shard_key", "value": 1},
                {"op": "delta_token_balance", "address": A, "token": "ZZZ", "value": "3"},
                {"op": "delta_token_balance", "address": A, "token": "QKC", "value": "1"},
                {"op": "delta_token_balance", "address": A, "token": "BTC", "value": "2"},
                {"op": "delta_token_balance", "address": A, "token": "AAA", "value": "7"},
                {"op": "delta_token_balance", "address": A, "token": "AAA", "value": "-7"},
            ],
        },
        {
            "name": "del_account_removes_leaf",
            "comment": "del_account clears balances, nonce, code and storage and "
            "unsets touched, so commit deletes the leaf (state.py:596-610)",
            "network": "devnet",
            "pre_alloc": {
                A
                + "00000001": {
                    "balances": {"QKC": "9"},
                    "code": "0x6001",
                    "storage": {"0x01": "0x2a"},
                }
            },
            "ops": [{"op": "del_account", "address": A}],
        },
        {
            "name": "revert_undoes_mutations",
            "comment": "snapshot/revert restores balances, nonce, code and storage; "
            "the post root equals the untouched allocation's",
            "network": "devnet",
            "pre_alloc": {A + "00000001": qkc_only},
            "ops": [
                {"op": "snapshot"},
                {"op": "delta_token_balance", "address": A, "token": "QKC", "value": "7"},
                {"op": "delta_token_balance", "address": B, "token": "QKC", "value": "5"},
                {"op": "increment_nonce", "address": A},
                {"op": "set_code", "address": A, "code": "0x6002"},
                {"op": "set_storage", "address": A, "key": "0x01", "value": "0x2a"},
                {"op": "revert"},
            ],
        },
        {
            "name": "zeroed_token_entry_is_not_absent",
            "comment": "a balance set to zero leaves the entry in the map, so the "
            "blob is an empty pair list rather than empty bytes; only an account "
            "that never held the token serializes to nothing (state.py:144-150)",
            "network": "devnet",
            "pre_alloc": {
                A + "00000001": {"balances": {"QKC": "5"}, "code": "0x6000"},
                B + "00000001": {"code": "0x6000"},
            },
            "ops": [
                {"op": "set_token_balance", "address": A, "token": "QKC", "value": "0"}
            ],
        },
        {
            "name": "revert_does_not_restore_reset_balances",
            "comment": "reset_balances journals the restore onto a misspelled "
            "attribute (state.py:192-198), so reverting brings back the token trie "
            "but not the balances; the account stays drained. The opening credit "
            "is what makes that observable: reset_balances marks nothing touched, "
            "so without it commit would skip the account and both a faithful and "
            "a naively correct implementation would agree",
            "network": "devnet",
            "pre_alloc": {A + "00000001": {"balances": {"QKC": "5"}, "code": "0x6000"}},
            "ops": [
                {"op": "delta_token_balance", "address": A, "token": "QKC", "value": "1"},
                {"op": "snapshot"},
                {"op": "reset_balances", "address": A},
                {"op": "revert"},
            ],
        },
        {
            "name": "revert_after_del_account",
            "comment": "reverting del_account leaves the trie untouched: unwinding "
            "its six steps ends at the touched flag set_nonce journaled, which was "
            "False, so commit skips the account rather than rewriting the leaf it "
            "would otherwise have drained (a selfdestruct in a reverted frame)",
            "network": "devnet",
            "pre_alloc": {
                A
                + "00000001": {
                    "balances": {"QKC": "5"},
                    "code": "0x6000",
                    "storage": {"0x01": "0x2a"},
                }
            },
            "ops": [
                {"op": "snapshot"},
                {"op": "del_account", "address": A},
                {"op": "revert"},
            ],
        },
        {
            "name": "revert_of_balance_leaves_zero_entry",
            "comment": "TokenBalances.set_balance journals the undo as "
            "_balances[token_id] = preval (state.py:166), which writes the key "
            "back rather than deleting it. Reverting the only credit an account "
            "ever received therefore leaves it holding zero rather than holding "
            "nothing, and the leaf is 00c0 instead of empty bytes. The nonce is "
            "bumped before the snapshot so the account is non-blank and touched "
            "for a reason the revert does not unwind",
            "network": "devnet",
            "pre_alloc": {B + "00000001": {"balances": {"QKC": "5"}}},
            "ops": [
                {"op": "increment_nonce", "address": A},
                {"op": "snapshot"},
                {"op": "delta_token_balance", "address": A, "token": "QKC", "value": "5"},
                {"op": "revert"},
            ],
        },
        {
            "name": "zero_entry_lost_on_read_back",
            "comment": "the zero entry lives only in the in-memory map: "
            "TokenBalances.__init__ rebuilds _balances from the pair list "
            "(state.py:104), which carries no zeros, so an account whose leaf is "
            "00c0 comes back holding nothing and re-serializes to empty bytes. "
            "Touching it after the commit boundary without writing a balance is "
            "what makes the leaf change on its own",
            "network": "devnet",
            "pre_alloc": {A + "00000001": {"balances": {"QKC": "5"}}},
            "ops": [
                {"op": "increment_nonce", "address": A},
                {"op": "set_token_balance", "address": A, "token": "QKC", "value": "0"},
                {"op": "commit"},
                {"op": "increment_nonce", "address": A},
            ],
        },
        {
            "name": "committed_storage_reopens",
            "comment": "a contract's storage survives a commit boundary: the "
            "second commit reads the slots back through the trie",
            "network": "devnet",
            "pre_alloc": {
                A + "00000001": {"balances": {"QKC": "5"}, "code": "0x6000"}
            },
            "ops": [
                {"op": "set_storage", "address": A, "key": "0x01", "value": "0x2a"},
                {"op": "commit"},
                {"op": "set_storage", "address": A, "key": "0x02", "value": "0x2b"},
            ],
        },
    ]
    return cases


# ---------------------------------------------------------------------------
# message-level vectors


def build_tx(spec, qkc_config):
    # A version 2 transaction is signed under Ethereum's rules, where the
    # network id in the signature is the chain's eth chain id rather than
    # QuarkChain's network id (test_utils.py:122).
    network_id = qkc_config.NETWORK_ID
    if spec.get("version", 0) == 2:
        network_id = qkc_config.CHAINS[spec.get("from_full_shard_key", 1) >> 16].ETH_CHAIN_ID
    tx = EvmTransaction(
        nonce=spec.get("nonce", 0),
        gasprice=spec.get("gas_price", 0),
        startgas=spec.get("start_gas", 21000),
        to=bytes.fromhex(spec.get("to", "")),
        value=int(spec.get("value", 0)),
        data=bytes.fromhex(spec.get("data", "0x")[2:]),
        gas_token_id=token_id_encode(spec.get("gas_token", "QKC")),
        transfer_token_id=token_id_encode(spec.get("transfer_token", "QKC")),
        from_full_shard_key=spec.get("from_full_shard_key", 1),
        to_full_shard_key=spec.get("to_full_shard_key", 1),
        network_id=network_id,
        version=spec.get("version", 0),
    )
    # The shard size behind the full shard keys is what decides whether the
    # transaction is cross-shard, and with it the intrinsic gas.
    tx.set_quark_chain_config(qkc_config)
    key = {"A": KEY_A, "B": KEY_B}[spec.get("signer", "A")]
    # network_id is already set above; passing it to sign() would write it back
    # outside the serializable's mutable context and raise.
    tx.sign(key)
    return tx


def dump_tx(tx):
    return {
        "nonce": tx.nonce,
        "gas_price": str(tx.gasprice),
        "start_gas": tx.startgas,
        "to": _hex(tx.to),
        "value": str(tx.value),
        "data": _hex(tx.data),
        "network_id": tx.network_id,
        "from_full_shard_key": tx.from_full_shard_key,
        "to_full_shard_key": tx.to_full_shard_key,
        "gas_token_id": tx.gas_token_id,
        "transfer_token_id": tx.transfer_token_id,
        "version": tx.version,
        "v": str(tx.v),
        "r": str(tx.r),
        "s": str(tx.s),
        "sender": _hex(tx.sender),
        "hash": _hex(tx.hash),
    }


def build_deposit(spec):
    return CrossShardTransactionDeposit(
        tx_hash=bytes.fromhex(spec["tx_hash"][2:]),
        from_address=_addr(spec["from"], spec.get("from_full_shard_key", 1)),
        to_address=_addr(spec["to"], spec.get("to_full_shard_key", 1)),
        value=int(spec.get("value", 0)),
        gas_price=int(spec.get("gas_price", 0)),
        gas_token_id=token_id_encode(spec.get("gas_token", "QKC")),
        transfer_token_id=token_id_encode(spec.get("transfer_token", "QKC")),
        gas_remained=int(spec.get("gas_remained", 0)),
        message_data=bytes.fromhex(spec.get("message_data", "0x")[2:]),
        create_contract=spec.get("create_contract", False),
        is_from_root_chain=spec.get("is_from_root_chain", False),
        refund_rate=spec.get("refund_rate", 100),
    )


def dump_deposit(deposit):
    return {
        "tx_hash": _hex(deposit.tx_hash),
        "from": _hex(deposit.from_address.recipient),
        "from_full_shard_key": deposit.from_address.full_shard_key,
        "to": _hex(deposit.to_address.recipient),
        "to_full_shard_key": deposit.to_address.full_shard_key,
        "value": str(deposit.value),
        "gas_price": str(deposit.gas_price),
        "gas_token_id": deposit.gas_token_id,
        "transfer_token_id": deposit.transfer_token_id,
        "gas_remained": str(deposit.gas_remained),
        "message_data": _hex(deposit.message_data),
        "create_contract": deposit.create_contract,
        "is_from_root_chain": deposit.is_from_root_chain,
        "refund_rate": deposit.refund_rate,
    }


def dump_receipt(receipt):
    return {
        "success": receipt.state_root == b"\x01",
        "cumulative_gas_used": receipt.gas_used,
        "bloom": "0x{:0512x}".format(receipt.bloom),
        "contract_address": _hex(receipt.contract_address),
        "contract_full_shard_key": receipt.contract_full_shard_key,
        "logs": [
            {
                "address": _hex(log.address),
                "topics": ["0x{:064x}".format(t) for t in log.topics],
                "data": _hex(log.data),
            }
            for log in receipt.logs
        ],
    }


def build_message_case(networks, case):
    cluster_config = networks[case["network"]]
    qkc_config = cluster_config.QUARKCHAIN
    full_shard_id = case.get("full_shard_id", 1)
    coinbase = _recipient(case.get("block_coinbase", C))
    state = make_evm_state(
        cluster_config,
        full_shard_id,
        timestamp=case["timestamp"],
        gas_limit=case.get("gas_limit", 12000000),
        block_number=case.get("block_number", 1),
        block_coinbase=coinbase,
        block_difficulty=case.get("block_difficulty", 1),
    )
    apply_alloc(state, case["pre_alloc"])
    state.commit()

    recipients = {a[:40] for a in case["pre_alloc"]}
    recipients.add(case.get("block_coinbase", C))

    # Building and dumping the input stays outside the guarded call below: only
    # the execution may raise on purpose. A case must also say which outcome it
    # is pinning, so that a broken constructor, a drifted API or a crash in the
    # success path cannot quietly become a "this is rejected" vector that the Go
    # side then asserts forever.
    expect = case["expect"]
    if expect not in ("success", "rejected"):
        raise SystemExit("case {}: expect must be success or rejected".format(case["name"]))

    inputs = []
    if "tx" in case:
        tx = build_tx(case["tx"], qkc_config)
        inputs.append({"kind": "transaction", "transaction": dump_tx(tx)})
        recipients.add(tx.sender.hex())
        if tx.to:
            recipients.add(tx.to.hex())

        def execute():
            return apply_transaction(state, tx, tx.hash)
    else:
        deposit = build_deposit(case["deposit"])
        inputs.append({"kind": "deposit", "deposit": dump_deposit(deposit)})
        recipients.add(deposit.from_address.recipient.hex())
        recipients.add(deposit.to_address.recipient.hex())

        def execute():
            return apply_xshard_deposit(state, deposit, case.get("gas_used_start", 0))

    if expect == "success":
        success, output = execute()
        result = {"success": bool(success), "output": _hex(bytes(output))}
    else:
        try:
            execute()
        except Exception as exc:  # noqa: BLE001 - the rejection is the vector
            # Consumers assert that execution is refused, not the wording: the
            # message is pyquarkchain's and carries no consensus meaning.
            result = {
                "rejected": True,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        else:
            raise SystemExit(
                "case {}: expected a rejection, execution succeeded".format(case["name"])
            )

    state.commit()
    return {
        "name": case["name"],
        "comment": case["comment"],
        "network": case["network"],
        "full_shard_id": full_shard_id,
        "expect": expect,
        "context": {
            "timestamp": case["timestamp"],
            "gas_limit": state.gas_limit,
            "block_number": state.block_number,
            "block_coinbase": _hex(coinbase),
            "block_difficulty": state.block_difficulty,
        },
        "pre_alloc": case["pre_alloc"],
        "inputs": inputs,
        # A deposit's starting gas is decided by the cursor traversal, above
        # apply_xshard_deposit. It has to be dumped or the consumer cannot
        # reproduce the run: it is an input, not a result.
        "gas_used_start": case.get("gas_used_start", 0),
        "result": result,
        "post_state_root": _hex(state.trie.root_hash),
        "gas_used": state.gas_used,
        "xshard_receive_gas_used": state.xshard_receive_gas_used,
        "block_fee_tokens": {
            str(token): str(value) for token, value in sorted(state.block_fee_tokens.items())
        },
        "receipts": [dump_receipt(r) for r in state.receipts],
        "xshard_deposit_receipts": [dump_receipt(r) for r in state.xshard_deposit_receipts],
        "xshard_list": [dump_deposit(d) for d in state.xshard_list],
        "accounts": observe(state, recipients, {}),
    }


# The general native token manager's address (specials.py:446). Whatever code
# sits here is what prices a foreign gas token.
GENERAL_NATIVE_TOKEN_ADDRESS = "514b430000000000000000000000000000000003"

# Stand-ins for the manager, used where a case is about the *chain's* handling
# of the answer rather than about the manager's own arithmetic. The real
# contract's admin interface is not exercised by pyquarkchain's own tests
# either: they monkey-patch pay_native_token_as_gas. Running fixed bytecode at
# the manager's address instead keeps both sides on the same EVM, so the vector
# still pins every consensus step around the answer: the quote taken and
# unwound during validation, the reserve check, the two balance moves that swap
# native token for genesis token, the refund at the returned rate and the burn
# of the remainder.
#
# Each returns the manager's two words: the refund rate, then the gas price in
# genesis token. mstore(0, rate); mstore(32, price); return(0, 64).
MANAGER_RATE_80_PRICE_2 = "0x6050600052600260205260406000f3"
MANAGER_RATE_100_PRICE_0 = "0x6064600052600060205260406000f3"

QKC_TRANSFER_MNT_ADDRESS = "000000000000000000000000000000514b430002"
QKC_DEPLOY_SYSTEM_CONTRACT_ADDRESS = "000000000000000000000000000000514b430003"

# The auction contract's address (specials.py:443). It is the only sender the
# mint precompile accepts, so a case about minting has to run code from here.
NON_RESERVED_NATIVE_TOKEN_ADDRESS = "514b430000000000000000000000000000000002"

# The two native-token precompiles (specials.py:394), reachable from the
# native-token fork onwards.
MINT_MNT_ADDRESS = "000000000000000000000000000000514b430004"
BALANCE_MNT_ADDRESS = "000000000000000000000000000000514b430005"

# Hand-written bytecode, kept minimal so a vector's inputs can be read without
# a compiler. Each is exactly what the case needs and nothing else.
#
#   ANSWER_RUNTIME       mstore(0, 42); return(0, 32)
#   ANSWER_INIT          returns ANSWER_RUNTIME as the deployed code
#   REVERT_RUNTIME       revert(0, 0)
#   INFINITE_RUNTIME     jumpdest; jump(0) -- runs until the gas is gone
#   LOG_RUNTIME          log1(0, 32, 0xbeef…) over zeroed memory
#   CREATE2_RUNTIME      create2 of an init code that deploys empty code
ANSWER_RUNTIME = "0x602a60005260206000f3"
ANSWER_INIT = "0x69602a60005260206000f3600052600a6016f3"
REVERT_RUNTIME = "0x60006000fd"
INFINITE_RUNTIME = "0x5b600056"
LOG_RUNTIME = "0x7fbeef" + "00" * 29 + "60206000a100"
CREATE2_RUNTIME = "0x6460006000f36000526000600560" + "1b" + "6000f500"

# CREATE2_WORD_GAS_RUNTIME expands memory to exactly 3072 bytes and then runs
# CREATE2 over all of it, as the last instruction in the code.
#
#   mstore(0x0be0, 0)          expand to ceil32(0x0be0 + 32) = 3072 bytes = 96 words
#   create2(0, 0, 0x0c00, 0)   96 words of init code, no further expansion
#
# It is built to sit on the boundary the word charge creates. Up to and
# including CREATE2's 32000 the frame spends 32327; the 6-per-word charge on
# top is 576 more. pyquarkchain subtracts that charge without checking
# (vm.py:689) and mem_extend only checks when it has to grow, so a frame given
# less than 32903 goes negative there rather than running out. Placing CREATE2
# last is what makes the difference observable: the main loop's `gas < 0` test
# is at the top of the next iteration, which never comes.
CREATE2_WORD_GAS_RUNTIME = "0x6000610be0526000610c0060006000f5"

# The same CREATE2 without the mstore in front, so the memory still has to grow.
# mem_extend then does check what it is about to spend, which makes an
# underfunded frame a plain out-of-gas on both sides -- the case that says a
# consumer must not mistake every short CREATE2 for the boundary above.
CREATE2_GROW_RUNTIME = "0x6000610c0060006000f5"


def general_native_token_runtime():
    """The manager's deployed code, taken out of its deployment bytecode.

    Allocating the runtime directly is how pyquarkchain's own tests stand the
    contract up (test_shard_state.py:3350): the shipped bytecode is a
    constructor whose supervisor argument is fixed, so a case that has to drive
    the admin interface cannot use it as-is.
    """
    runtime_start = GENERAL_NATIVE_TOKEN_CONTRACT_BYTECODE.find(
        bytes.fromhex("608060405260"), 1
    )
    return _hex(GENERAL_NATIVE_TOKEN_CONTRACT_BYTECODE[runtime_start:-32])


def word(value):
    return "{:064x}".format(value)


def selfdestruct_runtime(beneficiary_hex):
    """selfdestruct(beneficiary)."""
    return "0x73" + beneficiary_hex + "ff"


def _push(value, width=None):
    """PUSHn of the smallest width that holds value, or a fixed one."""
    if isinstance(value, str):
        raw = value
    else:
        raw = "{:x}".format(value)
        raw = "0" * (len(raw) % 2) + raw
        raw = raw or "00"
    if width is not None:
        raw = raw.rjust(width * 2, "0")
    size = len(raw) // 2
    if size == 0:
        raw, size = "00", 1
    return "{:02x}".format(0x5F + size) + raw


def _mstore(offset, value):
    return _push(value) + _push(offset) + "52"


def _call(to_hex, gas, args_offset, args_size, ret_offset, ret_size, value=0):
    """CALL, whose arguments are pushed in the reverse of their order."""
    return (
        _push(ret_size)
        + _push(ret_offset)
        + _push(args_size)
        + _push(args_offset)
        + _push(value)
        + _push(to_hex)
        + _push(gas)
        + "f1"
    )


def transfer_mnt_runtime(to_hex, token_id, value, forwarded_gas, tail=""):
    """call the transfer-token precompile with (to, token id, value), then tail.

    The precompile is handed a fixed amount of gas rather than everything left,
    so what it gives back on a refusal is visible in what the caller can still
    afford to do afterwards. The answer lands at memory 0x80.
    """
    return (
        "0x"
        + _mstore(0, to_hex)
        + _mstore(0x20, token_id)
        + _mstore(0x40, value)
        + _call(QKC_TRANSFER_MNT_ADDRESS, forwarded_gas, 0, 0x60, 0x80, 0x20)
        + _push(0)
        + "55"  # sstore(0, call result)
        + tail
    )


def deploy_system_contract_runtime(index):
    """call the deploy precompile, then return whatever it answered."""
    return (
        "0x"
        + _mstore(0, index)
        + _call(QKC_DEPLOY_SYSTEM_CONTRACT_ADDRESS, 1500000, 0, 0x20, 0x20, 0x20)
        + "50"  # drop the call result
        + _push(0x20)
        + _push(0x20)
        + "f3"  # return(0x20, 32)
    )


def return_memory_word(offset):
    """return the 32-byte word at offset."""
    return _push(0x20) + _push(offset) + "f3"


# create(0, 0, 0) then return the address it produced.
# create(0, 0, 0), then return the address it produced.
CREATE_AND_RETURN_RUNTIME = "0x" + _push(0) * 3 + "f0" + _push(0) + "52" + return_memory_word(0)


def call_runtime(callee_hex):
    """call(gas, callee, 0, 0, 0, 0, 0); stop."""
    return "0x" + "6000" * 5 + "73" + callee_hex + "5af100"


def mint_mnt_runtime(minter_hex, token_id, amount_word, args_size, forwarded_gas):
    """call the mint precompile with args_size bytes of the three-word argument.

    args_size below 96 is the point: extract32 reads past the calldata as zeros
    (vm.py:59), so a short call is not refused — it mints whatever the truncated
    amount word comes to.
    """
    return (
        "0x"
        + _mstore(0, minter_hex)
        + _mstore(0x20, token_id)
        + _mstore(0x40, amount_word)
        + _call(MINT_MNT_ADDRESS, forwarded_gas, 0, args_size, 0x80, 0x20)
        + _push(0)
        + "55"  # sstore(0, call result)
    )


def create_runtime(init_hex):
    """create(0, init code) and store the address it answered with in slot 0."""
    init = init_hex[2:] if init_hex.startswith("0x") else init_hex
    size = len(init) // 2
    return (
        "0x"
        + _push(init)
        + _push(0)
        + "52"  # mstore(0, init code), which lands it right-aligned
        + _push(size)
        + _push(32 - size)
        + _push(0)
        + "f0"  # create(value=0, offset=32-size, size)
        + _push(0)
        + "55"  # sstore(0, address)
    )


def returndatacopy_runtime(callee_hex, size):
    """call a contract that returns 32 bytes, then copy size bytes of the answer."""
    return (
        "0x"
        + _call(callee_hex, 100000, 0, 0, 0, 0)
        + "50"  # drop the call result
        + _push(size)
        + _push(0)
        + _push(0)
        + "3e"  # returndatacopy(0, 0, size)
        + _push(0)
        + _push(0)
        + "51"  # mload(0) ...
        + "55"  # ... sstore(0, that word)
    )


# Six SSTOREs covering every transition the legacy schedule prices differently,
# including two slots written twice in the one transaction. Under EIP-1283's
# net metering — which Constantinople had and Petersburg took back out — the
# repeats cost 200 instead of 5000, so the gas this case records is what says
# which schedule is running.
SSTORE_TIERS_RUNTIME = (
    "0x"
    + _push(0) + _push(0) + "55"       # 0 -> 0, on a clean slot
    + _push(7) + _push(2) + "55"       # 0 -> non-zero
    + _push(3) + _push(1) + "55"       # non-zero -> non-zero
    + _push(0) + _push(1) + "55"       # non-zero -> 0, same slot again: refund
    + _push(1) + _push(3) + "55"       # 0 -> non-zero
    + _push(2) + _push(3) + "55"       # non-zero -> non-zero, same slot again
)

# Init code that returns `size` zero bytes, so the deployment's code-store
# charge is chosen by the case.
def returning_init(size):
    return "0x" + _push(size) + _push(0) + "f3"


def message_cases():
    """A first, deliberately small set: S3-S6 grow it per semantics entry."""
    funded = {"balances": {"QKC": "1000000000000000000"}}
    # devnet has every hard fork switch at 0, so these run post-EVM throughout.
    return [
        {
            "name": "in_shard_transfer",
            "expect": "success",
            "comment": "plain in-shard QKC transfer: nonce increments before gas is "
            "bought, fee lands in the coinbase and in block_fee_tokens",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 0,
                "gas_price": 1000000000,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 100,
                "signer": "A",
            },
        },
        {
            "name": "in_shard_transfer_nonce_too_high",
            "expect": "rejected",
            "comment": "apply_transaction re-validates, so a future nonce is "
            "rejected and nothing is written",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 5,
                "gas_price": 1000000000,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 100,
                "signer": "A",
            },
        },
        {
            "name": "in_shard_transfer_insufficient_balance",
            "expect": "rejected",
            "comment": "the balance check covers value plus the whole gas budget",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": {"balances": {"QKC": "21000"}}},
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 100,
                "signer": "A",
            },
        },
        {
            "name": "xshard_deposit_to_empty_account",
            "expect": "success",
            "comment": "post-EVM deposit: the value is credited to the sender on "
            "this shard first, then moved by an EVM message (messages.py:368-377)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {},
            "gas_used_start": 9000,
            "deposit": {
                "tx_hash": "0x" + "11" * 32,
                "from": A,
                "to": B,
                "value": 1000,
                "gas_price": 1000000000,
                "gas_remained": 0,
            },
        },
        {
            "name": "native_token_transfer_with_default_gas",
            "expect": "success",
            "comment": "a foreign transfer token with the chain's own token for "
            "gas: no conversion, the fee stays in QKC and only the transfer "
            "moves QETH",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": {
                    "balances": {"QKC": "1000000000000000000", "QETH": "99999"}
                }
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1000000000,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 12345,
                "signer": "A",
                "transfer_token": "QETH",
            },
        },
        {
            "name": "native_token_transfer_sender_holds_none",
            "expect": "rejected",
            "comment": "validate_transaction (4): a token the sender holds none "
            "of is refused outright, where the chain's own token is spendable "
            "from empty",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 0,
                "gas_price": 1000000000,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 12345,
                "signer": "A",
                "transfer_token": "QETH",
            },
        },
        {
            "name": "native_token_as_gas_converted",
            "expect": "success",
            "comment": "gas paid in QETH: the manager is asked for a price, sells "
            "the genesis token that funds the frame and keeps the QETH, and the "
            "unused gas comes back at the rate it named — 80%, with the other "
            "20% burned to the zero address (messages.py:438-460, 268)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": {"balances": {"QETH": "10000000"}},
                GENERAL_NATIVE_TOKEN_ADDRESS + "00000001": {
                    "balances": {"QKC": "1000000000000000000"},
                    "code": MANAGER_RATE_80_PRICE_2,
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 3,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 12345,
                "signer": "A",
                "gas_token": "QETH",
                "transfer_token": "QETH",
            },
        },
        {
            "name": "native_token_as_gas_priced_at_zero",
            "expect": "rejected",
            "comment": "the manager answering zero is a refusal, not free gas "
            "(messages.py:243)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": {"balances": {"QETH": "10000000"}},
                GENERAL_NATIVE_TOKEN_ADDRESS + "00000001": {
                    "balances": {"QKC": "1000000000000000000"},
                    "code": MANAGER_RATE_100_PRICE_0,
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 3,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 12345,
                "signer": "A",
                "gas_token": "QETH",
                "transfer_token": "QETH",
            },
        },
        {
            "name": "native_token_as_gas_reserve_too_small",
            "expect": "rejected",
            "comment": "the manager has a price but not enough genesis token to "
            "settle the whole allowance at it (messages.py:253)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": {"balances": {"QETH": "10000000"}},
                GENERAL_NATIVE_TOKEN_ADDRESS + "00000001": {
                    "balances": {"QKC": "1000"},
                    "code": MANAGER_RATE_80_PRICE_2,
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 3,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 12345,
                "signer": "A",
                "gas_token": "QETH",
                "transfer_token": "QETH",
            },
        },
        {
            "name": "mnt_balance_precompile",
            "expect": "success",
            "comment": "the balance precompile reads any token for any address "
            "at a flat 400 gas (specials.py:365)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": {
                    "balances": {"QKC": "1000000000000000000", "QETH": "777"}
                }
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 100000,
                "to": BALANCE_MNT_ADDRESS,
                "value": 0,
                "signer": "A",
                # (address, token id) — the sender's own QETH balance.
                "data": "0x000000000000000000000000"
                + SENDER_A
                + "00000000000000000000000000000000000000000000000000000000001388f9",
            },
        },
        {
            "name": "mnt_mint_precompile_rejects_foreign_sender",
            "expect": "success",
            "comment": "minting is the auction contract's alone: called by anyone "
            "else the frame fails and its whole allowance is gone "
            "(specials.py:356)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 100000,
                "to": MINT_MNT_ADDRESS,
                "value": 0,
                "signer": "A",
                # (minter, token id, amount)
                "data": "0x000000000000000000000000"
                + SENDER_A
                + "00000000000000000000000000000000000000000000000000000000001388f9"
                + "00000000000000000000000000000000000000000000000000000000000003e8",
            },
        },
        {
            "name": "transfer_mnt_refusal_keeps_the_rest_of_the_gas",
            "expect": "success",
            "comment": "the transfer-token precompile refusing for want of "
            "balance costs only the call surcharge and hands the rest back "
            "(specials.py:264): the caller can still afford the store that "
            "follows, which it could not if the refusal had burned the frame",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                # The contract holds no QETH at all, so the transfer is refused
                # after the surcharge is charged.
                A + "00000001": {
                    "balances": {},
                    "code": transfer_mnt_runtime(B, 1280249, 10 ** 18, 60000, tail=_push(42) + _push(1) + "55" + "00"),
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 300000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "deploy_system_contract_returns_its_address",
            "expect": "success",
            "comment": "the deploy precompile answers with the address it "
            "deployed to, not with the code it deployed (messages.py:783), and "
            "a contract can read that off the return data",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {
                    "balances": {},
                    "code": deploy_system_contract_runtime(1),
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 2000000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "create_under_a_shard_key",
            "expect": "success",
            "comment": "a CREATE in an ordinary frame derives its address from "
            "rlp([sender, full_shard_key, nonce]) (messages.py:704). Paired "
            "with the case below, which reaches the same code without a key",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                B + "00000001": {"balances": {}, "code": CREATE_AND_RETURN_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 300000,
                "to": B,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "create_under_transfer_mnt_has_no_shard_key",
            "expect": "success",
            "comment": "the transfer-token precompile builds its child message "
            "without a shard key (specials.py:265), so the same code deploys to "
            "Ethereum's rlp([sender, nonce]) address instead — a different "
            "account and a different state root",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {
                    "balances": {},
                    "code": transfer_mnt_runtime(B, 35760, 0, 200000,
                                                 tail=return_memory_word(0x80)),
                },
                B + "00000001": {"balances": {}, "code": CREATE_AND_RETURN_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 500000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "contract_creation",
            "expect": "success",
            "comment": "a deployment: the address is "
            "keccak(rlp([sender, full_shard_key, nonce]))[12:], the top-level "
            "nonce does not move for the CREATE itself, and the receipt carries "
            "the address and its shard key (messages.py:704)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": "",
                "value": 0,
                "signer": "A",
                "data": ANSWER_INIT,
            },
        },
        {
            "name": "contract_call_returns_value",
            "expect": "success",
            "comment": "calling deployed code: the return data reaches the "
            "caller and the gas actually burned is what the receipt records",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": ANSWER_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "contract_revert",
            "expect": "success",
            "comment": "a reverting call is a failed transaction, not a rejected "
            "one: the state goes back, the gas is gone and the receipt says so",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": REVERT_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 1000,
                "signer": "A",
            },
        },
        {
            "name": "contract_out_of_gas",
            "expect": "success",
            "comment": "running out of gas costs the whole allowance and leaves "
            "the value where it started",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": INFINITE_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 50000,
                "to": A,
                "value": 1000,
                "signer": "A",
            },
        },
        {
            "name": "contract_selfdestruct",
            "expect": "success",
            "comment": "the suicide list is applied after the message, not at the "
            "opcode (messages.py:351): the balance moves to the beneficiary and "
            "del_account strips the account, which the end-of-block sweep then "
            "drops",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {
                    "balances": {"QKC": "5000"},
                    "code": selfdestruct_runtime(B),
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "contract_logs_and_bloom",
            "expect": "success",
            "comment": "one log with one topic: the receipt's bloom and the "
            "block's are built from it",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": LOG_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "contract_create2",
            "expect": "success",
            "comment": "CREATE2 from inside a frame: unlike a top-level CREATE, a "
            "nested one moves the creator's nonce",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": CREATE2_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "create2_word_gas_underflows",
            "expect": "rejected",
            "comment": "CREATE2's per-word charge is subtracted unchecked and the "
            "memory it is charged for needs no growing, so the frame carries a "
            "negative gas counter past the point anything would notice it; with "
            "CREATE2 last, the frame hands that counter back and apply_transaction "
            "trips its own `assert gas_remained >= 0` (messages.py:305). The "
            "transaction is not refused for a reason -- pyquarkchain cannot "
            "process it at all, so no block containing it exists",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": CREATE2_WORD_GAS_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                # 21000 intrinsic + 32500 for the frame: past CREATE2's 32000 but
                # 403 short of the word charge.
                "start_gas": 53500,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "create2_word_gas_covered",
            "expect": "success",
            "comment": "the same code with the word charge paid for, which is what "
            "makes the case above readable: same frame, 500 more gas, and the "
            "deployment goes through normally",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": CREATE2_WORD_GAS_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                # 21000 intrinsic + 33000 for the frame: 97 left after the charge.
                "start_gas": 54000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "create2_word_gas_oog_while_growing",
            "expect": "success",
            "comment": "the same shortfall with the memory still to grow: "
            "mem_extend checks before it spends, so both sides simply run out of "
            "gas and the transaction is processed with a failed receipt. This is "
            "the case that keeps the boundary above from swallowing an ordinary "
            "out-of-gas and abandoning a block that is fine",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": CREATE2_GROW_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                # 21000 intrinsic + 32500 for the frame, against 32894 needed.
                "start_gas": 53500,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "nested_call",
            "expect": "success",
            "comment": "a call inside a call: the inner frame's gas comes out of "
            "the outer one's and both are settled once, at the top",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": call_runtime(B)},
                B + "00000001": {"balances": {}, "code": ANSWER_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "eip155_transfer",
            "expect": "success",
            "comment": "a version 2 (EIP155) transaction, which devnet enables at "
            "genesis: the sender is recovered under Ethereum's rules and the "
            "shard keys must both be zero (messages.py:139-183)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 100,
                "signer": "A",
                "version": 2,
                "from_full_shard_key": 0,
                "to_full_shard_key": 0,
            },
        },
        {
            "name": "eip155_before_enable_timestamp",
            "expect": "rejected",
            "comment": "mainnet only enables the EIP155 signer in 2021; before "
            "that the version is refused whatever else it says",
            "network": "mainnet",
            "timestamp": 1569567600,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 100,
                "signer": "A",
                "version": 2,
                "from_full_shard_key": 0,
                "to_full_shard_key": 0,
            },
        },
        {
            "name": "eip155_non_default_token",
            "expect": "rejected",
            "comment": "the EIP155 signer has no field for a token, so both must "
            "be the chain's own",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": {
                    "balances": {"QKC": "1000000000000000000", "QETH": "99999"}
                }
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 100,
                "signer": "A",
                "version": 2,
                "from_full_shard_key": 0,
                "to_full_shard_key": 0,
                "transfer_token": "QETH",
            },
        },
        {
            "name": "eip155_non_zero_shard_key",
            "expect": "rejected",
            "comment": "a non-zero shard key is how a version 2 transaction would "
            "become cross-shard, which the signer does not support",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 30000,
                "to": SENDER_B,
                "value": 100,
                "signer": "A",
                "version": 2,
                "from_full_shard_key": 1,
                "to_full_shard_key": 1,
            },
        },
        {
            "name": "xshard_source_transfer",
            "expect": "success",
            "comment": "the source half of a cross-shard transfer: the value "
            "leaves, a deposit carries the whole remaining allowance to the "
            "target, and the surcharge is left out of both the fee and gas_used "
            "(messages.py:520-560)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 0,
                "gas_price": 2,
                "start_gas": 60000,
                "to": SENDER_B,
                "value": 1000,
                "signer": "A",
                "from_full_shard_key": 1,
                "to_full_shard_key": 0x10001,
            },
        },
        {
            "name": "xshard_source_contract_creation",
            "expect": "success",
            "comment": "a cross-shard deployment: the address is derived here, "
            "from the sender's nonce as it now stands, and the code is deployed "
            "on the target (messages.py:494)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {SENDER_A + "00000001": funded},
            "tx": {
                "nonce": 0,
                "gas_price": 2,
                "start_gas": 200000,
                "to": "",
                "value": 500,
                "signer": "A",
                "data": ANSWER_INIT,
                "from_full_shard_key": 1,
                "to_full_shard_key": 0x10001,
            },
        },
        {
            "name": "xshard_deposit_failed_message_leaves_funds_with_sender",
            "expect": "success",
            "comment": "the deposit credits the sender on this shard first and "
            "only then moves the value by a message; when that message fails the "
            "money stays with the sender here rather than going back "
            "(messages.py:368-377)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {A + "00000001": {"balances": {}, "code": REVERT_RUNTIME}},
            "gas_used_start": 9000,
            "deposit": {
                "tx_hash": "0x" + "22" * 32,
                "from": B,
                "to": A,
                "value": 1000,
                "gas_price": 1,
                "gas_remained": 100000,
            },
        },
        {
            "name": "mnt_balance_precompile_pads_short_calldata",
            "expect": "success",
            "comment": "the balance precompile reads its arguments with "
            "extract32 (vm.py:59), so calldata that stops after the address is "
            "not refused: the token id reads as zero and the call succeeds at "
            "the same flat 400 gas",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": {
                    "balances": {"QKC": "1000000000000000000", "QETH": "777"}
                }
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 100000,
                "to": BALANCE_MNT_ADDRESS,
                "value": 0,
                "signer": "A",
                # Only the address word; the token id is off the end.
                "data": "0x000000000000000000000000" + SENDER_A,
            },
        },
        {
            "name": "mnt_mint_precompile_pads_short_calldata",
            "expect": "success",
            "comment": "the mint precompile reads its amount with extract32 as "
            "well: 80 bytes of calldata leaves the amount word half off the end, "
            "and what is minted is that half padded with zeros, not a refusal",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                SENDER_B + "00000001": {"balances": {"QKC": "1000"}},
                NON_RESERVED_NATIVE_TOKEN_ADDRESS
                + "00000001": {
                    "balances": {},
                    # Only the auction contract may mint, so the call has to
                    # come from its address (specials.py:357).
                    "code": mint_mnt_runtime(
                        SENDER_B, 0x1388F9, 1 << 128, 0x50, 100000
                    ),
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": NON_RESERVED_NATIVE_TOKEN_ADDRESS,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "sstore_legacy_pricing",
            "expect": "success",
            "comment": "SSTORE is metered the pre-EIP-1283 way — 20000 for a "
            "clean zero to non-zero, 5000 otherwise, and a flat 15000 refund for "
            "clearing — including for a slot the same transaction writes twice",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A
                + "00000001": {
                    "balances": {},
                    "code": SSTORE_TIERS_RUNTIME,
                    "storage": {"0x1": "0x1"},
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "returndatacopy_within_the_answer",
            "expect": "success",
            "comment": "copying exactly as many bytes as the last call returned "
            "is in range (vm.py:542)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": returndatacopy_runtime(B, 32)},
                B + "00000001": {"balances": {}, "code": ANSWER_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "returndatacopy_past_the_answer",
            "expect": "success",
            "comment": "one byte past what the last call returned is an "
            "exception, and an exception costs the frame everything it had left",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": returndatacopy_runtime(B, 33)},
                B + "00000001": {"balances": {}, "code": ANSWER_RUNTIME},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "create_init_code_reverts",
            "expect": "success",
            "comment": "init code that reverts leaves no contract and pushes "
            "zero, and the creating frame keeps what the init code did not spend "
            "(messages.py:793)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {"balances": {}, "code": create_runtime(REVERT_RUNTIME)},
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 200000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "create_code_store_out_of_gas",
            "expect": "success",
            "comment": "paying for the deployed code is all or nothing: too "
            "little gas for the 200-per-byte charge reverts the deployment and "
            "takes the rest of the gas with it (messages.py:781)",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A
                + "00000001": {
                    "balances": {},
                    "code": create_runtime(returning_init(100)),
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 60000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
        {
            "name": "create_code_too_large",
            "expect": "success",
            "comment": "one byte over the 24576 limit fails the same way, "
            "whatever gas is left",
            "network": "devnet",
            "timestamp": 1,
            "pre_alloc": {
                SENDER_A + "00000001": funded,
                A
                + "00000001": {
                    "balances": {},
                    "code": create_runtime(returning_init(24577)),
                },
            },
            "tx": {
                "nonce": 0,
                "gas_price": 1,
                "start_gas": 8000000,
                "to": A,
                "value": 0,
                "signer": "A",
            },
        },
    ]


# ---------------------------------------------------------------------------
# block-level vectors


def make_shard_state(network_name, case):
    """A ShardState on its genesis, with the case's allocation as the shard's.

    The configuration is reloaded per case: the allocation is injected into the
    shard's GENESIS, so a shared config object would leak one case's accounts
    into the next. Proof-of-work and difficulty validation are switched off
    through the flags pyquarkchain itself exposes for that (DISABLE_POW_CHECK,
    SKIP_MINOR_DIFFICULTY_CHECK). Neither is read by run_block: they gate
    validate_block, which these vectors do not describe.
    """
    cluster_config = load_networks()[network_name]
    qkc_config = cluster_config.QUARKCHAIN
    qkc_config.DISABLE_POW_CHECK = True
    qkc_config.SKIP_MINOR_DIFFICULTY_CHECK = True

    full_shard_id = case["full_shard_id"]
    shard_config = qkc_config.shards[full_shard_id]
    # The case writes balances as decimal strings, the same as everywhere else
    # in these vectors; GenesisManager adds them to the account as they are.
    shard_config.GENESIS.ALLOC = {
        address: dict(
            entry,
            balances={token: int(value) for token, value in entry["balances"].items()},
        )
        for address, entry in case["genesis_alloc"].items()
    }

    env = Env(db=InMemoryDb())
    env.cluster_config = cluster_config
    state = ShardState(env=env, full_shard_id=full_shard_id)
    genesis_root_block = GenesisManager(qkc_config).create_root_block()
    state.init_genesis_state(genesis_root_block)
    return state, genesis_root_block


def remote_header(state, spec, prev_root_hash):
    """A minor block header from another shard, as the root chain carries it.

    Only the fields the cursor traversal reads are meaningful: the branch
    decides whether this shard is a neighbour of the sender, and
    hash_prev_root_block decides whether the sender's shard was allowed to send
    at that point (shard_state.py:172-186). The rest exists so the header
    hashes, and that hash is what the deposit list is keyed by.
    """
    return MinorBlockHeader(
        version=0,
        height=spec.get("height", 1),
        branch=Branch(spec["full_shard_id"]),
        coinbase_address=Address.create_empty_account(spec["full_shard_id"]),
        hash_prev_root_block=prev_root_hash,
        create_time=spec.get("create_time", 1),
        difficulty=spec.get("difficulty", 1),
        extra_data=spec.get("extra_data", "").encode(),
    )


def add_root_block(state, spec, dumped_root_blocks, dumped_xshard_lists):
    """Append one root block to the shard's view of the root chain.

    Remote deposit lists have to reach the shard before the root block that
    confirms them: add_root_block checks for each neighbour header that the
    list is already there (shard_state.py:1467).
    """
    root_block = state.root_tip.create_block_to_append()
    prev_root_hash = state.root_tip.get_hash()

    for header_spec in spec.get("minor_headers", []):
        if header_spec.get("local_tip"):
            root_block.add_minor_block_header(state.header_tip)
            continue
        header = remote_header(state, header_spec, prev_root_hash)
        deposits = [build_deposit(d) for d in header_spec.get("deposits", [])]
        state.add_cross_shard_tx_list_by_minor_block_hash(
            h=header.get_hash(), tx_list=CrossShardTransactionList(tx_list=deposits)
        )
        dumped_xshard_lists.append(
            {
                "minor_block_hash": _hex(header.get_hash()),
                "full_shard_id": header_spec["full_shard_id"],
                "deposits": [dump_deposit(d) for d in deposits],
            }
        )
        root_block.add_minor_block_header(header)

    coinbase = spec.get("coinbase")
    if coinbase is None:
        root_block.finalize()
    else:
        root_block.finalize(
            coinbase_tokens={
                token_id_encode(token): int(value)
                for token, value in coinbase.get("tokens", {}).items()
            },
            coinbase_address=_addr(coinbase["address"], coinbase["full_shard_key"]),
        )

    state.add_root_block(root_block)
    dumped_root_blocks.append(
        {
            "height": root_block.header.height,
            "hash": _hex(root_block.header.get_hash()),
            "block": _hex(root_block.serialize()),
        }
    )
    return root_block


def build_minor_block(state, spec, qkc_config):
    """Assemble the block finalize_and_add_block would, without running it yet.

    create_block_to_mine is deliberately not used: it stamps wall-clock tracking
    data into the block, picks up whatever the transaction queue holds, and runs
    the block a second time. Building the block here keeps the vector's input
    exactly what the case describes, including inputs a miner would never pick.
    """
    create_time = spec["timestamp"]
    coinbase = _addr(spec.get("coinbase", C), spec.get("coinbase_full_shard_key", state.full_shard_id))
    block = state.get_tip().create_block_to_append(
        create_time=create_time,
        address=coinbase,
        difficulty=state.get_next_block_difficulty(create_time),
    )
    block.header.hash_prev_root_block = state.root_tip.get_hash()
    gas_limit, xshard_gas_limit = state.get_gas_limit_all(
        spec.get("gas_limit"), spec.get("xshard_gas_limit")
    )
    block.header.evm_gas_limit = gas_limit
    block.meta.evm_xshard_gas_limit = xshard_gas_limit
    for tx_spec in spec.get("txs", []):
        block.add_tx(TypedTransaction(SerializedEvmTransaction.from_evm_tx(build_tx(tx_spec, qkc_config))))
    return block


def dump_cursor(info):
    return {
        "root_block_height": info.root_block_height,
        "minor_block_index": info.minor_block_index,
        "xshard_deposit_index": info.xshard_deposit_index,
    }


def build_block_case(case):
    network_name = case["network"]
    state, genesis_root_block = make_shard_state(network_name, case)
    qkc_config = state.env.quark_chain_config

    genesis_block = state.db.get_minor_block_by_height(0)
    recipients = {a[:40] for a in case["genesis_alloc"]}

    # The genesis root block is where every cursor starts: the shard's genesis
    # meta names its height, and the traversal's first deposit is that block's
    # own coinbase deposit.
    root_blocks = [
        {
            "height": genesis_root_block.header.height,
            "hash": _hex(genesis_root_block.header.get_hash()),
            "block": _hex(genesis_root_block.serialize()),
        }
    ]
    xshard_lists = []
    blocks = []

    for step in case["blocks"]:
        for root_spec in step.get("root_blocks", []):
            add_root_block(state, root_spec, root_blocks, xshard_lists)

        block = build_minor_block(state, step, qkc_config)
        recipients.add(block.header.coinbase_address.recipient.hex())
        for tx in block.tx_list:
            evm_tx = tx.tx.to_evm_tx()
            recipients.add(evm_tx.sender.hex())
            if evm_tx.to:
                recipients.add(evm_tx.to.hex())
        for entry in xshard_lists:
            for deposit in entry["deposits"]:
                recipients.add(deposit["from"][2:])
                recipients.add(deposit["to"][2:])

        expect = step.get("expect", "success")
        if expect not in ("success", "rejected"):
            raise SystemExit("case {}: expect must be success or rejected".format(case["name"]))

        consumed = []
        if expect == "rejected":
            try:
                state.run_block(block, consumed)
            except Exception as exc:  # noqa: BLE001 - the rejection is the vector
                blocks.append(
                    {
                        "comment": step.get("comment", ""),
                        "expect": "rejected",
                        "block": _hex(block.serialize()),
                        "result": {
                            "rejected": True,
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        },
                    }
                )
                break
            raise SystemExit(
                "case {}: expected a rejection, the block ran".format(case["name"])
            )

        evm_state = state.run_block(block, consumed)
        coinbase_amount_map = state.get_coinbase_amount_map(block.header.height)
        coinbase_amount_map.add(evm_state.block_fee_tokens)
        block.finalize(evm_state=evm_state, coinbase_amount_map=coinbase_amount_map)
        state.add_block(
            block,
            gas_limit=step.get("gas_limit"),
            xshard_gas_limit=step.get("xshard_gas_limit"),
            validate_time=False,
        )

        blocks.append(
            {
                "comment": step.get("comment", ""),
                "expect": "success",
                "block": _hex(block.serialize()),
                "result": {
                    "post_state_root": _hex(block.meta.hash_evm_state_root),
                    "receipt_root": _hex(block.meta.hash_evm_receipt_root),
                    "gas_used": str(block.meta.evm_gas_used),
                    "xshard_receive_gas_used": str(
                        block.meta.evm_cross_shard_receive_gas_used
                    ),
                    "cursor": dump_cursor(block.meta.xshard_tx_cursor_info),
                    "coinbase_amount_map": {
                        str(token): str(value)
                        for token, value in sorted(
                            block.header.coinbase_amount_map.balance_map.items()
                        )
                    },
                    "bloom": "0x{:0512x}".format(block.header.bloom),
                    "receipts": [dump_receipt(r) for r in evm_state.receipts],
                    "xshard_deposit_receipts": [
                        dump_receipt(r) for r in evm_state.xshard_deposit_receipts
                    ],
                    "produced_deposits": [dump_deposit(d) for d in evm_state.xshard_list],
                    "consumed_deposits": [dump_deposit(d) for d in consumed],
                    "accounts": observe(evm_state, recipients, {}),
                },
            }
        )

    return {
        "name": case["name"],
        "comment": case["comment"],
        "network": network_name,
        "full_shard_id": case["full_shard_id"],
        "genesis_alloc": case["genesis_alloc"],
        # The genesis block is the first block's parent: its meta carries the
        # cursor the first traversal resumes from, and its state root is what a
        # consumer must reach by applying the allocation on its own.
        "genesis": {
            "state_root": _hex(genesis_block.meta.hash_evm_state_root),
            "block": _hex(genesis_block.serialize()),
        },
        "root_blocks": root_blocks,
        "xshard_lists": xshard_lists,
        "blocks": blocks,
    }


def block_cases():
    """Block-level cases: whole minor blocks run through ShardState.run_block."""
    funded = {"balances": {"QKC": "1000000000000000000"}}
    devnet_alloc = {SENDER_A + "00000001": funded, SENDER_B + "00000001": funded}
    # devnet chain 0 shard 0 is under test; chain 1 shard 0 is the neighbour the
    # deposits arrive from. Both chains have SHARD_SIZE 1, so the shard id is
    # (chain << 16) | 1.
    local, remote = 0x00000001, 0x00010001
    genesis_time = 1556639999

    def transfer(nonce, value, signer="A", to=None, gas=21000, gas_price=1):
        return {
            "nonce": nonce,
            "gas_price": gas_price,
            "start_gas": gas,
            "to": to or (SENDER_B if signer == "A" else SENDER_A),
            "value": value,
            "signer": signer,
            "from_full_shard_key": 1,
            "to_full_shard_key": 1,
        }

    def deposit(tag, value, gas_remained=0, gas_price=1, to=None):
        return {
            "tx_hash": "0x" + tag * 32,
            "from": SENDER_B,
            "from_full_shard_key": remote,
            "to": to or SENDER_A,
            "to_full_shard_key": local,
            "value": str(value),
            "gas_price": str(gas_price),
            "gas_remained": str(gas_remained),
        }

    return [
        {
            "name": "devnet_chain_of_three_blocks",
            "comment": "three chained blocks of in-shard transfers: each block's "
            "state root is the next one's parent, the coinbase collects the "
            "reward plus that block's fees, and the sender's nonce advances "
            "across blocks",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": devnet_alloc,
            "blocks": [
                {
                    "comment": "one transfer, paid for at gas price 1",
                    "timestamp": genesis_time + 10,
                    "txs": [transfer(0, "1000")],
                },
                {
                    "comment": "two transfers from the same sender, in nonce order",
                    "timestamp": genesis_time + 20,
                    "txs": [transfer(1, "2000"), transfer(2, "3000")],
                },
                {
                    "comment": "an empty block still pays the coinbase reward",
                    "timestamp": genesis_time + 30,
                    "txs": [],
                },
            ],
        },
        {
            "name": "devnet_xshard_deposits_received",
            "comment": "a root block confirming a neighbour's minor block hands "
            "this shard two deposits; the next block consumes both and the "
            "cursor lands past them",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": devnet_alloc,
            "blocks": [
                {
                    "comment": "root block 1 exists only so the neighbour's header "
                    "can point at a root height above this shard's genesis root "
                    "height, which is what gives it permission to send",
                    "timestamp": genesis_time + 10,
                    "root_blocks": [{"minor_headers": [{"local_tip": True}]}],
                    "txs": [],
                },
                {
                    "comment": "root block 2 confirms the neighbour's block, so its "
                    "deposits are what this block receives before any transaction",
                    "timestamp": genesis_time + 20,
                    "root_blocks": [
                        {
                            "minor_headers": [
                                {
                                    "full_shard_id": remote,
                                    "height": 1,
                                    "deposits": [
                                        deposit("11", 500),
                                        deposit("22", 700),
                                    ],
                                }
                            ]
                        }
                    ],
                    "txs": [],
                },
            ],
        },
        {
            "name": "devnet_xshard_cursor_splits_across_blocks",
            "comment": "the cross-shard allowance is one deposit wide, so a root "
            "block carrying three of them is consumed over three blocks: each "
            "block stops on the soft limit and the next resumes from the cursor "
            "the previous one published (shard_state.py:1637)",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": devnet_alloc,
            "blocks": [
                {
                    "timestamp": genesis_time + 10,
                    "root_blocks": [{"minor_headers": [{"local_tip": True}]}],
                    "txs": [],
                },
                {
                    "comment": "first slice: one deposit, cursor stops inside the "
                    "neighbour's list",
                    "timestamp": genesis_time + 20,
                    "xshard_gas_limit": 9000,
                    "root_blocks": [
                        {
                            "minor_headers": [
                                {
                                    "full_shard_id": remote,
                                    "height": 1,
                                    "deposits": [
                                        deposit("11", 100),
                                        deposit("22", 200),
                                        deposit("33", 300),
                                    ],
                                }
                            ]
                        }
                    ],
                    "txs": [],
                },
                {
                    "comment": "second slice, resumed from the published cursor",
                    "timestamp": genesis_time + 30,
                    "xshard_gas_limit": 9000,
                    "txs": [],
                },
                {
                    "comment": "third slice takes the last deposit; the soft limit "
                    "breaks the loop on it, so the cursor stops on the deposit "
                    "rather than past the root block",
                    "timestamp": genesis_time + 40,
                    "xshard_gas_limit": 9000,
                    "txs": [],
                },
            ],
        },
        {
            "name": "devnet_root_chain_coinbase_deposit",
            "comment": "every root block the cursor passes produces a deposit for "
            "its own coinbase address, credited only when that address is in "
            "this shard (shard_state.py:110-137). The first root block's "
            "coinbase is in this shard, the second's is in the neighbour, and "
            "the second still produces a zero-value deposit",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": devnet_alloc,
            "blocks": [
                {
                    "timestamp": genesis_time + 10,
                    "root_blocks": [
                        {
                            "minor_headers": [{"local_tip": True}],
                            "coinbase": {
                                "address": A,
                                "full_shard_key": local,
                                "tokens": {"QKC": "120000"},
                            },
                        }
                    ],
                    "txs": [],
                },
                {
                    "timestamp": genesis_time + 20,
                    "root_blocks": [
                        {
                            "minor_headers": [],
                            "coinbase": {
                                "address": A,
                                "full_shard_key": remote,
                                "tokens": {"QKC": "120000"},
                            },
                        }
                    ],
                    "txs": [],
                },
            ],
        },
        {
            "name": "devnet_xshard_before_transactions",
            "comment": "a block with both halves: the deposits are credited before "
            "the transactions run, which is what lets the sender spend what it "
            "only just received in the same block",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": {SENDER_B + "00000001": funded},
            "blocks": [
                {
                    "timestamp": genesis_time + 10,
                    "root_blocks": [{"minor_headers": [{"local_tip": True}]}],
                    "txs": [],
                },
                {
                    "comment": "A holds nothing at the start of the block and pays "
                    "for this transfer out of the deposit",
                    "timestamp": genesis_time + 20,
                    "root_blocks": [
                        {
                            "minor_headers": [
                                {
                                    "full_shard_id": remote,
                                    "height": 1,
                                    "deposits": [deposit("11", 10000000)],
                                }
                            ]
                        }
                    ],
                    "txs": [transfer(0, "1000")],
                },
            ],
        },
        {
            "name": "devnet_native_token_gas_real_manager",
            "comment": "the real general native token manager, driven through its "
            "own interface: register QETH, propose an exchange rate of 1/30000 "
            "with a genesis-token deposit, set the refund rate to 60, then send "
            "a transaction that pays its gas in QETH at 60000 — which the "
            "contract prices at 2 (test_shard_state.py:3369)",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": {
                SENDER_A + "00000001": {
                    "balances": {"QKC": "1000000000000000000", "QETH": "10000000000"}
                },
                GENERAL_NATIVE_TOKEN_ADDRESS + "00000001": {
                    "balances": {},
                    "code": general_native_token_runtime(),
                    "storage": {
                        # caller, supervisor, minimum reserve to maintain,
                        # minimum reserve to start with.
                        "0x00": "0x" + GENERAL_NATIVE_TOKEN_ADDRESS.rjust(64, "0"),
                        "0x01": "0x" + SENDER_A.rjust(64, "0"),
                        "0x03": "0x" + word(30000),
                        "0x04": "0x" + word(1),
                    },
                },
            },
            "blocks": [
                {
                    "comment": "the three admin calls, in the order the contract "
                    "requires: nothing can be proposed for an unregistered token",
                    "timestamp": genesis_time + 10,
                    "txs": [
                        {
                            "nonce": 0,
                            "gas_price": 0,
                            "start_gas": 1000000,
                            "to": GENERAL_NATIVE_TOKEN_ADDRESS,
                            "value": "1",
                            "signer": "A",
                            "transfer_token": "QETH",
                            "data": "0xbf03314a",
                            "from_full_shard_key": 1,
                            "to_full_shard_key": 1,
                        },
                        {
                            "nonce": 1,
                            "gas_price": 0,
                            "start_gas": 1000000,
                            "to": GENERAL_NATIVE_TOKEN_ADDRESS,
                            "value": "100000",
                            "signer": "A",
                            "data": "0x735e0e19"
                            + word(1280249)
                            + word(1)
                            + word(30000),
                            "from_full_shard_key": 1,
                            "to_full_shard_key": 1,
                        },
                        {
                            "nonce": 2,
                            "gas_price": 0,
                            "start_gas": 1000000,
                            "to": GENERAL_NATIVE_TOKEN_ADDRESS,
                            "value": "0",
                            "signer": "A",
                            "data": "0x6d27af8c" + word(1280249) + word(60),
                            "from_full_shard_key": 1,
                            "to_full_shard_key": 1,
                        },
                    ],
                },
                {
                    "comment": "gas paid in QETH, priced by the contract: 60000 "
                    "QETH per gas becomes 2 QKC per gas, and 60% of the unused "
                    "gas comes back",
                    "timestamp": genesis_time + 20,
                    "txs": [
                        {
                            "nonce": 3,
                            "gas_price": 60000,
                            "start_gas": 30000,
                            "to": SENDER_B,
                            "value": "12345",
                            "signer": "A",
                            "gas_token": "QETH",
                            "transfer_token": "QETH",
                            "from_full_shard_key": 1,
                            "to_full_shard_key": 1,
                        }
                    ],
                },
            ],
        },
        {
            "name": "devnet_native_token_gas_block",
            "comment": "a whole block whose transaction pays gas in QETH: the "
            "manager's sale, the refund at the rate it named and the burn of the "
            "remainder all land in the block's state root, while the coinbase "
            "map stays in the genesis token",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": {
                SENDER_A + "00000001": {"balances": {"QETH": "10000000"}},
                GENERAL_NATIVE_TOKEN_ADDRESS + "00000001": {
                    "balances": {"QKC": "1000000000000000000"},
                    "code": MANAGER_RATE_80_PRICE_2,
                },
            },
            "blocks": [
                {
                    "timestamp": genesis_time + 10,
                    "txs": [
                        {
                            "nonce": 0,
                            "gas_price": 3,
                            "start_gas": 30000,
                            "to": SENDER_B,
                            "value": "12345",
                            "signer": "A",
                            "gas_token": "QETH",
                            "transfer_token": "QETH",
                            "from_full_shard_key": 1,
                            "to_full_shard_key": 1,
                        }
                    ],
                },
            ],
        },
        {
            "name": "devnet_selfdestruct_coinbase_resurrected_by_reward",
            "comment": "SELFDESTRUCT does not delete the account: del_account "
            "strips it once the message is over (messages.py:351), and the "
            "end-of-block sweep decides whether the leaf survives. Here the "
            "stripped account is the block's own coinbase, so the reward paid "
            "at the end of run_block brings it back — Ethereum's unconditional "
            "delete would drop it together with the reward",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {
                    "balances": {"QKC": "5000"},
                    "code": selfdestruct_runtime(B),
                },
            },
            "blocks": [
                {
                    "timestamp": genesis_time + 10,
                    "coinbase": A,
                    "txs": [
                        {
                            "nonce": 0,
                            "gas_price": 1,
                            "start_gas": 200000,
                            "to": A,
                            "value": "0",
                            "signer": "A",
                            "from_full_shard_key": 1,
                            "to_full_shard_key": 1,
                        }
                    ],
                },
            ],
        },
        {
            "name": "devnet_selfdestruct_then_paid_again",
            "comment": "the same rule inside one block: the account is stripped "
            "by the transaction that destroys it and brought back by the next "
            "transaction that pays it",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": {
                SENDER_A + "00000001": funded,
                A + "00000001": {
                    "balances": {"QKC": "5000"},
                    "code": selfdestruct_runtime(B),
                },
            },
            "blocks": [
                {
                    "timestamp": genesis_time + 10,
                    "txs": [
                        {
                            "nonce": 0,
                            "gas_price": 1,
                            "start_gas": 200000,
                            "to": A,
                            "value": "0",
                            "signer": "A",
                            "from_full_shard_key": 1,
                            "to_full_shard_key": 1,
                        },
                        {
                            "nonce": 1,
                            "gas_price": 1,
                            "start_gas": 200000,
                            "to": A,
                            "value": "777",
                            "signer": "A",
                            "from_full_shard_key": 1,
                            "to_full_shard_key": 1,
                        },
                    ],
                },
            ],
        },
        {
            "name": "devnet_block_rejected_bad_nonce",
            "comment": "a block carrying a transaction whose nonce is far ahead is "
            "refused whole: run_block re-validates every transaction and raises "
            "on the first failure, so nothing the block did is published",
            "network": "devnet",
            "full_shard_id": local,
            "genesis_alloc": devnet_alloc,
            "blocks": [
                {
                    "timestamp": genesis_time + 10,
                    "expect": "rejected",
                    "txs": [transfer(99, "1000")],
                }
            ],
        },
        {
            "name": "mainnet_xshard_across_evm_switch",
            "comment": "the same deposit shape on both sides of "
            "ENABLE_EVM_TIMESTAMP: before it the value is handed straight to the "
            "recipient with no receipt, at it the deposit runs as an EVM message "
            "and produces one (shard_state.py:1591, messages.py:368)",
            "network": "mainnet",
            "full_shard_id": local,
            "genesis_alloc": devnet_alloc,
            "blocks": [
                {
                    "timestamp": 1569567590,
                    "root_blocks": [{"minor_headers": [{"local_tip": True}]}],
                    "txs": [],
                },
                {
                    "comment": "pre-EVM: one second before the switch",
                    "timestamp": 1569567599,
                    "root_blocks": [
                        {
                            "minor_headers": [
                                {
                                    "full_shard_id": remote,
                                    "height": 1,
                                    "deposits": [deposit("11", 500)],
                                }
                            ]
                        }
                    ],
                    "txs": [],
                },
                {
                    "comment": "post-EVM: the switch is a >= on the block timestamp",
                    "timestamp": 1569567600,
                    "root_blocks": [
                        {
                            "minor_headers": [
                                {
                                    "full_shard_id": remote,
                                    "height": 2,
                                    "deposits": [deposit("22", 500)],
                                }
                            ]
                        }
                    ],
                    "txs": [],
                },
            ],
        },
    ]


# ---------------------------------------------------------------------------
# entry point


def check_genesis_selfcheck(state_vectors):
    """The genesis cases must land on the pinned minor genesis state roots."""
    with open(os.path.join(_TESTDATA_DIR, "minor_genesis_golden.json")) as f:
        pinned = {n["name"]: n for n in json.load(f)["networks"]}
    for vector in state_vectors:
        if not vector["name"].startswith("genesis_alloc_"):
            continue
        network = vector["name"][len("genesis_alloc_") :]
        want = pinned[network]["state_root"]
        got = vector["post_state_root"]
        if got != want:
            raise SystemExit(
                "genesis self-check failed for {}: state root {} != pinned {}".format(
                    network, got, want
                )
            )


def write(filename, comment, source, vectors):
    path = os.path.join(_OUT_DIR, filename)
    with open(path, "w") as f:
        json.dump(
            {"_comment": comment, "source": source, "cases": vectors},
            f,
            indent=2,
            sort_keys=False,
        )
        f.write("\n")
    print("wrote {} ({} cases)".format(path, len(vectors)))


def main():
    os.makedirs(_OUT_DIR, exist_ok=True)
    source = oracle_provenance()
    networks = load_networks()

    state_vectors = [build_state_case(networks, c) for c in state_cases(networks)]
    check_genesis_selfcheck(state_vectors)
    write(
        "state_level.json",
        [
            "Direct EvmState mutations and the state root they commit to, generated by",
            "qkc/testdata/gen_exec_golden.py against pyquarkchain. The genesis_alloc_*",
            "cases are the generator's self-check against minor_genesis_golden.json.",
            "Allocations are committed before a case's operations run, so the",
            "operations start from a state that has been through the trie.",
        ],
        source,
        state_vectors,
    )

    message_vectors = [build_message_case(networks, c) for c in message_cases()]
    write(
        "message_level.json",
        [
            "Single transactions and cross-shard deposits driven through",
            "apply_transaction / apply_xshard_deposit, generated by",
            "qkc/testdata/gen_exec_golden.py against pyquarkchain. Each case",
            "declares whether it pins a successful execution or a rejection.",
        ],
        source,
        message_vectors,
    )

    block_vectors = [build_block_case(c) for c in block_cases()]
    write(
        "block_level.json",
        [
            "Whole minor blocks run through ShardState.run_block, generated by",
            "qkc/testdata/gen_exec_golden.py against pyquarkchain. A case is a",
            "genesis allocation, a root chain built up alongside the shard, and",
            "the blocks in order; each block carries the serialized block itself",
            "and every consensus output running it produced. The allocation is",
            "the shard's GENESIS.ALLOC, so a consumer reaches the genesis state",
            "root by applying it and nothing else.",
        ],
        source,
        block_vectors,
    )


async def run():
    """Generate inside an event loop.

    Nothing here is asynchronous, but ShardState.add_block announces new heads
    and logs through asyncio.create_task (shard_state.py:286), which needs a
    running loop. Yielding once at the end lets those announcements run against
    the empty subscription manager instead of being destroyed pending.
    """
    main()
    await asyncio.sleep(0)


if __name__ == "__main__":
    asyncio.run(run())
