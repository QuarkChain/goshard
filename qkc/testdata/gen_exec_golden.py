#!/usr/bin/env python3
"""Generate execution golden vectors by driving pyquarkchain's own EvmState.

The vectors pin what goshard's execution layer must reproduce byte for byte:
a pre-allocation, a sequence of inputs, and the resulting state root plus every
consensus-visible side effect. Two granularities are emitted today:

  state    direct EvmState mutations (balances, nonce, code, storage, deletion)
           -> post state root. Covers the account and storage encoding rules.
  message  a signed transaction or a cross-shard deposit run through
           apply_transaction / apply_xshard_deposit -> post state root,
           receipts, gas counters, produced deposits, coinbase fees.

Block-level vectors (ShardState.run_block) need a root block body on the Go
side and are added once qkc/types carries one.

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
try:
    from quarkchain.cluster.cluster_config import ClusterConfig
    from quarkchain.core import Address, CrossShardTransactionDeposit
    from quarkchain.db import InMemoryDb
    from quarkchain.env import Env
    from quarkchain.evm.messages import apply_transaction, apply_xshard_deposit
    from quarkchain.evm.state import State as EvmState
    from quarkchain.evm.transactions import Transaction as EvmTransaction
    from quarkchain.evm.utils import privtoaddr
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
        network_id=qkc_config.NETWORK_ID,
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


if __name__ == "__main__":
    main()
