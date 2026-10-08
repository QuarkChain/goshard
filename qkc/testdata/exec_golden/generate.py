#!/usr/bin/env python3
"""Generate the MNT execution vectors from the pinned pyquarkchain oracle.

The feature in this directory stops at goshard's ``core.Message`` boundary, so
these vectors intentionally normalize pyquarkchain transactions into the
fields consumed by ``core.ApplyMessage``. Transaction signatures and receipts
are oracle inputs/outputs, not part of the Go feature boundary.

Usage:

    PYQUARKCHAIN=/path/to/pyquarkchain python qkc/testdata/exec_golden/generate.py

The checkout must be exactly the pinned commit. An exported tree without
``.git`` is accepted only when every decision-bearing module has its pinned
SHA-256 digest; this supports release archives without weakening provenance.
"""

import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys


PINNED_COMMIT = "ce274f3dea984da64dc66ff253ee6696a3f4b5fb"
PINNED_MODULES = {
    "quarkchain/evm/messages.py": "7d6cb97202b0e5e7bbe69abb210304708e4c15a87f02b6a3fce4da96b0b676fd",
    "quarkchain/evm/state.py": "de5216e6c10e606e00df124b8c3be46e29f5ea66509a94c03c457a5ecec6545d",
    "quarkchain/evm/transactions.py": "573f578989e912a1a6f2426faeb69c17ade3eaa47db7f221743fc7bf7c9e522d",
    "quarkchain/evm/specials.py": "c14638312c952401fbbad5a280bf14f17ed74bec491163d5ba26f3e5206bb46d",
    "quarkchain/evm/vm.py": "f6df931df9130d696c843361d01844b26703481d34d2032a64c5959a8572e96f",
    "quarkchain/evm/opcodes.py": "40d1eaab9bb6612adea1196e025f32c88d98a18471373ca34903c888729e1af8",
    "quarkchain/config.py": "cda1b5ca536bb8af61b09621a4f44315e9696b9d90a5aed75c352cac9fe65fe9",
    "quarkchain/constants.py": "d9007dc130ca7a1543618d24a90d285c4cdd1aaa2f33046e34c5c989dbb994f6",
}

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
PYQUARKCHAIN = Path(os.environ.get("PYQUARKCHAIN", os.getcwd())).resolve()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_oracle():
    actual = {}
    for relative, expected in PINNED_MODULES.items():
        path = PYQUARKCHAIN / relative
        if not path.is_file():
            raise SystemExit("missing pinned oracle module: {}".format(path))
        actual[relative] = digest(path)
        if actual[relative] != expected:
            raise SystemExit(
                "oracle module differs from {}: {}".format(PINNED_COMMIT, relative)
            )

    if (PYQUARKCHAIN / ".git").exists():
        try:
            head = subprocess.check_output(
                (
                    "git",
                    "-c",
                    "safe.directory={}".format(PYQUARKCHAIN),
                    "-C",
                    str(PYQUARKCHAIN),
                    "rev-parse",
                    "HEAD",
                ),
                text=True,
            ).strip()
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            raise SystemExit("cannot identify pyquarkchain checkout: {}".format(exc))
        if head != PINNED_COMMIT:
            raise SystemExit(
                "pyquarkchain HEAD must be {}, got {}".format(PINNED_COMMIT, head)
            )
    return {
        "pyquarkchain_commit": PINNED_COMMIT,
        "module_sha256": actual,
    }


SOURCE = verify_oracle()
sys.path.insert(0, str(PYQUARKCHAIN))

# ClusterConfig resolves the host name while importing. No cluster is started
# by this generator, so make that unrelated lookup deterministic.
socket.gethostbyname = lambda _host: "127.0.0.1"

from quarkchain.cluster.cluster_config import ClusterConfig  # noqa: E402
from quarkchain.constants import (  # noqa: E402
    GENERAL_NATIVE_TOKEN_CONTRACT_BYTECODE,
    NON_RESERVED_NATIVE_TOKEN_CONTRACT_BYTECODE,
)
from quarkchain.core import Address  # noqa: E402
from quarkchain.db import InMemoryDb  # noqa: E402
from quarkchain.env import Env  # noqa: E402
from quarkchain.evm.messages import (  # noqa: E402
    VMExt,
    apply_msg,
    apply_transaction,
    mk_contract_address,
    mk_contract_address2,
)
from quarkchain.evm.specials import proc_current_mnt_id  # noqa: E402
from quarkchain.evm.state import State  # noqa: E402
from quarkchain.evm.transactions import Transaction  # noqa: E402
from quarkchain.evm.utils import privtoaddr  # noqa: E402
from quarkchain.evm.vm import Message  # noqa: E402
from quarkchain.utils import Logger, token_id_encode  # noqa: E402


Logger.set_logging_level("warning")

KEY = bytes.fromhex("1" * 64)
SENDER = privtoaddr(KEY)
RECIPIENT = bytes.fromhex("22" * 20)
TARGET = bytes.fromhex("33" * 20)
CONTRACT_A = bytes.fromhex("aa" * 20)
CONTRACT_B = bytes.fromhex("bb" * 20)
COINBASE = bytes.fromhex("cc" * 20)
GENERAL_MANAGER = bytes.fromhex("514b430000000000000000000000000000000003")
QKC = token_id_encode("QKC")
QETH = token_id_encode("QETH")

RETURN_42 = "0x602a60005260206000f3"
REVERT = "0x60006000fd"
STOP = "0x00"
INFINITE = "0x5b600056"
CLEAR_SLOT_ZERO = "0x600060005500"
CREATE_AND_RETURN = "0x600060006000f060005260206000f3"
TRANSFER_MNT_CREATE_AND_RETURN = "0x73bbbb0000000000000000000000000000000000bb600052618bb060205260006040526020608060606000600073000000000000000000000000000000514b43000262030d40f160005560206080f3"
MANAGER_RATE_80_PRICE_2 = "0x6000546001016000556050600052600260205260406000f3"
MANAGER_RATE_100_PRICE_0 = "0x6000546001016000556064600052600060205260406000f3"
LOG_ONE = "0x602a600052600160206000a100"
CREATE2_AND_RETURN = "0x6009600060006000f560005260206000f3"


def hex_bytes(value):
    return "0x" + bytes(value).hex()


def load_devnet():
    with (REPO / "qkc/config/singularity/devnet.json").open() as stream:
        return ClusterConfig.from_dict(json.load(stream))


def new_state(cluster_config, timestamp, gas_limit):
    env = Env(db=InMemoryDb())
    env.cluster_config = cluster_config
    qkc_config = cluster_config.QUARKCHAIN
    state = State(env=env.evm_env, db=env.db, qkc_config=qkc_config)
    state.shard_config = qkc_config.shards[1]
    state.timestamp = timestamp
    state.gas_limit = gas_limit
    state.block_number = 1
    state.block_coinbase = COINBASE
    state.block_difficulty = 1
    return state


def apply_alloc(state, alloc):
    for address_hex, entry in alloc.items():
        address = Address.create_from(bytes.fromhex(address_hex))
        state.full_shard_key = address.full_shard_key
        recipient = address.recipient
        if entry.get("code") is not None:
            state.set_code(recipient, bytes.fromhex(entry["code"][2:]))
            state.set_nonce(recipient, 1)
        for slot, value in entry.get("storage", {}).items():
            state.set_storage_data(recipient, int(slot, 0), int(value, 0))
        for token, value in entry.get("balances", {}).items():
            state.delta_token_balance(recipient, token_id_encode(token), int(value))


def make_transaction(spec, qkc_config):
    tx = Transaction(
        nonce=spec.get("nonce", 0),
        gasprice=spec.get("gas_price", 0),
        startgas=spec.get("gas_limit", 21_000),
        to=bytes.fromhex(spec.get("to", "")),
        value=int(spec.get("value", 0)),
        data=bytes.fromhex(spec.get("data", "0x")[2:]),
        gas_token_id=token_id_encode(spec.get("gas_token", "QKC")),
        transfer_token_id=token_id_encode(spec.get("transfer_token", "QKC")),
        from_full_shard_key=1,
        to_full_shard_key=spec.get("to_full_shard_key", 1),
        network_id=qkc_config.NETWORK_ID,
        version=0,
    )
    tx.set_quark_chain_config(qkc_config)
    tx.sign(KEY)
    return tx


def normalize_alloc(alloc):
    result = []
    for address_hex, entry in sorted(alloc.items()):
        result.append(
            {
                "address": "0x" + address_hex[:40],
                "full_shard_key": int(address_hex[40:], 16),
                "balances": {
                    str(token_id_encode(token)): str(value)
                    for token, value in sorted(entry.get("balances", {}).items())
                },
                "code": entry.get("code", "0x"),
                "storage": entry.get("storage", {}),
            }
        )
    return result


def dump_log(log):
    return {
        "address": hex_bytes(log.address),
        "topics": ["0x{:064x}".format(topic) for topic in log.topics],
        "data": hex_bytes(log.data),
    }


def observe_account(state, address, tokens, slots):
    account = state.get_and_cache_account(address, should_cache=False)
    return {
        "address": hex_bytes(address),
        "exists": not account.is_blank(),
        "nonce": account.nonce,
        "code": hex_bytes(state.get_code(address)),
        "full_shard_key": account.full_shard_key,
        "balances": {
            str(token): str(state.get_balance(address, token)) for token in sorted(tokens)
        },
        "storage": {
            "0x{:064x}".format(slot): "0x{:064x}".format(
                state.get_storage_data(address, slot)
            )
            for slot in sorted(slots)
        },
    }


def local_cases():
    funded = {"balances": {"QKC": "1000000000000000000"}}
    return [
        {
            "name": "default_transfer",
            "timestamp": 1,
            "alloc": {SENDER.hex() + "00000001": funded},
            "tx": {
                "gas_price": 1_000_000_000,
                "gas_limit": 30_000,
                "to": RECIPIENT.hex(),
                "value": 100,
            },
        },
        {
            "name": "nonce_too_high_rejected",
            "timestamp": 1,
            "alloc": {SENDER.hex() + "00000001": funded},
            "tx": {
                "nonce": 5,
                "gas_price": 1,
                "gas_limit": 30_000,
                "to": RECIPIENT.hex(),
                "value": 100,
            },
            "rejected": True,
        },
        {
            "name": "intrinsic_gas_rejected",
            "timestamp": 1,
            "alloc": {SENDER.hex() + "00000001": funded},
            "tx": {
                "gas_price": 1,
                "gas_limit": 21_000,
                "to": RECIPIENT.hex(),
                "data": "0x01",
            },
            "rejected": True,
        },
        {
            "name": "combined_native_balance_rejected",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {"balances": {"QETH": "30000"}}
            },
            "tx": {
                "gas_price": 1,
                "gas_limit": 30_000,
                "to": RECIPIENT.hex(),
                "value": 1,
                "gas_token": "QETH",
                "transfer_token": "QETH",
            },
            "rejected": True,
        },
        {
            "name": "block_gas_pool_rejected",
            "timestamp": 1,
            "block_gas_limit": 25_000,
            "alloc": {SENDER.hex() + "00000001": funded},
            "tx": {
                "gas_price": 1,
                "gas_limit": 30_000,
                "to": RECIPIENT.hex(),
            },
            "rejected": True,
        },
        {
            "name": "native_transfer_default_gas",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {
                    "balances": {"QKC": "1000000000000000000", "QETH": "99999"}
                }
            },
            "tx": {
                "gas_price": 1_000_000_000,
                "gas_limit": 30_000,
                "to": RECIPIENT.hex(),
                "value": 12_345,
                "transfer_token": "QETH",
            },
        },
        {
            "name": "native_gas_conversion",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {"balances": {"QETH": "10000000"}},
                GENERAL_MANAGER.hex() + "00000001": {
                    "balances": {"QKC": "1000000000000000000"},
                    "code": MANAGER_RATE_80_PRICE_2,
                },
            },
            "tx": {
                "gas_price": 3,
                "gas_limit": 30_000,
                "to": RECIPIENT.hex(),
                "value": 12_345,
                "gas_token": "QETH",
                "transfer_token": "QETH",
            },
            "slots": {GENERAL_MANAGER: {0}},
        },
        {
            "name": "zero_price_native_gas_skips_conversion",
            "timestamp": 1,
            "alloc": {SENDER.hex() + "00000001": {"balances": {"QETH": "1"}}},
            "tx": {
                "gas_price": 0,
                "gas_limit": 30_000,
                "to": RECIPIENT.hex(),
                "gas_token": "QETH",
            },
        },
        {
            "name": "native_gas_conversion_user_revert",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {"balances": {"QETH": "10000000"}},
                GENERAL_MANAGER.hex() + "00000001": {
                    "balances": {"QKC": "1000000000000000000"},
                    "code": MANAGER_RATE_80_PRICE_2,
                },
                TARGET.hex() + "00000001": {"balances": {}, "code": REVERT},
            },
            "tx": {
                "gas_price": 3,
                "gas_limit": 200_000,
                "to": TARGET.hex(),
                "gas_token": "QETH",
            },
            "slots": {GENERAL_MANAGER: {0}},
        },
        {
            "name": "native_gas_zero_price_rejected",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {"balances": {"QETH": "10000000"}},
                GENERAL_MANAGER.hex() + "00000001": {
                    "balances": {"QKC": "1000000000000000000"},
                    "code": MANAGER_RATE_100_PRICE_0,
                },
            },
            "tx": {
                "gas_price": 3,
                "gas_limit": 30_000,
                "to": RECIPIENT.hex(),
                "value": 12_345,
                "gas_token": "QETH",
                "transfer_token": "QETH",
            },
            "slots": {GENERAL_MANAGER: {0}},
            "rejected": True,
        },
        {
            "name": "native_gas_reserve_rejected",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {"balances": {"QETH": "10000000"}},
                GENERAL_MANAGER.hex() + "00000001": {
                    "balances": {"QKC": "1000"},
                    "code": MANAGER_RATE_80_PRICE_2,
                },
            },
            "tx": {
                "gas_price": 3,
                "gas_limit": 30_000,
                "to": RECIPIENT.hex(),
                "gas_token": "QETH",
            },
            "slots": {GENERAL_MANAGER: {0}},
            "rejected": True,
        },
        {
            "name": "contract_revert",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                TARGET.hex() + "00000001": {"balances": {}, "code": REVERT},
            },
            "tx": {
                "gas_price": 1,
                "gas_limit": 200_000,
                "to": TARGET.hex(),
                "value": 1_000,
            },
        },
        {
            "name": "native_value_requires_acknowledgement",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {
                    "balances": {"QKC": "1000000000000000000", "QETH": "10"}
                },
                TARGET.hex() + "00000001": {"balances": {}, "code": STOP},
            },
            "tx": {
                "gas_price": 1,
                "gas_limit": 30_000,
                "to": TARGET.hex(),
                "value": 1,
                "transfer_token": "QETH",
            },
        },
        {
            "name": "contract_out_of_gas",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                CONTRACT_A.hex() + "00000001": {
                    "balances": {},
                    "code": INFINITE,
                },
            },
            "tx": {
                "gas_price": 1,
                "gas_limit": 50_000,
                "to": CONTRACT_A.hex(),
                "value": 1_000,
            },
        },
        {
            "name": "legacy_refund_cap",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                CONTRACT_A.hex() + "00000001": {
                    "balances": {},
                    "code": CLEAR_SLOT_ZERO,
                    "storage": {"0x0": "0x1"},
                },
            },
            "tx": {
                "gas_price": 1,
                "gas_limit": 200_000,
                "to": CONTRACT_A.hex(),
            },
            "slots": {CONTRACT_A: {0}},
        },
        {
            "name": "balance_mnt",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {
                    "balances": {"QKC": "1000000000000000000", "QETH": "777"}
                }
            },
            "tx": {
                "gas_price": 1,
                "gas_limit": 100_000,
                "to": "000000000000000000000000000000514b430005",
                "data": "0x" + "00" * 12 + SENDER.hex() + QETH.to_bytes(32, "big").hex(),
            },
        },
        {
            "name": "mint_mnt_rejects_foreign_sender",
            "timestamp": 1,
            "alloc": {SENDER.hex() + "00000001": funded},
            "tx": {
                "gas_price": 1,
                "gas_limit": 100_000,
                "to": "000000000000000000000000000000514b430004",
                "data": "0x"
                + "00" * 12
                + SENDER.hex()
                + QETH.to_bytes(32, "big").hex()
                + (1000).to_bytes(32, "big").hex(),
            },
        },
        {
            "name": "transfer_mnt_clears_child_shard_key",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                CONTRACT_A.hex() + "00000001": {
                    "balances": {},
                    "code": TRANSFER_MNT_CREATE_AND_RETURN,
                },
                CONTRACT_B.hex() + "00000001": {
                    "balances": {},
                    "code": CREATE_AND_RETURN,
                },
            },
            "tx": {
                "gas_price": 1,
                "gas_limit": 500_000,
                "to": CONTRACT_A.hex(),
            },
        },
    ]


def build_local_vector(cluster_config, case):
    state = new_state(
        cluster_config, case["timestamp"], case.get("block_gas_limit", 12_000_000)
    )
    apply_alloc(state, case["alloc"])
    state.commit()
    tx = make_transaction(case["tx"], cluster_config.QUARKCHAIN)

    accepted = True
    error_type = ""
    success = False
    output = b""
    try:
        success, output = apply_transaction(state, tx, tx.hash)
    except Exception as exc:  # noqa: BLE001 - rejection class is an oracle output
        accepted = False
        error_type = type(exc).__name__
    if accepted == bool(case.get("rejected")):
        raise SystemExit("case {} produced the wrong acceptance class".format(case["name"]))

    addresses = {SENDER, COINBASE, bytes(20), GENERAL_MANAGER}
    if tx.to:
        addresses.add(tx.to)
    if accepted and not tx.to and success and output:
        addresses.add(bytes(output))
    for item in case["alloc"]:
        addresses.add(bytes.fromhex(item[:40]))
    tokens = {QKC, QETH, tx.gas_token_id, tx.transfer_token_id}
    slots = case.get("slots", {})

    state.commit()
    receipt = state.receipts[-1] if state.receipts else None
    return {
        "name": case["name"],
        "context": {
            "timestamp": state.timestamp,
            "block_gas_limit": state.gas_limit,
            "coinbase": hex_bytes(COINBASE),
        },
        "pre_alloc": normalize_alloc(case["alloc"]),
        "message": {
            "from": hex_bytes(tx.sender),
            "to": hex_bytes(tx.to) if tx.to else None,
            "nonce": tx.nonce,
            "value": str(tx.value),
            "gas_limit": tx.startgas,
            "gas_price": str(tx.gasprice),
            "gas_token_id": tx.gas_token_id,
            "transfer_token_id": tx.transfer_token_id,
            "to_full_shard_key": tx.to_full_shard_key,
            "data": hex_bytes(tx.data),
        },
        "result": {
            "accepted": accepted,
            "error_type": error_type,
            "success": bool(success),
            "output": hex_bytes(output),
            "used_gas": state.gas_used,
            "gas_pool_remaining": state.gas_limit - state.gas_used,
            "full_shard_key": state.full_shard_key,
            "logs": [dump_log(log) for log in receipt.logs] if receipt else [],
            "post_state_root": hex_bytes(state.trie.root_hash),
            "accounts": [
                observe_account(state, address, tokens, slots.get(address, set()))
                for address in sorted(addresses)
            ],
        },
    }


def direct_vm_cases():
    funded = {"balances": {"QKC": "1000000000000000000"}}
    balance_data = b"\x00" * 12 + SENDER + QETH.to_bytes(32, "big")
    transfer_data = (
        b"\x00" * 12
        + RECIPIENT
        + QETH.to_bytes(32, "big")
        + (3).to_bytes(32, "big")
    )
    mint_data = (
        b"\x00" * 12
        + RECIPIENT
        + QETH.to_bytes(32, "big")
        + (7).to_bytes(32, "big")
    )
    return [
        {
            "name": "vm_return_42",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                TARGET.hex() + "00000001": {"balances": {}, "code": RETURN_42},
            },
            "message": {"to": TARGET, "gas": 100000},
        },
        {
            "name": "vm_revert",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                TARGET.hex() + "00000001": {"balances": {}, "code": REVERT},
            },
            "message": {"to": TARGET, "gas": 100000},
        },
        {
            "name": "vm_log_one",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                TARGET.hex() + "00000001": {"balances": {}, "code": LOG_ONE},
            },
            "message": {"to": TARGET, "gas": 100000},
        },
        {
            "name": "vm_log_out_of_gas",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                TARGET.hex() + "00000001": {"balances": {}, "code": LOG_ONE},
            },
            "message": {"to": TARGET, "gas": 1000},
        },
        {
            "name": "vm_mnt_acknowledgement_failure",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {"balances": {"QKC": "100", "QETH": "10"}},
                TARGET.hex() + "00000001": {"balances": {}, "code": STOP},
            },
            "message": {
                "to": TARGET,
                "gas": 100000,
                "value": 1,
                "transfer_token_id": QETH,
            },
        },
        {
            "name": "vm_current_mnt",
            "timestamp": 1,
            "alloc": {SENDER.hex() + "00000001": funded},
            "message": {
                "to": bytes.fromhex("000000000000000000000000000000514b430001"),
                "gas": 100000,
                "transfer_token_id": QETH,
            },
        },
        {
            "name": "vm_current_mnt_at_boundary_disabled",
            "timestamp": 0,
            "alloc": {SENDER.hex() + "00000001": funded},
            "message": {
                "to": bytes.fromhex("000000000000000000000000000000514b430001"),
                "gas": 100000,
                "transfer_token_id": QETH,
            },
        },
        {
            "name": "vm_balance_mnt",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": {"balances": {"QKC": "100", "QETH": "777"}}
            },
            "message": {
                "to": bytes.fromhex("000000000000000000000000000000514b430005"),
                "gas": 100000,
                "data": balance_data,
            },
        },
        {
            "name": "vm_transfer_mnt",
            "timestamp": 1,
            "alloc": {SENDER.hex() + "00000001": {"balances": {"QKC": "100", "QETH": "10"}}},
            "message": {
                "to": bytes.fromhex("000000000000000000000000000000514b430002"),
                "gas": 100000,
                "data": transfer_data,
            },
        },
        {
            "name": "vm_mint_mnt",
            "timestamp": 1,
            "alloc": {},
            "message": {
                "sender": bytes.fromhex("514b430000000000000000000000000000000002"),
                "to": bytes.fromhex("000000000000000000000000000000514b430004"),
                "gas": 100000,
                "data": mint_data,
            },
        },
        {
            "name": "vm_create2",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                CONTRACT_A.hex() + "00000001": {"balances": {}, "code": CREATE2_AND_RETURN},
            },
            "message": {"to": CONTRACT_A, "gas": 200000},
            "observe_output_address": True,
        },
        {
            "name": "vm_create2_out_of_gas",
            "timestamp": 1,
            "alloc": {
                SENDER.hex() + "00000001": funded,
                CONTRACT_A.hex() + "00000001": {"balances": {}, "code": CREATE2_AND_RETURN},
            },
            "message": {"to": CONTRACT_A, "gas": 32000},
        },
    ]


def build_direct_vm_vector(cluster_config, case):
    qkc_config = cluster_config.QUARKCHAIN
    state = new_state(cluster_config, case["timestamp"], 12000000)
    state.full_shard_key = case.get("full_shard_key", 1)
    apply_alloc(state, case["alloc"])
    state.commit()
    spec = case["message"]
    sender = spec.get("sender", SENDER)
    target = spec["to"]
    message = Message(
        sender,
        target,
        value=spec.get("value", 0),
        gas=spec.get("gas", 100000),
        data=spec.get("data", b""),
        to_full_shard_key=spec.get("to_full_shard_key"),
        gas_token_id=spec.get("gas_token_id", QKC),
        transfer_token_id=spec.get("transfer_token_id", QKC),
    )
    success, remaining_gas, output = apply_msg(VMExt(state, sender, 0), message)

    addresses = {sender, target, RECIPIENT, TARGET}
    for item in case["alloc"]:
        addresses.add(bytes.fromhex(item[:40]))
    if case.get("observe_output_address") and success and len(output) >= 20:
        addresses.add(bytes(output[-20:]))
    tokens = {QKC, QETH, message.gas_token_id, message.transfer_token_id}
    slots = case.get("slots", {})
    state.commit()
    return {
        "name": case["name"],
        "context": {
            "timestamp": state.timestamp,
            "full_shard_key": state.full_shard_key,
            "enable_evm_timestamp": qkc_config.ENABLE_EVM_TIMESTAMP,
            "enable_non_reserved_native_token_timestamp": qkc_config.ENABLE_NON_RESERVED_NATIVE_TOKEN_TIMESTAMP,
            "enable_general_native_token_timestamp": qkc_config.ENABLE_GENERAL_NATIVE_TOKEN_TIMESTAMP,
        },
        "pre_alloc": normalize_alloc(case["alloc"]),
        "message": {
            "from": hex_bytes(sender),
            "to": hex_bytes(target),
            "value": str(message.value),
            "gas": message.gas,
            "gas_token_id": message.gas_token_id,
            "transfer_token_id": message.transfer_token_id,
            "to_full_shard_key": message.to_full_shard_key,
            "data": hex_bytes(message.data.extract_all()),
        },
        "result": {
            "success": bool(success),
            "remaining_gas": remaining_gas,
            "output": hex_bytes(output),
            "logs": [dump_log(log) for log in state.logs],
            "post_state_root": hex_bytes(state.trie.root_hash),
            "accounts": [
                observe_account(state, address, tokens, slots.get(address, set()))
                for address in sorted(addresses)
            ],
        },
    }


def vm_vectors():
    sender = bytes.fromhex("1234567890abcdef1234567890abcdef12345678")
    salt = bytes.fromhex("11" * 32)
    init_hash = bytes.fromhex("22" * 32)
    current_ok = Message(sender, sender, gas=10, transfer_token_id=1234)
    current_oog = Message(sender, sender, gas=2, transfer_token_id=1234)
    current_cases = []
    for name, message in (("current_mnt", current_ok), ("current_mnt_oog", current_oog)):
        result, gas, output = proc_current_mnt_id(None, message)
        current_cases.append(
            {
                "name": name,
                "input_gas": 10 if name == "current_mnt" else 2,
                "transfer_token_id": 1234,
                "success": bool(result),
                "remaining_gas": gas,
                "output": hex_bytes(output),
                "token_id_queried": message.token_id_queried,
            }
        )
    cluster_config = load_devnet()
    return {
        "execution": [
            build_direct_vm_vector(cluster_config, case) for case in direct_vm_cases()
        ],
        "create_addresses": [
            {
                "name": "legacy_create",
                "sender": hex_bytes(sender),
                "nonce": 7,
                "address": hex_bytes(mk_contract_address(sender, 7, None)),
            },
        ],
        "create2": {
            "sender": hex_bytes(sender),
            "salt": hex_bytes(salt),
            "init_code_hash": hex_bytes(init_hash),
            "address": hex_bytes(mk_contract_address2(sender, salt, init_hash)),
        },
        "current_mnt": current_cases,
        "system_contracts": [
            {
                "name": name,
                "length": len(code),
                "sha256": hashlib.sha256(code).hexdigest(),
            }
            for name, code in (
                ("non_reserved_native_token", NON_RESERVED_NATIVE_TOKEN_CONTRACT_BYTECODE),
                ("general_native_token", GENERAL_NATIVE_TOKEN_CONTRACT_BYTECODE),
            )
        ],
    }


def write(name, payload):
    path = HERE / name
    with path.open("w", newline="\n") as stream:
        json.dump(payload, stream, indent=2)
        stream.write("\n")
    print("wrote {}".format(path))


def main():
    cluster_config = load_devnet()
    write(
        "vm_level.json",
        {
            "source": SOURCE,
            "cases": vm_vectors(),
        },
    )
    write(
        "local_message_level.json",
        {
            "source": SOURCE,
            "cases": [
                build_local_vector(cluster_config, case) for case in local_cases()
            ],
        },
    )


if __name__ == "__main__":
    main()
