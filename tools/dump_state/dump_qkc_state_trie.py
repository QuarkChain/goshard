#!/usr/bin/env python3
"""
dump_qkc_state_trie.py

Extract the full state trie from a goquarkchain (pyquarkchain) LevelDB/RocksDB,
export each node (raw bytes + decoded content + account data for leaves) to JSON.

Usage:
    python dump_qkc_state_trie.py \
        --db-path /path/to/goquarkchain/data/shard-N \
        --height  12345          # omit for latest canonical block \
        --output  trie_dump.json \
        --limit   0              # 0 = no limit on accounts

The resulting JSON can be fed to goshard's trie-root verification tests to
confirm goshard produces the same root hash as the original chain.

Dependencies (install via pip):
    rocksdict
    rlp

No pyquarkchain import needed — MinorBlock bytes are parsed directly.
"""

import argparse
import hashlib
import json
import sys
import os

# ── MinorBlock binary parser ──────────────────────────────────────────────────
# Extracts hash_evm_state_root, height, create_time, and tx_count directly from
# raw MinorBlock bytes — no pyquarkchain import required.
#
# MinorBlockHeader field layout (pyquarkchain/quarkchain/core.py):
#   version:uint32(4) branch:uint32(4) height:uint64(8)
#   coinbase_address(24)  coinbase_amount_map:PrependedSizeMap(4,biguint,biguint)
#   hash_prev_minor_block(32) hash_prev_root_block(32) evm_gas_limit:uint256(32)
#   hash_meta(32) create_time:uint64(8) difficulty:biguint nonce:uint64(8)
#   bloom:uint2048(256) extra_data:PrependedSizeBytes(2) mixhash(32)
# MinorBlockMeta immediately follows (no length prefix):
#   hash_merkle_root(32) hash_evm_state_root(32) ...
def _parse_minor_block(raw: bytes) -> tuple:
    """Return (state_root: bytes, height: int, create_time: int, tx_count: int)."""
    pos = 0

    def ru(n):
        nonlocal pos
        v = int.from_bytes(raw[pos:pos + n], "big")
        pos += n
        return v

    def rb(n):
        nonlocal pos
        pos += n

    def skip_biguint():       # BigUintSerializer: 1B length prefix + bytes
        nonlocal pos
        pos += 1 + raw[pos]

    def skip_prepended(w):    # PrependedSizeBytesSerializer: w-byte length + bytes
        nonlocal pos
        pos += w + int.from_bytes(raw[pos:pos + w], "big")

    # MinorBlockHeader
    ru(4)               # version
    ru(4)               # branch
    height = ru(8)      # height
    rb(24)              # coinbase_address (20B recipient + 4B full_shard_key)
    for _ in range(ru(4)):   # coinbase_amount_map: 4B count, then biguint pairs
        skip_biguint()
        skip_biguint()
    rb(32 + 32 + 32 + 32)   # hash_prev_minor_block, hash_prev_root_block, evm_gas_limit, hash_meta
    create_time = ru(8) # create_time
    skip_biguint()      # difficulty
    ru(8)               # nonce
    rb(256)             # bloom (uint2048)
    skip_prepended(2)   # extra_data
    rb(32)              # mixhash

    # MinorBlockMeta
    rb(32)              # hash_merkle_root
    state_root = raw[pos:pos + 32]
    pos += 32           # hash_evm_state_root ← what we need
    rb(32 + 32 + 32)    # hash_evm_receipt_root, evm_gas_used, evm_cross_shard_receive_gas_used
    rb(24)              # xshard_tx_cursor_info (3 × uint64)
    rb(32)              # evm_xshard_gas_limit

    tx_count = ru(4)    # tx_list: PrependedSizeListSerializer(4, ...)
    return state_root, height, create_time, tx_count

# ── RLP decoding (minimal, no rlp library required for simple cases) ──────────
# We use the 'rlp' package for robustness.
try:
    import rlp as _rlp_lib
    def rlp_decode(data: bytes):
        return _rlp_lib.decode(data)
except ImportError:
    print("ERROR: 'rlp' package not found. Install with: pip install rlp", file=sys.stderr)
    sys.exit(1)


# ── constants ──────────────────────────────────────────────────────────────────
BLANK_ROOT = bytes.fromhex("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
BLANK_NODE = b""
NIBBLE_TERMINATOR = 16

TOKEN_ID_QKC = 35760  # token_id_encode("QKC")



# ── DB wrapper ────────────────────────────────────────────────────────────────
try:
    from rocksdict import Rdict, Options, AccessType, DBCompressionType
except ImportError:
    print("ERROR: rocksdict not found. Install with: pip install rocksdict", file=sys.stderr)
    sys.exit(1)


class RawDb:
    """Read-only wrapper around a pyquarkchain shard RocksDB.

    Options mirror pyquarkchain's PersistentDb exactly so the comparator
    and compression always match what wrote the database.
    """

    def __init__(self, path: str):
        opts = Options(raw_mode=True)
        opts.create_if_missing(False)
        opts.set_max_open_files(100000)
        opts.set_write_buffer_size(128 * 1024 * 1024)
        opts.set_max_write_buffer_number(3)
        opts.set_target_file_size_base(67108864)
        opts.set_compression_type(DBCompressionType.snappy())
        self._db = Rdict(path, opts, access_type=AccessType.read_only())

    def get(self, key: bytes):
        return self._db.get(key)

    def close(self):
        self._db.close()

    def scan_key_prefixes(self, n: int = 30) -> dict:
        from collections import Counter
        counts: Counter = Counter()
        samples: dict = {}
        try:
            for i, k in enumerate(self._db.keys()):
                if i >= 10000:
                    break
                if isinstance(k, bytes):
                    p = k[:4]
                    counts[p] += 1
                    samples.setdefault(p, k)
        except Exception as ex:
            return {f"<iteration failed: {ex}>": 0}
        return {samples[p].hex(): counts[p] for p, _ in counts.most_common(n)}


# ── nibble / HP encoding helpers ──────────────────────────────────────────────
def _bin_to_nibbles(data: bytes) -> list:
    out = []
    for b in data:
        out.append(b >> 4)
        out.append(b & 0x0F)
    return out


def _hp_decode(packed: bytes):
    """
    Decode Hex-Prefix encoded key.
    Returns (nibbles_without_terminator, is_leaf).
    """
    nibbles = _bin_to_nibbles(packed)
    if not nibbles:
        return [], False
    flags = nibbles[0]
    is_leaf = flags >= 2
    is_odd = flags % 2 == 1
    if is_odd:
        return nibbles[1:], is_leaf   # odd: drop flag nibble
    else:
        return nibbles[2:], is_leaf   # even: drop flag nibble + padding nibble


def _nibbles_to_hex(nibbles: list) -> str:
    """Convert nibble list to compact hex string (for display)."""
    return "".join(f"{n:x}" for n in nibbles)


# ── trie node decoding ────────────────────────────────────────────────────────
def _decode_node_ref(ref) -> bytes | None:
    """
    A node reference is either:
      - 32 bytes  → stored as hash in DB
      - < 32 bytes (list or empty) → inline node, not a DB key
    Returns the hash if it's a DB reference, else None.
    """
    if isinstance(ref, bytes) and len(ref) == 32:
        return ref
    return None


def decode_trie_node(node_hash: bytes, node_bytes: bytes, raw_node):
    """
    Build a human-readable dict for one trie node.
    raw_node: the RLP-decoded node (list of bytes).
    """
    info = {
        "hash":  node_hash.hex(),
        "bytes": node_bytes.hex(),
        "size":  len(node_bytes),
    }

    if raw_node == BLANK_NODE or raw_node == []:
        info["type"] = "blank"
        return info

    n = len(raw_node)

    if n == 17:
        info["type"] = "branch"
        children = []
        for i in range(16):
            child = raw_node[i]
            ref = _decode_node_ref(child)
            if ref:
                children.append(ref.hex())
            elif child == b"":
                children.append(None)
            else:
                # inline node (< 32 bytes RLP)
                children.append(child.hex() if isinstance(child, bytes) else str(child))
        info["children"] = children
        info["value"] = raw_node[16].hex() if raw_node[16] else None

    elif n == 2:
        packed_key = raw_node[0]
        nibbles, is_leaf = _hp_decode(packed_key)
        info["key_nibbles"] = _nibbles_to_hex(nibbles)

        if is_leaf:
            info["type"] = "leaf"
            info["value_bytes"] = raw_node[1].hex() if isinstance(raw_node[1], bytes) else None
        else:
            info["type"] = "extension"
            child = raw_node[1]
            ref = _decode_node_ref(child)
            info["child"] = ref.hex() if ref else (child.hex() if isinstance(child, bytes) else None)
            info["child_inline"] = ref is None

    else:
        info["type"] = f"unknown({n})"

    return info


# ── account decoding ──────────────────────────────────────────────────────────
def decode_token_balances(tb_bytes: bytes) -> dict:
    """
    Decode raw TokenBalances bytes.
    Format: b'\x00' + rlp([TokenBalancePair, ...])  (list format)
            b'\x01' + 32-byte trie root             (trie format, not decoded)
    """
    if not tb_bytes:
        return {}
    prefix = tb_bytes[0:1]
    if prefix == b"\x00":
        try:
            pairs = rlp_decode(tb_bytes[1:])
            result = {}
            for pair in pairs:
                token_id = int.from_bytes(pair[0], "big") if pair[0] else 0
                balance  = int.from_bytes(pair[1], "big") if pair[1] else 0
                if balance:
                    result[str(token_id)] = str(balance)
            return result
        except Exception as e:
            return {"_error": f"list decode failed: {e}", "_raw": tb_bytes.hex()}
    elif prefix == b"\x01":
        trie_root = tb_bytes[1:]
        return {"_trie_root": trie_root.hex(), "_note": "trie format (>16 tokens), not decoded"}
    else:
        return {"_error": f"unknown prefix 0x{tb_bytes[0]:02x}", "_raw": tb_bytes.hex()}


def decode_account(leaf_value: bytes) -> dict:
    """
    Decode a QKC _Account RLP blob.
    Fields: [nonce, token_balances(bytes), storage_root(32B), code_hash(32B),
              full_shard_key(BigEndianInt4), optional(bytes)]
    """
    try:
        parts = rlp_decode(leaf_value)
        if not isinstance(parts, list) or len(parts) < 4:
            return {"_error": "unexpected RLP structure", "_raw": leaf_value.hex()}

        nonce        = int.from_bytes(parts[0], "big") if parts[0] else 0
        tb_bytes     = parts[1] if isinstance(parts[1], bytes) else b""
        storage_root = parts[2].hex() if isinstance(parts[2], bytes) else None
        code_hash    = parts[3].hex() if isinstance(parts[3], bytes) else None
        full_shard_key = int.from_bytes(parts[4], "big") if len(parts) > 4 and parts[4] else 0

        token_balances = decode_token_balances(tb_bytes)

        return {
            "nonce":          nonce,
            "qkc_balance":    token_balances.pop(str(TOKEN_ID_QKC), "0"),
            "mnt_balances":   token_balances,
            "storage_root":   storage_root,
            "code_hash":      code_hash,
            "full_shard_key": full_shard_key,
        }
    except Exception as e:
        return {"_error": str(e), "_raw": leaf_value.hex()}


# ── block lookup ──────────────────────────────────────────────────────────────
#
# pyquarkchain DB key schema:
#   b"mi_%d" % height   → 32-byte minor block hash at that height
#   b"mblock_" + hash   → serialized full MinorBlock bytes


def get_state_root_from_db(db: RawDb, height: int | None) -> tuple[bytes, dict]:
    """
    Look up state root from a pyquarkchain shard RocksDB.
    Scans backwards from `height` (default: latest) to find the nearest
    height whose state trie is actually persisted in the DB (~every 128 blocks).
    No pyquarkchain import needed — uses _parse_minor_block() directly.
    """
    # ── find starting hash ─────────────────────────────────────────────────────
    if height is None:
        raw_hash = None
        # mi_N keys (b"mi_%d" % height) are the canonical chain index.
        # Keys are stored as text so RocksDB sorts them lexicographically, not
        # numerically — we can't just seek to the last mi_ key.
        # Binary search finds the max height in O(log N) ≈ 29 DB lookups.
        MAX_HEIGHT = 500_000_000
        lo, hi = 0, MAX_HEIGHT
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if db.get(b"mi_%d" % mid) is not None:
                lo = mid
            else:
                hi = mid - 1
        raw_hash = db.get(b"mi_%d" % lo) if lo > 0 else None
        if raw_hash is not None:
            height = lo
        if raw_hash is None:
            print("DEBUG: no 'mi_N' key found. Scanning DB key prefixes...", file=sys.stderr)
            prefixes = db.scan_key_prefixes()
            for k_hex, cnt in prefixes.items():
                readable = bytes.fromhex(k_hex).decode("utf-8", errors="replace")
                print(f"  prefix={k_hex}  readable={readable!r}  count={cnt}", file=sys.stderr)
            raise RuntimeError(
                "Could not find any minor block hash key ('mi_N') in DB.\n"
                "Check --db-path."
            )
    else:
        raw_hash = db.get(b"mi_%d" % height)
        if raw_hash is None:
            raise RuntimeError(f"Block at height {height} not found (key: mi_{height})")

    # ── scan backwards to a height whose state trie is persisted ──────────────
    start_height = height
    state_root = None
    create_time = 0
    tx_count = 0
    while height >= 0:
        raw_block = db.get(b"mblock_" + raw_hash)
        if raw_block is not None:
            state_root, _h, create_time, tx_count = _parse_minor_block(raw_block)
            if state_root != BLANK_ROOT and db.get(state_root) is not None:
                if height != start_height:
                    print(
                        f"  State trie not persisted at height {start_height}; "
                        f"using height {height}",
                        flush=True,
                    )
                break

        height -= 1
        if height < 0:
            raise RuntimeError(
                "Could not find any persisted state trie. "
                "The DB may be pruned."
            )
        raw_hash = db.get(b"mi_%d" % height)
        if raw_hash is None:
            continue

    meta = {
        "height":      height,
        "block_hash":  raw_hash.hex(),
        "state_root":  state_root.hex(),
        "timestamp":   create_time,
        "tx_count":    tx_count,
    }
    return state_root, meta


# ── trie traversal ────────────────────────────────────────────────────────────
def traverse_trie(db: RawDb, root_hash: bytes, limit_accounts: int = 0):
    """
    BFS traversal of the Merkle Patricia Trie rooted at root_hash.

    Returns:
        nodes:    list of node dicts (all nodes, for root-hash recomputation)
        accounts: list of account dicts (leaf values decoded)
        stats:    summary dict
    """
    nodes    = []
    accounts = []
    visited  = set()
    queue    = [root_hash]
    account_count = 0

    while queue:
        node_hash = queue.pop(0)
        if node_hash in visited or node_hash == BLANK_ROOT or node_hash == BLANK_NODE:
            continue
        visited.add(node_hash)

        node_bytes = db.get(node_hash)
        if node_bytes is None:
            # Node missing from DB (pruned or wrong shard)
            nodes.append({
                "hash":  node_hash.hex(),
                "bytes": None,
                "type":  "missing",
            })
            continue

        try:
            raw_node = rlp_decode(node_bytes)
        except Exception as e:
            nodes.append({
                "hash":  node_hash.hex(),
                "bytes": node_bytes.hex(),
                "type":  "rlp_error",
                "error": str(e),
            })
            continue

        info = decode_trie_node(node_hash, node_bytes, raw_node)
        nodes.append(info)

        node_type = info.get("type", "")

        if node_type == "branch":
            for child_hex in info["children"]:
                if child_hex:
                    child_bytes = bytes.fromhex(child_hex)
                    if child_bytes not in visited:
                        queue.append(child_bytes)

        elif node_type == "extension":
            if not info.get("child_inline") and info.get("child"):
                child_bytes = bytes.fromhex(info["child"])
                if child_bytes not in visited:
                    queue.append(child_bytes)
            elif info.get("child_inline") and info.get("child"):
                # inline child: decode directly
                inline_bytes = bytes.fromhex(info["child"])
                try:
                    inline_raw = rlp_decode(inline_bytes)
                    inline_hash = hashlib.sha3_256(inline_bytes).digest()
                    inline_info = decode_trie_node(inline_hash, inline_bytes, inline_raw)
                    inline_info["inline"] = True
                    nodes.append(inline_info)
                    # recurse into inline children too
                    if inline_info.get("type") == "branch":
                        for c in inline_info.get("children", []):
                            if c:
                                cb = bytes.fromhex(c)
                                if cb not in visited:
                                    queue.append(cb)
                except Exception:
                    pass

        elif node_type == "leaf":
            value_bytes_hex = info.get("value_bytes")
            if value_bytes_hex:
                value_bytes = bytes.fromhex(value_bytes_hex)
                acc = decode_account(value_bytes)
                acc["key_nibbles"] = info.get("key_nibbles", "")
                acc["leaf_hash"]   = node_hash.hex()
                acc["leaf_bytes"]  = value_bytes_hex
                accounts.append(acc)
                account_count += 1
                if limit_accounts and account_count >= limit_accounts:
                    break

    stats = {
        "total_nodes":    len(nodes),
        "total_accounts": len(accounts),
        "node_types":     {},
    }
    for n in nodes:
        t = n.get("type", "unknown")
        stats["node_types"][t] = stats["node_types"].get(t, 0) + 1

    return nodes, accounts, stats


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Export goquarkchain state trie to JSON")
    parser.add_argument("--db-path",    required=True,  help="Path to goquarkchain shard RocksDB directory")
    parser.add_argument("--height",     type=int,       default=None,   help="Block height (default: latest)")
    parser.add_argument("--state-root", default=None,   help="State root hash hex (skip block lookup)")
    parser.add_argument("--output",     default="trie_dump.json",       help="Output JSON file path")
    parser.add_argument("--limit",      type=int,       default=0,      help="Max accounts to decode (0=unlimited)")
    parser.add_argument("--nodes-only", action="store_true",            help="Skip account decoding (faster)")
    parser.add_argument("--indent",     type=int,       default=None,   help="JSON indent (None=compact)")
    args = parser.parse_args()

    print(f"Opening DB: {args.db_path}", flush=True)
    db = RawDb(args.db_path)

    block_meta = {}

    if args.state_root:
        state_root = bytes.fromhex(args.state_root.removeprefix("0x"))
        block_meta = {"state_root": args.state_root}
        print(f"Using state root: {args.state_root}", flush=True)
    else:
        print(f"Looking up block at height: {args.height or 'latest'}", flush=True)
        state_root, block_meta = get_state_root_from_db(db, args.height)
        print(f"Block height:  {block_meta['height']}", flush=True)
        print(f"Block hash:    {block_meta['block_hash']}", flush=True)
        print(f"State root:    {block_meta['state_root']}", flush=True)

    if state_root == BLANK_ROOT or state_root == b"":
        print("WARNING: state root is BLANK_ROOT — empty state trie", flush=True)

    print(f"Traversing trie (limit_accounts={args.limit})...", flush=True)
    nodes, accounts, stats = traverse_trie(db, state_root, limit_accounts=args.limit)
    db.close()

    print(f"  Nodes found:    {stats['total_nodes']}", flush=True)
    print(f"  Accounts found: {stats['total_accounts']}", flush=True)
    print(f"  Node types:     {stats['node_types']}", flush=True)

    output = {
        "block":    block_meta,
        "stats":    stats,
        # flat map hash→bytes for goshard to load as a node store
        "node_store": {n["hash"]: n["bytes"] for n in nodes if n.get("bytes")},
        # full node list with decoded info
        "nodes":    nodes,
    }
    if not args.nodes_only:
        output["accounts"] = accounts

    print(f"Writing {args.output}...", flush=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=args.indent)

    size_mb = os.path.getsize(args.output) / 1024 / 1024
    print(f"Done. Output: {args.output} ({size_mb:.1f} MB)", flush=True)


if __name__ == "__main__":
    main()
