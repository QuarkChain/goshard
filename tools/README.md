# tools — mainnet compatibility

Two-step workflow to verify goshard's state trie is bit-for-bit compatible with a live goquarkchain chain.

```
tools/
  dump_state/          # Step 1: export state trie from pyquarkchain RocksDB → JSON
  verify_state/        # Step 2: recompute trie root in Go, confirm it matches
```

## Prerequisites

```bash
pip install rocksdict rlp
go build ./...          # goshard must build cleanly
```

## Step 1 — dump state trie

```bash
python tools/dump_state/dump_qkc_state_trie.py \
    --db-path  /path/to/pyquarkchain/data/shard-0 \
    --height   10000000 \        # omit for latest
    --output   trie_dump.json
```

If `--height` lookup is needed (minor block deserialization), add pyquarkchain to PYTHONPATH:

```bash
PYTHONPATH=/path/to/pyquarkchain python tools/dump_state/dump_qkc_state_trie.py ...
```

Alternatively, skip the block lookup by passing `--state-root <hex>` directly.

Output `trie_dump.json` contains:
- `block` — height, hash, state root, timestamp
- `node_store` — flat map of `hash_hex → rlp_bytes_hex` for all trie nodes
- `accounts` — decoded leaf values (nonce, QKC balance, MNT balances, storage root, code hash)
- `stats` — node type counts

## Step 2 — verify trie root

```bash
go run ./tools/verify_state \
    --input trie_dump.json \
    --check-accounts        # optional: also iterate leaves via StateAccount.DecodeRLP
```

Expected output on success:

```
✓  ROOT HASH MATCH — goshard trie is compatible with goquarkchain
```

## Unit tests (no DB required)

The token-balance and account RLP unit tests can be run without any external data:

```bash
go test ./core/types/ -run "TestTokenBalances|TestStateAccount|TestDecodeQKC" -v
```
