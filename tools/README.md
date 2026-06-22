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

The script parses MinorBlock bytes directly — **no pyquarkchain install needed**.

```bash
python tools/dump_state/dump_qkc_state_trie.py \
    --db-path  /path/to/pyquarkchain/data/shard-0 \
    --output   trie_dump.json
# --height 10000000   # optional; omit to use the latest block
```

If you already know the state root, pass it directly to skip the block lookup:

```bash
python tools/dump_state/dump_qkc_state_trie.py \
    --db-path    /path/to/pyquarkchain/data/shard-0 \
    --state-root d9ff31bb61e359cdba7e32134d5c4319a1ba332e0505398067a9534f395adf48 \
    --output     trie_dump.json
```

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
