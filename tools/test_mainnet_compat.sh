#!/usr/bin/env bash
# test_mainnet_compat.sh
#
# End-to-end compatibility test between goquarkchain (pyquarkchain) and goshard.
#
# Steps:
#   1. [Python] Export a state trie from a live goquarkchain RocksDB
#   2. [Go]     Run goshard unit tests (account RLP + token balances)
#   3. [Go]     Recompute the trie root from the exported node store
#              and confirm it matches the original chain root
#
# Usage:
#   bash tools/test_mainnet_compat.sh \
#       --db       /path/to/shard-0.db  \
#       --pyqkc    /path/to/pyquarkchain \
#       --height   500000                 # optional; default = latest
#
# All flags are optional if the defaults in the CONFIG block below are set.

set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG — edit these or pass as flags
# ──────────────────────────────────────────────────────────────────────────────
PYQKC_DIR=""           # path to pyquarkchain checkout
DB_PATH=""             # path to goquarkchain shard RocksDB directory
HEIGHT=""              # block height to export; empty = latest
LIMIT=0                # max accounts to decode (0 = unlimited)
DUMP_FILE="trie_dump.json"
GOSHARD_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# ──────────────────────────────────────────────────────────────────────────────
# parse flags
# ──────────────────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --db)     DB_PATH="$2";  shift 2 ;;
    --pyqkc)  PYQKC_DIR="$2"; shift 2 ;;
    --height) HEIGHT="$2";   shift 2 ;;
    --limit)  LIMIT="$2";    shift 2 ;;
    --dump)   DUMP_FILE="$2"; shift 2 ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

# ──────────────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

info()  { echo -e "${YELLOW}▶ $*${NC}"; }
ok()    { echo -e "${GREEN}✓ $*${NC}"; }
fail()  { echo -e "${RED}✗ $*${NC}"; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

# ──────────────────────────────────────────────────────────────────────────────
# Step 0: preflight checks
# ──────────────────────────────────────────────────────────────────────────────
info "Step 0: preflight checks"

require_cmd python3
require_cmd go

[[ -d "$GOSHARD_DIR" ]] || fail "goshard directory not found: $GOSHARD_DIR"

if [[ -n "$DB_PATH" ]]; then
  [[ -d "$DB_PATH" ]] || fail "DB directory not found: $DB_PATH"
fi

# Check Python deps
python3 - <<'EOF'
import sys
missing = []
for pkg in ("rocksdict", "rlp"):
    try: __import__(pkg)
    except ImportError: missing.append(pkg)
if missing:
    print(f"Missing Python packages: {', '.join(missing)}")
    print(f"Install with: pip install {' '.join(missing)}")
    sys.exit(1)
EOF
ok "Python deps OK (rocksdict, rlp)"

# ──────────────────────────────────────────────────────────────────────────────
# Step 1: goshard unit tests (no DB needed)
# ──────────────────────────────────────────────────────────────────────────────
info "Step 1: goshard unit tests — account RLP + token balances"

cd "$GOSHARD_DIR"

go test ./core/types/ \
  -run "TestTokenBalances|TestStateAccount|TestDecodeQKC" \
  -v -count=1 2>&1 | grep -E "^(=== RUN|--- (PASS|FAIL)|FAIL|ok )" \
  || fail "goshard unit tests FAILED"

ok "goshard unit tests passed"

# ──────────────────────────────────────────────────────────────────────────────
# Step 2: export trie from goquarkchain DB  (skipped if --db not provided)
# ──────────────────────────────────────────────────────────────────────────────
if [[ -z "$DB_PATH" ]]; then
  echo ""
  echo "  --db not provided; skipping live-DB export (Steps 2–3)."
  echo "  To run the full flow, re-run with:"
  echo "    --db /path/to/shard-0.db  [--pyqkc /path/to/pyquarkchain]"
  echo ""
  ok "Done (unit tests only)"
  exit 0
fi

info "Step 2: export state trie from goquarkchain DB"
echo "  DB:     $DB_PATH"
echo "  Height: ${HEIGHT:-latest}"
echo "  Output: $DUMP_FILE"

DUMP_SCRIPT="$GOSHARD_DIR/tools/generatedata/dump_qkc_state_trie.py"
[[ -f "$DUMP_SCRIPT" ]] || fail "Export script not found: $DUMP_SCRIPT"

HEIGHT_ARG=""
[[ -n "$HEIGHT" ]] && HEIGHT_ARG="--height $HEIGHT"

# Build PYTHONPATH: include pyquarkchain if provided (needed for block lookup)
PYPATH="${PYQKC_DIR:-}"
[[ -n "$PYPATH" ]] && export PYTHONPATH="$PYPATH:${PYTHONPATH:-}"

python3 "$DUMP_SCRIPT" \
  --db-path "$DB_PATH" \
  $HEIGHT_ARG \
  --limit "$LIMIT" \
  --output "$DUMP_FILE" \
  || fail "trie export FAILED"

[[ -f "$DUMP_FILE" ]] || fail "Dump file not created: $DUMP_FILE"
DUMP_SIZE=$(du -sh "$DUMP_FILE" | cut -f1)
ok "Trie exported → $DUMP_FILE ($DUMP_SIZE)"

# quick sanity: dump must contain a non-empty node_store
python3 - <<EOF
import json, sys
with open("$DUMP_FILE") as f:
    d = json.load(f)
ns = d.get("node_store", {})
if not ns:
    print("ERROR: node_store is empty — trie traversal produced no nodes")
    sys.exit(1)
acct = d.get("stats", {}).get("total_accounts", 0)
print(f"  node_store entries : {len(ns)}")
print(f"  accounts decoded   : {acct}")
print(f"  state root         : {d['block'].get('state_root', '?')}")
EOF

# ──────────────────────────────────────────────────────────────────────────────
# Step 3: recompute trie root in goshard and compare
# ──────────────────────────────────────────────────────────────────────────────
info "Step 3: recompute trie root in goshard"

go run "$GOSHARD_DIR/tools/verify_trie_root" \
  --input "$DUMP_FILE" \
  --check-accounts \
  || fail "trie root verification FAILED — goshard root does not match goquarkchain"

ok "Trie root matches — goshard is compatible with goquarkchain mainnet"

# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════"
ok "All steps passed"
echo "  1. goshard unit tests (RLP + token balances)  ✓"
echo "  2. trie export from goquarkchain DB            ✓"
echo "  3. trie root recomputed in goshard             ✓"
echo "════════════════════════════════════════════════════"
