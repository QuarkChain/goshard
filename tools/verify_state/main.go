// Copyright 2026-2027, QuarkChain.
// verify_state loads a trie node store exported by dump_state/dump_qkc_state_trie.py
// and recomputes the trie root hash, confirming goshard produces the same
// result as the original goquarkchain chain.
//
// Usage:
//
//	go run ./tools/verify_state --input trie_dump.json
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
)

// ─── JSON schema produced by dump_qkc_state_trie.py ──────────────────────────

type dumpFile struct {
	Block     map[string]any    `json:"block"`
	Stats     dumpStats         `json:"stats"`
	NodeStore map[string]string `json:"node_store"` // hash_hex -> bytes_hex
	Accounts  []dumpAccount     `json:"accounts"`
}

type dumpStats struct {
	TotalNodes    int            `json:"total_nodes"`
	TotalAccounts int            `json:"total_accounts"`
	NodeTypes     map[string]int `json:"node_types"`
}

type dumpAccount struct {
	KeyNibbles  string            `json:"key_nibbles"`
	LeafHash    string            `json:"leaf_hash"`
	LeafBytes   string            `json:"leaf_bytes"`
	Nonce       uint64            `json:"nonce"`
	QKCBalance  string            `json:"qkc_balance"`
	MntBalances map[string]string `json:"mnt_balances"`
	StorageRoot string            `json:"storage_root"`
	CodeHash    string            `json:"code_hash"`
}

// ─── in-memory node database built from the dump ─────────────────────────────

// flatDB is a simple key-value store that satisfies hashdb's disk interface.
// We pre-populate it with all nodes from the dump.
type flatDB struct {
	kv map[common.Hash][]byte
}

func newFlatDB(nodeStore map[string]string) (*flatDB, error) {
	db := &flatDB{kv: make(map[common.Hash][]byte, len(nodeStore))}
	for hashHex, bytesHex := range nodeStore {
		hashHex = strings.TrimPrefix(hashHex, "0x")
		bytesHex = strings.TrimPrefix(bytesHex, "0x")

		h, err := hex.DecodeString(hashHex)
		if err != nil {
			return nil, fmt.Errorf("invalid hash hex %q: %w", hashHex, err)
		}
		b, err := hex.DecodeString(bytesHex)
		if err != nil {
			return nil, fmt.Errorf("invalid bytes hex %q: %w", bytesHex, err)
		}
		db.kv[common.BytesToHash(h)] = b
	}
	return db, nil
}

// ─── trie root recomputation ──────────────────────────────────────────────────

func recomputeRoot(nodeStore map[string]string, stateRootHex string) (common.Hash, error) {
	stateRootHex = strings.TrimPrefix(stateRootHex, "0x")
	rootBytes, err := hex.DecodeString(stateRootHex)
	if err != nil {
		return common.Hash{}, fmt.Errorf("bad state root: %w", err)
	}
	stateRoot := common.BytesToHash(rootBytes)

	// Build an in-memory ethdb that holds all the dumped nodes. While loading,
	// verify each node's identity: keccak256(blob) must equal its key, since a
	// trie node is addressed by the hash of its content. This is what actually
	// validates the dump — trie.New + Hash() below cannot, because a freshly
	// opened root returns its cached open-at hash without re-hashing, making
	// root == stateRoot a tautology.
	mem := memorydb.New()
	mismatch := 0
	rootSeen := stateRoot == types.EmptyRootHash
	for hashHex, bytesHex := range nodeStore {
		hashHex = strings.TrimPrefix(hashHex, "0x")
		bytesHex = strings.TrimPrefix(bytesHex, "0x")

		h, err := hex.DecodeString(hashHex)
		if err != nil {
			return common.Hash{}, fmt.Errorf("bad hash %q: %w", hashHex, err)
		}
		b, err := hex.DecodeString(bytesHex)
		if err != nil {
			return common.Hash{}, fmt.Errorf("bad bytes %q: %w", bytesHex, err)
		}
		key := common.BytesToHash(h)
		if got := crypto.Keccak256Hash(b); got != key {
			fmt.Printf("  HASH MISMATCH key=%s  keccak256(blob)=%s\n", key.Hex(), got.Hex())
			mismatch++
		}
		if key == stateRoot {
			rootSeen = true
		}
		if err := mem.Put(h, b); err != nil {
			return common.Hash{}, fmt.Errorf("memdb put: %w", err)
		}
	}
	fmt.Printf("  Nodes hashed: %d  mismatches: %d\n", len(nodeStore), mismatch)
	if mismatch > 0 {
		return common.Hash{}, fmt.Errorf("%d node(s) do not hash to their key", mismatch)
	}
	if !rootSeen {
		return common.Hash{}, fmt.Errorf("state root %s not present in node store", stateRoot.Hex())
	}

	// Wrap in a triedb backed by hashdb (which reads from the memdb).
	diskdb := rawdb.NewDatabase(mem)
	trieDB := triedb.NewDatabase(diskdb, &triedb.Config{
		HashDB: hashdb.Defaults,
	})

	// Open the trie at the expected root.
	tr, err := trie.New(trie.TrieID(stateRoot), trieDB)
	if err != nil {
		return common.Hash{}, fmt.Errorf("trie.New: %w", err)
	}

	// Traverse the whole tree and confirm every hashed node is reachable from
	// the root: a missing node surfaces as an iterator error, and the reachable
	// count must equal the store size or the dump carries orphan nodes.
	it, err := tr.NodeIterator(nil)
	if err != nil {
		return common.Hash{}, fmt.Errorf("NodeIterator: %w", err)
	}
	reached := make(map[common.Hash]struct{}, len(nodeStore))
	for it.Next(true) {
		if h := it.Hash(); h != (common.Hash{}) {
			reached[h] = struct{}{}
		}
	}
	if err := it.Error(); err != nil {
		return common.Hash{}, fmt.Errorf("traversal error (missing node?): %w", err)
	}
	fmt.Printf("  Nodes reachable from root: %d  (store: %d)\n", len(reached), len(nodeStore))
	if len(reached) != len(nodeStore) {
		return common.Hash{}, fmt.Errorf("reachable node count %d != store size %d", len(reached), len(nodeStore))
	}

	// Compute root hash (Hash() re-hashes in-memory without writing).
	root := tr.Hash()
	return root, nil
}

// ─── account cross-check ──────────────────────────────────────────────────────

// verifyAccounts opens the trie and iterates all leaves, decoding each account
// via StateAccount.DecodeRLP then re-encoding to verify byte-level round-trip.
func verifyAccounts(nodeStore map[string]string, stateRootHex string) error {
	mem := memorydb.New()
	for hashHex, bytesHex := range nodeStore {
		hashHex = strings.TrimPrefix(hashHex, "0x")
		bytesHex = strings.TrimPrefix(bytesHex, "0x")
		h, _ := hex.DecodeString(hashHex)
		b, _ := hex.DecodeString(bytesHex)
		_ = mem.Put(h, b)
	}

	diskdb := rawdb.NewDatabase(mem)
	trieDB := triedb.NewDatabase(diskdb, &triedb.Config{HashDB: hashdb.Defaults})

	rootBytes, _ := hex.DecodeString(strings.TrimPrefix(stateRootHex, "0x"))
	stateRoot := common.BytesToHash(rootBytes)

	tr, err := trie.New(trie.TrieID(stateRoot), trieDB)
	if err != nil {
		return fmt.Errorf("trie.New: %w", err)
	}

	it, err := tr.NodeIterator(nil)
	if err != nil {
		return fmt.Errorf("NodeIterator: %w", err)
	}
	leafCount := 0
	mismatch := 0

	for it.Next(true) {
		if !it.Leaf() {
			continue
		}
		leafCount++

		leafVal := it.LeafBlob()
		var acc types.StateAccount
		if err := rlp.DecodeBytes(leafVal, &acc); err != nil {
			fmt.Printf("  DECODE ERR key=%x: %v\n", it.LeafKey(), err)
			mismatch++
			continue
		}
		reenc, err := rlp.EncodeToBytes(&acc)
		if err != nil {
			fmt.Printf("  ENCODE ERR key=%x: %v\n", it.LeafKey(), err)
			mismatch++
			continue
		}
		if !bytes.Equal(reenc, leafVal) {
			fmt.Printf("  ROUND-TRIP MISMATCH key=%x\n    orig: %x\n    reenc:%x\n",
				it.LeafKey(), leafVal, reenc)
			mismatch++
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	fmt.Printf("  Account leaves iterated: %d  mismatches: %d\n", leafCount, mismatch)
	if mismatch > 0 {
		return fmt.Errorf("%d accounts failed round-trip encode via StateAccount", mismatch)
	}
	return nil
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	inputFile := flag.String("input", "trie_dump.json", "JSON file from dump_qkc_state_trie.py")
	flag.Parse()

	// ── load JSON ──────────────────────────────────────────────────────────
	fmt.Printf("Loading %s ...\n", *inputFile)
	f, err := os.Open(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: open: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	var dump dumpFile
	if err := json.NewDecoder(f).Decode(&dump); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: JSON decode: %v\n", err)
		os.Exit(1)
	}

	stateRootStr := ""
	if v, ok := dump.Block["state_root"].(string); ok {
		stateRootStr = v
	}
	fmt.Printf("State root from dump: %s\n", stateRootStr)
	fmt.Printf("Node store entries:   %d\n", len(dump.NodeStore))
	fmt.Printf("Accounts in dump:     %d\n", dump.Stats.TotalAccounts)

	// ── recompute root ─────────────────────────────────────────────────────
	fmt.Println("\nRecomputing trie root via goshard trie package...")
	computed, err := recomputeRoot(dump.NodeStore, stateRootStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Expected root: %s\n", stateRootStr)
	fmt.Printf("Computed root: %s\n", computed.Hex())

	expected := common.HexToHash(stateRootStr)
	if computed == expected {
		fmt.Println("\n✓  ROOT HASH MATCH — goshard trie is compatible with goquarkchain")
	} else {
		fmt.Println("\n✗  ROOT HASH MISMATCH")
		os.Exit(1)
	}

	// ── account decode/re-encode round-trip ───────────────────────────────
	fmt.Println("\nVerifying account decode/re-encode round-trip via StateAccount...")
	if err := verifyAccounts(dump.NodeStore, stateRootStr); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✓  All accounts decoded and re-encoded to identical bytes")
}
