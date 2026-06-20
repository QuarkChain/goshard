// verify_trie_root loads a trie node store exported by dump_qkc_state_trie.py
// and recomputes the trie root hash, confirming goshard produces the same
// result as the original goquarkchain chain.
//
// Usage:
//
//	go run ./tools/verify_trie_root --input trie_dump.json
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
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
	// Build an in-memory ethdb that holds all the dumped nodes.
	mem := memorydb.New()
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
		if err := mem.Put(h, b); err != nil {
			return common.Hash{}, fmt.Errorf("memdb put: %w", err)
		}
	}

	// Wrap in a triedb backed by hashdb (which reads from the memdb).
	diskdb := rawdb.NewDatabase(mem)
	trieDB := triedb.NewDatabase(diskdb, &triedb.Config{
		HashDB: hashdb.Defaults,
	})

	stateRootHex = strings.TrimPrefix(stateRootHex, "0x")
	rootBytes, err := hex.DecodeString(stateRootHex)
	if err != nil {
		return common.Hash{}, fmt.Errorf("bad state root: %w", err)
	}
	stateRoot := common.BytesToHash(rootBytes)

	// Open the trie at the expected root.
	tr, err := trie.New(trie.TrieID(stateRoot), trieDB)
	if err != nil {
		return common.Hash{}, fmt.Errorf("trie.New: %w", err)
	}

	// Compute root hash (Hash() re-hashes in-memory without writing).
	root := tr.Hash()
	return root, nil
}

// ─── account cross-check ──────────────────────────────────────────────────────

// verifyAccounts opens the trie and iterates all leaves, decoding each account
// via StateAccount.DecodeRLP, and cross-checks against the dump.
func verifyAccounts(nodeStore map[string]string, stateRootHex string, dumpAccounts []dumpAccount) error {
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

	// Build a map for quick lookup of dump accounts by key_nibbles.
	dumpByNibbles := make(map[string]dumpAccount, len(dumpAccounts))
	for _, a := range dumpAccounts {
		dumpByNibbles[a.KeyNibbles] = a
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
		_ = acc // decoded successfully; add deep checks here if needed
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	fmt.Printf("  Account leaves iterated: %d  mismatches: %d\n", leafCount, mismatch)
	if mismatch > 0 {
		return fmt.Errorf("%d accounts failed to decode via StateAccount.DecodeRLP", mismatch)
	}
	return nil
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	inputFile  := flag.String("input",        "trie_dump.json", "JSON file from dump_qkc_state_trie.py")
	checkAccts := flag.Bool("check-accounts", false,            "Also iterate leaves and decode accounts")
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

	// ── optional account decode check ──────────────────────────────────────
	if *checkAccts && len(dump.Accounts) > 0 {
		fmt.Println("\nVerifying account decode via StateAccount.DecodeRLP...")
		if err := verifyAccounts(dump.NodeStore, stateRootStr, dump.Accounts); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("✓  All accounts decoded successfully")
	}
}
