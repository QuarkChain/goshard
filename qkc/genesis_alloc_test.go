// Copyright 2026-2027, QuarkChain.

package qkc

import (
	"bytes"
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

// TestGenesisAllocRootFromFixture pins the genesis state root of each real
// network against pyquarkchain's own
// GenesisManager.create_minor_block().meta.hash_evm_state_root. This is what
// proves the QuarkChain account encoding — per-token balance blob, fixed-width
// full shard key and all — is byte-identical to Python's. Regeneration:
// qkc/config/singularity/README.md.
func TestGenesisAllocRootFromFixture(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		root string
	}{
		{
			name: "mainnet",
			path: fixtureMainnet,
			root: "0x699737e3597ea304b7d2e2f4ecbf8ab6348688287c59cec8599cf7a4f7c82153",
		},
		{
			name: "devnet",
			path: fixtureDevnet,
			root: "0x76b7e413ee8a10d27ad5158ce91b8b8e61d6af8805b965a4ce11b93db0286ed1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadFixture(t, tc.path)
			alloc := cfg.Quarkchain.GetShardConfigByFullShardID(firstShardID).Genesis.Alloc
			if len(alloc) == 0 {
				t.Fatal("fixture has an empty ALLOC")
			}

			root, err := genesisAllocRoot(alloc)
			if err != nil {
				t.Fatalf("genesisAllocRoot: %v", err)
			}
			if root != common.HexToHash(tc.root) {
				t.Errorf("genesis state root mismatch\n got %s\nwant %s", root.Hex(), tc.root)
			}
		})
	}
}

// TestCommitGenesisAllocPersists: committing leaves the state readable from the
// database — the trie root resolves and the accounts are where the trie says.
func TestCommitGenesisAllocPersists(t *testing.T) {
	cfg := loadFixture(t, fixtureMainnet)
	alloc := cfg.Quarkchain.GetShardConfigByFullShardID(firstShardID).Genesis.Alloc

	db := rawdb.NewMemoryDatabase()
	root, err := commitGenesisAlloc(db, alloc)
	if err != nil {
		t.Fatalf("commitGenesisAlloc: %v", err)
	}
	// Hashing without persisting must agree with the committed root.
	if hashed, err := genesisAllocRoot(alloc); err != nil || hashed != root {
		t.Fatalf("genesisAllocRoot = %s, %v, want the committed root %s", hashed, err, root)
	}
	if !rawdb.HasLegacyTrieNode(db, root) {
		t.Fatalf("committed state root %s is not in the database", root)
	}
}

func TestCommitGenesisAllocRejectsDuplicateRecipient(t *testing.T) {
	recipient := account.BytesToIdentityRecipient(common.FromHex("32c53c6c2b57a4a5b0b5ccc9d82c8fff7f0a1122"))
	first := account.NewAddress(recipient, 0)
	second := account.NewAddress(recipient, 1)
	alloc := map[account.Address]config.Allocation{
		first:  {Balances: map[string]*big.Int{"QKC": big.NewInt(1)}},
		second: {Balances: map[string]*big.Int{"QKC": big.NewInt(2)}},
	}

	_, err := commitGenesisAlloc(rawdb.NewMemoryDatabase(), alloc)
	if err == nil || !strings.Contains(err.Error(), "duplicate recipient") ||
		!strings.Contains(err.Error(), first.ToHex()) || !strings.Contains(err.Error(), second.ToHex()) {
		t.Fatalf("commitGenesisAlloc err = %v, want duplicate recipient error naming both addresses", err)
	}
}

func TestCommitGenesisAllocSkipsBlankAccounts(t *testing.T) {
	addr := account.CreatEmptyAddress(1)
	for _, tc := range []struct {
		name       string
		allocation config.Allocation
	}{
		{name: "empty"},
		{
			name: "zero balance",
			allocation: config.Allocation{
				Balances: map[string]*big.Int{"QKC": new(big.Int)},
			},
		},
		{
			name: "storage only",
			allocation: config.Allocation{
				Storage: map[common.Hash]common.Hash{
					common.HexToHash("0x01"): common.HexToHash("0x02"),
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, err := commitGenesisAlloc(rawdb.NewMemoryDatabase(), map[account.Address]config.Allocation{
				addr: tc.allocation,
			})
			if err != nil {
				t.Fatalf("commitGenesisAlloc: %v", err)
			}
			if root != coretypes.EmptyRootHash {
				t.Errorf("state root = %s, want empty root %s", root, coretypes.EmptyRootHash)
			}
		})
	}
}

func TestCommitGenesisAllocPreservesExplicitEmptyCode(t *testing.T) {
	var allocation config.Allocation
	if err := json.Unmarshal([]byte(`{"code":"0x"}`), &allocation); err != nil {
		t.Fatalf("decode allocation: %v", err)
	}
	addr := account.CreatEmptyAddress(1)
	db := rawdb.NewMemoryDatabase()
	root, err := commitGenesisAlloc(db, map[account.Address]config.Allocation{addr: allocation})
	if err != nil {
		t.Fatalf("commitGenesisAlloc: %v", err)
	}
	acct := readAccount(t, db, root, addr)
	if acct.Nonce != 1 || acct.CodeHash != coretypes.EmptyCodeHash {
		t.Errorf("account = nonce %d/code %s, want nonce 1 with empty code hash", acct.Nonce, acct.CodeHash)
	}
}

// TestGenesisAllocAccountEncoding covers the pieces the real fixtures cannot:
// contract code (nonce 1) and storage slots. The account layout is asserted by
// decoding the leaf back out of the trie.
func TestGenesisAllocAccountEncoding(t *testing.T) {
	addr := account.NewAddress(
		account.BytesToIdentityRecipient(common.FromHex("32c53c6c2b57a4a5b0b5ccc9d82c8fff7f0a1122")),
		0x000075b2,
	)
	code := []byte{0x60, 0x00, 0x60, 0x00}
	slot := common.HexToHash("0x01")
	alloc := map[account.Address]config.Allocation{
		addr: {
			Balances: map[string]*big.Int{"QKC": big.NewInt(1000)},
			Code:     code,
			Storage:  map[common.Hash]common.Hash{slot: common.HexToHash("0xdead")},
		},
	}

	db := rawdb.NewMemoryDatabase()
	root, err := commitGenesisAlloc(db, alloc)
	if err != nil {
		t.Fatalf("commitGenesisAlloc: %v", err)
	}

	acct := readAccount(t, db, root, addr)
	if acct.Nonce != 1 {
		t.Errorf("nonce = %d, want 1 for an account with code", acct.Nonce)
	}
	if want := crypto.Keccak256Hash(code); acct.CodeHash != want {
		t.Errorf("code hash = %s, want %s", acct.CodeHash, want)
	}
	if got := rawdb.ReadCode(db, acct.CodeHash); len(got) != len(code) {
		t.Errorf("stored code = %x, want %x", got, code)
	}
	if acct.Root == coretypes.EmptyRootHash {
		t.Error("storage root is empty, want the slot's trie root")
	}
	// The full shard key is a fixed four-byte string, leading zeros included.
	if want := [4]byte{0x00, 0x00, 0x75, 0xb2}; acct.FullShardKey != want {
		t.Errorf("full shard key = %x, want %x", acct.FullShardKey, want)
	}
	if len(acct.TokenBalances) == 0 || acct.TokenBalances[0] != 0x00 {
		t.Errorf("token balances blob = %x, want the 0x00-tagged list form", acct.TokenBalances)
	}

	// An account without code keeps nonce 0 and the empty code hash.
	plain := account.CreatEmptyAddress(1)
	db2 := rawdb.NewMemoryDatabase()
	root2, err := commitGenesisAlloc(db2, map[account.Address]config.Allocation{
		plain: {Balances: map[string]*big.Int{"QKC": big.NewInt(7)}},
	})
	if err != nil {
		t.Fatalf("commitGenesisAlloc(plain): %v", err)
	}
	bare := readAccount(t, db2, root2, plain)
	if bare.Nonce != 0 || bare.CodeHash != coretypes.EmptyCodeHash || bare.Root != coretypes.EmptyRootHash {
		t.Errorf("plain account = %+v, want nonce 0 with empty code and storage", bare)
	}
}

// TestCommitGenesisAllocPersistsStorage: an allocated slot survives the commit.
// The account leaf naming a storage root is not enough — the storage trie's own
// nodes have to reach the disk, or every read of that contract's state fails
// once the genesis database is reopened.
func TestCommitGenesisAllocPersistsStorage(t *testing.T) {
	addr := account.NewAddress(
		account.BytesToIdentityRecipient(common.FromHex("32c53c6c2b57a4a5b0b5ccc9d82c8fff7f0a1122")),
		0x000075b2,
	)
	slot := common.HexToHash("0x01")
	value := common.HexToHash("0xdead")
	db := rawdb.NewMemoryDatabase()
	root, err := commitGenesisAlloc(db, map[account.Address]config.Allocation{
		addr: {
			Balances: map[string]*big.Int{"QKC": big.NewInt(1000)},
			Code:     []byte{0x60, 0x00, 0x60, 0x00},
			Storage:  map[common.Hash]common.Hash{slot: value},
		},
	})
	if err != nil {
		t.Fatalf("commitGenesisAlloc: %v", err)
	}
	acct := readAccount(t, db, root, addr)
	if acct.Root == coretypes.EmptyRootHash {
		t.Fatal("storage root is empty, want the slot's trie root")
	}
	if !rawdb.HasLegacyTrieNode(db, acct.Root) {
		t.Errorf("storage root %s is not in the database", acct.Root)
	}

	tdb := triedb.NewDatabase(db, triedb.HashDefaults)
	defer tdb.Close()
	owner := crypto.Keccak256Hash(addr.Recipient.Bytes())
	storage, err := trie.NewStateTrie(trie.StorageTrieID(root, owner, acct.Root), tdb)
	if err != nil {
		t.Fatalf("open storage trie at %s: %v", acct.Root, err)
	}
	got, err := storage.GetStorage(common.Address{}, slot.Bytes())
	if err != nil {
		t.Fatalf("read slot %s: %v", slot, err)
	}
	// Slots hold integers: the value is stored with its leading zeros stripped.
	if want := value.Bytes()[common.HashLength-2:]; !bytes.Equal(got, want) {
		t.Errorf("slot %s = %x, want %x", slot, got, want)
	}
}

// readAccount decodes one leaf back out of a committed state trie.
func readAccount(t *testing.T, db ethdb.Database, root common.Hash, addr account.Address) *state.Account {
	t.Helper()
	tdb := triedb.NewDatabase(db, triedb.HashDefaults)
	defer tdb.Close()

	tr, err := trie.NewStateTrie(trie.StateTrieID(root), tdb)
	if err != nil {
		t.Fatalf("open state trie at %s: %v", root, err)
	}
	// StateTrie hashes the key itself, as pyquarkchain's SecureTrie does.
	enc := tr.MustGet(addr.Recipient.Bytes())
	if len(enc) == 0 {
		t.Fatalf("account %s missing from the trie", addr.ToHex())
	}
	acct := new(state.Account)
	if err := rlp.DecodeBytes(enc, acct); err != nil {
		t.Fatalf("decode account: %v", err)
	}
	return acct
}
