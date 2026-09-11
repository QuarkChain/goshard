// Copyright 2026-2027, QuarkChain.

package qkc

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

// genesisAllocRoot returns the state root a genesis allocation hashes to without
// persisting anything, mirroring how geth splits hashing from flushing
// (core/genesis.go hashAlloc/flushAlloc): the derived state is built in an
// ephemeral database and discarded.
func genesisAllocRoot(alloc map[account.Address]config.Allocation) (common.Hash, error) {
	return commitGenesisAlloc(rawdb.NewMemoryDatabase(), alloc)
}

// CheckGenesisState reports whether the state a genesis block's meta seals can
// still be opened against db. It is the reopen counterpart of the commit below:
// the genesis block is stored as an identity, separately from the state it names,
// so a datadir can hold the block and have lost the trie underneath it.
//
// Only the root node is resolved, as geth's HasState does — a trie whose root
// resolves but whose interior was truncated still reports as present, and fails
// later when the missing node is walked into. Catching that costs a full traversal
// of the allocation on every boot.
//
// An empty allocation hashes to the empty root, which is never written down; it is
// present by definition.
func CheckGenesisState(db ethdb.Database, root common.Hash) error {
	if root == coretypes.EmptyRootHash {
		return nil
	}
	tdb := triedb.NewDatabase(db, triedb.HashDefaults)
	defer tdb.Close()
	if _, err := trie.NewStateTrie(trie.StateTrieID(root), tdb); err != nil {
		return fmt.Errorf("open genesis state %s: %w", root, err)
	}
	return nil
}

// commitGenesisAlloc materializes the genesis allocation into db and returns the
// resulting state root — pyquarkchain's genesis state write
// (quarkchain/genesis.py:55-87) over geth's trie.
func commitGenesisAlloc(db ethdb.Database, alloc map[account.Address]config.Allocation) (common.Hash, error) {
	seenRecipients := make(map[account.Recipient]account.Address, len(alloc))
	for addr := range alloc {
		if other, ok := seenRecipients[addr.Recipient]; ok {
			first, second := other.ToHex(), addr.ToHex()
			if second < first {
				first, second = second, first
			}
			return common.Hash{}, fmt.Errorf("duplicate recipient %s in genesis allocation addresses %s and %s", addr.Recipient, first, second)
		}
		seenRecipients[addr.Recipient] = addr
	}

	sdb := state.NewDatabase(db)
	defer sdb.TrieDB().Close()
	statedb, err := state.New(coretypes.EmptyRootHash, sdb)
	if err != nil {
		return common.Hash{}, fmt.Errorf("open genesis state trie: %w", err)
	}
	for addr, allocation := range alloc {
		statedb.SetFullShardKey(addr.FullShardKey)
		// An allocated contract starts at nonce 1.
		if allocation.CodePresent || allocation.Code != nil {
			statedb.SetCode(addr.Recipient, allocation.Code)
			statedb.SetNonce(addr.Recipient, 1)
		}
		for key, value := range allocation.Storage {
			statedb.SetState(addr.Recipient, key, value)
		}
		for token, value := range allocation.Balances {
			// The name is checked even for an entry that contributes no balance, so a
			// malformed allocation is reported rather than silently dropped.
			tokenID, err := qkcCommon.TokenIDEncodeChecked(token)
			if err != nil {
				return common.Hash{}, fmt.Errorf("genesis account %s: %w", addr.ToHex(), err)
			}
			if value != nil {
				statedb.DeltaTokenBalance(addr.Recipient, tokenID, value)
			}
		}
	}
	root, err := statedb.Commit(0)
	if err != nil {
		return common.Hash{}, fmt.Errorf("commit genesis state: %w", err)
	}
	if err := sdb.TrieDB().Commit(root, false); err != nil {
		return common.Hash{}, fmt.Errorf("flush genesis state: %w", err)
	}
	return root, nil
}
