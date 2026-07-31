// Copyright 2026-2027, QuarkChain.

package qkc

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// genesisAllocRoot returns the state root a genesis allocation hashes to without
// persisting anything, mirroring how geth splits hashing from flushing
// (core/genesis.go hashAlloc/flushAlloc): the derived state is built in an
// ephemeral database and discarded.
func genesisAllocRoot(alloc map[account.Address]config.Allocation) (common.Hash, error) {
	return commitGenesisAlloc(rawdb.NewMemoryDatabase(), alloc)
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

	tdb := triedb.NewDatabase(db, triedb.HashDefaults)
	defer tdb.Close()

	stateTrie, err := trie.NewStateTrie(trie.StateTrieID(coretypes.EmptyRootHash), tdb)
	if err != nil {
		return common.Hash{}, fmt.Errorf("open genesis state trie: %w", err)
	}
	nodes := trienode.NewMergedNodeSet()
	batch := db.NewBatch()

	for addr, allocation := range alloc {
		acct, storageNodes, err := genesisAccount(tdb, batch, addr, allocation)
		if err != nil {
			return common.Hash{}, fmt.Errorf("genesis account %s: %w", addr.ToHex(), err)
		}
		// Match pyquarkchain's account-existence rule: storage and the full shard
		// key do not keep an otherwise blank account in the state trie. Its storage
		// nodes go with it — dropped before the merge, since no leaf reaches them.
		if acct.Nonce == 0 && len(acct.TokenBalances) == 0 && acct.CodeHash == coretypes.EmptyCodeHash {
			continue
		}
		if storageNodes != nil {
			if err := nodes.Merge(storageNodes); err != nil {
				return common.Hash{}, err
			}
		}
		enc, err := rlp.EncodeToBytes(acct)
		if err != nil {
			return common.Hash{}, fmt.Errorf("genesis account %s: encode: %w", addr.ToHex(), err)
		}
		// The state trie is keyed by the recipient; StateTrie hashes the key, as
		// pyquarkchain's SecureTrie does.
		stateTrie.MustUpdate(addr.Recipient.Bytes(), enc)
	}

	root, set := stateTrie.Commit(false)
	if set != nil {
		if err := nodes.Merge(set); err != nil {
			return common.Hash{}, err
		}
	}
	if err := tdb.Update(root, coretypes.EmptyRootHash, 0, nodes, nil); err != nil {
		return common.Hash{}, fmt.Errorf("update genesis trie nodes: %w", err)
	}
	if err := tdb.Commit(root, false); err != nil {
		return common.Hash{}, fmt.Errorf("commit genesis state: %w", err)
	}
	if err := batch.Write(); err != nil {
		return common.Hash{}, fmt.Errorf("write genesis code: %w", err)
	}
	return root, nil
}

// genesisAccount turns one allocation entry into a state-trie leaf, returning the
// storage trie's nodes when the entry has storage.
func genesisAccount(tdb *triedb.Database, batch ethdb.Batch, addr account.Address, allocation config.Allocation) (*state.Account, *trienode.NodeSet, error) {
	acct := &state.Account{Root: coretypes.EmptyRootHash, CodeHash: coretypes.EmptyCodeHash}
	binary.BigEndian.PutUint32(acct.FullShardKey[:], addr.FullShardKey)

	balances := qkcCommon.NewEmptyTokenBalances()
	for token, value := range allocation.Balances {
		if value == nil || value.Sign() == 0 {
			continue
		}
		if value.Sign() < 0 {
			return nil, nil, fmt.Errorf("token %s: negative balance %s", token, value)
		}
		amount, overflow := uint256.FromBig(value)
		if overflow {
			return nil, nil, fmt.Errorf("token %s: balance overflows 256 bits (%s)", token, value)
		}
		balances.SetValue(amount, qkcCommon.TokenIDEncode(token))
	}
	blob, err := balances.SerializeToBytes()
	if err != nil {
		return nil, nil, fmt.Errorf("encode token balances: %w", err)
	}
	acct.TokenBalances = blob

	// An allocated contract starts at nonce 1, as in pyquarkchain
	// (quarkchain/genesis.py:68).
	if allocation.CodePresent || allocation.Code != nil {
		acct.CodeHash = crypto.Keccak256Hash(allocation.Code)
		acct.Nonce = 1
		rawdb.WriteCode(batch, acct.CodeHash, allocation.Code)
	}
	if len(allocation.Storage) == 0 {
		return acct, nil, nil
	}

	owner := crypto.Keccak256Hash(addr.Recipient.Bytes())
	storageTrie, err := trie.NewStateTrie(trie.StorageTrieID(coretypes.EmptyRootHash, owner, coretypes.EmptyRootHash), tdb)
	if err != nil {
		return nil, nil, fmt.Errorf("open storage trie: %w", err)
	}
	for key, value := range allocation.Storage {
		// Slots hold integers: the stored value is the big-endian number with its
		// leading zeros stripped, and a zero value is simply absent.
		trimmed := new(big.Int).SetBytes(value[:])
		if trimmed.Sign() == 0 {
			continue
		}
		enc, err := rlp.EncodeToBytes(trimmed.Bytes())
		if err != nil {
			return nil, nil, fmt.Errorf("encode storage slot %s: %w", key.Hex(), err)
		}
		storageTrie.MustUpdate(key.Bytes(), enc)
	}
	storageRoot, set := storageTrie.Commit(false)
	acct.Root = storageRoot
	return acct, set, nil
}
