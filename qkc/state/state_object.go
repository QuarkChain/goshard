// Copyright 2026-2027, QuarkChain.

package state

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// stateObject is one account being worked on, mirroring pyquarkchain's
// evm.state.Account (quarkchain/evm/state.py:205).
type stateObject struct {
	db      *StateDB
	address account.Recipient

	nonce        uint64
	balances     *qkcCommon.TokenBalances
	storageRoot  common.Hash
	codeHash     common.Hash
	fullShardKey uint32

	code       []byte
	codeLoaded bool
	dirtyCode  bool

	// storage is pyquarkchain's storage_cache: it holds both slots read through
	// from the trie and slots waiting to be written, which is why commit walks
	// all of it. Writing back a value that was only read is a no-op for the root.
	storage     map[common.Hash]common.Hash
	storageTrie *trie.StateTrie

	// touched and deleted are the two flags commit dispatches on. del_account
	// leaves the account touched=false, deleted=true, which is what makes it a
	// deletion rather than a blank leaf (state.py:596-610).
	touched bool
	deleted bool
}

// newObject is Account.blank_account (state.py:268): a fresh account takes the
// state's current full shard key, which then travels into the leaf and into
// contract address derivation.
func newObject(db *StateDB, addr account.Recipient, fullShardKey uint32) *stateObject {
	return &stateObject{
		db:           db,
		address:      addr,
		balances:     qkcCommon.NewEmptyTokenBalances(),
		storageRoot:  coretypes.EmptyRootHash,
		codeHash:     coretypes.EmptyCodeHash,
		fullShardKey: fullShardKey,
		storage:      make(map[common.Hash]common.Hash),
	}
}

func newObjectFromAccount(db *StateDB, addr account.Recipient, acct *Account) (*stateObject, error) {
	balances, err := qkcCommon.NewTokenBalances(acct.TokenBalances)
	if err != nil {
		return nil, fmt.Errorf("account %s: token balances: %w", addr.Hex(), err)
	}
	return &stateObject{
		db:           db,
		address:      addr,
		nonce:        acct.Nonce,
		balances:     balances,
		storageRoot:  acct.Root,
		codeHash:     acct.CodeHash,
		fullShardKey: uint32(acct.FullShardKey[0])<<24 | uint32(acct.FullShardKey[1])<<16 | uint32(acct.FullShardKey[2])<<8 | uint32(acct.FullShardKey[3]),
		storage:      make(map[common.Hash]common.Hash),
	}, nil
}

// empty is pyquarkchain's is_blank (state.py:285). Storage does not enter into
// it: an account holding nothing but storage is blank, and commit drops it.
func (o *stateObject) empty() bool {
	return o.nonce == 0 && o.balances.IsBlank() && o.codeHash == coretypes.EmptyCodeHash
}

func (o *stateObject) getCode() []byte {
	if o.codeLoaded {
		return o.code
	}
	o.codeLoaded = true
	if o.codeHash != coretypes.EmptyCodeHash {
		o.code = rawdb.ReadCode(o.db.db, o.codeHash)
		if len(o.code) == 0 {
			o.db.setError(fmt.Errorf("account %s: code %s missing from the database", o.address.Hex(), o.codeHash))
		}
	}
	return o.code
}

func (o *stateObject) setCode(code []byte) {
	o.code, o.codeHash, o.codeLoaded = code, crypto.Keccak256Hash(code), true
	o.dirtyCode = true
}

// setStorageRoot drops the open trie along with the root it was opened at, so
// the next read reopens at the new root. reset_storage overwrites the root
// directly (state.py:620), and so does reverting that overwrite.
func (o *stateObject) setStorageRoot(root common.Hash) {
	o.storageRoot, o.storageTrie = root, nil
}

func (o *stateObject) getTrie() (*trie.StateTrie, error) {
	if o.storageTrie != nil {
		return o.storageTrie, nil
	}
	owner := crypto.Keccak256Hash(o.address.Bytes())
	tr, err := trie.NewStateTrie(trie.StorageTrieID(o.db.root, owner, o.storageRoot), o.db.triedb)
	if err != nil {
		return nil, fmt.Errorf("account %s: open storage trie %s: %w", o.address.Hex(), o.storageRoot, err)
	}
	o.storageTrie = tr
	return tr, nil
}

func (o *stateObject) getState(key common.Hash) common.Hash {
	if value, ok := o.storage[key]; ok {
		return value
	}
	if o.storageRoot == coretypes.EmptyRootHash {
		o.storage[key] = common.Hash{}
		return common.Hash{}
	}
	tr, err := o.getTrie()
	if err != nil {
		o.db.setError(err)
		return common.Hash{}
	}
	// GetStorage strips the RLP string wrapper, leaving the big-endian integer
	// with its leading zeros already gone — the encoding genesis_alloc.go writes.
	enc, err := tr.GetStorage(common.Address{}, key.Bytes())
	if err != nil {
		o.db.setError(fmt.Errorf("account %s: read storage %s: %w", o.address.Hex(), key, err))
		return common.Hash{}
	}
	value := common.BytesToHash(enc)
	o.storage[key] = value
	return value
}

// commitStorage flushes the pending slots and republishes the storage root,
// mirroring Account.commit (state.py:235-243). It returns the storage trie's
// nodes when there is anything to merge.
func (o *stateObject) commitStorage() (*trienode.NodeSet, error) {
	if len(o.storage) == 0 {
		return nil, nil
	}
	tr, err := o.getTrie()
	if err != nil {
		return nil, err
	}
	for key, value := range o.storage {
		if value == (common.Hash{}) {
			// A zero slot is absent, not stored as zero.
			if err := tr.DeleteStorage(common.Address{}, key.Bytes()); err != nil {
				return nil, fmt.Errorf("account %s: delete storage %s: %w", o.address.Hex(), key, err)
			}
			continue
		}
		trimmed := common.TrimLeftZeroes(value.Bytes())
		if err := tr.UpdateStorage(common.Address{}, key.Bytes(), trimmed); err != nil {
			return nil, fmt.Errorf("account %s: write storage %s: %w", o.address.Hex(), key, err)
		}
	}
	o.storage = make(map[common.Hash]common.Hash)

	root, set := tr.Commit(false)
	o.storageRoot = root
	// The trie is spent once committed; the next read reopens it at the new root.
	o.storageTrie = nil
	return set, nil
}

func (o *stateObject) encode() ([]byte, error) {
	blob, err := o.balances.SerializeToBytes()
	if err != nil {
		return nil, fmt.Errorf("account %s: %w", o.address.Hex(), err)
	}
	acct := &Account{
		Nonce:         o.nonce,
		TokenBalances: blob,
		Root:          o.storageRoot,
		CodeHash:      o.codeHash,
	}
	acct.FullShardKey[0] = byte(o.fullShardKey >> 24)
	acct.FullShardKey[1] = byte(o.fullShardKey >> 16)
	acct.FullShardKey[2] = byte(o.fullShardKey >> 8)
	acct.FullShardKey[3] = byte(o.fullShardKey)
	return rlp.EncodeToBytes(acct)
}
