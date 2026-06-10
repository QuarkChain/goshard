// Copyright 2026-2027, QuarkChain.

package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

const QuarkChainTokenTrieThreshold = 16

// QuarkChainAccount is the QKC EVM account shape stored in the account trie.
// Storage and Code are genesis-construction inputs and are not encoded directly.
type QuarkChainAccount struct {
	Nonce         uint64
	TokenBalances map[uint64]*big.Int
	StorageRoot   common.Hash
	Storage       map[common.Hash]common.Hash
	CodeHash      []byte
	Code          []byte
	FullShardKey  uint32
	Optional      []byte
}

type quarkChainAccountRLP struct {
	Nonce         uint64
	TokenBalances []byte
	StorageRoot   common.Hash
	CodeHash      []byte
	FullShardKey  quarkChainUint32
	Optional      []byte
}

type quarkChainUint32 uint32

func (u quarkChainUint32) EncodeRLP(w io.Writer) error {
	var enc [5]byte
	enc[0] = 0x84
	binary.BigEndian.PutUint32(enc[1:], uint32(u))
	_, err := w.Write(enc[:])
	return err
}

func (u *quarkChainUint32) DecodeRLP(s *rlp.Stream) error {
	raw, err := s.Raw()
	if err != nil {
		return err
	}
	if len(raw) != 5 || raw[0] != 0x84 {
		return fmt.Errorf("invalid qkc uint32 rlp encoding: %x", raw)
	}
	*u = quarkChainUint32(binary.BigEndian.Uint32(raw[1:]))
	return nil
}

type quarkChainTokenBalancePair struct {
	TokenID uint64
	Balance *big.Int
}

// NewQuarkChainMemoryTrieDB returns an ephemeral trie database suitable for
// computing QKC state roots without polluting an existing chain database.
func NewQuarkChainMemoryTrieDB() *triedb.Database {
	config := *triedb.HashDefaults
	config.Preimages = true
	return triedb.NewDatabase(rawdb.NewMemoryDatabase(), &config)
}

// QuarkChainTokenIDKey returns QKC's 32-byte big-endian token trie key.
func QuarkChainTokenIDKey(tokenID uint64) []byte {
	key := make([]byte, 32)
	binary.BigEndian.PutUint64(key[24:], tokenID)
	return key
}

// EncodeQuarkChainTokenBalances serializes token balances using QKC's account
// field encoding. Large balance sets are committed into a token secure trie.
func EncodeQuarkChainTokenBalances(balances map[uint64]*big.Int, db *triedb.Database) ([]byte, error) {
	pairs, err := quarkChainTokenPairs(balances)
	if err != nil {
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, nil
	}
	if len(pairs) <= QuarkChainTokenTrieThreshold {
		payload, err := rlp.EncodeToBytes(pairs)
		if err != nil {
			return nil, err
		}
		return append([]byte{0x00}, payload...), nil
	}
	if db == nil {
		db = NewQuarkChainMemoryTrieDB()
	}
	tokenTrie, err := trie.NewSecure(types.EmptyRootHash, common.Hash{}, types.EmptyRootHash, db)
	if err != nil {
		return nil, err
	}
	for _, pair := range pairs {
		value, err := rlp.EncodeToBytes(pair.Balance)
		if err != nil {
			return nil, err
		}
		tokenTrie.MustUpdate(QuarkChainTokenIDKey(pair.TokenID), value)
	}
	root, nodes := tokenTrie.Commit(false)
	if err := quarkChainCommitTrie(db, root, types.EmptyRootHash, nodes); err != nil {
		return nil, err
	}
	encoded := make([]byte, 33)
	encoded[0] = 0x01
	copy(encoded[1:], root.Bytes())
	return encoded, nil
}

// BuildQuarkChainStorageRoot builds the QKC storage trie root for genesis data.
func BuildQuarkChainStorageRoot(storage map[common.Hash]common.Hash, db *triedb.Database) (common.Hash, error) {
	if len(storage) == 0 {
		return types.EmptyRootHash, nil
	}
	if db == nil {
		db = NewQuarkChainMemoryTrieDB()
	}
	storageTrie, err := trie.NewSecure(types.EmptyRootHash, common.Hash{}, types.EmptyRootHash, db)
	if err != nil {
		return common.Hash{}, err
	}
	keys := make([]common.Hash, 0, len(storage))
	for key := range storage {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b common.Hash) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, key := range keys {
		value := storage[key]
		if value == (common.Hash{}) {
			continue
		}
		encoded, err := rlp.EncodeToBytes(bytes.TrimLeft(value[:], "\x00"))
		if err != nil {
			return common.Hash{}, err
		}
		storageTrie.MustUpdate(key[:], encoded)
	}
	root, nodes := storageTrie.Commit(false)
	if err := quarkChainCommitTrie(db, root, types.EmptyRootHash, nodes); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}

// BuildQuarkChainStateRoot builds the QKC account trie root for the provided
// accounts. The trie key is the 20-byte EVM recipient address.
func BuildQuarkChainStateRoot(accounts map[common.Address]QuarkChainAccount, db *triedb.Database) (common.Hash, error) {
	if len(accounts) == 0 {
		return types.EmptyRootHash, nil
	}
	if db == nil {
		db = NewQuarkChainMemoryTrieDB()
	}
	accountTrie, err := trie.NewSecure(types.EmptyRootHash, common.Hash{}, types.EmptyRootHash, db)
	if err != nil {
		return common.Hash{}, err
	}
	addresses := make([]common.Address, 0, len(accounts))
	for addr := range accounts {
		addresses = append(addresses, addr)
	}
	slices.SortFunc(addresses, func(a, b common.Address) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, addr := range addresses {
		blob, err := EncodeQuarkChainAccount(accounts[addr], db)
		if err != nil {
			return common.Hash{}, fmt.Errorf("encode qkc account %s: %w", addr, err)
		}
		accountTrie.MustUpdate(addr.Bytes(), blob)
	}
	root, nodes := accountTrie.Commit(false)
	if err := quarkChainCommitTrie(db, root, types.EmptyRootHash, nodes); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}

// EncodeQuarkChainAccount encodes a QKC account as it appears in the QKC state trie.
func EncodeQuarkChainAccount(account QuarkChainAccount, db *triedb.Database) ([]byte, error) {
	tokenBalances, err := EncodeQuarkChainTokenBalances(account.TokenBalances, db)
	if err != nil {
		return nil, err
	}
	storageRoot := account.StorageRoot
	if len(account.Storage) != 0 {
		storageRoot, err = BuildQuarkChainStorageRoot(account.Storage, db)
		if err != nil {
			return nil, err
		}
	}
	if storageRoot == (common.Hash{}) {
		storageRoot = types.EmptyRootHash
	}
	codeHash := account.CodeHash
	if len(codeHash) == 0 {
		if len(account.Code) != 0 {
			hash := crypto.Keccak256Hash(account.Code)
			codeHash = hash.Bytes()
		} else {
			codeHash = types.EmptyCodeHash.Bytes()
		}
	}
	if len(codeHash) != common.HashLength {
		return nil, fmt.Errorf("invalid qkc code hash length %d", len(codeHash))
	}
	return rlp.EncodeToBytes(quarkChainAccountRLP{
		Nonce:         account.Nonce,
		TokenBalances: tokenBalances,
		StorageRoot:   storageRoot,
		CodeHash:      codeHash,
		FullShardKey:  quarkChainUint32(account.FullShardKey),
		Optional:      account.Optional,
	})
}

func quarkChainTokenPairs(balances map[uint64]*big.Int) ([]quarkChainTokenBalancePair, error) {
	if len(balances) == 0 {
		return nil, nil
	}
	ids := make([]uint64, 0, len(balances))
	for tokenID, balance := range balances {
		if balance == nil || balance.Sign() == 0 {
			continue
		}
		if balance.Sign() < 0 {
			return nil, errors.New("qkc token balance cannot be negative")
		}
		ids = append(ids, tokenID)
	}
	slices.Sort(ids)
	pairs := make([]quarkChainTokenBalancePair, 0, len(ids))
	for _, tokenID := range ids {
		pairs = append(pairs, quarkChainTokenBalancePair{
			TokenID: tokenID,
			Balance: new(big.Int).Set(balances[tokenID]),
		})
	}
	return pairs, nil
}

func quarkChainCommitTrie(db *triedb.Database, root common.Hash, parent common.Hash, nodes *trienode.NodeSet) error {
	if db == nil || nodes == nil {
		return nil
	}
	return db.Update(root, parent, 0, trienode.NewWithNodeSet(nodes), nil)
}
