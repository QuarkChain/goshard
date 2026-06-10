// Copyright 2026-2027, QuarkChain.

package state

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

// DecodeQuarkChainAccount decodes a QKC account as stored in the QKC account trie.
func DecodeQuarkChainAccount(blob []byte, db *triedb.Database) (QuarkChainAccount, error) {
	var enc quarkChainAccountRLP
	if err := rlp.DecodeBytes(blob, &enc); err != nil {
		return QuarkChainAccount{}, err
	}
	balances, err := DecodeQuarkChainTokenBalances(enc.TokenBalances, db)
	if err != nil {
		return QuarkChainAccount{}, err
	}
	return QuarkChainAccount{
		Nonce:         enc.Nonce,
		TokenBalances: balances,
		StorageRoot:   enc.StorageRoot,
		CodeHash:      append([]byte(nil), enc.CodeHash...),
		FullShardKey:  uint32(enc.FullShardKey),
		Optional:      append([]byte(nil), enc.Optional...),
	}, nil
}

// DecodeQuarkChainTokenBalances decodes QKC's inline or token-trie balance field.
// Token-trie decoding needs the secure-trie preimages that are available for
// tries built through this package.
func DecodeQuarkChainTokenBalances(blob []byte, db *triedb.Database) (map[uint64]*big.Int, error) {
	balances := make(map[uint64]*big.Int)
	if len(blob) == 0 {
		return balances, nil
	}
	switch blob[0] {
	case 0x00:
		var pairs []quarkChainTokenBalancePair
		if err := rlp.DecodeBytes(blob[1:], &pairs); err != nil {
			return nil, err
		}
		for _, pair := range pairs {
			if pair.Balance == nil || pair.Balance.Sign() == 0 {
				continue
			}
			if pair.Balance.Sign() < 0 {
				return nil, fmt.Errorf("negative qkc token balance for token %d", pair.TokenID)
			}
			balances[pair.TokenID] = new(big.Int).Set(pair.Balance)
		}
		return balances, nil
	case 0x01:
		if len(blob) != 1+common.HashLength {
			return nil, fmt.Errorf("invalid qkc token trie balance length %d", len(blob))
		}
		if db == nil {
			return nil, errors.New("qkc token trie balances require trie database")
		}
		root := common.BytesToHash(blob[1:])
		tokenTrie, err := trie.NewSecure(root, common.Hash{}, root, db)
		if err != nil {
			return nil, err
		}
		iter := trie.NewIterator(tokenTrie.MustNodeIterator(nil))
		for iter.Next() {
			key := tokenTrie.GetKey(iter.Key)
			if len(key) == 0 {
				return nil, fmt.Errorf("missing qkc token trie preimage for leaf %x", iter.Key)
			}
			if len(key) != common.HashLength {
				return nil, fmt.Errorf("invalid qkc token trie key length %d", len(key))
			}
			var balance big.Int
			if err := rlp.DecodeBytes(iter.Value, &balance); err != nil {
				return nil, err
			}
			if balance.Sign() <= 0 {
				continue
			}
			balances[binary.BigEndian.Uint64(key[24:])] = new(big.Int).Set(&balance)
		}
		if err := iter.Err; err != nil {
			return nil, err
		}
		return balances, nil
	default:
		return nil, fmt.Errorf("unknown qkc token balance encoding selector %#x", blob[0])
	}
}

// DecodeQuarkChainTokenBalance reads one token balance from an encoded balance field.
func DecodeQuarkChainTokenBalance(blob []byte, tokenID uint64, db *triedb.Database) (*big.Int, error) {
	if len(blob) == 0 {
		return new(big.Int), nil
	}
	if blob[0] != 0x01 {
		balances, err := DecodeQuarkChainTokenBalances(blob, db)
		if err != nil {
			return nil, err
		}
		if balance := balances[tokenID]; balance != nil {
			return new(big.Int).Set(balance), nil
		}
		return new(big.Int), nil
	}
	if len(blob) != 1+common.HashLength {
		return nil, fmt.Errorf("invalid qkc token trie balance length %d", len(blob))
	}
	if db == nil {
		return nil, errors.New("qkc token trie balance lookup requires trie database")
	}
	root := common.BytesToHash(blob[1:])
	tokenTrie, err := trie.NewSecure(root, common.Hash{}, root, db)
	if err != nil {
		return nil, err
	}
	value := tokenTrie.MustGet(QuarkChainTokenIDKey(tokenID))
	if len(value) == 0 {
		return new(big.Int), nil
	}
	var balance big.Int
	if err := rlp.DecodeBytes(value, &balance); err != nil {
		return nil, err
	}
	return &balance, nil
}

// QuarkChainTokenBalance returns a copy of a token balance.
func (a QuarkChainAccount) QuarkChainTokenBalance(tokenID uint64) *big.Int {
	if a.TokenBalances == nil || a.TokenBalances[tokenID] == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(a.TokenBalances[tokenID])
}

// SetQuarkChainTokenBalance sets or deletes a token balance.
func (a *QuarkChainAccount) SetQuarkChainTokenBalance(tokenID uint64, balance *big.Int) error {
	if balance != nil && balance.Sign() < 0 {
		return errors.New("qkc token balance cannot be negative")
	}
	if a.TokenBalances == nil {
		a.TokenBalances = make(map[uint64]*big.Int)
	}
	if balance == nil || balance.Sign() == 0 {
		delete(a.TokenBalances, tokenID)
		return nil
	}
	a.TokenBalances[tokenID] = new(big.Int).Set(balance)
	return nil
}

// AddQuarkChainTokenBalance adds delta to a token balance and rejects negatives.
func (a *QuarkChainAccount) AddQuarkChainTokenBalance(tokenID uint64, delta *big.Int) error {
	if delta == nil || delta.Sign() == 0 {
		return nil
	}
	next := a.QuarkChainTokenBalance(tokenID)
	next.Add(next, delta)
	return a.SetQuarkChainTokenBalance(tokenID, next)
}

// QuarkChainCreateAddress creates the recipient for a QKC contract creation.
func QuarkChainCreateAddress(sender common.Address, nonce uint64, fullShardKey uint32) common.Address {
	encoded, err := rlp.EncodeToBytes([]interface{}{sender.Bytes(), uint64(fullShardKey), nonce})
	if err != nil {
		panic(err)
	}
	return common.BytesToAddress(crypto.Keccak256(encoded)[12:])
}
