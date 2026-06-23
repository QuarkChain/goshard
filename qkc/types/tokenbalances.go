// Copyright 2026-2027, QuarkChain.

// TokenBalances is a map-only port of pyquarkchain's TokenBalanceMap
// (quarkchain/core.py), which serializes as PrependedSizeMap(4, biguint, biguint)
// with zero values skipped and keys kept sorted for determinism. goquarkchain's
// equivalent is trie-backed and drags in triedb; the root genesis header only ever
// needs the (empty) map encoding, so this keeps just the map form.

package types

import (
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// TokenBalances maps a token id to a balance. It implements serialize.Serializable
// so it can be embedded in RootBlockHeader and hashed byte-identically to
// pyquarkchain's TokenBalanceMap.
type TokenBalances struct {
	balances map[uint64]*big.Int
}

// NewEmptyTokenBalances returns an empty balance map. Its serialization is the
// four-byte zero count, matching pyquarkchain's TokenBalanceMap({}).
func NewEmptyTokenBalances() *TokenBalances {
	return &TokenBalances{balances: make(map[uint64]*big.Int)}
}

// NewTokenBalances returns a balance map copied from data. nil balances and nil
// data are tolerated; callers retain ownership of data.
func NewTokenBalances(data map[uint64]*big.Int) *TokenBalances {
	t := NewEmptyTokenBalances()
	for k, v := range data {
		if v == nil {
			continue
		}
		t.balances[k] = new(big.Int).Set(v)
	}
	return t
}

// Balances returns a copy of the underlying token→balance map.
func (t *TokenBalances) Balances() map[uint64]*big.Int {
	out := make(map[uint64]*big.Int, len(t.balances))
	for k, v := range t.balances {
		out[k] = new(big.Int).Set(v)
	}
	return out
}

// Len reports the number of non-zero balances, i.e. how many entries are written
// by Serialize.
func (t *TokenBalances) Len() int {
	n := 0
	for _, v := range t.balances {
		if v != nil && v.Sign() != 0 {
			n++
		}
	}
	return n
}

// sortedNonZeroKeys returns the token ids with a non-zero balance, ascending.
// Zero balances are skipped to mirror pyquarkchain's skip_func (k, v) -> v == 0.
func (t *TokenBalances) sortedNonZeroKeys() []uint64 {
	keys := make([]uint64, 0, len(t.balances))
	for k, v := range t.balances {
		if v != nil && v.Sign() != 0 {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// Serialize encodes the map as PrependedSizeMap(4, biguint, biguint): a four-byte
// big-endian count of non-zero entries, followed by each (token id, balance) pair
// in ascending key order, both as biguint.
func (t *TokenBalances) Serialize(w *[]byte) error {
	keys := t.sortedNonZeroKeys()
	if err := serialize.Serialize(w, uint32(len(keys))); err != nil {
		return err
	}
	for _, k := range keys {
		if err := serialize.Serialize(w, new(big.Int).SetUint64(k)); err != nil {
			return err
		}
		if err := serialize.Serialize(w, t.balances[k]); err != nil {
			return err
		}
	}
	return nil
}

// Deserialize is the inverse of Serialize. Zero balances are dropped on read to
// keep the round-trip canonical, matching pyquarkchain's skip_func on decode.
func (t *TokenBalances) Deserialize(bb *serialize.ByteBuffer) error {
	t.balances = make(map[uint64]*big.Int)
	num, err := bb.GetUInt32()
	if err != nil {
		return err
	}
	for i := uint32(0); i < num; i++ {
		k := new(big.Int)
		if err := serialize.Deserialize(bb, k); err != nil {
			return err
		}
		v := new(big.Int)
		if err := serialize.Deserialize(bb, v); err != nil {
			return err
		}
		if v.Sign() == 0 {
			continue
		}
		t.balances[k.Uint64()] = v
	}
	return nil
}
