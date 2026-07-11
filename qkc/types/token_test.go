// Copyright 2026-2027, QuarkChain.

// Token balance tests exercise pyquarkchain-compatible QKC wire bytes.
// Adaptation: trie.NewDatabase(ethdb.NewMemDatabase()) ->
// triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil); new(trie.Trie).Hash() -> EmptyTrieHash.

package types

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	qCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
)

func testU256(v uint64) *uint256.Int {
	return new(uint256.Int).SetUint64(v)
}

func TestNewTokenBalanceMap(t *testing.T) {
	m0 := make(map[uint64]*uint256.Int)
	m0[3234] = testU256(1000)
	m0[0] = testU256(0)
	m0[3567] = testU256(0)
	tb := NewTokenBalancesWithMap(m0)
	t.Logf("token balance map：%v", tb.balances)
}

func TestTokenBalancesJSONEmptyRoundTrip(t *testing.T) {
	encoded, err := json.Marshal(NewEmptyTokenBalances())
	assert.NoError(t, err)

	var decoded TokenBalances
	assert.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, 0, decoded.Len())
	assert.True(t, decoded.IsBlank())
}

func TestTokenBalances_Add(t *testing.T) {
	check := func(f string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s mismatch: got %v, want %v", f, got, want)
		}
	}
	m0 := make(map[uint64]*uint256.Int)
	m0[3567] = testU256(0)
	tb := NewTokenBalancesWithMap(m0)
	m1 := make(map[uint64]*uint256.Int)
	m1[3234] = testU256(10)
	tb1 := NewTokenBalancesWithMap(m1)
	tb.Add(tb1.balances)
	m3 := make(map[uint64]*uint256.Int)
	m3[3567] = testU256(0)
	m3[3234] = testU256(10)
	check("", tb.balances, m3)
}

func TestEncodingInTrie(t *testing.T) {
	encoding := common.FromHex("0x01848d4271e44ea41466fe355561dd43b166c927d2eca0a8dd901a8e6469ecdeb1")
	mapping := make(map[uint64]*uint256.Int, 0)
	for index := 0; index < 17; index++ {
		mapping[qCommon.TokenIDEncode("Q"+string(byte(65+index)))] = testU256(uint64(index*1000 + 42))
	}

	db := triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)
	b0, err := NewTokenBalances(nil, db)
	if err != nil {
		panic(encoding)
	}
	b0.Add(mapping)
	b0.Commit()
	sData, err := b0.SerializeToBytes()
	if err != nil {
		panic(encoding)
	}
	assert.Equal(t, sData, encoding)
	assert.True(t, b0.tokenTrie != nil)

	b1, err := NewTokenBalances(encoding, db)
	assert.NoError(t, err)

	sData, err = b1.SerializeToBytes()
	assert.NoError(t, err)
	assert.Equal(t, sData, encoding)

	assert.Equal(t, len(b1.balances), 0)
	assert.Equal(t, b1.tokenTrie != nil, true)
	assert.Equal(t, !b1.IsBlank(), true)
	assert.Equal(t, b1.GetTokenBalance(qCommon.TokenIDEncode("QC")), mapping[qCommon.TokenIDEncode("QC")])
	assert.Equal(t, len(b1.balances), 1)

	_, err = b1.SerializeToBytes()
	assert.Error(t, err)
	b1.Commit()
	data, err := b1.SerializeToBytes()
	assert.NoError(t, err)
	assert.Equal(t, data, encoding)
}

func TestEncodeChangeFromDictToTrie(t *testing.T) {
	db := triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)
	b, err := NewTokenBalances(nil, db)
	assert.NoError(t, err)
	mapping := make(map[uint64]*uint256.Int, 0)
	for index := 0; index < 16; index++ {
		mapping[qCommon.TokenIDEncode("Q"+string(byte(65+index)))] = testU256(uint64(index*1000 + 42))
	}
	b.Add(mapping)
	b.Commit()
	sData, err := b.SerializeToBytes()
	assert.NoError(t, err)
	assert.True(t, bytes.HasPrefix(sData, []byte{0}))
	assert.True(t, b.tokenTrie == nil)

	newToken := qCommon.TokenIDEncode("QKC")
	b.SetValue(testU256(123), newToken)
	assert.Equal(t, b.GetTokenBalance(newToken), testU256(123))
	b.Commit()
	assert.True(t, b.tokenTrie != nil)
	sData, err = b.SerializeToBytes()
	assert.NoError(t, err)
	assert.True(t, bytes.HasPrefix(sData, []byte{1}))
	root1 := b.tokenTrie.Hash()

	for tokenID, _ := range mapping {
		b.SetValue(new(uint256.Int), tokenID)
	}

	assert.True(t, b.GetTokenBalance(qCommon.TokenIDEncode("QA")).Uint64() == 0)
	b.Commit()
	serialized, err := b.SerializeToBytes()
	assert.NoError(t, err)
	root2 := b.tokenTrie.Hash()
	assert.True(t, bytes.Equal(serialized, append([]byte{1}, root2.Bytes()...)))
	assert.True(t, root2.String() != root1.String())

	assert.True(t, len(b.balances) == 0)
	assert.True(t, b.GetTokenBalance(qCommon.TokenIDEncode("QB")).Uint64() == 0)
	//# balance map truncated, entries with 0 value will be ignored
	assert.True(t, len(b.balances) == 0)
	assert.True(t, !b.IsBlank(), true)

	b.SetValue(new(uint256.Int), newToken)
	b.Commit()
	assert.True(t, b.tokenTrie.Hash().String() == EmptyTrieHash.String())
	assert.True(t, len(b.balances) == 0)
}

func TestResetBalanceInTrieAndRevert(t *testing.T) {
	db := triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)
	b, err := NewTokenBalances(nil, db)
	assert.NoError(t, err)
	mapping := make(map[uint64]*uint256.Int, 0)
	for index := 0; index < 17; index++ {
		mapping[qCommon.TokenIDEncode("Q"+string(byte(65+index)))] = testU256(uint64(index*1000 + 42))
	}
	b.Add(mapping)
	b.Commit()

	b.SetValue(testU256(999), 999)
	assert.True(t, b.GetTokenBalance(999).Uint64() == 999)

	//TODO test revert?
	//py handle revert in token_balances
	//go handle revert in token_balances's caller
}
