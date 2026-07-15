// Copyright 2026-2027, QuarkChain.

// Token balance tests exercise pyquarkchain-compatible QKC wire bytes.

package types

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
)

func testU256(v uint64) *uint256.Int {
	return new(uint256.Int).SetUint64(v)
}

func testU256Decimal(t *testing.T, v string) *uint256.Int {
	t.Helper()
	data, err := uint256.FromDecimal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
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

func TestTokenBalancesJSONLargeValueRoundTrip(t *testing.T) {
	const (
		maxTokenID = ^uint64(0)
		largeValue = "1208925819614629174706176"
	)
	original := NewTokenBalancesWithMap(map[uint64]*uint256.Int{
		maxTokenID: testU256Decimal(t, largeValue),
	})

	encoded, err := json.Marshal(original)
	assert.NoError(t, err)
	assert.Contains(t, string(encoded), "18446744073709551615:"+largeValue)

	var decoded TokenBalances
	assert.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, original.GetTokenBalance(maxTokenID), decoded.GetTokenBalance(maxTokenID))
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

func TestTokenBalancesAlwaysUsesListEncoding(t *testing.T) {
	mapping := make(map[uint64]*uint256.Int, 0)
	for index := 0; index < 17; index++ {
		mapping[uint64(index+1)] = testU256(uint64(index*1000 + 42))
	}

	b0 := NewTokenBalancesWithMap(mapping)
	b0.Commit()
	data, err := b0.SerializeToBytes()
	assert.NoError(t, err)
	assert.Equal(t, byte(0), data[0])

	b1, err := NewTokenBalances(data)
	assert.NoError(t, err)
	assert.Equal(t, len(mapping), b1.Len())
	for tokenID, balance := range mapping {
		assert.Equal(t, balance, b1.GetTokenBalance(tokenID))
	}
}

func TestTokenBalancesRejectsTrieEncoding(t *testing.T) {
	data := make([]byte, 33)
	data[0] = byte(1)

	_, err := NewTokenBalances(data)
	assert.ErrorContains(t, err, "trie")
}

func TestTokenBalancesRLPRoundTripWithoutDB(t *testing.T) {
	mapping := make(map[uint64]*uint256.Int, 0)
	mapping[1] = testU256(42)
	mapping[3] = testU256(99)

	b := NewTokenBalancesWithMap(mapping)
	encoded, err := rlp.EncodeToBytes(b)
	assert.NoError(t, err)

	var decoded TokenBalances
	assert.NoError(t, rlp.DecodeBytes(encoded, &decoded))
	assert.Equal(t, testU256(42), decoded.GetTokenBalance(1))
	assert.Equal(t, testU256(99), decoded.GetTokenBalance(3))
	assert.Equal(t, new(uint256.Int), decoded.GetTokenBalance(2))
}
