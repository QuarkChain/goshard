// Copyright 2026-2027, QuarkChain.

// Token balance tests exercise pyquarkchain-compatible QKC wire bytes.

package common

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/qkc/serialize"
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

func TestDefaultTokenID(t *testing.T) {
	// DefaultTokenID must equal TokenIDEncode("QKC") = 35760.
	assert.Equal(t, uint64(35760), DefaultTokenID)
	assert.Equal(t, TokenIDEncode("QKC"), DefaultTokenID)
}

// TestValidateTokenName pins the domain to pyquarkchain's [0-9A-Z]{1,12}: the names
// TokenIDEncode accepts pass and encode identically through the checked form, and
// everything it would panic on comes back as an error.
func TestValidateTokenName(t *testing.T) {
	for _, name := range []string{"QKC", "0", "QETC", "TOKEN123", TOKENMAX} {
		assert.NoError(t, ValidateTokenName(name))
		id, err := TokenIDEncodeChecked(name)
		assert.NoError(t, err)
		assert.Equal(t, TokenIDEncode(name), id)
	}
	for _, name := range []string{
		"",              // TokenIDEncode indexes str[len-1] and panics
		"lowercase",     // the reported case: "unknown character 108"
		"QKC-2",         // punctuation
		"QKÇ",           // non-ASCII, checked byte-wise like the encoder
		"ZZZZZZZZZZZZZ", // 13 characters
	} {
		assert.Error(t, ValidateTokenName(name), "name %q", name)
		_, err := TokenIDEncodeChecked(name)
		assert.Error(t, err, "name %q", name)
	}
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
	assert.NoError(t, tb.Add(map[uint64]*uint256.Int{3234: testU256(10)}))
	m3 := make(map[uint64]*uint256.Int)
	m3[3567] = testU256(0)
	m3[3234] = testU256(10)
	check("", tb.balances, m3)
}

func TestTokenBalancesSub(t *testing.T) {
	tb := NewTokenBalancesWithMap(map[uint64]*uint256.Int{
		1: testU256(10),
	})

	assert.NoError(t, tb.Sub(map[uint64]*uint256.Int{1: testU256(4)}))
	assert.Equal(t, testU256(6), tb.GetTokenBalance(1))
}

func TestTokenBalancesSubRejectsUnderflow(t *testing.T) {
	tb := NewTokenBalancesWithMap(map[uint64]*uint256.Int{
		1: testU256(10),
		2: testU256(10),
	})

	err := tb.Sub(map[uint64]*uint256.Int{
		1: testU256(11),
		2: testU256(5),
	})
	assert.ErrorContains(t, err, "underflow")
	assert.Equal(t, testU256(10), tb.GetTokenBalance(1))
	assert.Equal(t, testU256(10), tb.GetTokenBalance(2))
}

func TestTokenBalancesAddRejectsOverflow(t *testing.T) {
	max := new(uint256.Int).SetAllOne()
	tb := NewTokenBalancesWithMap(map[uint64]*uint256.Int{
		1: max,
	})

	err := tb.Add(map[uint64]*uint256.Int{1: testU256(1)})
	assert.ErrorContains(t, err, "overflow")
	assert.Equal(t, max, tb.GetTokenBalance(1))
}

func TestTokenBalancesUsesListEncodingUpToThreshold(t *testing.T) {
	mapping := make(map[uint64]*uint256.Int, 0)
	for index := 0; index < TokenTrieThreshold; index++ {
		mapping[uint64(index+1)] = testU256(uint64(index*1000 + 42))
	}

	b0 := NewTokenBalancesWithMap(mapping)
	data, err := b0.SerializeToBytes()
	assert.NoError(t, err)
	assert.Equal(t, tokenBalanceListPrefix, data[0])

	b1, err := NewTokenBalances(data)
	assert.NoError(t, err)
	assert.Equal(t, len(mapping), b1.Len())
	for tokenID, balance := range mapping {
		assert.Equal(t, balance, b1.GetTokenBalance(tokenID))
	}
}

func TestTokenBalancesRejectsMoreThanThreshold(t *testing.T) {
	mapping := make(map[uint64]*uint256.Int, 0)
	for index := 0; index < TokenTrieThreshold+1; index++ {
		mapping[uint64(index+1)] = testU256(uint64(index*1000 + 42))
	}

	b0 := NewTokenBalancesWithMap(mapping)
	_, err := b0.SerializeToBytes()
	assert.ErrorContains(t, err, "exceed")
}

func TestTokenBalancesDecodeRLPRejectsMoreThanThreshold(t *testing.T) {
	pairs := make([]*TokenBalancePair, 0, TokenTrieThreshold+1)
	for index := 0; index < TokenTrieThreshold+1; index++ {
		pairs = append(pairs, &TokenBalancePair{
			TokenID: uint64(index + 1),
			Balance: testU256(uint64(index + 1)),
		})
	}
	list, err := rlp.EncodeToBytes(pairs)
	assert.NoError(t, err)
	stateData := append([]byte{tokenBalanceListPrefix}, list...)
	encoded, err := rlp.EncodeToBytes(stateData)
	assert.NoError(t, err)

	var decoded TokenBalances
	err = rlp.DecodeBytes(encoded, &decoded)
	assert.ErrorContains(t, err, "exceed")
}

func TestTokenBalancesRejectsTrieEncoding(t *testing.T) {
	data := make([]byte, 33)
	data[0] = tokenBalanceTriePrefix

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

func TestTokenBalancesZeroOnlyStateEncodingPythonGolden(t *testing.T) {
	empty := NewEmptyTokenBalances()
	emptyInner, err := empty.SerializeToBytes()
	assert.NoError(t, err)
	assert.Empty(t, emptyInner)
	emptyOuter, err := rlp.EncodeToBytes(empty)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x80}, emptyOuter)

	zeroOnly := NewTokenBalancesWithMap(map[uint64]*uint256.Int{
		1: testU256(0),
	})
	zeroOnlyInner, err := zeroOnly.SerializeToBytes()
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0xc0}, zeroOnlyInner)
	zeroOnlyOuter, err := rlp.EncodeToBytes(zeroOnly)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x82, 0x00, 0xc0}, zeroOnlyOuter)
}

func TestTokenBalancesQKCSerializePythonGolden(t *testing.T) {
	golden, err := hex.DecodeString("00000002020ca0016d020def030f423f")
	assert.NoError(t, err)

	tb := NewTokenBalancesWithMap(map[uint64]*uint256.Int{
		3232: testU256(109),
		0:    testU256(0),
		3567: testU256(999999),
	})
	encoded, err := serialize.SerializeToBytes(tb)
	assert.NoError(t, err)
	assert.Equal(t, golden, encoded)

	var decoded TokenBalances
	assert.NoError(t, serialize.DeserializeFromBytes(golden, &decoded))
	assert.Equal(t, 2, decoded.Len())
	assert.Equal(t, testU256(109), decoded.GetTokenBalance(3232))
	assert.Equal(t, testU256(999999), decoded.GetTokenBalance(3567))
	assert.Equal(t, new(uint256.Int), decoded.GetTokenBalance(0))
}

func TestTokenBalancesQKCDeserializeDuplicatePythonGolden(t *testing.T) {
	golden, err := hex.DecodeString("0000000201010101010100")
	assert.NoError(t, err)

	var decoded TokenBalances
	assert.NoError(t, serialize.DeserializeFromBytes(golden, &decoded))
	assert.Equal(t, 0, decoded.Len())
	assert.Equal(t, new(uint256.Int), decoded.GetTokenBalance(1))

	encoded, err := serialize.SerializeToBytes(&decoded)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00}, encoded)
}
