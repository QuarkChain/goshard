// Copyright 2026-2027, QuarkChain.

package types

import (
	"encoding/hex"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	pyqkcAccountQKC  = decodeStateAccountHex("f853018900c7c6828bb08203e8a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470840000000180")
	pyqkcAccountMNT  = decodeStateAccountHex("f858058e00ccc4648201f4c6828bb08207d0a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470840000000180")
	pyqkcAccountZero = decodeStateAccountHex("f84a8080a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470840000000180")
)

func decodeStateAccountHex(input string) []byte {
	data, err := hex.DecodeString(input)
	if err != nil {
		panic(err)
	}
	return data
}

func TestStateAccountPyquarkchainEncoding(t *testing.T) {
	tests := []struct {
		name string
		acct StateAccount
		want []byte
	}{
		{
			name: "QKC balance",
			acct: StateAccount{
				Nonce: 1,
				MntBalances: qkccommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{
					qkccommon.DefaultTokenID: uint256.NewInt(1000),
				}),
				Root: EmptyRootHash, CodeHash: EmptyCodeHash[:], FullShardKey: 1,
			},
			want: pyqkcAccountQKC,
		},
		{
			name: "QKC and MNT balances",
			acct: StateAccount{
				Nonce: 5,
				MntBalances: qkccommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{
					100:                      uint256.NewInt(500),
					qkccommon.DefaultTokenID: uint256.NewInt(2000),
				}),
				Root:         EmptyRootHash,
				CodeHash:     EmptyCodeHash[:],
				FullShardKey: 1,
			},
			want: pyqkcAccountMNT,
		},
		{
			name: "zero account",
			acct: StateAccount{MntBalances: qkccommon.NewEmptyTokenBalances(), Root: EmptyRootHash, CodeHash: EmptyCodeHash[:], FullShardKey: 1},
			want: pyqkcAccountZero,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := rlp.EncodeToBytes(&test.acct)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestStateAccountPyquarkchainDecoding(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		nonce    uint64
		balances map[uint64]*uint256.Int
	}{
		{name: "QKC balance", input: pyqkcAccountQKC, nonce: 1, balances: map[uint64]*uint256.Int{qkccommon.DefaultTokenID: uint256.NewInt(1000)}},
		{name: "QKC and MNT balances", input: pyqkcAccountMNT, nonce: 5, balances: map[uint64]*uint256.Int{100: uint256.NewInt(500), qkccommon.DefaultTokenID: uint256.NewInt(2000)}},
		{name: "zero account", input: pyqkcAccountZero, balances: map[uint64]*uint256.Int{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var acct StateAccount
			require.NoError(t, rlp.DecodeBytes(test.input, &acct))
			assert.Equal(t, test.nonce, acct.Nonce)
			assert.Equal(t, EmptyRootHash, acct.Root)
			assert.Equal(t, EmptyCodeHash[:], acct.CodeHash)
			assert.Equal(t, uint32(1), acct.FullShardKey)
			if assert.NotNil(t, acct.MntBalances) {
				assert.Equal(t, test.balances, acct.MntBalances.GetBalanceMap())
			}
		})
	}
}

func TestStateAccountExplicitZeroBalanceEncoding(t *testing.T) {
	acct := NewEmptyStateAccount()
	encoded, err := rlp.EncodeToBytes(acct)
	require.NoError(t, err)
	var wire qkcAccountRLP
	require.NoError(t, rlp.DecodeBytes(encoded, &wire))
	assert.Empty(t, wire.TokenBal)

	previous := acct.MntBalances.GetTokenBalance(qkccommon.DefaultTokenID)
	acct.MntBalances.SetValue(uint256.NewInt(1), qkccommon.DefaultTokenID)
	acct.MntBalances.SetValue(previous, qkccommon.DefaultTokenID)
	encoded, err = rlp.EncodeToBytes(acct)
	require.NoError(t, err)
	require.NoError(t, rlp.DecodeBytes(encoded, &wire))
	assert.Equal(t, []byte{0x00, 0xc0}, wire.TokenBal)
}

func TestStateAccountCopyMntBalances(t *testing.T) {
	original := StateAccount{
		MntBalances: qkccommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{
			100:                      uint256.NewInt(2),
			qkccommon.DefaultTokenID: uint256.NewInt(1),
		}),
		FullShardKey: 3,
	}
	copied := original.Copy()
	copied.MntBalances.SetValue(uint256.NewInt(4), 100)
	copied.MntBalances.SetValue(uint256.NewInt(5), qkccommon.DefaultTokenID)

	assert.Equal(t, uint256.NewInt(2), original.MntBalances.GetTokenBalance(100))
	assert.Equal(t, uint256.NewInt(4), copied.MntBalances.GetTokenBalance(100))
	assert.Equal(t, uint256.NewInt(1), original.MntBalances.GetTokenBalance(qkccommon.DefaultTokenID))
	assert.Equal(t, uint256.NewInt(5), copied.MntBalances.GetTokenBalance(qkccommon.DefaultTokenID))
	assert.Equal(t, original.FullShardKey, copied.FullShardKey)
}

func TestStateAccountBalance(t *testing.T) {
	acct := StateAccount{MntBalances: NewQKCTokenBalances(uint256.NewInt(42))}
	if got := acct.Balance(); got.Cmp(uint256.NewInt(42)) != 0 {
		t.Fatalf("QKC balance mismatch: have %v, want 42", got)
	}
	acct.SetBalance(uint256.NewInt(43))
	if got := acct.Balance(); got.Cmp(uint256.NewInt(43)) != 0 {
		t.Fatalf("updated QKC balance mismatch: have %v, want 43", got)
	}
	acct.SetBalance(nil)
	if got := acct.Balance(); !got.IsZero() {
		t.Fatalf("nil QKC balance should set zero, have %v", got)
	}
	newAcct := &StateAccount{}
	newAcct.SetBalance(uint256.NewInt(7))
	if got := newAcct.Balance(); got.Cmp(uint256.NewInt(7)) != 0 {
		t.Fatalf("QKC balance on nil map mismatch: have %v, want 7", got)
	}
	if got := (&StateAccount{}).Balance(); !got.IsZero() {
		t.Fatalf("nil balance map should return zero, have %v", got)
	}
}

func TestSlimAccountPreservesQuarkChainFields(t *testing.T) {
	original := StateAccount{
		Nonce: 7,
		MntBalances: qkccommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{
			100:                      uint256.NewInt(500),
			qkccommon.DefaultTokenID: uint256.NewInt(2000),
		}),
		Root:         EmptyRootHash,
		CodeHash:     EmptyCodeHash.Bytes(),
		FullShardKey: 0x12345678,
	}
	slim := SlimAccountRLP(original)
	decoded, err := FullAccount(slim)
	require.NoError(t, err)

	assert.Equal(t, original.Nonce, decoded.Nonce)
	assert.Equal(t, original.MntBalances.GetBalanceMap(), decoded.MntBalances.GetBalanceMap())
	assert.Equal(t, original.Root, decoded.Root)
	assert.Equal(t, original.CodeHash, decoded.CodeHash)
	assert.Equal(t, original.FullShardKey, decoded.FullShardKey)

	want, err := rlp.EncodeToBytes(&original)
	require.NoError(t, err)
	got, err := FullAccountRLP(slim)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestSlimAccountPreservesZeroOnlyWireEncoding(t *testing.T) {
	original := *NewEmptyStateAccount()
	original.MntBalances.SetValue(new(uint256.Int), qkccommon.DefaultTokenID)

	slim := SlimAccountRLP(original)
	want, err := rlp.EncodeToBytes(&original)
	require.NoError(t, err)
	got, err := FullAccountRLP(slim)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	decoded, err := FullAccount(slim)
	require.NoError(t, err)
	assert.Empty(t, decoded.MntBalances.GetBalanceMap())
}

func TestStateAccountRejectsUnsupportedQKCEncoding(t *testing.T) {
	tests := []struct {
		name string
		wire qkcAccountRLP
		want string
	}{
		{name: "token trie", wire: qkcAccountRLP{TokenBal: append([]byte{1}, make([]byte, common.HashLength)...), Root: EmptyRootHash}, want: "trie"},
		{name: "unknown token encoding", wire: qkcAccountRLP{TokenBal: []byte{2}, Root: EmptyRootHash}, want: "enum byte"},
		{name: "optional field", wire: qkcAccountRLP{Root: EmptyRootHash, Optional: []byte{1}}, want: "optional field"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := rlp.EncodeToBytes(&test.wire)
			require.NoError(t, err)
			var acct StateAccount
			assert.ErrorContains(t, rlp.DecodeBytes(encoded, &acct), test.want)
		})
	}
}
