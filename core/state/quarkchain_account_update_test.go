// Copyright 2026-2027, QuarkChain.

package state

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestDecodeQuarkChainAccountRoundTrip(t *testing.T) {
	db := NewQuarkChainMemoryTrieDB()
	account := QuarkChainAccount{
		Nonce:         7,
		TokenBalances: map[uint64]*big.Int{1: big.NewInt(100), 3: big.NewInt(300)},
		StorageRoot:   types.EmptyRootHash,
		CodeHash:      types.EmptyCodeHash.Bytes(),
		FullShardKey:  0x00010002,
	}
	blob, err := EncodeQuarkChainAccount(account, db)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeQuarkChainAccount(blob, db)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Nonce != account.Nonce {
		t.Fatalf("wrong nonce: got %d want %d", decoded.Nonce, account.Nonce)
	}
	if decoded.FullShardKey != account.FullShardKey {
		t.Fatalf("wrong full shard key: got %#x want %#x", decoded.FullShardKey, account.FullShardKey)
	}
	if decoded.StorageRoot != account.StorageRoot {
		t.Fatalf("wrong storage root: got %s want %s", decoded.StorageRoot, account.StorageRoot)
	}
	if !bytes.Equal(decoded.CodeHash, account.CodeHash) {
		t.Fatalf("wrong code hash: got %x want %x", decoded.CodeHash, account.CodeHash)
	}
	for tokenID, want := range account.TokenBalances {
		if got := decoded.QuarkChainTokenBalance(tokenID); got.Cmp(want) != 0 {
			t.Fatalf("wrong token %d balance: got %s want %s", tokenID, got, want)
		}
	}
}

func TestQuarkChainTokenBalanceUpdate(t *testing.T) {
	account := QuarkChainAccount{}
	if err := account.AddQuarkChainTokenBalance(35760, big.NewInt(100)); err != nil {
		t.Fatal(err)
	}
	if err := account.AddQuarkChainTokenBalance(35760, big.NewInt(-40)); err != nil {
		t.Fatal(err)
	}
	if got := account.QuarkChainTokenBalance(35760); got.Cmp(big.NewInt(60)) != 0 {
		t.Fatalf("wrong balance: got %s want 60", got)
	}
	if err := account.AddQuarkChainTokenBalance(35760, big.NewInt(-61)); err == nil {
		t.Fatal("expected negative resulting balance to fail")
	}
	if got := account.QuarkChainTokenBalance(35760); got.Cmp(big.NewInt(60)) != 0 {
		t.Fatalf("failed update should not mutate balance: got %s want 60", got)
	}
	if err := account.SetQuarkChainTokenBalance(35760, new(big.Int)); err != nil {
		t.Fatal(err)
	}
	if got := account.QuarkChainTokenBalance(35760); got.Sign() != 0 {
		t.Fatalf("zero balance should be deleted, got %s", got)
	}
}

func TestDecodeQuarkChainTokenTrieBalance(t *testing.T) {
	db := NewQuarkChainMemoryTrieDB()
	balances := make(map[uint64]*big.Int)
	for tokenID := uint64(1); tokenID <= QuarkChainTokenTrieThreshold+1; tokenID++ {
		balances[tokenID] = new(big.Int).SetUint64(tokenID * 10)
	}
	encoded, err := EncodeQuarkChainTokenBalances(balances, db)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != 0x01 {
		t.Fatalf("expected trie-encoded balances, got %x", encoded)
	}
	got, err := DecodeQuarkChainTokenBalance(encoded, 17, db)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cmp(big.NewInt(170)) != 0 {
		t.Fatalf("wrong trie token lookup: got %s want 170", got)
	}
	decoded, err := DecodeQuarkChainTokenBalances(encoded, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(balances) {
		t.Fatalf("wrong decoded balance count: got %d want %d", len(decoded), len(balances))
	}
}

func TestQuarkChainCreateAddress(t *testing.T) {
	tests := []struct {
		sender       common.Address
		nonce        uint64
		fullShardKey uint32
		want         common.Address
	}{
		{
			sender:       common.HexToAddress("0xc4fba3740f95d25b2196c9437fdb005359296d36"),
			nonce:        0,
			fullShardKey: 0x0007d31c,
			want:         common.HexToAddress("0xb6f634022e3a803367b18387835449ebafa7e7b6"),
		},
		{
			sender:       common.HexToAddress("0x1111111111111111111111111111111111111111"),
			nonce:        7,
			fullShardKey: 0x00010002,
			want:         common.HexToAddress("0x841bcbc78759d1e0c8d079ea16e8e9ab9a48fbf3"),
		},
	}
	for _, tt := range tests {
		if got := QuarkChainCreateAddress(tt.sender, tt.nonce, tt.fullShardKey); got != tt.want {
			t.Fatalf("wrong qkc contract address: got %s want %s", got, tt.want)
		}
	}
}
