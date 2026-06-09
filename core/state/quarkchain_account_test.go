// Copyright 2026-2027, QuarkChain.

package state

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
)

func TestQuarkChainTokenIDKey(t *testing.T) {
	key := QuarkChainTokenIDKey(0x0102030405060708)
	want := common.FromHex("0x0000000000000000000000000000000000000000000000000102030405060708")
	if !bytes.Equal(key, want) {
		t.Fatalf("wrong token key: got %x want %x", key, want)
	}
}

func TestQuarkChainUint32RLP(t *testing.T) {
	blob, err := rlp.EncodeToBytes(quarkChainUint32(0x01020304))
	if err != nil {
		t.Fatal(err)
	}
	if want := "8401020304"; hex.EncodeToString(blob) != want {
		t.Fatalf("wrong qkc uint32 rlp: got %x want %s", blob, want)
	}
	var decoded quarkChainUint32
	if err := rlp.DecodeBytes(blob, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != quarkChainUint32(0x01020304) {
		t.Fatalf("wrong decoded value: got %#x", uint32(decoded))
	}
}

func TestQuarkChainTokenBalancesInline(t *testing.T) {
	encoded, err := EncodeQuarkChainTokenBalances(map[uint64]*big.Int{
		3: big.NewInt(4),
		1: big.NewInt(2),
		2: big.NewInt(0),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 || encoded[0] != 0x00 {
		t.Fatalf("expected inline token balance encoding, got %x", encoded)
	}
	var pairs []quarkChainTokenBalancePair
	if err := rlp.DecodeBytes(encoded[1:], &pairs); err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 2 {
		t.Fatalf("wrong pair count: got %d", len(pairs))
	}
	if pairs[0].TokenID != 1 || pairs[0].Balance.Cmp(big.NewInt(2)) != 0 {
		t.Fatalf("wrong first pair: %#v", pairs[0])
	}
	if pairs[1].TokenID != 3 || pairs[1].Balance.Cmp(big.NewInt(4)) != 0 {
		t.Fatalf("wrong second pair: %#v", pairs[1])
	}
}

func TestQuarkChainTokenBalancesTrie(t *testing.T) {
	db := NewQuarkChainMemoryTrieDB()
	balances := make(map[uint64]*big.Int)
	for i := uint64(1); i <= QuarkChainTokenTrieThreshold+1; i++ {
		balances[i] = new(big.Int).SetUint64(i * 100)
	}
	encoded, err := EncodeQuarkChainTokenBalances(balances, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != 33 || encoded[0] != 0x01 {
		t.Fatalf("expected token trie encoding, got %x", encoded)
	}
	root := common.BytesToHash(encoded[1:])
	tokenTrie, err := trie.NewSecure(root, common.Hash{}, root, db)
	if err != nil {
		t.Fatal(err)
	}
	for tokenID, wantBalance := range balances {
		blob := tokenTrie.MustGet(QuarkChainTokenIDKey(tokenID))
		if len(blob) == 0 {
			t.Fatalf("missing token %d", tokenID)
		}
		var got big.Int
		if err := rlp.DecodeBytes(blob, &got); err != nil {
			t.Fatal(err)
		}
		if got.Cmp(wantBalance) != 0 {
			t.Fatalf("wrong token %d balance: got %s want %s", tokenID, &got, wantBalance)
		}
	}
}

func TestBuildQuarkChainStorageRoot(t *testing.T) {
	db := NewQuarkChainMemoryTrieDB()
	storage := map[common.Hash]common.Hash{
		common.HexToHash("0x01"): common.HexToHash("0x02"),
		common.HexToHash("0x03"): {},
	}
	root, err := BuildQuarkChainStorageRoot(storage, db)
	if err != nil {
		t.Fatal(err)
	}
	if root == types.EmptyRootHash {
		t.Fatal("expected non-empty storage root")
	}
	storageTrie, err := trie.NewSecure(root, common.Hash{}, root, db)
	if err != nil {
		t.Fatal(err)
	}
	blob := storageTrie.MustGet(common.HexToHash("0x01").Bytes())
	_, content, _, err := rlp.Split(blob)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(content, []byte{0x02}) {
		t.Fatalf("wrong storage content: got %x", content)
	}
	if blob := storageTrie.MustGet(common.HexToHash("0x03").Bytes()); len(blob) != 0 {
		t.Fatalf("zero storage value should be omitted, got %x", blob)
	}
}

func TestBuildQuarkChainStateRoot(t *testing.T) {
	db := NewQuarkChainMemoryTrieDB()
	addrA := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addrB := common.HexToAddress("0x2222222222222222222222222222222222222222")
	accounts := map[common.Address]QuarkChainAccount{
		addrB: {
			TokenBalances: map[uint64]*big.Int{2: big.NewInt(200)},
			FullShardKey:  0x00020003,
		},
		addrA: {
			Nonce:         1,
			TokenBalances: map[uint64]*big.Int{1: big.NewInt(100)},
			Storage: map[common.Hash]common.Hash{
				common.HexToHash("0x01"): common.HexToHash("0x0a"),
			},
			Code:         []byte{0x60, 0x00},
			FullShardKey: 0x00010002,
		},
	}
	root, err := BuildQuarkChainStateRoot(accounts, db)
	if err != nil {
		t.Fatal(err)
	}
	rootAgain, err := BuildQuarkChainStateRoot(accounts, NewQuarkChainMemoryTrieDB())
	if err != nil {
		t.Fatal(err)
	}
	if root != rootAgain {
		t.Fatalf("state root is not deterministic: got %s want %s", root, rootAgain)
	}
	accountTrie, err := trie.NewSecure(root, common.Hash{}, root, db)
	if err != nil {
		t.Fatal(err)
	}
	blob := accountTrie.MustGet(addrA.Bytes())
	if len(blob) == 0 {
		t.Fatal("missing account")
	}
	var account quarkChainAccountRLP
	if err := rlp.DecodeBytes(blob, &account); err != nil {
		t.Fatal(err)
	}
	if account.Nonce != 1 {
		t.Fatalf("wrong nonce: got %d", account.Nonce)
	}
	if uint32(account.FullShardKey) != 0x00010002 {
		t.Fatalf("wrong full shard key: got %#x", uint32(account.FullShardKey))
	}
	if account.StorageRoot == types.EmptyRootHash {
		t.Fatal("expected non-empty account storage root")
	}
	if !bytes.Equal(account.CodeHash, common.BytesToHash([]byte{
		0x07, 0xad, 0x11, 0x8d, 0x6c, 0xc8, 0x64, 0x2c,
		0x86, 0xc0, 0x38, 0x27, 0xf2, 0x76, 0xd8, 0xb7,
		0x91, 0xa6, 0x5e, 0x5c, 0x99, 0xa3, 0x84, 0x5f,
		0xaf, 0x18, 0x6b, 0xe7, 0x20, 0xa1, 0x45, 0x5d,
	}).Bytes()) {
		t.Fatalf("wrong code hash: got %x", account.CodeHash)
	}
}
