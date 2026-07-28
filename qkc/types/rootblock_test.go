// Copyright 2026-2027, QuarkChain.

package types

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/holiman/uint256"
)

// Golden values produced by pyquarkchain (the compatibility source of truth). See
// qkc/config/singularity/README.md for the regeneration one-liner; the synthetic
// case is built directly in quarkchain.core.RootBlockHeader with every field
// non-zero. These pin both the serialized bytes (so a layout regression is caught
// precisely) and the resulting hash/seal-hash.
func rep(b byte, n int) []byte {
	s := make([]byte, n)
	for i := range s {
		s[i] = b
	}
	return s
}

func sig65(b byte) (s [65]byte) {
	copy(s[:], rep(b, 65))
	return s
}

func TestRootBlockHeaderSerializeAndHash(t *testing.T) {
	cases := []struct {
		name     string
		header   *RootBlockHeader
		ser      string
		hash     string
		sealHash string
	}{
		{
			// The real QuarkChain mainnet (singularity) root genesis, built exactly
			// as qkc.CreateRootBlock builds it from ROOT.GENESIS.
			name: "mainnet_root_genesis",
			header: &RootBlockHeader{
				Time:            1556639999,
				Coinbase:        account.CreatEmptyAddress(0),
				CoinbaseAmount:  qkcCommon.NewEmptyTokenBalances(),
				Difficulty:      big.NewInt(10000000000000),
				TotalDifficulty: big.NewInt(10000000000000),
			},
			ser:      "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005cc870ff0609184e72a0000609184e72a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			hash:     "4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51",
			sealHash: "e7dcdecc09e724ad81e493d70dedcd6d9ea0ee830d7ab2528a5648f2a0cf8178",
		},
		{
			// The QuarkChain devnet (singularity) root genesis.
			name: "devnet_root_genesis",
			header: &RootBlockHeader{
				Time:            1556639999,
				Coinbase:        account.CreatEmptyAddress(0),
				CoinbaseAmount:  qkcCommon.NewEmptyTokenBalances(),
				Difficulty:      big.NewInt(100000),
				TotalDifficulty: big.NewInt(100000),
			},
			ser:      "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005cc870ff030186a0030186a00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
			hash:     "5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d",
			sealHash: "055a7b410a50c098c52c123983e3596c5914ba227bf9e2c0c93309ff8f650d41",
		},
		{
			// Every field non-zero, including a multi-entry coinbase amount map
			// with a zero balance (token id 2) that must be skipped.
			name: "synthetic_all_fields",
			header: &RootBlockHeader{
				Version:         1,
				Number:          2,
				ParentHash:      common.BytesToHash(rep(0x01, 32)),
				MinorHeaderHash: common.BytesToHash(rep(0x02, 32)),
				Root:            common.BytesToHash(rep(0x03, 32)),
				Coinbase:        account.NewAddress(common.BytesToAddress(rep(0xaa, 20)), 0x00010001),
				CoinbaseAmount: qkcCommon.NewTokenBalancesWithMap(map[uint64]*uint256.Int{
					1:       uint256.NewInt(100),
					2:       uint256.NewInt(0),
					1000000: uint256.NewInt(999),
				}),
				Time:            1600000000,
				Difficulty:      big.NewInt(1000000),
				TotalDifficulty: big.NewInt(2000000),
				Nonce:           42,
				Extra:           []byte("hello"),
				MixDigest:       common.BytesToHash(rep(0x04, 32)),
				Signature:       sig65(0x05),
			},
			ser:      "0000000100000002010101010101010101010101010101010101010101010101010101010101010102020202020202020202020202020202020202020202020202020202020202020303030303030303030303030303030303030303030303030303030303030303aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa000100010000000201010164030f42400203e7000000005f5e1000030f4240031e8480000000000000002a000568656c6c6f04040404040404040404040404040404040404040404040404040404040404040505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505050505",
			hash:     "316595066fd9795df3ba42b3f0e6b3da03bf0f75e14a9f425270095a8768ed4c",
			sealHash: "32e26c6a06cbb68460f4b2c79379e157ac62373cf5f0a7175079ac19d80a9c93",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := serialize.SerializeToBytes(tc.header)
			if err != nil {
				t.Fatalf("serialize: %v", err)
			}
			if h := hex.EncodeToString(got); h != tc.ser {
				t.Errorf("serialized bytes mismatch\n got %s\nwant %s", h, tc.ser)
			}
			if h := tc.header.Hash(); h != common.HexToHash(tc.hash) {
				t.Errorf("hash mismatch\n got %s\nwant 0x%s", h.Hex(), tc.hash)
			}
			if h := tc.header.SealHash(); h != common.HexToHash(tc.sealHash) {
				t.Errorf("seal hash mismatch\n got %s\nwant 0x%s", h.Hex(), tc.sealHash)
			}
		})
	}
}

func TestEmptyTokenBalancesSerialize(t *testing.T) {
	var w []byte
	if err := qkcCommon.NewEmptyTokenBalances().Serialize(&w); err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if h := hex.EncodeToString(w); h != "00000000" {
		t.Fatalf("empty map serialize = %s, want 00000000", h)
	}
}
