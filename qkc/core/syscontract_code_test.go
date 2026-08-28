// Copyright 2026-2027, QuarkChain.

package core

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// TestSystemContractCode pins the three system contracts' bytecode against
// pyquarkchain's. Deploying one writes its code hash into the account leaf, so
// a single wrong byte is a state root divergence at the first block that
// touches the contract — and for the general native token manager, at every
// transaction that pays gas in a foreign token.
func TestSystemContractCode(t *testing.T) {
	tests := []struct {
		name string
		code []byte
		size int
		hash string
	}{
		{
			name: "root chain posw",
			code: rootChainPoSWContractCode,
			size: 1824,
			hash: "0x94487baff2ef8c83a91c7b5d9d11181800e646d36fda7b6a7737b1a337a12383",
		},
		{
			name: "non-reserved native token",
			code: nonReservedNativeTokenContractCode,
			size: 7173,
			hash: "0x4ff76cb7620ebfc65486c2408b1b5f921038e065dd66abbb12334906980a7559",
		},
		{
			name: "general native token",
			code: generalNativeTokenContractCode,
			size: 6508,
			hash: "0xd6e79984e2f572a34cd3b7902a87e0b525cf8d9696d770b8dc4170ce0ac9cbb7",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if len(test.code) != test.size {
				t.Errorf("code is %d bytes, want %d", len(test.code), test.size)
			}
			if got := crypto.Keccak256Hash(test.code); got != common.HexToHash(test.hash) {
				t.Errorf("code hash = %s, want %s", got, test.hash)
			}
		})
	}
}
