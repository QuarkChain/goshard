// Copyright 2026-2027, QuarkChain.

package types_test

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	qkctypes "github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/trie"
)

type derivablePayloads [][]byte

func (p derivablePayloads) Len() int { return len(p) }

func (p derivablePayloads) Bytes(i int) []byte { return p[i] }

func TestDeriveShaGoldenVectors(t *testing.T) {
	tests := []struct {
		name string
		list derivablePayloads
		want common.Hash
	}{
		{
			name: "empty",
			list: nil,
			want: common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"),
		},
		{
			name: "single",
			list: derivablePayloads{[]byte("cat")},
			want: common.HexToHash("0xb423fb4e634b237f9e4fe311a0b72e299540b2407f2fe06f262cac177dd755bd"),
		},
		{
			name: "multi",
			list: derivablePayloads{[]byte("cat"), []byte("dog"), []byte("fish")},
			want: common.HexToHash("0x47fdad14c87a0b6acdce6fc6c4d65e315d3e0db6276ae0ae510b1681a28974d3"),
		},
	}

	hasher := trie.NewListHasher()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := qkctypes.DeriveSha(test.list, hasher); got != test.want {
				t.Fatalf("DeriveSha mismatch: got %s, want %s", got.Hex(), test.want.Hex())
			}
		})
	}
}
