// Copyright 2026-2027, QuarkChain.

package types

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

type merkleGoldenItem struct {
	Num     uint32
	Payload []byte
}

type derivablePayloads [][]byte

func (p derivablePayloads) Len() int { return len(p) }

func (p derivablePayloads) Bytes(i int) []byte { return p[i] }

type unserializableMerkleItem struct {
	Value chan int
}

func TestCalculateMerkleRootGoldenVectors(t *testing.T) {
	items := []merkleGoldenItem{
		{Num: 1, Payload: []byte("a")},
		{Num: 2, Payload: []byte("bc")},
		{Num: 3, Payload: []byte("def")},
		{Num: 0xffffffff, Payload: []byte("z")},
	}
	tests := []struct {
		name string
		list []merkleGoldenItem
		want common.Hash
	}{
		{
			name: "empty",
			list: nil,
			want: common.HexToHash("0xdaa77426c30c02a43d9fba4e841a6556c524d47030762eb14dc4af897e605d9b"),
		},
		{
			name: "single",
			list: items[:1],
			want: common.HexToHash("0x5e71929d5019754c39559f18172f7f1c078d587d0a6d981368dd604a1fc0b9bd"),
		},
		{
			name: "odd",
			list: items[:3],
			want: common.HexToHash("0x6d0c353541f8b8782c9bdd30fcd2c4d6abf871b252507305885e6f3551b2ca55"),
		},
		{
			name: "even",
			list: items,
			want: common.HexToHash("0x83b489a2d7e8dd12cafcb6ea77beab24e5c02cd4c845394b74d868b90b96b818"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CalculateMerkleRoot(test.list); got != test.want {
				t.Fatalf("CalculateMerkleRoot mismatch: got %s, want %s", got.Hex(), test.want.Hex())
			}
		})
	}
}

func TestDeriveShaGoldenVectors(t *testing.T) {
	// TODO: Add receipt-trie golden vectors in the receipt/types PR once Receipt is introduced.
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DeriveSha(test.list); got != test.want {
				t.Fatalf("DeriveSha mismatch: got %s, want %s", got.Hex(), test.want.Hex())
			}
		})
	}
}

func TestEmptyTrieHashGoldenVector(t *testing.T) {
	want := common.HexToHash("0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	if EmptyTrieHash != want {
		t.Fatalf("EmptyTrieHash mismatch: got %s, want %s", EmptyTrieHash.Hex(), want.Hex())
	}
}

func TestCalculateMerkleRootRejectsInvalidInput(t *testing.T) {
	assertPanicContains(t, "expect slice input", func() {
		CalculateMerkleRoot(nil)
	})
	assertPanicContains(t, "expect slice input", func() {
		CalculateMerkleRoot(1)
	})
}

func TestCalculateMerkleRootPanicsOnSerializationError(t *testing.T) {
	assertPanicContains(t, "not serializable", func() {
		CalculateMerkleRoot([]unserializableMerkleItem{{}})
	})
}

func assertPanicContains(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q", want)
		}
		if !strings.Contains(fmt.Sprint(r), want) {
			t.Fatalf("panic mismatch: got %v, want substring %q", r, want)
		}
	}()
	fn()
}
