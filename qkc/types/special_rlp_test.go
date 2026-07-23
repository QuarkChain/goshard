// Copyright 2026-2027, QuarkChain.

package types

import (
	"encoding/hex"
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
)

func TestUint32RLPGoldenVectors(t *testing.T) {
	tests := []struct {
		name string
		in   Uint32
		want string
	}{
		{
			name: "zero",
			in:   Uint32(0),
			want: "8400000000",
		},
		{
			name: "max",
			in:   Uint32(0xffffffff),
			want: "84ffffffff",
		},
		{
			name: "big endian",
			in:   Uint32(0x01020304),
			want: "8401020304",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := rlp.EncodeToBytes(&test.in)
			if err != nil {
				t.Fatalf("EncodeToBytes failed: %v", err)
			}
			if got := hex.EncodeToString(encoded); got != test.want {
				t.Fatalf("Uint32 RLP mismatch: got %s, want %s", got, test.want)
			}

			var decoded Uint32
			if err := rlp.DecodeBytes(encoded, &decoded); err != nil {
				t.Fatalf("DecodeBytes failed: %v", err)
			}
			if decoded != test.in {
				t.Fatalf("Uint32 RLP round trip mismatch: got %d, want %d", decoded, test.in)
			}
		})
	}
}
