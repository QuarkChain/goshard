// Copyright 2026-2027, QuarkChain.

package types

import "testing"

type hashGoldenInput struct {
	A uint32
	B []byte
	C uint32
}

type unserializableHashInput struct {
	Prefix uint32
	Value  chan int
}

func TestSerHashGoldenVectors(t *testing.T) {
	input := hashGoldenInput{A: 0x01020304, B: []byte("hello"), C: 0x0a0b0c0d}
	tests := []struct {
		name        string
		excludeList map[string]bool
		want        string
	}{
		{
			name: "all fields",
			want: "0x58e4fa83e9c9479b45b1b34443a8cfcef0fa91b7ee99d9eb3c1d0f8c4a304d8f",
		},
		{
			name:        "exclude bytes field",
			excludeList: map[string]bool{"B": true},
			want:        "0x4742824113969f339dfb9dfe1e64a976c261e46ca7a89c065c07e26c87c71db7",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := serHash(input, test.excludeList).Hex(), test.want; got != want {
				t.Fatalf("serHash mismatch: got %s, want %s", got, want)
			}
		})
	}
}

func TestSerHashPanicsOnSerializationError(t *testing.T) {
	assertPanicContains(t, "not serializable", func() {
		serHash(unserializableHashInput{Prefix: 1}, nil)
	})
}
