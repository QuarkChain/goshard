// Copyright 2026-2027, QuarkChain.

package types

import "testing"

type unserializableHashInput struct {
	Prefix uint32
	Value  chan int
}

func TestSerHashPanicsOnSerializationError(t *testing.T) {
	assertPanicContains(t, "not serializable", func() {
		serHash(unserializableHashInput{Prefix: 1}, nil)
	})
}
