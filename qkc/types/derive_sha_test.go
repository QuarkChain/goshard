// Copyright 2026-2027, QuarkChain.

package types

import (
	"fmt"
	"strings"
	"testing"
)

type unserializableMerkleItem struct {
	Value chan int
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
