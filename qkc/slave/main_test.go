// Copyright 2026-2027, QuarkChain.

package slave

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// When the real chain is injected into these smoke tests, keep the allowlist
	// empty for slave-owned goroutines and fix their blocking Stop path instead.
	goleak.VerifyTestMain(m)
}
