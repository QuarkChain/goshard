// Copyright 2026-2027, QuarkChain.

package types

import (
	"fmt"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// serHash returns keccak256(qkc_serialize(val)), optionally excluding the named
// struct fields from the encoding. It mirrors pyquarkchain's
// sha3_256(obj.serialize()) — sha3_256 there is keccak (legacy Keccak-256), the
// same digest as geth's crypto.Keccak256 — so hashes match byte-for-byte.
//
// val must be a struct value (not a pointer): SerializeStructWithout walks its
// fields in declaration order. A serialization failure is a programmer error in
// the field layout, so it panics rather than returning a zero hash that would
// silently pass a compatibility test.
func serHash(val interface{}, exclude map[string]bool) common.Hash {
	w := make([]byte, 0, 256)
	if err := serialize.SerializeStructWithout(reflect.ValueOf(val), &w, exclude); err != nil {
		panic(fmt.Sprintf("qkc/types: serialize %T for hashing: %v", val, err))
	}
	return crypto.Keccak256Hash(w)
}
