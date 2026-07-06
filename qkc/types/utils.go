// Copyright 2026-2027, QuarkChain.

// Ported from github.com/QuarkChain/goquarkchain/core/types (byte-compatible).
// Adaptation: sha3.NewKeccak256() -> crypto.Keccak256Hash() (identical Keccak-256 digest).

package types

import (
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

type writeCounter common.StorageSize

func (c *writeCounter) Write(b []byte) (int, error) {
	*c += writeCounter(len(b))
	return len(b), nil
}

func serHash(val interface{}, excludeList map[string]bool) (h common.Hash) {
	var bytes []byte
	if err := serialize.SerializeStructWithout(reflect.ValueOf(val), &bytes, excludeList); err != nil {
		panic(err)
	}
	return crypto.Keccak256Hash(bytes)
}
