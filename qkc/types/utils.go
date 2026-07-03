// Copyright 2026-2027, QuarkChain.

// Ported from github.com/QuarkChain/goquarkchain/core/types (byte-compatible).
// Adaptation: sha3.NewKeccak256() -> crypto.NewKeccakState() (identical Keccak-256 digest).

package types

import (
	"hash"
	"reflect"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

type writeCounter common.StorageSize

func (c *writeCounter) Write(b []byte) (int, error) {
	*c += writeCounter(len(b))
	return len(b), nil
}

type hashBuf struct {
	bytes *[]byte
	hw    hash.Hash
}

func newBuf() *hashBuf {
	return &hashBuf{bytes: new([]byte), hw: crypto.NewKeccakState()}
}

func (h *hashBuf) reset() {
	*h.bytes = (*h.bytes)[:0]
	h.hw.Reset()
}

func (h *hashBuf) getHash() (hash common.Hash) {
	h.hw.Write(*h.bytes)
	h.hw.Sum(hash[:0])
	return
}

var (
	bufPool = sync.Pool{
		New: func() interface{} { return &hashBuf{bytes: new([]byte), hw: crypto.NewKeccakState()} },
	}
)

func serHash(val interface{}, excludeList map[string]bool) (h common.Hash) {
	buf := bufPool.Get().(*hashBuf)
	if buf == nil {
		buf = newBuf()
	}
	buf.reset()
	defer bufPool.Put(buf)
	if err := serialize.SerializeStructWithout(reflect.ValueOf(val), buf.bytes, excludeList); err != nil {
		panic(err)
	}
	return buf.getHash()
}
