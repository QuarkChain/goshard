// Copyright 2026-2027, QuarkChain.

package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// =============================================================================
// Custom wire types for Python format compatibility
// =============================================================================
//
// Python uses 4-byte length prefix for nested slices in some wire messages.
// Go's serialize framework defaults to 1-byte prefix for nested elements.
// These custom types enforce 4-byte prefix to match Python wire format.

// PrependedSizeBytes4 is []byte with 4-byte length prefix (matches Python PrependedSizeBytesSerializer(4)).
type PrependedSizeBytes4 []byte

func (p PrependedSizeBytes4) Serialize(w *[]byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(p)))
	*w = append(*w, lenBuf...)
	*w = append(*w, p...)
	return nil
}

func (p *PrependedSizeBytes4) Deserialize(bb *serialize.ByteBuffer) error {
	length, err := bb.GetUInt32()
	if err != nil {
		return err
	}

	if length > math.MaxInt32 || int(length) > bb.Remaining() {
		return fmt.Errorf("PrependedSizeBytes4.Deserialize: length %d exceeds remaining %d", length, bb.Remaining())
	}

	bytes, err := bb.ReadBytes(int(length))
	if err != nil {
		return err
	}

	*p = PrependedSizeBytes4(bytes)
	return nil
}

var _ serialize.Serializable = (*PrependedSizeBytes4)(nil)

// PrependedSizeHashList4 is [][HashLength]byte with 4-byte length prefix (matches Python PrependedSizeListSerializer(4, hash256)).
type PrependedSizeHashList4 [][HashLength]byte

func (p PrependedSizeHashList4) Serialize(w *[]byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(p)))
	*w = append(*w, lenBuf...)

	for _, hash := range p {
		*w = append(*w, hash[:]...)
	}
	return nil
}

func (p *PrependedSizeHashList4) Deserialize(bb *serialize.ByteBuffer) error {
	length, err := bb.GetUInt32()
	if err != nil {
		return err
	}

	if length > math.MaxInt32/HashLength || int(length)*HashLength > bb.Remaining() {
		return fmt.Errorf("PrependedSizeHashList4.Deserialize: length %d exceeds capacity", length)
	}

	list := make([][HashLength]byte, int(length))
	for i := 0; i < int(length); i++ {
		hashBytes, err := bb.ReadBytes(HashLength)
		if err != nil {
			return err
		}
		var hash [HashLength]byte
		copy(hash[:], hashBytes)
		list[i] = hash
	}

	*p = PrependedSizeHashList4(list)
	return nil
}

var _ serialize.Serializable = (*PrependedSizeHashList4)(nil)

// PrependedSizeCoinbaseMap4 is a block-hash → TokenBalances map with a 4-byte
// length prefix (matches Python
// PrependedSizeMapSerializer(4, hash256, TokenBalanceMap)). Keys are emitted in
// ascending byte order, mirroring the Python serializer's sorted() iteration.
//
// Values must be non-nil: Python map values are never None, and a nil
// *TokenBalances entry indicates a caller bug. Serialize returns an error for
// it (encoding it as an empty map would silently lose the distinction).
type PrependedSizeCoinbaseMap4 map[[HashLength]byte]*qkcCommon.TokenBalances

func (p PrependedSizeCoinbaseMap4) Serialize(w *[]byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(p)))
	*w = append(*w, lenBuf...)

	keys := make([][HashLength]byte, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	for _, key := range keys {
		value := p[key]
		if value == nil {
			return fmt.Errorf("PrependedSizeCoinbaseMap4.Serialize: nil TokenBalances value for key %x", key[:])
		}
		*w = append(*w, key[:]...)
		if err := value.Serialize(w); err != nil {
			return err
		}
	}
	return nil
}

func (p *PrependedSizeCoinbaseMap4) Deserialize(bb *serialize.ByteBuffer) error {
	length, err := bb.GetUInt32()
	if err != nil {
		return err
	}

	if uint64(length) > math.MaxInt32 || int64(length)*int64(HashLength) > int64(bb.Remaining()) {
		return fmt.Errorf("PrependedSizeCoinbaseMap4.Deserialize: length %d exceeds remaining %d", length, bb.Remaining())
	}

	m := make(map[[HashLength]byte]*qkcCommon.TokenBalances, int(length))
	for i := 0; i < int(length); i++ {
		hashBytes, err := bb.ReadBytes(HashLength)
		if err != nil {
			return err
		}
		var hash [HashLength]byte
		copy(hash[:], hashBytes)

		value := qkcCommon.NewEmptyTokenBalances()
		if err := value.Deserialize(bb); err != nil {
			return err
		}
		m[hash] = value
	}
	*p = m
	return nil
}

var _ serialize.Serializable = (*PrependedSizeCoinbaseMap4)(nil)
