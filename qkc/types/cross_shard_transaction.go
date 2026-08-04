// Copyright 2026-2027, QuarkChain.

// Cross-shard transactions follow pyquarkchain-compatible QKC wire encoding.

package types

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

const crossShardTransactionListVersion = uint32(1)

// CrossShardTransactionDeposit matches pyquarkchain's current
// CrossShardTransactionDeposit.FIELDS order.
type CrossShardTransactionDeposit struct {
	TxHash          common.Hash
	From            account.Address
	To              account.Address
	Value           *serialize.Uint256
	GasPrice        *serialize.Uint256
	GasTokenID      uint64
	TransferTokenID uint64
	GasRemained     *serialize.Uint256
	MessageData     []byte `bytesizeofslicelen:"4"`
	CreateContract  bool
	IsFromRootChain bool
	RefundRate      uint8
}

// CrossShardTransactionList contains deposits in the latest format.
type CrossShardTransactionList struct {
	TXList []*CrossShardTransactionDeposit
}

func NewCrossShardTransactionList(txList []*CrossShardTransactionDeposit) *CrossShardTransactionList {
	if txList == nil {
		txList = make([]*CrossShardTransactionDeposit, 0)
	}
	return &CrossShardTransactionList{
		TXList: txList,
	}
}

// crossShardTransactionListV1 is the version 1 encoding.
type crossShardTransactionListV1 struct {
	TXList  []*CrossShardTransactionDeposit `bytesizeofslicelen:"4"`
	Version uint32
}

// Serialize encodes the list using the latest version.
func (c *CrossShardTransactionList) Serialize(w *[]byte) error {
	if c == nil {
		return fmt.Errorf("nil cross-shard transaction list")
	}
	return serialize.Serialize(w, crossShardTransactionListV1{
		TXList:  c.TXList,
		Version: crossShardTransactionListVersion,
	})
}

// Deserialize decodes a cross-shard transaction list.
func (c *CrossShardTransactionList) Deserialize(bb *serialize.ByteBuffer) error {
	if c == nil {
		return fmt.Errorf("nil cross-shard transaction list")
	}
	b, err := bb.ReadRemaining()
	if err != nil {
		return err
	}
	decoded, err := FromBytesToCrossShardTransactionList(b)
	if err != nil {
		return err
	}
	c.TXList = decoded.TXList
	return nil
}

// FromBytesToCrossShardTransactionList reads the version from the last four
// bytes and converts the deposits to the latest format.
func FromBytesToCrossShardTransactionList(b []byte) (*CrossShardTransactionList, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("cross-shard transaction list is missing its version")
	}
	version := binary.BigEndian.Uint32(b[len(b)-4:])
	switch version {
	case crossShardTransactionListVersion:
		var decoded crossShardTransactionListV1
		if err := serialize.DeserializeFromBytes(b, &decoded); err != nil {
			return nil, err
		}
		if decoded.Version != crossShardTransactionListVersion {
			return nil, fmt.Errorf("unsupported cross-shard transaction list version %d", decoded.Version)
		}
		return NewCrossShardTransactionList(decoded.TXList), nil
	default:
		return nil, fmt.Errorf("unsupported cross-shard transaction list version %d", version)
	}
}
