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

// Version 1 is the first format written by goshard for cross-shard transaction
// lists received from slaves. The lists are persisted and read for execution
// after AddRootBlock. Compatibility therefore starts at version 1; formats
// written only by pyquarkchain or goquarkchain are outside this storage path.
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

// CrossShardTransactionList is the current in-memory representation. Lists
// decoded from older database formats are converted to this representation,
// and Serialize always writes the latest format.
type CrossShardTransactionList struct {
	TXList  []*CrossShardTransactionDeposit
	version uint32
}

func (c *CrossShardTransactionList) Version() uint32 {
	return c.version
}

func NewCrossShardTransactionList(txList []*CrossShardTransactionDeposit) *CrossShardTransactionList {
	if txList == nil {
		txList = make([]*CrossShardTransactionDeposit, 0)
	}
	return &CrossShardTransactionList{
		TXList:  txList,
		version: crossShardTransactionListVersion,
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

// Deserialize decodes only the latest cross-shard transaction list format,
// which is currently version 1.
func (c *CrossShardTransactionList) Deserialize(bb *serialize.ByteBuffer) error {
	if c == nil {
		return fmt.Errorf("nil cross-shard transaction list")
	}
	var decoded crossShardTransactionListV1
	if err := serialize.Deserialize(bb, &decoded); err != nil {
		return err
	}
	if decoded.Version != crossShardTransactionListVersion {
		return fmt.Errorf("unsupported cross-shard transaction list version %d", decoded.Version)
	}
	c.TXList = decoded.TXList
	c.version = decoded.Version
	return nil
}

// FromBytesToCrossShardTransactionList decodes a list read from the database.
// Database records may have been written in different goshard versions, so the
// wire version is read from the last four bytes and dispatched below.
func FromBytesToCrossShardTransactionList(b []byte) (*CrossShardTransactionList, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("cross-shard transaction list is missing its version")
	}
	version := binary.BigEndian.Uint32(b[len(b)-4:])
	// Although version 1 is the only format today, use a switch so each future
	// version can have a separate decoder and conversion to CrossShardTransactionList.
	switch version {
	case crossShardTransactionListVersion:
		var decoded crossShardTransactionListV1
		if err := serialize.DeserializeFromBytes(b, &decoded); err != nil {
			return nil, err
		}
		return &CrossShardTransactionList{
			TXList:  decoded.TXList,
			version: decoded.Version,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported cross-shard transaction list version %d", version)
	}
}
