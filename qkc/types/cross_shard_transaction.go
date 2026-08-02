// Copyright 2026-2027, QuarkChain.

// Cross-shard transactions use versioned QKC wire encoding.

package types

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

const crossShardTransactionListVersion = uint32(0)

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

// CrossShardTransactionList contains decoded cross-shard transaction deposits.
type CrossShardTransactionList struct {
	TXList []*CrossShardTransactionDeposit
}

type crossShardTransactionListEnvelope struct {
	Version uint32
	Payload []byte `bytesizeofslicelen:"4"`
}

type crossShardTransactionListV0 struct {
	TXList []*CrossShardTransactionDeposit `bytesizeofslicelen:"4"`
}

func NewCrossShardTransactionList(txList []*CrossShardTransactionDeposit) *CrossShardTransactionList {
	if txList == nil {
		txList = make([]*CrossShardTransactionDeposit, 0)
	}
	return &CrossShardTransactionList{
		TXList: txList,
	}
}

// Serialize writes a versioned cross-shard transaction list envelope.
func (c *CrossShardTransactionList) Serialize(w *[]byte) error {
	if c == nil {
		return fmt.Errorf("nil cross-shard transaction list")
	}
	payload, err := serialize.SerializeToBytes(crossShardTransactionListV0{TXList: c.TXList})
	if err != nil {
		return err
	}
	return serialize.Serialize(w, crossShardTransactionListEnvelope{
		Version: crossShardTransactionListVersion,
		Payload: payload,
	})
}

// Deserialize reads a versioned envelope and normalizes its payload.
func (c *CrossShardTransactionList) Deserialize(bb *serialize.ByteBuffer) error {
	if c == nil {
		return fmt.Errorf("nil cross-shard transaction list")
	}
	var envelope crossShardTransactionListEnvelope
	if err := serialize.Deserialize(bb, &envelope); err != nil {
		return err
	}
	switch envelope.Version {
	case crossShardTransactionListVersion:
		var payload crossShardTransactionListV0
		if err := serialize.DeserializeFromBytes(envelope.Payload, &payload); err != nil {
			return err
		}
		c.TXList = payload.TXList
		return nil
	default:
		return fmt.Errorf("unsupported cross-shard transaction list version %d", envelope.Version)
	}
}
