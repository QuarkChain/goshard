// Copyright 2026-2027, QuarkChain.

// Cross-shard transactions follow pyquarkchain-compatible QKC wire encoding.

package types

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

const crossShardTransactionListVersion = uint32(1)

type CrossShardTransactionDepositV0 struct {
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
	// Follow pyquarkchain (core.py CrossShardTransactionDepositV0.FIELDS):
	// is_from_root_chain is serialized last for master/slave wire compatibility.
	IsFromRootChain bool
}

type CrossShardTransactionDeposit struct {
	CrossShardTransactionDepositV0
	RefundRate uint8
}

// CrossShardTransactionList is pyquarkchain's CrossShardTransactionList
// version 1 wire type.
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

// Serialize writes pyquarkchain CrossShardTransactionList.FIELDS order:
// tx_list followed by version(uint32).
func (c *CrossShardTransactionList) Serialize(w *[]byte) error {
	if c == nil {
		return fmt.Errorf("nil cross-shard transaction list")
	}
	if err := serialize.SerializeWithTags(w, c.TXList, serialize.Tags{ByteSizeOfSliceLen: 4}); err != nil {
		return err
	}
	return serialize.Serialize(w, crossShardTransactionListVersion)
}

// Deserialize reads the current pyquarkchain CrossShardTransactionList version.
func (c *CrossShardTransactionList) Deserialize(bb *serialize.ByteBuffer) error {
	if c == nil {
		return fmt.Errorf("nil cross-shard transaction list")
	}
	var txList []*CrossShardTransactionDeposit
	if err := serialize.DeserializeWithTags(bb, &txList, serialize.Tags{ByteSizeOfSliceLen: 4}); err != nil {
		return err
	}
	var version uint32
	if err := serialize.Deserialize(bb, &version); err != nil {
		return err
	}
	if version != crossShardTransactionListVersion {
		return fmt.Errorf("unsupported cross-shard transaction list version %d", version)
	}
	c.TXList = txList
	return nil
}
