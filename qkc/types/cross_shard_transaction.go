// Ported verbatim from github.com/QuarkChain/goquarkchain/core/types (byte-compatible).

package types

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

const MaxUint24 = uint32(1<<24 - 1)

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
	// Follow pyquarkchain (core.py CrossShardTransactionDepositV0.FIELDS): is_from_root_chain
	// is serialized LAST. goquarkchain places it right after TransferTokenID, which makes the
	// x-shard deposit bytes incompatible with a pyquarkchain master.
	IsFromRootChain bool
}

type CrossShardTransactionDeposit struct {
	CrossShardTransactionDepositV0
	RefundRate uint8
}

type crossShardTransactionDepositListV1 struct {
	TXList []*CrossShardTransactionDeposit `bytesizeofslicelen:"4"`
}

type CrossShardTransactionDepositList struct {
	TXList []*CrossShardTransactionDeposit `bytesizeofslicelen:"4"`
}

func NewCrossShardTransactionDepositList(txList []*CrossShardTransactionDeposit) *CrossShardTransactionDepositList {
	if txList == nil {
		txList = make([]*CrossShardTransactionDeposit, 0)
	}
	return &CrossShardTransactionDepositList{
		TXList: txList,
	}
}

func (c *CrossShardTransactionDepositList) Serialize(w *[]byte) error {
	list := crossShardTransactionDepositListV1{c.TXList}
	bytes, err := serialize.SerializeToBytes(list)
	if err != nil {
		return err
	}
	bytes[0] = 1
	*w = append(*w, bytes...)
	return nil
}

func (c *CrossShardTransactionDepositList) Deserialize(bb *serialize.ByteBuffer) error {
	size, err := bb.GetUInt32()
	if err != nil {
		return err
	}
	version := size >> 24
	size = size & MaxUint24
	txList := make([]*CrossShardTransactionDeposit, size)
	switch version {
	case 0:
		for i := 0; i < int(size); i++ {
			cstx := CrossShardTransactionDepositV0{}
			if err := serialize.Deserialize(bb, &cstx); err != nil {
				return err
			}
			txList[i] = &CrossShardTransactionDeposit{cstx, 100}
		}
	case 1:
		for i := 0; i < int(size); i++ {
			cstx := CrossShardTransactionDeposit{}
			if err := serialize.Deserialize(bb, &cstx); err != nil {
				return err
			}
			txList[i] = &cstx
		}
	default:
		return fmt.Errorf("not support version %v", version)

	}
	c.TXList = txList
	return nil
}
