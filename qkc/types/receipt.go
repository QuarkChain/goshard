// Copyright 2026-2027, QuarkChain.

// Receipt provides QKC consensus receipt encoding.
// Modified from go-ethereum under GNU Lesser General Public License

package types

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	receiptStatusFailedRLP     = []byte{}
	receiptStatusSuccessfulRLP = []byte{0x01}
)

const (
	// ReceiptStatusFailed is the status code of a transaction if execution failed.
	ReceiptStatusFailed = uint64(0)

	// ReceiptStatusSuccessful is the status code of a transaction if execution succeeded.
	ReceiptStatusSuccessful = uint64(1)
)

// Receipt represents the results of a transaction.
type Receipt struct {
	// Consensus fields
	Status               uint64            `json:"status"`
	CumulativeGasUsed    uint64            `json:"cumulativeGasUsed"`
	Bloom                Bloom             `json:"logsBloom"`
	Logs                 []*coretypes.Log  `json:"logs"`
	ContractAddress      account.Recipient `json:"contractAddress"`
	ContractFullShardKey uint32            `json:"contractFullShardKey"`

	// Implementation fields (don't reorder!)
	TxHash  common.Hash `json:"transactionHash"`
	GasUsed uint64      `json:"gasUsed"`
}

// receiptRLP is the consensus encoding of a receipt.
type receiptRLP struct {
	Status               []byte
	CumulativeGasUsed    uint64
	Bloom                Bloom
	Logs                 []*coretypes.Log
	ContractAddress      []byte
	ContractFullShardKey *Uint32
}

// NewReceipt creates a barebone transaction receipt.
func NewReceipt(failed bool, cumulativeGasUsed uint64) *Receipt {
	r := &Receipt{CumulativeGasUsed: cumulativeGasUsed}
	if failed {
		r.Status = ReceiptStatusFailed
	} else {
		r.Status = ReceiptStatusSuccessful
	}
	return r
}

func (r *Receipt) GetPrevGasUsed() uint64 {
	prev, _ := r.prevGasUsed()
	return prev
}

func (r *Receipt) prevGasUsed() (uint64, error) {
	if r.GasUsed > r.CumulativeGasUsed {
		return 0, fmt.Errorf("gas used %d exceeds cumulative gas used %d", r.GasUsed, r.CumulativeGasUsed)
	}
	return r.CumulativeGasUsed - r.GasUsed, nil
}

// EncodeRLP implements rlp.Encoder, and flattens the consensus fields of a receipt
// into an RLP stream.
func (r *Receipt) EncodeRLP(w io.Writer) error {
	status, err := r.statusEncoding()
	if err != nil {
		return err
	}
	contractAddress := r.ContractAddress.Bytes()
	if account.IsSameReceipt(common.Address{}, r.ContractAddress) {
		contractAddress = make([]byte, 0)
	}
	contractFullShardKey := Uint32(r.ContractFullShardKey)
	data := &receiptRLP{
		status,
		r.CumulativeGasUsed,
		r.Bloom,
		r.Logs,
		contractAddress,
		&contractFullShardKey}
	return rlp.Encode(w, data)
}

// DecodeRLP implements rlp.Decoder, and loads the consensus fields of a receipt
// from an RLP stream.
func (r *Receipt) DecodeRLP(s *rlp.Stream) error {
	var dec receiptRLP
	if err := s.Decode(&dec); err != nil {
		return err
	}
	if err := r.setStatus(dec.Status); err != nil {
		return err
	}
	if len(dec.ContractAddress) != 0 && len(dec.ContractAddress) != common.AddressLength {
		return fmt.Errorf("invalid contract address length %d", len(dec.ContractAddress))
	}
	r.CumulativeGasUsed, r.Bloom, r.Logs, r.ContractAddress, r.ContractFullShardKey = dec.CumulativeGasUsed,
		dec.Bloom, dec.Logs, common.BytesToAddress(dec.ContractAddress), dec.ContractFullShardKey.GetValue()
	return nil
}

func (r *Receipt) setStatus(status []byte) error {
	switch {
	case len(status) == 1 && status[0] == receiptStatusSuccessfulRLP[0]:
		r.Status = ReceiptStatusSuccessful
	case len(status) == 0:
		r.Status = ReceiptStatusFailed
	default:
		return fmt.Errorf("invalid receipt status %x", status)
	}
	return nil
}

func (r *Receipt) statusEncoding() ([]byte, error) {
	switch r.Status {
	case ReceiptStatusFailed:
		return receiptStatusFailedRLP, nil
	case ReceiptStatusSuccessful:
		return receiptStatusSuccessfulRLP, nil
	default:
		return nil, fmt.Errorf("invalid receipt status %d", r.Status)
	}
}

// Size returns the approximate memory used by all internal contents. It is used
// to approximate and limit the memory consumption of various caches.
func (r *Receipt) Size() common.StorageSize {
	size := common.StorageSize(unsafe.Sizeof(*r))

	size += common.StorageSize(len(r.Logs)) * common.StorageSize(unsafe.Sizeof(coretypes.Log{}))
	for _, log := range r.Logs {
		size += common.StorageSize(len(log.Topics)*common.HashLength + len(log.Data))
	}
	return size
}

// Receipts is a wrapper around a Receipt array to implement DerivableList.
type Receipts []*Receipt

// Len returns the number of receipts in this list.
func (r Receipts) Len() int { return len(r) }

// GetRlp returns the RLP encoding of one receipt from the list.
func (r Receipts) Bytes(i int) []byte {
	bytes, err := rlp.EncodeToBytes(r[i])
	if err != nil {
		panic(err)
	}
	return bytes
}
