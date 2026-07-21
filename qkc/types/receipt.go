// Copyright 2026-2027, QuarkChain.

// Receipt uses pyquarkchain-compatible QKC wire and storage encoding.
// Modified from go-ethereum under GNU Lesser General Public License

package types

import (
	"fmt"
	"io"
	"math"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
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

type receiptStorageRLP struct {
	Status               []byte
	CumulativeGasUsed    uint64
	Bloom                Bloom
	TxHash               common.Hash
	ContractAddress      account.Recipient
	ContractFullShardKey uint32
	Logs                 []*receiptStorageLog
	GasUsed              uint64
}

type receiptStorageLog struct {
	Recipient   account.Recipient
	Topics      []common.Hash
	Data        []byte
	BlockNumber uint64
	TxHash      common.Hash
	TxIndex     uint32
	BlockHash   common.Hash
	Index       uint32
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

// Deserialize deserialize the QKC minor block
func (r *Receipt) Deserialize(bb *serialize.ByteBuffer) error {
	var rs ClusterTransactionReceipt
	if err := serialize.Deserialize(bb, &rs); err != nil {
		return err
	}
	if err := r.setStatus(rs.Success); err != nil {
		return err
	}
	if rs.PrevGasUsed > rs.GasUsed {
		return fmt.Errorf("prev gas used %d exceeds cumulative gas used %d", rs.PrevGasUsed, rs.GasUsed)
	}
	r.CumulativeGasUsed, r.Bloom, r.GasUsed, r.ContractAddress, r.ContractFullShardKey = rs.GasUsed, rs.Bloom,
		rs.GasUsed-rs.PrevGasUsed, rs.ContractAddress.Recipient, rs.ContractAddress.FullShardKey
	r.Logs = make([]*coretypes.Log, len(rs.Logs))
	for i, log := range rs.Logs {
		r.Logs[i] = log.toCoreLog()
	}
	return nil
}

// Serialize serialize the QKC minor block.
func (r *Receipt) Serialize(w *[]byte) error {
	receipt, err := newClusterTransactionReceipt(r)
	if err != nil {
		return err
	}
	return receipt.Serialize(w)
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

// ReceiptForStorage is a wrapper around a Receipt that flattens and parses the
// entire content of a receipt, as opposed to only the consensus fields originally.
type ReceiptForStorage Receipt

// EncodeRLP implements rlp.Encoder, and flattens all content fields of a receipt
// into an RLP stream.
func (r *ReceiptForStorage) EncodeRLP(w io.Writer) error {
	status, err := (*Receipt)(r).statusEncoding()
	if err != nil {
		return err
	}
	enc := &receiptStorageRLP{
		Status:               status,
		CumulativeGasUsed:    r.CumulativeGasUsed,
		Bloom:                r.Bloom,
		TxHash:               r.TxHash,
		ContractAddress:      r.ContractAddress,
		ContractFullShardKey: r.ContractFullShardKey,
		Logs:                 make([]*receiptStorageLog, len(r.Logs)),
		GasUsed:              r.GasUsed,
	}
	for i, log := range r.Logs {
		storageLog, err := logForStorage(log)
		if err != nil {
			return fmt.Errorf("log %d: %w", i, err)
		}
		enc.Logs[i] = storageLog
	}
	return rlp.Encode(w, enc)
}

// DecodeRLP implements rlp.Decoder, and loads both consensus and implementation
// fields of a receipt from an RLP stream.
func (r *ReceiptForStorage) DecodeRLP(s *rlp.Stream) error {
	var dec receiptStorageRLP
	if err := s.Decode(&dec); err != nil {
		return err
	}
	if err := (*Receipt)(r).setStatus(dec.Status); err != nil {
		return err
	}
	// Assign the consensus fields
	r.CumulativeGasUsed, r.Bloom = dec.CumulativeGasUsed, dec.Bloom
	r.Logs = make([]*coretypes.Log, len(dec.Logs))
	for i, log := range dec.Logs {
		r.Logs[i] = log.toCoreLog()
	}
	// Assign the implementation fields
	r.TxHash, r.ContractAddress, r.ContractFullShardKey, r.GasUsed = dec.TxHash, dec.ContractAddress,
		dec.ContractFullShardKey, dec.GasUsed
	return nil
}

func logForStorage(log *coretypes.Log) (*receiptStorageLog, error) {
	if log == nil {
		return nil, nil
	}
	if log.TxIndex > math.MaxUint32 {
		return nil, fmt.Errorf("log tx index %d exceeds uint32", log.TxIndex)
	}
	if log.Index > math.MaxUint32 {
		return nil, fmt.Errorf("log index %d exceeds uint32", log.Index)
	}
	return &receiptStorageLog{
		Recipient:   log.Address,
		Topics:      copyHashSlice(log.Topics),
		Data:        common.CopyBytes(log.Data),
		BlockNumber: log.BlockNumber,
		TxHash:      log.TxHash,
		TxIndex:     uint32(log.TxIndex),
		BlockHash:   log.BlockHash,
		Index:       uint32(log.Index),
	}, nil
}

func (l *receiptStorageLog) toCoreLog() *coretypes.Log {
	if l == nil {
		return nil
	}
	return &coretypes.Log{
		Address:     l.Recipient,
		Topics:      copyHashSlice(l.Topics),
		Data:        common.CopyBytes(l.Data),
		BlockNumber: l.BlockNumber,
		TxHash:      l.TxHash,
		TxIndex:     uint(l.TxIndex),
		BlockHash:   l.BlockHash,
		Index:       uint(l.Index),
	}
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
