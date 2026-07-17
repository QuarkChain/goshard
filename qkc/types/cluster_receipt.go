// Copyright 2026-2027, QuarkChain.

// Cluster receipt/log wrappers match pyquarkchain's master/slave serialize
// schema. Internal execution and receipt RLP keep using geth core/types.Log.

package types

import (
	"fmt"
	"math"

	"github.com/ethereum/go-ethereum/common"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// ClusterLog is pyquarkchain's quarkchain.core.Log: the master/slave RPC log
// representation, not the execution-layer log model.
type ClusterLog struct {
	Recipient   common.Address
	Topics      []common.Hash
	Data        []byte
	BlockNumber uint64
	BlockHash   common.Hash
	TxIndex     uint32
	TxHash      common.Hash
	Index       uint32
}

// NewClusterLog converts geth's execution log into pyquarkchain's cluster log.
func NewClusterLog(log *coretypes.Log) (*ClusterLog, error) {
	if log == nil {
		return nil, fmt.Errorf("nil log")
	}
	if log.TxIndex > math.MaxUint32 {
		return nil, fmt.Errorf("log tx index %d exceeds uint32", log.TxIndex)
	}
	if log.Index > math.MaxUint32 {
		return nil, fmt.Errorf("log index %d exceeds uint32", log.Index)
	}
	return &ClusterLog{
		Recipient:   log.Address,
		Topics:      copyHashSlice(log.Topics),
		Data:        common.CopyBytes(log.Data),
		BlockNumber: log.BlockNumber,
		BlockHash:   log.BlockHash,
		TxIndex:     uint32(log.TxIndex),
		TxHash:      log.TxHash,
		Index:       uint32(log.Index),
	}, nil
}

func (l *ClusterLog) toCoreLog() *coretypes.Log {
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

// Serialize writes pyquarkchain Log.FIELDS order explicitly:
// recipient, topics, data, block_number, block_hash, tx_idx, tx_hash, log_idx.
func (l *ClusterLog) Serialize(w *[]byte) error {
	if l == nil {
		return fmt.Errorf("nil cluster log")
	}
	for _, step := range []func() error{
		func() error { return serialize.Serialize(w, l.Recipient) },
		func() error { return serialize.Serialize(w, l.Topics) },
		func() error {
			return serialize.SerializeWithTags(w, l.Data, serialize.Tags{ByteSizeOfSliceLen: 4})
		},
		func() error { return serialize.Serialize(w, l.BlockNumber) },
		func() error { return serialize.Serialize(w, l.BlockHash) },
		func() error { return serialize.Serialize(w, l.TxIndex) },
		func() error { return serialize.Serialize(w, l.TxHash) },
		func() error { return serialize.Serialize(w, l.Index) },
	} {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

func (l *ClusterLog) Deserialize(bb *serialize.ByteBuffer) error {
	if l == nil {
		return fmt.Errorf("nil cluster log")
	}
	for _, step := range []func() error{
		func() error { return serialize.Deserialize(bb, &l.Recipient) },
		func() error { return serialize.Deserialize(bb, &l.Topics) },
		func() error {
			return serialize.DeserializeWithTags(bb, &l.Data, serialize.Tags{ByteSizeOfSliceLen: 4})
		},
		func() error { return serialize.Deserialize(bb, &l.BlockNumber) },
		func() error { return serialize.Deserialize(bb, &l.BlockHash) },
		func() error { return serialize.Deserialize(bb, &l.TxIndex) },
		func() error { return serialize.Deserialize(bb, &l.TxHash) },
		func() error { return serialize.Deserialize(bb, &l.Index) },
	} {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

// ClusterTransactionReceipt is pyquarkchain's quarkchain.core.TransactionReceipt.
type ClusterTransactionReceipt struct {
	Success         []byte
	GasUsed         uint64
	PrevGasUsed     uint64
	Bloom           Bloom
	ContractAddress account.Address
	Logs            []*ClusterLog
}

func newClusterTransactionReceipt(r *Receipt) (*ClusterTransactionReceipt, error) {
	if r == nil {
		return nil, fmt.Errorf("nil receipt")
	}
	prevGasUsed, err := r.prevGasUsed()
	if err != nil {
		return nil, err
	}
	logs := make([]*ClusterLog, len(r.Logs))
	for i, log := range r.Logs {
		logs[i], err = NewClusterLog(log)
		if err != nil {
			return nil, fmt.Errorf("log %d: %w", i, err)
		}
	}
	return &ClusterTransactionReceipt{
		Success:         r.statusEncoding(),
		GasUsed:         r.CumulativeGasUsed,
		PrevGasUsed:     prevGasUsed,
		Bloom:           r.Bloom,
		ContractAddress: account.Address{Recipient: r.ContractAddress, FullShardKey: r.ContractFullShardKey},
		Logs:            logs,
	}, nil
}

// Serialize writes pyquarkchain TransactionReceipt.FIELDS order explicitly:
// success, gas_used, prev_gas_used, bloom, contract_address, logs.
func (r *ClusterTransactionReceipt) Serialize(w *[]byte) error {
	if r == nil {
		return fmt.Errorf("nil cluster transaction receipt")
	}
	for _, step := range []func() error{
		func() error { return serialize.Serialize(w, r.Success) },
		func() error { return serialize.Serialize(w, r.GasUsed) },
		func() error { return serialize.Serialize(w, r.PrevGasUsed) },
		func() error { return serialize.Serialize(w, r.Bloom) },
		func() error { return serialize.Serialize(w, r.ContractAddress) },
		func() error {
			return serialize.SerializeWithTags(w, r.Logs, serialize.Tags{ByteSizeOfSliceLen: 4})
		},
	} {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClusterTransactionReceipt) Deserialize(bb *serialize.ByteBuffer) error {
	if r == nil {
		return fmt.Errorf("nil cluster transaction receipt")
	}
	for _, step := range []func() error{
		func() error { return serialize.Deserialize(bb, &r.Success) },
		func() error { return serialize.Deserialize(bb, &r.GasUsed) },
		func() error { return serialize.Deserialize(bb, &r.PrevGasUsed) },
		func() error { return serialize.Deserialize(bb, &r.Bloom) },
		func() error { return serialize.Deserialize(bb, &r.ContractAddress) },
		func() error {
			return serialize.DeserializeWithTags(bb, &r.Logs, serialize.Tags{ByteSizeOfSliceLen: 4})
		},
	} {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

func copyHashSlice(in []common.Hash) []common.Hash {
	if in == nil {
		return nil
	}
	out := make([]common.Hash, len(in))
	copy(out, in)
	return out
}
