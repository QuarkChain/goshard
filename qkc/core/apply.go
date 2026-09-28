// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	ethcore "github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	qkctypes "github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

// ApplyTransaction validates and applies one intra-shard transaction. A VM
// execution error produces a failed receipt; an admission error or the MNT
// sentinel rejects the block and is returned to the caller.
func ApplyTransaction(ctx *ExecutionContext, evm *vm.EVM, gp *ethcore.GasPool, statedb *state.StateDB, tx *qkctypes.Transaction, txIndex int) (*qkctypes.Receipt, []byte, error) {
	if evm == nil {
		return nil, nil, fmt.Errorf("%w: nil EVM", ErrInvalidTransaction)
	}
	sender, err := TxSender(ctx, tx)
	if err != nil {
		return nil, nil, err
	}
	if err := ValidateTransaction(ctx, statedb, gp, tx, sender, evm.Context.Time); err != nil {
		return nil, nil, err
	}
	price, overflow := uint256.FromBig(tx.GasPrice())
	if overflow {
		return nil, nil, fmt.Errorf("%w: gas price overflows uint256", ErrInvalidTransaction)
	}
	value, overflow := uint256.FromBig(tx.Value())
	if overflow {
		return nil, nil, fmt.Errorf("%w: value overflows uint256", ErrInvalidTransaction)
	}

	stateSnapshot := statedb.Snapshot()
	poolSnapshot := gp.Snapshot()
	revert := func() {
		statedb.RevertToSnapshot(stateSnapshot)
		gp.Set(poolSnapshot)
	}

	txHash := tx.Hash()
	statedb.SetTxContext(txHash, txIndex)
	evm.Context.EVMEnableTimestamp = ctx.QKCConfig.EnableEvmTimeStamp
	evm.Context.MNTEnableTimestamp = ctx.QKCConfig.EnableNonReservedNativeTokenTimestamp
	evm.SetTxContext(vm.TxContext{
		Origin:           sender,
		GasPrice:         price,
		FromFullShardKey: tx.FromFullShardKey(),
		ToFullShardKey:   tx.ToFullShardKey(),
	})

	if err := gp.SubGas(tx.Gas()); err != nil {
		revert()
		return nil, nil, fmt.Errorf("%w: %v", ErrBlockGasLimitReached, err)
	}
	statedb.SetNonce(sender, tx.Nonce()+1, tracing.NonceChangeEoACall)
	gasCost := new(uint256.Int).Mul(price, uint256.NewInt(tx.Gas()))
	statedb.SubBalance(sender, gasCost, tracing.BalanceDecreaseGasBuy)

	intrinsic := IntrinsicGas(tx)
	executionGas := tx.Gas() - intrinsic
	var (
		output  []byte
		created common.Address
		left    vm.GasBudget
		vmErr   error
	)
	if tx.To() == nil {
		output, created, left, vmErr = evm.QKCCreateContract(sender, tx.Data(), vm.NewGasBudget(executionGas), value, tx.TransferTokenID(), tx.ToFullShardKey(), nil)
	} else {
		output, left, vmErr = evm.QKCApplyMessage(sender, *tx.To(), tx.Data(), vm.NewGasBudget(executionGas), value, tx.TransferTokenID(), tx.ToFullShardKey())
	}
	if errors.Is(vmErr, vm.ErrQKCUnsupportedMNT) {
		revert()
		return nil, nil, vm.ErrQKCUnsupportedMNT
	}

	gasRemaining := left.RegularGas
	gasUsed := tx.Gas() - gasRemaining
	if vmErr == nil {
		refund := min(statedb.GetRefund(), gasUsed/params.RefundQuotient)
		gasRemaining += refund
		gasUsed -= refund
		if tx.To() == nil {
			output = created.Bytes()
		}
	} else {
		output = nil
		created = common.Address{}
	}
	if gasRemaining != 0 {
		refund := new(uint256.Int).Mul(price, uint256.NewInt(gasRemaining))
		statedb.AddBalance(sender, refund, tracing.BalanceIncreaseGasReturn)
	}
	fee := localFee(ctx, tx.GasPrice(), gasUsed)
	feeValue, overflow := uint256.FromBig(fee)
	if overflow {
		revert()
		return nil, nil, fmt.Errorf("%w: transaction fee overflows uint256", ErrInvalidTransaction)
	}
	statedb.AddBalance(evm.Context.Coinbase, feeValue, tracing.BalanceIncreaseRewardTransactionFee)
	if err := gp.ReturnGas(gasRemaining, gasUsed); err != nil {
		revert()
		return nil, nil, err
	}

	statedb.Finalise(true)
	var blockNumber uint64
	if evm.Context.BlockNumber != nil {
		blockNumber = evm.Context.BlockNumber.Uint64()
	}
	logs := statedb.GetLogs(txHash, blockNumber, common.Hash{}, evm.Context.Time)
	receipt := &qkctypes.Receipt{
		Status:               qkctypes.ReceiptStatusSuccessful,
		CumulativeGasUsed:    gp.CumulativeUsed(),
		Logs:                 logs,
		ContractAddress:      created,
		ContractFullShardKey: tx.ToFullShardKey(),
		TxHash:               txHash,
		GasUsed:              gasUsed,
	}
	if vmErr != nil {
		receipt.Status = qkctypes.ReceiptStatusFailed
	}
	receipt.Bloom = qkctypes.CreateBloom(qkctypes.Receipts{receipt})
	return receipt, output, nil
}

func localFee(ctx *ExecutionContext, gasPrice *big.Int, gasUsed uint64) *big.Int {
	fee := new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gasUsed))
	if ctx.QKCConfig.LocalFeeRate == nil {
		return fee
	}
	fee.Mul(fee, ctx.QKCConfig.LocalFeeRate.Num())
	return fee.Div(fee, ctx.QKCConfig.LocalFeeRate.Denom())
}
