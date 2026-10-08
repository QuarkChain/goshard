// Copyright 2026-2027, QuarkChain.

package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/holiman/uint256"
)

type qkcStateHookSuspender interface {
	SuspendQKCStateHooks() func()
}

var (
	ErrQKCInvalidToken  = errors.New("invalid QKC token")
	ErrQKCGasConversion = errors.New("QKC gas conversion failed")
)

func (st *stateTransition) validateBalances(defaultTokenID uint64) error {
	msg := st.msg
	value := msg.Value
	if value == nil {
		value = new(uint256.Int)
	}
	for _, tokenID := range []uint64{msg.TransferTokenID, msg.GasTokenID} {
		if tokenID != defaultTokenID && st.state.GetBalanceByTokenID(msg.From, tokenID).IsZero() {
			return fmt.Errorf("%w: non-default token %d has zero balance", ErrQKCInvalidToken, tokenID)
		}
	}
	gasCost, overflow := mulGasPrice(msg.GasLimit, msg.GasPrice)
	if overflow {
		return fmt.Errorf("%w: gas cost overflow", ErrInsufficientFunds)
	}
	if msg.GasTokenID == msg.TransferTokenID {
		combined, overflow := new(uint256.Int).AddOverflow(gasCost, value)
		if overflow || st.state.GetBalanceByTokenID(msg.From, msg.GasTokenID).Cmp(combined) < 0 {
			return fmt.Errorf("%w: address %v", ErrInsufficientFunds, msg.From.Hex())
		}
		return nil
	}
	if st.state.GetBalanceByTokenID(msg.From, msg.GasTokenID).Cmp(gasCost) < 0 ||
		st.state.GetBalanceByTokenID(msg.From, msg.TransferTokenID).Cmp(value) < 0 {
		return fmt.Errorf("%w: address %v", ErrInsufficientFunds, msg.From.Hex())
	}
	return nil
}

func (st *stateTransition) validateGasConversion(defaultTokenID uint64) error {
	msg := st.msg
	if msg.GasTokenID == defaultTokenID || msg.GasPrice.IsZero() {
		return nil
	}
	refundRate, convertedPrice, err := st.dryRunPayAsGas()
	if err != nil || convertedPrice.IsZero() {
		if err == nil {
			err = errors.New("zero converted gas price")
		}
		return fmt.Errorf("%w: %v", ErrQKCGasConversion, err)
	}
	reserveCost, overflow := mulGasPrice(msg.GasLimit, convertedPrice)
	if overflow || st.state.GetBalanceByTokenID(vm.GeneralNativeTokenManagerAddress(), defaultTokenID).Cmp(reserveCost) < 0 {
		return fmt.Errorf("%w: insufficient manager reserve", ErrQKCGasConversion)
	}
	st.refundRate = refundRate
	st.convertedGasPrice = convertedPrice
	return nil
}

func mulGasPrice(gas uint64, price *uint256.Int) (*uint256.Int, bool) {
	return new(uint256.Int).MulOverflow(new(uint256.Int).SetUint64(gas), price)
}

func (st *stateTransition) canCredit(addr common.Address, amount *uint256.Int, tokenID uint64) bool {
	if amount.IsZero() {
		return true
	}
	_, overflow := new(uint256.Int).AddOverflow(st.state.GetBalanceByTokenID(addr, tokenID), amount)
	return !overflow
}

type qkcCredit struct {
	addr    common.Address
	amount  *uint256.Int
	tokenID uint64
}

func (st *stateTransition) validateCredits(credits ...qkcCredit) bool {
	type creditKey struct {
		addr    common.Address
		tokenID uint64
	}
	totals := make(map[creditKey]*uint256.Int, len(credits))
	for _, credit := range credits {
		if credit.amount.IsZero() {
			continue
		}
		key := creditKey{addr: credit.addr, tokenID: credit.tokenID}
		if totals[key] == nil {
			totals[key] = new(uint256.Int)
		}
		var overflow bool
		totals[key], overflow = new(uint256.Int).AddOverflow(totals[key], credit.amount)
		if overflow {
			return false
		}
	}
	for key, amount := range totals {
		if !st.canCredit(key.addr, amount, key.tokenID) {
			return false
		}
	}
	return true
}

func (st *stateTransition) dryRunPayAsGas() (*big.Int, *uint256.Int, error) {
	snapshot := st.state.Snapshot()
	savedContext := st.evm.TxContext
	savedExecutionFullShardKey := st.evm.QKCExecutionFullShardKey()
	savedTracer := st.evm.Config.Tracer
	resumeStateHooks := func() {}
	if suspender, ok := st.state.(qkcStateHookSuspender); ok {
		resumeStateHooks = suspender.SuspendQKCStateHooks()
	}
	st.evm.Config.Tracer = nil
	defer func() {
		st.state.RevertToSnapshot(snapshot)
		st.evm.SetTxContext(savedContext)
		st.evm.SetQKCExecutionFullShardKey(savedExecutionFullShardKey)
		st.evm.Config.Tracer = savedTracer
		resumeStateHooks()
	}()
	return st.payAsGas(st.evm)
}

func (st *stateTransition) payAsGas(executionEVM *vm.EVM) (*big.Int, *uint256.Int, error) {
	data := make([]byte, 4+32*3)
	copy(data[:4], common.FromHex("0x5ae8f7f1"))
	binary.BigEndian.PutUint64(data[4+24:4+32], st.msg.GasTokenID)
	binary.BigEndian.PutUint64(data[4+32+24:4+64], st.msg.GasLimit)
	st.msg.GasPrice.WriteToSlice(data[4+64 : 4+96])

	savedContext := executionEVM.TxContext
	// Pyquarkchain leaves both token IDs at vm.Message's zero default for this
	// synthetic, zero-value manager self-call. Zero is the literal token "0",
	// not an unspecified-token sentinel.
	executionEVM.SetTxContext(vm.TxContext{
		Origin:          vm.GeneralNativeTokenManagerAddress(),
		GasPrice:        new(uint256.Int),
		GasTokenID:      0,
		TransferTokenID: 0,
	})
	output, _, err := executionEVM.Call(
		vm.GeneralNativeTokenManagerAddress(),
		vm.GeneralNativeTokenManagerAddress(),
		data,
		vm.NewGasBudget(1_000_000),
		new(uint256.Int),
	)
	executionEVM.SetTxContext(savedContext)
	if err != nil {
		return nil, nil, fmt.Errorf("manager call failed: %w", err)
	}
	// Python's int.from_bytes accepts short slices and preserves the full
	// 256-bit refund word, so mirror output[:32] and output[32:64] exactly.
	refundBytes := output
	if len(refundBytes) > 32 {
		refundBytes = refundBytes[:32]
	}
	var priceBytes []byte
	if len(output) > 32 {
		priceBytes = output[32:]
		if len(priceBytes) > 32 {
			priceBytes = priceBytes[:32]
		}
	}
	return new(big.Int).SetBytes(refundBytes), new(uint256.Int).SetBytes(priceBytes), nil
}

func (st *stateTransition) settle(defaultTokenID uint64, convertedPrice *uint256.Int, refundRate *big.Int, usedGas uint64) error {
	remainingValue, overflow := mulGasPrice(st.gasRemaining.RegularGas, convertedPrice)
	if overflow {
		return ErrQKCGasConversion
	}
	refundBig := new(big.Int).Mul(remainingValue.ToBig(), refundRate)
	refundBig.Div(refundBig, big.NewInt(100))
	refund, overflow := uint256.FromBig(refundBig)
	if overflow {
		return ErrQKCGasConversion
	}
	burn := new(uint256.Int).Sub(remainingValue, refund)
	fee, overflow := mulGasPrice(usedGas, convertedPrice)
	if overflow {
		return ErrQKCGasConversion
	}
	feeBig := fee.ToBig()
	if localFeeRate := st.evm.QuarkChainConfig().LocalFeeRate; localFeeRate != nil {
		feeBig.Mul(feeBig, localFeeRate.Num())
		feeBig.Div(feeBig, localFeeRate.Denom())
	}
	localFee, overflow := uint256.FromBig(feeBig)
	if overflow {
		return ErrQKCGasConversion
	}
	if !st.validateCredits(
		qkcCredit{addr: st.msg.From, amount: refund, tokenID: defaultTokenID},
		qkcCredit{addr: common.Address{}, amount: burn, tokenID: defaultTokenID},
		qkcCredit{addr: st.evm.Context.Coinbase, amount: localFee, tokenID: defaultTokenID},
	) {
		return fmt.Errorf("%w: settlement balance overflow", ErrQKCGasConversion)
	}
	st.state.AddBalanceByTokenID(st.msg.From, refund, defaultTokenID, tracing.BalanceIncreaseGasReturn)
	if !burn.IsZero() {
		st.state.AddBalanceByTokenID(common.Address{}, burn, defaultTokenID, tracing.BalanceChangeUnspecified)
	}
	st.state.AddBalanceByTokenID(st.evm.Context.Coinbase, localFee, defaultTokenID, tracing.BalanceIncreaseRewardTransactionFee)
	return st.gp.ReturnGas(st.gasRemaining.RegularGas, usedGas)
}
