// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
	qkcparams "github.com/ethereum/go-ethereum/qkc/params"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func newQKCMessageTest(t *testing.T, timestamp uint64) (*vm.EVM, *state.StateDB, *qkcconfig.QuarkChainConfig) {
	t.Helper()
	db, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	config.EnableNonReservedNativeTokenTimestamp = 0
	config.EnableGeneralNativeTokenTimestamp = 0
	blockCtx := vm.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    Transfer,
		Coinbase:    common.HexToAddress("0xc01b45e"),
		BlockNumber: big.NewInt(0),
		Difficulty:  big.NewInt(1),
		Time:        timestamp,
		GasLimit:    1_000_000,
	}
	chainConfig := qkcparams.DefaultConstantinople
	db.SetQuarkChainConfig(config)
	return vm.NewEVM(blockCtx, db, &chainConfig, vm.Config{}), db, config
}

func legacyQKCMessage(from common.Address, to *common.Address, defaultTokenID uint64) *Message {
	key := uint32(0)
	return &Message{
		From:            from,
		To:              to,
		Nonce:           0,
		Value:           new(uint256.Int),
		GasLimit:        22_000,
		GasPrice:        uint256.NewInt(1),
		GasTokenID:      defaultTokenID,
		TransferTokenID: defaultTokenID,
		ToFullShardKey:  &key,
	}
}

func TestApplyQKCMessageDefaultTokenSettlement(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x1000")
	recipient := common.HexToAddress("0x2000")
	defaultTokenID := config.GetDefaultChainTokenID()
	db.AddBalanceByTokenID(sender, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.Value = uint256.NewInt(7)
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.Equal(t, uint64(21_000), result.UsedGas)
	require.Equal(t, uint64(21_000), result.MaxUsedGas)
	require.Equal(t, uint64(1), db.GetNonce(sender))
	require.Equal(t, uint64(78_993), db.GetBalanceByTokenID(sender, defaultTokenID).Uint64())
	require.Equal(t, uint64(7), db.GetBalanceByTokenID(recipient, defaultTokenID).Uint64())
	require.Equal(t, uint64(10_500), db.GetBalanceByTokenID(evm.Context.Coinbase, defaultTokenID).Uint64())
	require.Equal(t, uint64(79_000), gp.Gas())
	require.Equal(t, uint64(21_000), gp.CumulativeUsed())
}

func TestApplyQKCMessagePayAsGasCommitsOnce(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x1000")
	recipient := common.HexToAddress("0x2000")
	manager := vm.GeneralNativeTokenManagerAddress()
	defaultTokenID := config.GetDefaultChainTokenID()
	gasTokenID := uint64(1)

	// Increment storage slot 0, then return refundRate=100 and convertedPrice=2.
	// The final value proves the validation call was reverted and only the
	// committed payAsGas call mutated state.
	db.SetCode(manager, common.FromHex("0x6000546001016000556064600052600260205260406000f3"), tracing.CodeChangeUnspecified)
	db.AddBalanceByTokenID(manager, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	db.AddBalanceByTokenID(sender, uint256.NewInt(50_000), gasTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.GasTokenID = gasTokenID
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.Equal(t, common.BytesToHash([]byte{1}), db.GetState(manager, common.Hash{}))
	require.Equal(t, uint64(56_000), db.GetBalanceByTokenID(manager, defaultTokenID).Uint64())
	require.Equal(t, uint64(22_000), db.GetBalanceByTokenID(manager, gasTokenID).Uint64())
	require.Equal(t, uint64(28_000), db.GetBalanceByTokenID(sender, gasTokenID).Uint64())
	require.Equal(t, uint64(2_000), db.GetBalanceByTokenID(sender, defaultTokenID).Uint64())
	require.Equal(t, uint64(21_000), db.GetBalanceByTokenID(evm.Context.Coinbase, defaultTokenID).Uint64())
	require.Equal(t, uint64(21_000), result.UsedGas)
	require.Equal(t, uint64(79_000), gp.Gas())
}

func TestQKCPayAsGasPreservesWideRefundRate(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	manager := vm.GeneralNativeTokenManagerAddress()
	// Return refundRate=2^64 and convertedPrice=1. Pyquarkchain decodes the
	// entire first 32-byte word instead of narrowing it to a Go uint64.
	db.SetCode(manager, common.FromHex("0x68010000000000000000600052600160205260406000f3"), tracing.CodeChangeUnspecified)
	msg := legacyQKCMessage(common.HexToAddress("0x2300"), nil, config.GetDefaultChainTokenID())
	msg.GasTokenID = 1
	transition := newStateTransition(evm, msg, NewGasPool(100_000))

	refundRate, convertedPrice, err := transition.payAsGas(evm)
	require.NoError(t, err)
	require.Equal(t, "18446744073709551616", fmt.Sprint(refundRate))
	require.Equal(t, uint64(1), convertedPrice.Uint64())
}

func TestApplyQKCMessagePayAsGasDryRunDoesNotEmitTraceFrames(t *testing.T) {
	db, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	config.EnableNonReservedNativeTokenTimestamp = 0
	config.EnableGeneralNativeTokenTimestamp = 0
	manager := vm.GeneralNativeTokenManagerAddress()
	managerCalls := 0
	managerStorageChanges := 0
	tracer := &tracing.Hooks{
		OnEnter: func(_ int, _ byte, _ common.Address, to common.Address, _ []byte, _ uint64, _ *big.Int) {
			if to == manager {
				managerCalls++
			}
		},
		OnStorageChange: func(addr common.Address, _, _, _ common.Hash) {
			if addr == manager {
				managerStorageChanges++
			}
		},
	}
	blockCtx := vm.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    Transfer,
		Coinbase:    common.HexToAddress("0xc01b45e"),
		BlockNumber: big.NewInt(0),
		Difficulty:  big.NewInt(1),
		Time:        1,
		GasLimit:    1_000_000,
	}
	chainConfig := qkcparams.DefaultConstantinople
	hooked := state.NewHookedState(db, tracer)
	db.SetQuarkChainConfig(config)
	evm := vm.NewEVM(blockCtx, hooked, &chainConfig, vm.Config{Tracer: tracer})
	defer evm.Release()
	sender := common.HexToAddress("0x1100")
	recipient := common.HexToAddress("0x1200")
	defaultTokenID := config.GetDefaultChainTokenID()
	gasTokenID := uint64(1)
	db.SetCode(manager, common.FromHex("0x6000546001016000556064600052600260205260406000f3"), tracing.CodeChangeUnspecified)
	db.AddBalanceByTokenID(manager, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	db.AddBalanceByTokenID(sender, uint256.NewInt(50_000), gasTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.GasTokenID = gasTokenID

	result, err := ApplyMessage(evm, msg, NewGasPool(100_000))
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.Equal(t, 1, managerCalls)
	require.Equal(t, 1, managerStorageChanges)
	require.Equal(t, common.BytesToHash([]byte{1}), db.GetState(manager, common.Hash{}))
}

func TestApplyQKCMessagePreservesMissingExecutionShardKey(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x1600")
	recipient := common.HexToAddress("0x1700")
	defaultTokenID := config.GetDefaultChainTokenID()
	db.AddBalanceByTokenID(sender, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	db.SetFullShardKey(0)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.ToFullShardKey = nil

	result, err := ApplyMessage(evm, msg, NewGasPool(100_000))
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.Nil(t, evm.QKCExecutionFullShardKey())
}

func TestApplyQKCMessagePayAsGasPersistsAcrossUserRevert(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x2100")
	target := common.HexToAddress("0x2200")
	manager := vm.GeneralNativeTokenManagerAddress()
	defaultTokenID := config.GetDefaultChainTokenID()
	gasTokenID := uint64(1)

	// Increment storage slot 0, then return refundRate=100 and convertedPrice=2.
	// The manager call is part of transaction prepayment and must remain committed
	// even when the subsequent user frame reverts.
	db.SetCode(manager, common.FromHex("0x6000546001016000556064600052600260205260406000f3"), tracing.CodeChangeUnspecified)
	db.SetCode(target, common.FromHex("0x60006000fd"), tracing.CodeChangeUnspecified)
	db.AddBalanceByTokenID(manager, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	db.AddBalanceByTokenID(sender, uint256.NewInt(50_000), gasTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &target, defaultTokenID)
	msg.GasTokenID = gasTokenID
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
	require.Empty(t, result.ReturnData)
	require.Equal(t, common.BytesToHash([]byte{1}), db.GetState(manager, common.Hash{}))
	require.Equal(t, uint64(56_000), db.GetBalanceByTokenID(manager, defaultTokenID).Uint64())
	require.Equal(t, uint64(22_000), db.GetBalanceByTokenID(manager, gasTokenID).Uint64())
	require.Equal(t, uint64(28_000), db.GetBalanceByTokenID(sender, gasTokenID).Uint64())
	require.Equal(t, uint64(1_988), db.GetBalanceByTokenID(sender, defaultTokenID).Uint64())
	require.Equal(t, uint64(21_006), db.GetBalanceByTokenID(evm.Context.Coinbase, defaultTokenID).Uint64())
	require.Equal(t, uint64(21_006), result.UsedGas)
	require.Equal(t, uint64(78_994), gp.Gas())
	require.Equal(t, uint64(21_006), gp.CumulativeUsed())
}

func TestApplyQKCMessageRejectionIsAtomic(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x1000")
	recipient := common.HexToAddress("0x2000")
	defaultTokenID := config.GetDefaultChainTokenID()
	db.AddBalanceByTokenID(sender, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.GasTokenID = 1 // A non-default zero balance is rejected.
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.ErrorIs(t, err, ErrQKCInvalidToken)
	require.Nil(t, result)
	require.Zero(t, db.GetNonce(sender))
	require.Equal(t, uint64(100_000), db.GetBalanceByTokenID(sender, defaultTokenID).Uint64())
	require.Equal(t, uint64(100_000), gp.Gas())
	require.Zero(t, gp.CumulativeUsed())
}

func TestApplyQKCMessageSkipNonceChecks(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x2100")
	recipient := common.HexToAddress("0x2200")
	defaultTokenID := config.GetDefaultChainTokenID()
	db.AddBalanceByTokenID(sender, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.Nonce = 7
	msg.SkipNonceChecks = true

	result, err := ApplyMessage(evm, msg, NewGasPool(100_000))
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.Equal(t, uint64(1), db.GetNonce(sender))
}

func TestApplyQKCMessageVMFailureStillSettlesGas(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x3000")
	recipient := common.HexToAddress("0x4000")
	defaultTokenID := config.GetDefaultChainTokenID()
	db.AddBalanceByTokenID(sender, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	db.SetCode(recipient, common.FromHex("0x60006000fd"), tracing.CodeChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
	require.Empty(t, result.ReturnData)
	require.Equal(t, uint64(21_006), result.UsedGas)
	require.Equal(t, uint64(21_006), result.MaxUsedGas)
	require.Equal(t, uint64(1), db.GetNonce(sender))
	require.Equal(t, uint64(78_994), db.GetBalanceByTokenID(sender, defaultTokenID).Uint64())
	require.Equal(t, uint64(10_503), db.GetBalanceByTokenID(evm.Context.Coinbase, defaultTokenID).Uint64())
	require.Equal(t, uint64(78_994), gp.Gas())
	require.Equal(t, uint64(21_006), gp.CumulativeUsed())
}

func TestApplyQKCMessageSettlementOverflowIsAtomic(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x4100")
	recipient := common.HexToAddress("0x4200")
	defaultTokenID := config.GetDefaultChainTokenID()
	db.AddBalanceByTokenID(sender, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	db.AddBalanceByTokenID(evm.Context.Coinbase, new(uint256.Int).SetAllOne(), defaultTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.ErrorIs(t, err, ErrQKCGasConversion)
	require.Nil(t, result)
	require.Zero(t, db.GetNonce(sender))
	require.Equal(t, uint64(100_000), db.GetBalanceByTokenID(sender, defaultTokenID).Uint64())
	require.True(t, db.GetBalanceByTokenID(evm.Context.Coinbase, defaultTokenID).Eq(new(uint256.Int).SetAllOne()))
	require.Equal(t, uint64(100_000), gp.Gas())
	require.NoError(t, db.Error())
}

func TestApplyQKCMessageZeroPriceSkipsGasConversion(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x5000")
	recipient := common.HexToAddress("0x6000")
	defaultTokenID := config.GetDefaultChainTokenID()
	gasTokenID := uint64(1)
	db.AddBalanceByTokenID(sender, uint256.NewInt(1), gasTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.GasTokenID = gasTokenID
	msg.GasPrice = new(uint256.Int)
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.Equal(t, uint64(1), db.GetBalanceByTokenID(sender, gasTokenID).Uint64())
	require.Zero(t, db.GetBalanceByTokenID(vm.GeneralNativeTokenManagerAddress(), gasTokenID).Uint64())
	require.Zero(t, db.GetBalanceByTokenID(evm.Context.Coinbase, defaultTokenID).Uint64())
	require.Equal(t, uint64(79_000), gp.Gas())
}

func TestApplyQKCMessageRefundRateBurnsRemainder(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0x7000")
	recipient := common.HexToAddress("0x8000")
	manager := vm.GeneralNativeTokenManagerAddress()
	defaultTokenID := config.GetDefaultChainTokenID()
	gasTokenID := uint64(1)
	// Return refundRate=50 and convertedPrice=2.
	db.SetCode(manager, common.FromHex("0x6032600052600260205260406000f3"), tracing.CodeChangeUnspecified)
	db.AddBalanceByTokenID(manager, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	db.AddBalanceByTokenID(sender, uint256.NewInt(50_000), gasTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.GasTokenID = gasTokenID
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.Equal(t, uint64(1_000), db.GetBalanceByTokenID(sender, defaultTokenID).Uint64())
	require.Equal(t, uint64(1_000), db.GetBalanceByTokenID(common.Address{}, defaultTokenID).Uint64())
	require.Equal(t, uint64(21_000), db.GetBalanceByTokenID(evm.Context.Coinbase, defaultTokenID).Uint64())
	require.Equal(t, uint64(56_000), db.GetBalanceByTokenID(manager, defaultTokenID).Uint64())
	require.Equal(t, uint64(22_000), db.GetBalanceByTokenID(manager, gasTokenID).Uint64())
}

func TestApplyQKCMessageAcknowledgementFailureIsVMResult(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0xb000")
	target := common.HexToAddress("0xc000")
	defaultTokenID := config.GetDefaultChainTokenID()
	transferTokenID := uint64(1)
	db.AddBalanceByTokenID(sender, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	db.AddBalanceByTokenID(sender, uint256.NewInt(1), transferTokenID, tracing.BalanceChangeUnspecified)
	db.SetCode(target, []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified)
	msg := legacyQKCMessage(sender, &target, defaultTokenID)
	msg.TransferTokenID = transferTokenID
	msg.Value = uint256.NewInt(1)
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, vm.ErrExecutionReverted)
	require.Empty(t, result.ReturnData)
	require.Equal(t, uint64(21_000), result.UsedGas)
	require.Equal(t, uint64(1), db.GetNonce(sender))
	require.Equal(t, uint64(1), db.GetBalanceByTokenID(sender, transferTokenID).Uint64())
	require.Zero(t, db.GetBalanceByTokenID(target, transferTokenID).Uint64())
	require.Equal(t, uint64(79_000), db.GetBalanceByTokenID(sender, defaultTokenID).Uint64())
	require.Equal(t, uint64(79_000), gp.Gas())
}

func TestApplyQKCGasConversionRejectionRevertsDryRun(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0xd000")
	recipient := common.HexToAddress("0xe000")
	manager := vm.GeneralNativeTokenManagerAddress()
	defaultTokenID := config.GetDefaultChainTokenID()
	gasTokenID := uint64(1)
	// Increment slot 0 but return convertedPrice=0, which rejects the message.
	db.SetCode(manager, common.FromHex("0x6000546001016000556064600052600060205260406000f3"), tracing.CodeChangeUnspecified)
	db.AddBalanceByTokenID(sender, uint256.NewInt(50_000), gasTokenID, tracing.BalanceChangeUnspecified)
	db.SetFullShardKey(7)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.GasTokenID = gasTokenID
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.ErrorIs(t, err, ErrQKCGasConversion)
	require.Nil(t, result)
	require.Equal(t, common.Hash{}, db.GetState(manager, common.Hash{}))
	require.Zero(t, db.GetNonce(sender))
	require.Equal(t, uint64(50_000), db.GetBalanceByTokenID(sender, gasTokenID).Uint64())
	require.Equal(t, uint64(100_000), gp.Gas())
	require.Zero(t, gp.CumulativeUsed())
}

func TestApplyQKCGasConversionDecodesShortOutputLikePyquarkchain(t *testing.T) {
	evm, db, config := newQKCMessageTest(t, 1)
	defer evm.Release()
	sender := common.HexToAddress("0xf000")
	recipient := common.HexToAddress("0xf100")
	manager := vm.GeneralNativeTokenManagerAddress()
	defaultTokenID := config.GetDefaultChainTokenID()
	gasTokenID := uint64(1)

	// Mutate storage and return refundRate=100 followed by one price byte. Python
	// slices output[32:64], so the 33-byte result decodes convertedPrice=1.
	db.SetCode(manager, common.FromHex("0x6000546001016000556064600052600160205360216000f3"), tracing.CodeChangeUnspecified)
	db.AddBalanceByTokenID(manager, uint256.NewInt(100_000), defaultTokenID, tracing.BalanceChangeUnspecified)
	db.AddBalanceByTokenID(sender, uint256.NewInt(50_000), gasTokenID, tracing.BalanceChangeUnspecified)
	msg := legacyQKCMessage(sender, &recipient, defaultTokenID)
	msg.GasTokenID = gasTokenID
	gp := NewGasPool(100_000)

	result, err := ApplyMessage(evm, msg, gp)
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.Equal(t, common.BytesToHash([]byte{1}), db.GetState(manager, common.Hash{}))
	require.Equal(t, uint64(78_000), db.GetBalanceByTokenID(manager, defaultTokenID).Uint64())
	require.Equal(t, uint64(22_000), db.GetBalanceByTokenID(manager, gasTokenID).Uint64())
	require.Equal(t, uint64(28_000), db.GetBalanceByTokenID(sender, gasTokenID).Uint64())
	require.Equal(t, uint64(1_000), db.GetBalanceByTokenID(sender, defaultTokenID).Uint64())
	require.Equal(t, uint64(21_000), result.UsedGas)
	require.Equal(t, uint64(79_000), gp.Gas())
	require.Equal(t, uint64(21_000), gp.CumulativeUsed())
}
