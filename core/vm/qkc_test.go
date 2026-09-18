// Copyright 2026-2027, QuarkChain.

package vm

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func qkcTestBlockContext(timestamp uint64) BlockContext {
	return BlockContext{
		CanTransfer: func(db StateDB, from common.Address, value *uint256.Int) bool {
			return db.GetBalance(from).Cmp(value) >= 0
		},
		Transfer: func(db StateDB, from, to common.Address, value *uint256.Int, _ *params.Rules) {
			db.SubBalance(from, value, tracing.BalanceChangeTransfer)
			db.AddBalance(to, value, tracing.BalanceChangeTransfer)
		},
		BlockNumber: big.NewInt(1),
		Difficulty:  big.NewInt(1),
		Time:        timestamp,
	}
}

func newQKCTestEVM(t *testing.T, defaultToken uint64, origin common.Address, timestamp uint64) (*EVM, *state.StateDB) {
	t.Helper()
	statedb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	evm := NewEVM(qkcTestBlockContext(timestamp), statedb, petersburgOnlyChainConfig(), Config{})
	evm.SetTxContext(TxContext{Origin: origin})
	require.NoError(t, evm.SetQKCContext(&QKCContext{
		DefaultChainToken: defaultToken,
		FromFullShardKey:  1,
		EvmEnableTs:       0,
		MntEnableTs:       0,
	}))
	t.Cleanup(evm.Release)
	return evm, statedb
}

func setQKCTestBalance(statedb *state.StateDB, addr common.Address, tokenID, amount uint64) {
	statedb.SetBalanceByTokenID(addr, uint256.NewInt(amount), tokenID, tracing.BalanceChangeUnspecified)
}

func TestQKCProfileRejectsUnsupportedRules(t *testing.T) {
	statedb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	blockContext := qkcTestBlockContext(1)
	random := common.Hash{}
	blockContext.Random = &random
	evm := NewEVM(blockContext, statedb, params.AllDevChainProtocolChanges, Config{})
	t.Cleanup(evm.Release)
	err = evm.SetQKCContext(&QKCContext{DefaultChainToken: qkccommon.DefaultTokenID})
	require.ErrorIs(t, err, ErrQKCUnsupportedRules)
}

func TestQKCProfileUsesChainDefaultToken(t *testing.T) {
	qeth := qkccommon.TokenIDEncode("QETH")
	sender := common.HexToAddress("0x1001")
	recipient := common.HexToAddress("0x1002")
	evm, statedb := newQKCTestEVM(t, qeth, sender, 1)
	setQKCTestBalance(statedb, sender, qkccommon.DefaultTokenID, 11)
	setQKCTestBalance(statedb, sender, qeth, 9)
	setQKCTestBalance(statedb, recipient, qkccommon.DefaultTokenID, 3)
	setQKCTestBalance(statedb, recipient, qeth, 1)

	initialGas := NewGasBudget(100_000)
	_, left, err := evm.QKCApplyMessage(sender, recipient, nil, initialGas, uint256.NewInt(4), qeth, 1)
	require.NoError(t, err)
	require.Equal(t, initialGas, left)
	require.Equal(t, uint64(5), statedb.GetBalanceByTokenID(sender, qeth).Uint64())
	require.Equal(t, uint64(5), statedb.GetBalanceByTokenID(recipient, qeth).Uint64())
	require.Equal(t, uint64(11), statedb.GetBalance(sender).Uint64())
	require.Equal(t, uint64(3), statedb.GetBalance(recipient).Uint64())
}

func TestQKCProfileBalanceOpcodeUsesChainDefaultToken(t *testing.T) {
	qeth := qkccommon.TokenIDEncode("QETH")
	caller := common.HexToAddress("0x2001")
	contract := common.HexToAddress("0x2002")
	target := common.HexToAddress("0x2003")
	code := append([]byte{byte(PUSH20)}, target.Bytes()...)
	code = append(code, byte(BALANCE), byte(PUSH1), 0, byte(MSTORE), byte(PUSH1), 32, byte(PUSH1), 0, byte(RETURN))

	evm, statedb := newQKCTestEVM(t, qeth, caller, 1)
	statedb.SetCode(contract, code, tracing.CodeChangeUnspecified)
	setQKCTestBalance(statedb, target, qkccommon.DefaultTokenID, 11)
	setQKCTestBalance(statedb, target, qeth, 42)
	output, _, err := evm.QKCApplyMessage(caller, contract, nil, NewGasBudget(100_000), new(uint256.Int), qeth, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(42), new(uint256.Int).SetBytes(output).Uint64())

	plain := NewEVM(qkcTestBlockContext(1), statedb, petersburgOnlyChainConfig(), Config{})
	t.Cleanup(plain.Release)
	output, _, err = plain.Call(caller, contract, nil, NewGasBudget(100_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, uint64(11), new(uint256.Int).SetBytes(output).Uint64())
}

func TestQKCProfileSelfdestructUsesChainDefaultToken(t *testing.T) {
	qeth := qkccommon.TokenIDEncode("QETH")
	caller := common.HexToAddress("0x3001")
	contract := common.HexToAddress("0x3002")
	beneficiary := common.HexToAddress("0x3003")
	code := append([]byte{byte(PUSH20)}, beneficiary.Bytes()...)
	code = append(code, byte(SELFDESTRUCT))

	evm, statedb := newQKCTestEVM(t, qeth, caller, 1)
	statedb.SetCode(contract, code, tracing.CodeChangeUnspecified)
	setQKCTestBalance(statedb, contract, qkccommon.DefaultTokenID, 5)
	setQKCTestBalance(statedb, contract, qeth, 7)
	_, _, err := evm.QKCApplyMessage(caller, contract, nil, NewGasBudget(100_000), new(uint256.Int), qeth, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(7), statedb.GetBalanceByTokenID(beneficiary, qeth).Uint64())
	require.Zero(t, statedb.GetBalanceByTokenID(beneficiary, qkccommon.DefaultTokenID).Uint64())
	require.Zero(t, statedb.GetBalanceByTokenID(contract, qeth).Uint64())
	require.Equal(t, uint64(5), statedb.GetBalance(contract).Uint64())
	require.True(t, statedb.HasSelfDestructed(contract))
	statedb.Finalise(true)
	require.False(t, statedb.Exist(contract))
	require.Zero(t, statedb.GetBalance(contract).Uint64())
}

func TestQKCProfileParentRevertRestoresSelfdestruct(t *testing.T) {
	qeth := qkccommon.TokenIDEncode("QETH")
	caller := common.HexToAddress("0x3011")
	parent := common.HexToAddress("0x3012")
	child := common.HexToAddress("0x3013")
	beneficiary := common.HexToAddress("0x3014")
	childCode := append([]byte{byte(PUSH20)}, beneficiary.Bytes()...)
	childCode = append(childCode, byte(SELFDESTRUCT))
	parentCode := append([]byte{byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH20)}, child.Bytes()...)
	parentCode = append(parentCode, byte(PUSH2), 0xff, 0xff, byte(CALL), byte(POP), byte(PUSH1), 0, byte(PUSH1), 0, byte(REVERT))

	evm, statedb := newQKCTestEVM(t, qeth, caller, 1)
	statedb.SetCode(parent, parentCode, tracing.CodeChangeUnspecified)
	statedb.SetCode(child, childCode, tracing.CodeChangeUnspecified)
	setQKCTestBalance(statedb, child, qeth, 7)
	_, _, err := evm.QKCApplyMessage(caller, parent, nil, NewGasBudget(150_000), new(uint256.Int), qeth, 1)
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, uint64(7), statedb.GetBalanceByTokenID(child, qeth).Uint64())
	require.Zero(t, statedb.GetBalanceByTokenID(beneficiary, qeth).Uint64())
	require.False(t, statedb.HasSelfDestructed(child))
}

func TestQKCProfileCreateAddressAndTopLevelNonce(t *testing.T) {
	caller := common.HexToAddress("0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a")
	evm, statedb := newQKCTestEVM(t, qkccommon.DefaultTokenID, caller, 1)
	statedb.SetNonce(caller, 1, tracing.NonceChangeUnspecified) // admission already incremented it
	wantAddress := common.HexToAddress("0x2e51de24dc44092078776c0c6d31d5837cf8e13f")
	setQKCTestBalance(statedb, wantAddress, qkccommon.DefaultTokenID, 777)
	setQKCTestBalance(statedb, wantAddress, qkccommon.TokenIDEncode("QETH"), 888)

	_, address, _, err := evm.QKCCreateContract(caller, hexutil.MustDecode("0x60006000f3"), NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1, nil)
	require.NoError(t, err)
	require.Equal(t, wantAddress, address)
	require.Equal(t, uint64(1), statedb.GetNonce(caller))
	require.Equal(t, uint64(1), statedb.GetNonce(address))
	require.Equal(t, uint64(777), statedb.GetBalance(address).Uint64())
	require.Equal(t, uint64(888), statedb.GetBalanceByTokenID(address, qkccommon.TokenIDEncode("QETH")).Uint64())
}

func TestQKCProfileCreateUsesNativeTracing(t *testing.T) {
	caller := common.HexToAddress("0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a")
	initCode := hexutil.MustDecode("0x60006000f3")
	wantAddress := QKCContractAddress(caller, 1, 0)
	var (
		enterCount int
		exitCount  int
		enterType  byte
		enterTo    common.Address
	)
	hooks := &tracing.Hooks{
		OnEnter: func(_ int, typ byte, _ common.Address, to common.Address, _ []byte, _ uint64, _ *big.Int) {
			enterCount++
			enterType = typ
			enterTo = to
		},
		OnExit: func(_ int, _ []byte, _ uint64, _ error, _ bool) {
			exitCount++
		},
	}
	statedb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	evm := NewEVM(qkcTestBlockContext(1), statedb, petersburgOnlyChainConfig(), Config{Tracer: hooks})
	evm.SetTxContext(TxContext{Origin: caller})
	require.NoError(t, evm.SetQKCContext(&QKCContext{
		DefaultChainToken: qkccommon.DefaultTokenID,
		FromFullShardKey:  1,
	}))
	t.Cleanup(evm.Release)
	statedb.SetNonce(caller, 1, tracing.NonceChangeUnspecified)

	_, address, _, err := evm.QKCCreateContract(caller, initCode, NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1, nil)
	require.NoError(t, err)
	require.Equal(t, wantAddress, address)
	require.Equal(t, 1, enterCount)
	require.Equal(t, byte(CREATE), enterType)
	require.Equal(t, wantAddress, enterTo)
	require.Equal(t, 1, exitCount)
}

func TestQKCProfileNestedCreateChecksDefaultTokenBalance(t *testing.T) {
	qeth := qkccommon.TokenIDEncode("QETH")
	origin := common.HexToAddress("0x3101")
	creator := common.HexToAddress("0x3102")
	evm, statedb := newQKCTestEVM(t, qeth, origin, 1)
	setQKCTestBalance(statedb, creator, qkccommon.DefaultTokenID, 100)
	setQKCTestBalance(statedb, creator, qeth, 1)
	statedb.SetNonce(creator, 1, tracing.NonceChangeUnspecified)

	initialGas := NewGasBudget(100_000)
	_, _, left, err := evm.Create(creator, hexutil.MustDecode("0x60006000f3"), initialGas, uint256.NewInt(2))
	require.ErrorIs(t, err, ErrInsufficientBalance)
	require.Equal(t, initialGas, left)
	require.Equal(t, uint64(1), statedb.GetNonce(creator))
}

func TestQKCProfileDelegateCallDoesNotCheckInheritedValueBalance(t *testing.T) {
	originCaller := common.HexToAddress("0x3201")
	caller := common.HexToAddress("0x3202")
	target := common.HexToAddress("0x3203")
	evm, _ := newQKCTestEVM(t, qkccommon.DefaultTokenID, originCaller, 1)

	initialGas := NewGasBudget(100_000)
	_, left, err := evm.DelegateCall(originCaller, caller, target, nil, initialGas, uint256.NewInt(7))
	require.NoError(t, err)
	require.Equal(t, initialGas, left)
}

func TestQKCProfileUnsupportedMNTAbandonsMessage(t *testing.T) {
	qeth := qkccommon.TokenIDEncode("QETH")
	caller := common.HexToAddress("0x4001")
	contract := common.HexToAddress("0x4002")
	callBalanceMNT := []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE)}
	callBalanceMNT = append(callBalanceMNT, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH20))
	callBalanceMNT = append(callBalanceMNT, qkcBalanceMntAddress.Bytes()...)
	callBalanceMNT = append(callBalanceMNT, byte(PUSH2), 0xff, 0xff, byte(CALL), byte(STOP))

	evm, statedb := newQKCTestEVM(t, qkccommon.DefaultTokenID, caller, 1)
	statedb.SetCode(contract, callBalanceMNT, tracing.CodeChangeUnspecified)
	_, left, err := evm.QKCApplyMessage(caller, contract, nil, NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1)
	require.ErrorIs(t, err, ErrQKCUnsupportedMNT)
	require.NotZero(t, left.RegularGas)
	require.Equal(t, common.Hash{}, statedb.GetState(contract, common.Hash{}))

	other, _ := newQKCTestEVM(t, qeth, caller, 1)
	_, _, err = other.QKCApplyMessage(caller, contract, nil, NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1)
	require.ErrorIs(t, err, ErrQKCUnsupportedMNT)

	inactive, _ := newQKCTestEVM(t, qkccommon.DefaultTokenID, caller, 0)
	_, _, err = inactive.QKCApplyMessage(caller, qkcBalanceMntAddress, nil, NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1)
	require.NoError(t, err)
}

func TestQKCProfileGasChargeUnderflowIsFrameOOG(t *testing.T) {
	tests := []struct {
		name       string
		code       string
		gas        uint64
		coveredGas uint64
	}{
		{"create2 word gas", "0x6000610be0526000610c0060006000f5", 32_700, 33_000},
		{"log data gas", "0x60006101e0526102006000a0", 4_500, 5_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			caller := common.HexToAddress("0x5001")
			contract := common.HexToAddress("0x5002")
			evm, statedb := newQKCTestEVM(t, qkccommon.DefaultTokenID, caller, 1)
			statedb.SetCode(contract, hexutil.MustDecode(test.code), tracing.CodeChangeUnspecified)
			_, left, err := evm.QKCApplyMessage(caller, contract, nil, NewGasBudget(test.gas), new(uint256.Int), qkccommon.DefaultTokenID, 1)
			require.ErrorIs(t, err, ErrOutOfGas)
			require.Zero(t, left.RegularGas)

			coveredEVM, coveredState := newQKCTestEVM(t, qkccommon.DefaultTokenID, caller, 1)
			coveredState.SetCode(contract, hexutil.MustDecode(test.code), tracing.CodeChangeUnspecified)
			_, left, err = coveredEVM.QKCApplyMessage(caller, contract, nil, NewGasBudget(test.coveredGas), new(uint256.Int), qkccommon.DefaultTokenID, 1)
			require.NoError(t, err)
			require.NotZero(t, left.RegularGas)
		})
	}
}

func TestQKCProfilePOSWGate(t *testing.T) {
	sender := common.HexToAddress("0x6001")
	recipient := common.HexToAddress("0x6002")
	evm, statedb := newQKCTestEVM(t, qkccommon.DefaultTokenID, sender, 1)
	setQKCTestBalance(statedb, sender, qkccommon.DefaultTokenID, 10)
	evm.QKC.SenderDisallowMap = map[common.Address]*uint256.Int{sender: uint256.NewInt(8)}

	_, left, err := evm.QKCApplyMessage(sender, recipient, nil, NewGasBudget(100_000), uint256.NewInt(3), qkccommon.DefaultTokenID, 1)
	require.ErrorIs(t, err, ErrQKCSenderDisallowed)
	require.Zero(t, left.RegularGas)
	require.Equal(t, uint64(10), statedb.GetBalance(sender).Uint64())
	require.Zero(t, statedb.GetBalance(recipient).Uint64())
}
