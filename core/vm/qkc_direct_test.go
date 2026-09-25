// Copyright 2026-2027, QuarkChain.

package vm

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func newQKCDirectEVM(t *testing.T, block BlockContext, origin common.Address, shardKey uint32) (*EVM, *state.StateDB) {
	t.Helper()
	statedb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	evm := NewEVM(qkcTestBlockContext(block), statedb, petersburgOnlyChainConfig(), Config{})
	evm.SetTxContext(TxContext{Origin: origin, ToFullShardKey: shardKey})
	t.Cleanup(evm.Release)
	return evm, statedb
}

func qkcTestBlockContext(block BlockContext) BlockContext {
	if block.CanTransfer == nil {
		block.CanTransfer = func(db StateDB, addr common.Address, amount *uint256.Int) bool {
			return db.GetBalance(addr).Cmp(amount) >= 0
		}
	}
	if block.Transfer == nil {
		block.Transfer = func(db StateDB, sender, recipient common.Address, amount *uint256.Int, _ *params.Rules) {
			db.SubBalance(sender, amount, tracing.BalanceChangeTransfer)
			db.AddBalance(recipient, amount, tracing.BalanceChangeTransfer)
		}
	}
	return block
}

func persistQKCState(t *testing.T, evm *EVM, statedb *state.StateDB) *state.StateDB {
	t.Helper()
	root, err := statedb.Commit(0, false, false)
	require.NoError(t, err)
	persisted, err := state.New(root, statedb.Database())
	require.NoError(t, err)
	evm.StateDB = persisted
	evm.SetTxContext(evm.TxContext)
	return persisted
}

func TestQKCCreateAddressUsesFullShardKey(t *testing.T) {
	caller := common.HexToAddress("0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a")
	evm, statedb := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
	statedb.SetNonce(caller, 1, tracing.NonceChangeUnspecified) // admission increments before CREATE
	statedb.SetBalance(caller, uint256.NewInt(10), tracing.BalanceChangeUnspecified)

	_, address, _, err := evm.QKCCreateContract(caller, common.FromHex("0x60006000f3"), NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1, nil)
	require.NoError(t, err)
	require.Equal(t, QKCContractAddress(caller, 1, 0), address)
	require.Equal(t, uint64(1), statedb.GetNonce(caller))
	require.Equal(t, uint64(1), statedb.GetNonce(address))
}

func TestNonQKCCreateRetainsGethNonceLifecycle(t *testing.T) {
	caller := common.HexToAddress("0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a")
	evm, statedb := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
	statedb.SetBalance(caller, uint256.NewInt(10), tracing.BalanceChangeUnspecified)

	_, address, _, err := evm.Create(caller, common.FromHex("0x60006000f3"), NewGasBudget(100_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, crypto.CreateAddress(caller, 0), address)
	require.Equal(t, uint64(1), statedb.GetNonce(caller))
}

func TestQKCBalanceOpcodeReadsQKC(t *testing.T) {
	caller := common.HexToAddress("0x2001")
	contract := common.HexToAddress("0x2002")
	target := common.HexToAddress("0x2003")
	evm, statedb := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
	code := append([]byte{byte(PUSH20)}, target.Bytes()...)
	code = append(code, byte(BALANCE), byte(PUSH1), 0, byte(MSTORE), byte(PUSH1), 32, byte(PUSH1), 0, byte(RETURN))
	statedb.SetCode(contract, code, tracing.CodeChangeUnspecified)
	statedb.SetBalance(target, uint256.NewInt(11), tracing.BalanceChangeUnspecified)

	output, _, err := evm.Call(caller, contract, nil, NewGasBudget(100_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, uint64(11), new(uint256.Int).SetBytes(output).Uint64())
}

func TestQKCSelfdestructMovesQKCBalance(t *testing.T) {
	caller := common.HexToAddress("0x3001")
	contract := common.HexToAddress("0x3002")
	beneficiary := common.HexToAddress("0x3003")
	evm, statedb := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
	code := append([]byte{byte(PUSH20)}, beneficiary.Bytes()...)
	code = append(code, byte(SELFDESTRUCT))
	statedb.SetCode(contract, code, tracing.CodeChangeUnspecified)
	statedb.SetBalance(contract, uint256.NewInt(7), tracing.BalanceChangeUnspecified)

	_, _, err := evm.Call(caller, contract, nil, NewGasBudget(100_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, uint64(7), statedb.GetBalance(beneficiary).Uint64())
	require.Zero(t, statedb.GetBalance(contract).Uint64())
	require.True(t, statedb.HasSelfDestructed(contract))
}

func TestQKCMessageRevertRollsBackTransfer(t *testing.T) {
	caller := common.HexToAddress("0x4001")
	contract := common.HexToAddress("0x4002")
	evm, statedb := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
	statedb.SetBalance(caller, uint256.NewInt(10), tracing.BalanceChangeUnspecified)
	statedb.SetCode(contract, common.FromHex("0x60006000fd"), tracing.CodeChangeUnspecified)

	_, _, err := evm.Call(caller, contract, nil, NewGasBudget(100_000), uint256.NewInt(3))
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, uint64(10), statedb.GetBalance(caller).Uint64())
	require.Zero(t, statedb.GetBalance(contract).Uint64())
}

func TestQKCNestedSelfdestructRevertsWithParent(t *testing.T) {
	caller := common.HexToAddress("0x6001")
	parent := common.HexToAddress("0x6002")
	child := common.HexToAddress("0x6003")
	beneficiary := common.HexToAddress("0x6004")
	evm, statedb := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
	childCode := append([]byte{byte(PUSH20)}, beneficiary.Bytes()...)
	childCode = append(childCode, byte(SELFDESTRUCT))
	parentCode := append([]byte{byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH20)}, child.Bytes()...)
	parentCode = append(parentCode, byte(PUSH2), 0xff, 0xff, byte(CALL), byte(POP), byte(PUSH1), 0, byte(PUSH1), 0, byte(REVERT))
	statedb.SetCode(parent, parentCode, tracing.CodeChangeUnspecified)
	statedb.SetCode(child, childCode, tracing.CodeChangeUnspecified)
	statedb.SetBalance(child, uint256.NewInt(7), tracing.BalanceChangeUnspecified)

	_, _, err := evm.Call(caller, parent, nil, NewGasBudget(150_000), new(uint256.Int))
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, uint64(7), statedb.GetBalance(child).Uint64())
	require.Zero(t, statedb.GetBalance(beneficiary).Uint64())
	require.False(t, statedb.HasSelfDestructed(child))
}

func TestQKCActiveMNTPrecompileAbandonsMessage(t *testing.T) {
	caller := common.HexToAddress("0x5001")
	evm, _ := newQKCDirectEVM(t, BlockContext{
		BlockNumber:        big.NewInt(1),
		Time:               1,
		EVMEnableTimestamp: 0,
	}, caller, 1)

	_, _, err := evm.Call(caller, qkcBalanceMNTAddress, nil, NewGasBudget(100_000), new(uint256.Int))
	require.ErrorIs(t, err, ErrQKCUnsupportedMNT)
}

func TestQKCNestedMNTPrecompileAbandonsMessage(t *testing.T) {
	caller := common.HexToAddress("0x7001")
	parent := common.HexToAddress("0x7002")
	block := BlockContext{
		BlockNumber:        big.NewInt(1),
		Time:               1,
		EVMEnableTimestamp: 0,
		MNTEnableTimestamp: 0,
	}
	tests := []struct {
		name   string
		opcode OpCode
	}{
		{name: "call", opcode: CALL},
		{name: "delegatecall", opcode: DELEGATECALL},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evm, statedb := newQKCDirectEVM(t, block, caller, 1)
			code := []byte{byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE)}
			if test.opcode == CALL {
				code = append(code, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0)
			} else {
				code = append(code, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0)
			}
			code = append(code, byte(PUSH20))
			code = append(code, qkcBalanceMNTAddress.Bytes()...)
			code = append(code, byte(PUSH2), 0xff, 0xff, byte(test.opcode), byte(STOP))
			statedb.SetCode(parent, code, tracing.CodeChangeUnspecified)

			_, _, err := evm.QKCApplyMessage(caller, parent, nil, NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1)
			require.ErrorIs(t, err, ErrQKCUnsupportedMNT)
			require.Equal(t, common.Hash{}, statedb.GetState(parent, common.Hash{}))
		})
	}
}

func TestQKCMNTPrecompileInInitCodeAbandonsMessage(t *testing.T) {
	caller := common.HexToAddress("0x7101")
	contract := common.HexToAddress("0x7102")
	evm, statedb := newQKCDirectEVM(t, BlockContext{
		BlockNumber:        big.NewInt(1),
		Time:               1,
		EVMEnableTimestamp: 0,
		MNTEnableTimestamp: 0,
	}, caller, 1)
	initCode := []byte{byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH20)}
	initCode = append(initCode, qkcBalanceMNTAddress.Bytes()...)
	initCode = append(initCode, byte(PUSH2), 0xff, 0xff, byte(CALL), byte(STOP))

	_, _, _, err := evm.QKCCreateContract(caller, initCode, NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1, &contract)
	require.ErrorIs(t, err, ErrQKCUnsupportedMNT)
	require.False(t, statedb.Exist(contract))
}

func TestQKCCreatePreservesPreexistingAccountStorage(t *testing.T) {
	caller := common.HexToAddress("0x7201")
	contract := common.HexToAddress("0x7202")
	key := common.HexToHash("0x01")
	value := common.HexToHash("0x2a")
	evm, statedb := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
	statedb.SetBalance(contract, uint256.NewInt(7), tracing.BalanceChangeUnspecified)
	statedb.SetState(contract, key, value)
	statedb = persistQKCState(t, evm, statedb)

	_, address, _, err := evm.QKCCreateContract(caller, common.FromHex("0x60006000f3"), NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1, &contract)
	require.NoError(t, err)
	require.Equal(t, contract, address)
	require.Equal(t, uint64(7), statedb.GetBalance(contract).Uint64())
	require.Equal(t, value, statedb.GetState(contract, key))
	statedb.Finalise(true)
	root, err := statedb.Commit(0, false, false)
	require.NoError(t, err)
	statedb, err = state.New(root, statedb.Database())
	require.NoError(t, err)
	require.Equal(t, uint64(7), statedb.GetBalance(contract).Uint64())
	require.Equal(t, value, statedb.GetState(contract, key))
}

func TestQKCCreateRevertPreservesExistingStorage(t *testing.T) {
	caller := common.HexToAddress("0x7281")
	contract := common.HexToAddress("0x7282")
	key := common.HexToHash("0x01")
	value := common.HexToHash("0x2a")
	evm, statedb := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
	statedb.SetBalance(contract, uint256.NewInt(7), tracing.BalanceChangeUnspecified)
	statedb.SetState(contract, key, value)
	statedb = persistQKCState(t, evm, statedb)

	_, _, _, err := evm.QKCCreateContract(caller, common.FromHex("0x60006000fd"), NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1, &contract)
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, uint64(7), statedb.GetBalance(contract).Uint64())
	require.Equal(t, value, statedb.GetState(contract, key))
	statedb.Finalise(true)
	root, err := statedb.Commit(0, false, false)
	require.NoError(t, err)
	statedb, err = state.New(root, statedb.Database())
	require.NoError(t, err)
	require.Equal(t, uint64(7), statedb.GetBalance(contract).Uint64())
	require.Equal(t, value, statedb.GetState(contract, key))
}

func TestQKCEthereumPrecompileRequiresTimestampAfterZero(t *testing.T) {
	caller := common.HexToAddress("0x7301")
	precompile := common.HexToAddress("0x01")

	inactive, _ := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
	output, gas, err := inactive.Call(caller, precompile, nil, NewGasBudget(10_000), new(uint256.Int))
	require.NoError(t, err)
	require.Empty(t, output)
	require.Equal(t, uint64(10_000), gas.RegularGas)

	active, _ := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1), Time: 1}, caller, 1)
	_, gas, err = active.Call(caller, precompile, nil, NewGasBudget(10_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, uint64(7_000), gas.RegularGas)
}
