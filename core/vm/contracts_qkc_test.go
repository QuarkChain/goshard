// Copyright 2026-2027, QuarkChain.

package vm

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

type qkcPrecompileProbe struct {
	called          bool
	evm             *EVM
	caller          common.Address
	address         common.Address
	value           uint64
	transferTokenID uint64
	readOnly        bool
	depth           int
	input           []byte
}

func (*qkcPrecompileProbe) RequiredGas([]byte) uint64  { return 3 }
func (*qkcPrecompileProbe) Run([]byte) ([]byte, error) { return nil, nil }
func (*qkcPrecompileProbe) Name() string               { return "QKC_TEST_PROBE" }

func (p *qkcPrecompileProbe) RunWithEVM(input []byte, evm *EVM, contract *Contract) ([]byte, error) {
	p.called = true
	p.evm = evm
	p.caller = contract.Caller()
	p.address = contract.Address()
	p.value = contract.value.Uint64()
	p.transferTokenID = contract.TransferTokenID
	p.readOnly = contract.IsStatic
	p.depth = contract.Depth
	p.input = bytes.Clone(input)
	return []byte{0x2a}, nil
}

func newQKCPrecompileTestEVM(t *testing.T) *EVM {
	t.Helper()
	evm, _ := newQKCPrecompileTestEVMAt(t, 0, nil)
	return evm
}

func newQKCPrecompileTestEVMAt(t *testing.T, timestamp uint64, config *qkcconfig.QuarkChainConfig) (*EVM, *state.StateDB) {
	t.Helper()
	db, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	if config != nil {
		db.SetQuarkChainConfig(config)
	}
	blockCtx := BlockContext{
		CanTransfer: func(db StateDB, addr common.Address, amount *uint256.Int) bool {
			return db.GetBalance(addr).Cmp(amount) >= 0
		},
		Transfer: func(db StateDB, from, to common.Address, amount *uint256.Int, _ *params.Rules) {
			db.SubBalance(from, amount, tracing.BalanceChangeTransfer)
			db.AddBalance(to, amount, tracing.BalanceChangeTransfer)
		},
		BlockNumber: big.NewInt(0),
		Time:        timestamp,
		Difficulty:  big.NewInt(1),
	}
	return NewEVM(blockCtx, db, petersburgOnlyChainConfig(), Config{}), db
}

func installQKCPrecompileProbe(evm *EVM, target common.Address) *qkcPrecompileProbe {
	probe := new(qkcPrecompileProbe)
	precompiles := ActivePrecompiledContracts(evm.chainRules)
	precompiles[target] = probe
	evm.SetPrecompiles(precompiles)
	return probe
}

func TestQKCEVMAwarePrecompileDispatch(t *testing.T) {
	origin := common.HexToAddress("0x1000")
	caller := common.HexToAddress("0x2000")
	target := common.HexToAddress("0xf001")
	input := []byte{1, 2, 3}
	const transferTokenID = uint64(123)

	tests := []struct {
		name            string
		wantCaller      common.Address
		wantAddress     common.Address
		wantValue       uint64
		wantReadOnly    bool
		initialReadOnly bool
		run             func(*EVM) ([]byte, GasBudget, error)
	}{
		{
			name:        "CALL",
			wantCaller:  caller,
			wantAddress: target,
			run: func(evm *EVM) ([]byte, GasBudget, error) {
				return evm.Call(caller, target, input, NewGasBudget(10), new(uint256.Int))
			},
		},
		{
			name:            "CALL_IN_STATIC_CONTEXT",
			wantCaller:      caller,
			wantAddress:     target,
			wantReadOnly:    true,
			initialReadOnly: true,
			run: func(evm *EVM) ([]byte, GasBudget, error) {
				return evm.Call(caller, target, input, NewGasBudget(10), new(uint256.Int))
			},
		},
		{
			name:        "CALLCODE",
			wantCaller:  caller,
			wantAddress: caller,
			run: func(evm *EVM) ([]byte, GasBudget, error) {
				return evm.CallCode(caller, target, input, NewGasBudget(10), new(uint256.Int))
			},
		},
		{
			name:        "DELEGATECALL",
			wantCaller:  origin,
			wantAddress: caller,
			wantValue:   7,
			run: func(evm *EVM) ([]byte, GasBudget, error) {
				return evm.DelegateCall(origin, caller, target, input, NewGasBudget(10), uint256.NewInt(7))
			},
		},
		{
			name:         "STATICCALL",
			wantCaller:   caller,
			wantAddress:  target,
			wantReadOnly: true,
			run: func(evm *EVM) ([]byte, GasBudget, error) {
				return evm.StaticCall(caller, target, input, NewGasBudget(10))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evm := newQKCPrecompileTestEVM(t)
			defer evm.Release()
			evm.SetTxContext(TxContext{TransferTokenID: transferTokenID})
			probe := installQKCPrecompileProbe(evm, target)
			evm.readOnly = test.initialReadOnly
			evm.depth = 7

			output, gas, err := test.run(evm)
			require.NoError(t, err)
			require.Equal(t, []byte{0x2a}, output)
			require.Equal(t, uint64(7), gas.RegularGas)
			require.True(t, probe.called)
			require.Same(t, evm, probe.evm)
			require.Equal(t, test.wantCaller, probe.caller)
			require.Equal(t, test.wantAddress, probe.address)
			require.Equal(t, test.wantValue, probe.value)
			require.Equal(t, transferTokenID, probe.transferTokenID)
			require.Equal(t, test.wantReadOnly, probe.readOnly)
			require.Equal(t, 7, probe.depth)
			require.Equal(t, input, probe.input)
			require.Equal(t, test.initialReadOnly, evm.readOnly)
		})
	}
	if _, ok := activePrecompiledContracts(petersburgOnlyChainConfig().Rules(big.NewInt(0), false, 0))[target]; ok {
		t.Fatalf("probe address %s leaked into the package precompile registry", target)
	}
}

func TestTransferTokenIDPropagatesToNestedCalls(t *testing.T) {
	caller := common.HexToAddress("0x1000")
	parent := common.HexToAddress("0x2000")
	target := common.HexToAddress("0xf001")
	const transferTokenID = uint64(123)

	for _, opcode := range []OpCode{CALL, CALLCODE, DELEGATECALL, STATICCALL} {
		t.Run(opcode.String(), func(t *testing.T) {
			evm := newQKCPrecompileTestEVM(t)
			defer evm.Release()
			evm.SetTxContext(TxContext{TransferTokenID: transferTokenID})
			probe := installQKCPrecompileProbe(evm, target)
			evm.StateDB.SetCode(parent, callPrecompileCode(opcode, target), tracing.CodeChangeUnspecified)

			_, _, err := evm.Call(caller, parent, nil, NewGasBudget(100_000), new(uint256.Int))
			require.NoError(t, err)
			require.True(t, probe.called)
			require.Equal(t, transferTokenID, probe.transferTokenID)
		})
	}
}

func callPrecompileCode(opcode OpCode, target common.Address) []byte {
	zeroCount := 4
	if opcode == CALL || opcode == CALLCODE {
		zeroCount++ // CALL and CALLCODE also pop value.
	}
	code := []byte{byte(PUSH1), 0}
	for i := 1; i < zeroCount; i++ {
		code = append(code, byte(DUP1))
	}
	code = append(code, byte(PUSH20))
	code = append(code, target[:]...)
	return append(code, byte(GAS), byte(opcode), byte(STOP))
}

func TestCurrentMntIDActivationAndGas(t *testing.T) {
	require.Equal(t, uint64(3), new(currentMntID).RequiredGas(nil))

	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 5

	for _, test := range []struct {
		name      string
		timestamp uint64
		wantGas   uint64
		wantToken bool
	}{
		{name: "after activation", timestamp: 6, wantGas: 7, wantToken: true},
		{name: "at activation boundary", timestamp: 5, wantGas: 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			evm, _ := newQKCPrecompileTestEVMAt(t, test.timestamp, config)
			defer evm.Release()
			evm.SetTxContext(TxContext{TransferTokenID: 123})

			output, gas, err := evm.Call(common.Address{1}, currentMntIDAddr, nil, NewGasBudget(10), new(uint256.Int))
			require.NoError(t, err)
			require.Equal(t, test.wantGas, gas.RegularGas)
			if test.wantToken {
				require.Len(t, output, 32)
				require.Equal(t, uint64(123), binary.BigEndian.Uint64(output[24:]))
			} else {
				require.Empty(t, output)
			}
		})
	}
}

func TestTokenIDQueryIsPerCallFrame(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	caller := common.HexToAddress("0x1000")
	target := common.HexToAddress("0x2000")

	for _, test := range []struct {
		name       string
		code       []byte
		qkcEnabled bool
		wantErr    bool
	}{
		{name: "QKC disabled", code: []byte{byte(STOP)}},
		{name: "not queried", code: []byte{byte(STOP)}, qkcEnabled: true, wantErr: true},
		{name: "CALL marks parent queried", code: callPrecompileCode(CALL, currentMntIDAddr), qkcEnabled: true},
		{name: "STATICCALL marks parent queried", code: callPrecompileCode(STATICCALL, currentMntIDAddr), qkcEnabled: true},
		{name: "CALLCODE only marks child queried", code: callPrecompileCode(CALLCODE, currentMntIDAddr), qkcEnabled: true, wantErr: true},
		{name: "DELEGATECALL only marks child queried", code: callPrecompileCode(DELEGATECALL, currentMntIDAddr), qkcEnabled: true, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var qkcConfig *qkcconfig.QuarkChainConfig
			if test.qkcEnabled {
				qkcConfig = config
			}
			evm, db := newQKCPrecompileTestEVMAt(t, 1, qkcConfig)
			defer evm.Release()
			db.AddBalance(caller, uint256.NewInt(10), tracing.BalanceChangeUnspecified)
			db.SetCode(target, test.code, tracing.CodeChangeUnspecified)
			evm.SetTxContext(TxContext{TransferTokenID: 1})

			_, _, err := evm.Call(caller, target, nil, NewGasBudget(100_000), uint256.NewInt(1))
			if test.wantErr {
				require.ErrorIs(t, err, ErrExecutionReverted)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTokenIDQueryCheckAppliesToStandardPrecompilesWithCode(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	caller := common.HexToAddress("0x1000")
	precompileAddr := common.BytesToAddress([]byte{4})

	for _, opcode := range []OpCode{CALL, CALLCODE, DELEGATECALL} {
		t.Run(opcode.String(), func(t *testing.T) {
			evm, db := newQKCPrecompileTestEVMAt(t, 1, config)
			defer evm.Release()
			db.AddBalance(caller, uint256.NewInt(10), tracing.BalanceChangeUnspecified)
			db.SetCode(precompileAddr, []byte{byte(STOP)}, tracing.CodeChangeUnspecified)
			evm.SetTxContext(TxContext{TransferTokenID: 1})

			var err error
			switch opcode {
			case CALL:
				_, _, err = evm.Call(caller, precompileAddr, nil, NewGasBudget(100_000), uint256.NewInt(1))
			case CALLCODE:
				_, _, err = evm.CallCode(caller, precompileAddr, nil, NewGasBudget(100_000), uint256.NewInt(1))
			case DELEGATECALL:
				_, _, err = evm.DelegateCall(common.Address{1}, caller, precompileAddr, nil, NewGasBudget(100_000), uint256.NewInt(1))
			}
			require.ErrorIs(t, err, ErrExecutionReverted)
		})
	}
}

func TestTransferTokenIDPropagatesToCreateFrames(t *testing.T) {
	caller := common.HexToAddress("0x1000")
	parent := common.HexToAddress("0x2000")
	target := common.HexToAddress("0xf001")
	initcode := callPrecompileCode(CALL, target)

	t.Run("direct CREATE", func(t *testing.T) {
		evm := newQKCPrecompileTestEVM(t)
		defer evm.Release()
		probe := installQKCPrecompileProbe(evm, target)
		evm.SetTxContext(TxContext{TransferTokenID: qkccommon.DefaultTokenID})

		_, _, _, err := evm.Create(caller, initcode, NewGasBudget(100_000), new(uint256.Int))
		require.NoError(t, err)
		require.True(t, probe.called)
		require.Equal(t, qkccommon.DefaultTokenID, probe.transferTokenID)
	})

	for _, opcode := range []OpCode{CREATE, CREATE2} {
		t.Run("nested "+opcode.String(), func(t *testing.T) {
			evm := newQKCPrecompileTestEVM(t)
			defer evm.Release()
			probe := installQKCPrecompileProbe(evm, target)
			evm.StateDB.SetCode(parent, createContractCode(opcode, initcode, 1), tracing.CodeChangeUnspecified)
			evm.SetTxContext(TxContext{TransferTokenID: qkccommon.DefaultTokenID})

			_, _, err := evm.Call(caller, parent, nil, NewGasBudget(200_000), new(uint256.Int))
			require.NoError(t, err)
			require.True(t, probe.called)
			require.Equal(t, qkccommon.DefaultTokenID, probe.transferTokenID)
		})
	}
}

func createContractCode(opcode OpCode, initcode []byte, salt uint64) []byte {
	prefix := []byte{
		byte(PUSH1), byte(len(initcode)),
		byte(PUSH1), 0, // initcode offset, filled below
		byte(PUSH1), 0,
		byte(CODECOPY),
	}
	if opcode == CREATE2 {
		prefix = append(prefix, byte(PUSH1), byte(salt))
	}
	prefix = append(prefix,
		byte(PUSH1), byte(len(initcode)),
		byte(PUSH1), 0,
		byte(PUSH1), 0,
		byte(opcode), byte(POP), byte(STOP),
	)
	prefix[3] = byte(len(prefix))
	return append(prefix, initcode...)
}
