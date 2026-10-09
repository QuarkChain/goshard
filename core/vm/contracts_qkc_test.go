// Copyright 2026-2027, QuarkChain.

package vm

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

type qkcPrecompileProbe struct {
	called   bool
	evm      *EVM
	caller   common.Address
	address  common.Address
	value    uint64
	readOnly bool
	depth    int
	input    []byte
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
	p.readOnly = contract.IsStatic
	p.depth = contract.Depth
	p.input = bytes.Clone(input)
	return []byte{0x2a}, nil
}

func newQKCPrecompileTestEVM(t *testing.T) *EVM {
	t.Helper()
	db, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	blockCtx := BlockContext{
		CanTransfer: func(db StateDB, addr common.Address, amount *uint256.Int) bool {
			return db.GetBalance(addr).Cmp(amount) >= 0
		},
		Transfer: func(db StateDB, from, to common.Address, amount *uint256.Int, _ *params.Rules) {
			db.SubBalance(from, amount, tracing.BalanceChangeTransfer)
			db.AddBalance(to, amount, tracing.BalanceChangeTransfer)
		},
		BlockNumber: big.NewInt(0),
		Difficulty:  big.NewInt(1),
	}
	return NewEVM(blockCtx, db, petersburgOnlyChainConfig(), Config{})
}

func TestQKCEVMAwarePrecompileDispatch(t *testing.T) {
	origin := common.HexToAddress("0x1000")
	caller := common.HexToAddress("0x2000")
	target := common.HexToAddress("0xf001")
	input := []byte{1, 2, 3}

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
			probe := new(qkcPrecompileProbe)
			precompiles := ActivePrecompiledContracts(evm.chainRules)
			precompiles[target] = probe
			evm.SetPrecompiles(precompiles)
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
