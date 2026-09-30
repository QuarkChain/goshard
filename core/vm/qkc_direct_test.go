// Copyright 2026-2027, QuarkChain.

package vm

import (
	"math"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func newQKCDirectEVM(t *testing.T, block BlockContext, origin common.Address, shardKey uint32) (*EVM, *state.StateDB) {
	t.Helper()
	return newConfiguredEVM(t, block, origin, shardKey, qkcconfig.NewQuarkChainConfig())
}

func newConfiguredEVM(t *testing.T, block BlockContext, origin common.Address, shardKey uint32, cfg *qkcconfig.QuarkChainConfig) (*EVM, *state.StateDB) {
	t.Helper()
	statedb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	evm := NewEVM(qkcTestBlockContext(block), statedb, petersburgOnlyChainConfig(), Config{QKCConfig: cfg})
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

func TestQKCCreateRespectsPOSWLock(t *testing.T) {
	caller := common.HexToAddress("0x1901")
	evm, statedb := newQKCDirectEVM(t, BlockContext{
		BlockNumber:       big.NewInt(1),
		SenderDisallowMap: map[common.Address]*uint256.Int{caller: uint256.NewInt(8)},
	}, caller, 1)
	statedb.SetBalance(caller, uint256.NewInt(10), tracing.BalanceChangeUnspecified)
	statedb.SetNonce(caller, 1, tracing.NonceChangeUnspecified) // admission increments before CREATE

	_, address, gas, err := evm.QKCCreateContract(caller, nil, NewGasBudget(100_000), uint256.NewInt(3), qkccommon.DefaultTokenID, 1, nil)
	require.ErrorIs(t, err, ErrQKCSenderDisallowed)
	require.Equal(t, common.Address{}, address)
	require.Zero(t, gas.RegularGas)
	require.Equal(t, uint64(10), statedb.GetBalance(caller).Uint64())
	require.Equal(t, uint64(1), statedb.GetNonce(caller))
	require.False(t, statedb.Exist(QKCContractAddress(caller, 1, 0)))

	_, address, gas, err = evm.QKCCreateContract(caller, nil, NewGasBudget(100_000), uint256.NewInt(2), qkccommon.DefaultTokenID, 1, nil)
	require.NoError(t, err)
	require.Equal(t, QKCContractAddress(caller, 1, 0), address)
	require.Positive(t, gas.RegularGas)
	require.Equal(t, uint64(8), statedb.GetBalance(caller).Uint64())
	require.Equal(t, uint64(2), statedb.GetBalance(address).Uint64())
}

func TestQKCNestedCreateRespectsPOSWLock(t *testing.T) {
	caller := common.HexToAddress("0x1911")
	creator := common.HexToAddress("0x1912")
	for _, opcode := range []OpCode{CREATE, CREATE2} {
		t.Run(opcode.String(), func(t *testing.T) {
			evm, statedb := newQKCDirectEVM(t, BlockContext{
				BlockNumber:       big.NewInt(1),
				SenderDisallowMap: map[common.Address]*uint256.Int{creator: uint256.NewInt(8)},
			}, caller, 1)
			code := []byte{}
			if opcode == CREATE2 {
				code = append(code, byte(PUSH1), 0) // salt
			}
			code = append(code, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 3, byte(opcode))
			code = append(code, byte(PUSH1), 0, byte(MSTORE), byte(PUSH1), 32, byte(PUSH1), 0, byte(RETURN))
			statedb.SetCode(creator, code, tracing.CodeChangeUnspecified)
			statedb.SetBalance(creator, uint256.NewInt(10), tracing.BalanceChangeUnspecified)

			output, gas, err := evm.QKCApplyMessage(caller, creator, nil, NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 1)
			require.NoError(t, err)
			require.Equal(t, make([]byte, 32), output)      // failed CREATE pushes zero
			require.Less(t, gas.RegularGas, uint64(10_000)) // failed child consumes its gas
			require.Equal(t, uint64(10), statedb.GetBalance(creator).Uint64())
			require.Zero(t, statedb.GetNonce(creator))
			if opcode == CREATE {
				require.False(t, statedb.Exist(QKCContractAddress(creator, 1, 0)))
			} else {
				require.False(t, statedb.Exist(crypto.CreateAddress2(creator, [32]byte{}, crypto.Keccak256(nil))))
			}
		})
	}
}

func TestNonQKCCreateRetainsGethNonceLifecycle(t *testing.T) {
	caller := common.HexToAddress("0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a")
	evm, statedb := newConfiguredEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1, nil)
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

func TestQKCActiveMNTPrecompileReturnsError(t *testing.T) {
	caller := common.HexToAddress("0x5001")
	cfg := qkcconfig.NewQuarkChainConfig()
	cfg.EnableNonReservedNativeTokenTimestamp = 0
	evm, _ := newConfiguredEVM(t, BlockContext{BlockNumber: big.NewInt(1), Time: 1}, caller, 1, cfg)

	_, _, err := evm.Call(caller, qkcBalanceMNTAddress, nil, NewGasBudget(100_000), new(uint256.Int))
	require.ErrorIs(t, err, ErrQKCUnsupportedMNT)
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

func TestQKCPrecompileActivationUsesChainConfig(t *testing.T) {
	cfg := qkcconfig.NewQuarkChainConfig()
	cfg.EnableEvmTimeStamp = 10
	cfg.EnableNonReservedNativeTokenTimestamp = 20
	for _, tc := range []struct {
		name      string
		config    *qkcconfig.QuarkChainConfig
		timestamp uint64
		evmActive bool
		mntActive bool
	}{
		{"before evm", cfg, 9, false, false},
		{"at evm", cfg, 10, false, false},
		{"after evm", cfg, 11, true, false},
		{"before mnt", cfg, 19, true, false},
		{"at mnt", cfg, 20, true, false},
		{"after mnt", cfg, 21, true, true},
		{"default mnt disabled", qkcconfig.NewQuarkChainConfig(), math.MaxUint64, true, false},
		{"ethereum", nil, math.MaxUint64, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caller := common.HexToAddress("0x7401")
			evm, statedb := newConfiguredEVM(t, BlockContext{BlockNumber: big.NewInt(1), Time: tc.timestamp}, caller, 1, tc.config)
			for _, target := range []struct {
				address common.Address
				active  bool
			}{
				{qkcCurrentMNTIDAddress, tc.evmActive},
				{qkcTransferMNTAddress, tc.evmActive},
				{qkcDeploySystemContractAddress, tc.evmActive},
				{qkcMintMNTAddress, tc.mntActive},
				{qkcBalanceMNTAddress, tc.mntActive},
			} {
				// Inactive system addresses execute ordinary account code.
				statedb.SetCode(target.address, common.FromHex("0x602a60005260206000f3"), tracing.CodeChangeUnspecified)
				evm.SetTxContext(TxContext{Origin: caller, ToFullShardKey: 1})
				output, _, err := evm.Call(caller, target.address, nil, NewGasBudget(100_000), new(uint256.Int))
				if target.active {
					require.ErrorIs(t, err, ErrQKCUnsupportedMNT, target.address)
				} else {
					require.NoError(t, err, target.address)
					require.Equal(t, uint64(42), new(uint256.Int).SetBytes(output).Uint64(), target.address)
				}
			}
		})
	}
}

func TestQKCCreateThroughCallEntryPoints(t *testing.T) {
	caller := common.HexToAddress("0x7501")
	creator := common.HexToAddress("0x7502")
	for _, entry := range []string{"call", "qkc message"} {
		for _, opcode := range []OpCode{CREATE, CREATE2} {
			t.Run(entry+"/"+opcode.String(), func(t *testing.T) {
				evm, statedb := newQKCDirectEVM(t, BlockContext{BlockNumber: big.NewInt(1)}, caller, 1)
				statedb.SetNonce(creator, 7, tracing.NonceChangeUnspecified)
				for shardKey := uint32(1); shardKey <= 2; shardKey++ {
					var code []byte
					want := QKCContractAddress(creator, shardKey, statedb.GetNonce(creator))
					if opcode == CREATE2 {
						code = append(code, byte(PUSH1), byte(shardKey)) // salt
						want = crypto.CreateAddress2(creator, uint256.NewInt(uint64(shardKey)).Bytes32(), crypto.Keccak256(nil))
					}
					code = append(code, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(opcode))
					code = append(code, byte(PUSH1), 0, byte(MSTORE), byte(PUSH1), 32, byte(PUSH1), 0, byte(RETURN))
					statedb.SetCode(creator, code, tracing.CodeChangeUnspecified)
					evm.SetTxContext(TxContext{Origin: caller, ToFullShardKey: shardKey})
					var output []byte
					var err error
					if entry == "call" {
						output, _, err = evm.Call(caller, creator, nil, NewGasBudget(100_000), new(uint256.Int))
					} else {
						output, _, err = evm.QKCApplyMessage(caller, creator, nil, NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, shardKey)
					}
					require.NoError(t, err)
					require.Equal(t, want, common.BytesToAddress(output))
					require.Equal(t, uint64(7+shardKey), statedb.GetNonce(creator))
					require.True(t, statedb.Exist(want))
				}
			})
		}
	}
}

func TestNonQKCExecutionIgnoresQKCRules(t *testing.T) {
	caller := common.HexToAddress("0x7601")
	to := common.HexToAddress("0x7602")
	evm, statedb := newConfiguredEVM(t, BlockContext{
		BlockNumber:       big.NewInt(1),
		SenderDisallowMap: map[common.Address]*uint256.Int{caller: uint256.NewInt(10)},
	}, caller, 1, nil)
	statedb.SetBalance(caller, uint256.NewInt(10), tracing.BalanceChangeUnspecified)
	_, _, err := evm.Call(caller, to, nil, NewGasBudget(100_000), uint256.NewInt(1))
	require.NoError(t, err)
	require.Equal(t, uint64(1), statedb.GetBalance(to).Uint64())

	// Ethereum precompiles are available even at timestamp zero.
	_, gas, err := evm.Call(caller, common.HexToAddress("0x01"), nil, NewGasBudget(10_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, uint64(7_000), gas.RegularGas)

	_, _, err = evm.QKCApplyMessage(caller, to, nil, NewGasBudget(100_000), uint256.NewInt(1), qkccommon.DefaultTokenID, 2)
	require.ErrorIs(t, err, errQKCConfigMissing)
	_, _, _, err = evm.QKCCreateContract(caller, nil, NewGasBudget(100_000), new(uint256.Int), qkccommon.DefaultTokenID, 2, nil)
	require.ErrorIs(t, err, errQKCConfigMissing)
	require.Equal(t, uint64(9), statedb.GetBalance(caller).Uint64())
	require.Zero(t, statedb.GetNonce(caller))
	require.Equal(t, uint32(1), evm.TxContext.ToFullShardKey)
}
