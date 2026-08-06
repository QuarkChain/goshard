// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func testMNTConfig() *qkcconfig.QuarkChainConfig {
	return &qkcconfig.QuarkChainConfig{}
}

// testCanTransfer / testTransfer mirror core/evm.go's CanTransfer/Transfer so the
// vm package (which cannot import core) can exercise the real value-transfer path
// inside evm.Call. They route on tokenID exactly as the production functions do.
func testCanTransfer(db StateDB, addr common.Address, amount *uint256.Int, tokenID uint64) bool {
	if tokenID == qkccommon.DefaultTokenID {
		return db.GetBalance(addr).Cmp(amount) >= 0
	}
	return db.GetMntBalance(addr, tokenID).Cmp(amount) >= 0
}

func testTransfer(db StateDB, sender, recipient common.Address, amount *uint256.Int, _ *params.Rules, tokenID uint64) {
	if tokenID == qkccommon.DefaultTokenID {
		db.SubBalance(sender, amount, 0)
		db.AddBalance(recipient, amount, 0)
		return
	}
	db.SubMntBalance(sender, amount, tokenID)
	db.AddMntBalance(recipient, amount, tokenID)
}

// TestTransferMntNoDoubleTransfer guards against the precompile moving MNT
// balances directly AND letting the inner evm.Call transfer again. The value
// must move exactly once: caller -value, recipient +value.
func TestTransferMntNoDoubleTransfer(t *testing.T) {
	statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())

	caller := common.HexToAddress("0xCA11E2")
	to := common.HexToAddress("0xDE57")
	const tokenID = uint64(123456)

	statedb.CreateAccount(caller)
	statedb.AddMntBalance(caller, uint256.NewInt(1000), tokenID)

	blockCtx := BlockContext{
		CanTransfer: testCanTransfer,
		Transfer:    testTransfer,
		BlockNumber: big.NewInt(1),
		Time:        1,
	}
	evm := NewEVM(blockCtx, statedb, params.TestChainConfig, Config{QKCConfig: testMNTConfig()})
	evm.SetTxContext(TxContext{Origin: caller, TransferTokenID: qkccommon.DefaultTokenID})

	// calldata: to(32) + tokenID(32) + value(32), no extra data.
	value := uint256.NewInt(400)
	input := make([]byte, 96)
	copy(input[12:32], to.Bytes())
	new(uint256.Int).SetUint64(tokenID).WriteToSlice(input[32:64])
	value.WriteToSlice(input[64:96])

	// Dispatch through evm.Call to the precompile address — this is the real path
	// where the inner transfer happens. Outer call carries zero QKC value.
	gas := NewGasBudget(1_000_000)
	_, _, err := evm.Call(caller, transferMntAddr, input, gas, new(uint256.Int))
	require.NoError(t, err)

	// Exactly one transfer: caller 1000-400=600, recipient 0+400=400.
	require.Equal(t, uint256.NewInt(600), statedb.GetMntBalance(caller, tokenID), "caller MNT balance after single transfer")
	require.Equal(t, uint256.NewInt(400), statedb.GetMntBalance(to, tokenID), "recipient MNT balance after single transfer")
}

// TestTokenIDQueriedPropagation guards against the precompile setting TokenIDQueried
// on a throwaway contract that's discarded before the evm.Call check, leaving the
// recipient frame unmarked and causing unconditional revert. The fix propagates the
// flag via markTokenIDQueried in opCall/opCallCode/opDelegateCall/opStaticCall when
// the callee is currentMntID, so the recipient's frame gets marked and the transfer
// succeeds.
func TestTokenIDQueriedPropagation(t *testing.T) {
	statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())

	caller := common.HexToAddress("0xCA11E2")
	// Deploy a minimal contract that calls currentMntID (0x514b430001) when invoked.
	// PUSH20 currentMntID; PUSH1 0 ×4 (retSize,retOff,argSize,argOff); DUP5(value=0);
	// DUP6(addr); GAS; CALL — invokes currentMntID, which marks the recipient frame.
	recipientCode := common.Hex2Bytes("73000000000000000000000000000000514b430001600060006000600084855af1")
	recipient := common.HexToAddress("0xCCCC")

	statedb.CreateAccount(caller)
	statedb.CreateAccount(recipient)
	statedb.SetCode(recipient, recipientCode, 0)
	const tokenID = uint64(99999)
	statedb.AddMntBalance(caller, uint256.NewInt(500), tokenID)

	blockCtx := BlockContext{
		CanTransfer: testCanTransfer,
		Transfer:    testTransfer,
		BlockNumber: big.NewInt(1),
		Time:        1,
	}
	evm := NewEVM(blockCtx, statedb, params.TestChainConfig, Config{QKCConfig: testMNTConfig()})
	evm.SetTxContext(TxContext{Origin: caller, TransferTokenID: qkccommon.DefaultTokenID})

	// transferMnt calldata: to(32) + tokenID(32) + value(32), no extra data.
	value := uint256.NewInt(100)
	input := make([]byte, 96)
	copy(input[12:32], recipient.Bytes())
	new(uint256.Int).SetUint64(tokenID).WriteToSlice(input[32:64])
	value.WriteToSlice(input[64:96])

	gas := NewGasBudget(1_000_000)
	_, _, err := evm.Call(caller, transferMntAddr, input, gas, new(uint256.Int))
	require.NoError(t, err, "transferMnt to contract that calls currentMntID should succeed")

	// Verify the transfer happened (caller -100, recipient +100).
	require.Equal(t, uint256.NewInt(400), statedb.GetMntBalance(caller, tokenID), "caller balance after acknowledged transfer")
	require.Equal(t, uint256.NewInt(100), statedb.GetMntBalance(recipient, tokenID), "recipient balance after acknowledged transfer")
}

func TestMNTPrecompileActivation(t *testing.T) {
	evmTime, mntTime := uint64(10), uint64(20)
	config := &qkcconfig.QuarkChainConfig{EnableEvmTimeStamp: evmTime, EnableNonReservedNativeTokenTimestamp: mntTime}

	tests := []struct {
		time   uint64
		qkcEVM bool
		qkcMNT bool
	}{
		{time: 10},
		{time: 11, qkcEVM: true},
		{time: 20, qkcEVM: true},
		{time: 21, qkcEVM: true, qkcMNT: true},
	}
	for _, test := range tests {
		contracts := activePrecompiledContractsQKC(params.TestChainConfig.Rules(big.NewInt(1), false, test.time), test.time, config)
		_, hasEVM := contracts[currentMntIDAddr]
		_, hasMNT := contracts[mintMNTAddr]
		require.Equal(t, test.qkcEVM, hasEVM, "QKCEVM activation at timestamp %d", test.time)
		require.Equal(t, test.qkcMNT, hasMNT, "QKCMNT activation at timestamp %d", test.time)
	}
}

func TestMNTBalanceRouting(t *testing.T) {
	statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	account := common.HexToAddress("0x1234")
	caller := common.HexToAddress("0xCA11E2")
	statedb.AddBalance(account, uint256.NewInt(100), 0)
	statedb.AddMntBalance(account, uint256.NewInt(7), 0)

	blockCtx := BlockContext{
		CanTransfer: testCanTransfer,
		Transfer:    testTransfer,
		BlockNumber: big.NewInt(1),
		Time:        1,
	}
	evm := NewEVM(blockCtx, statedb, params.TestChainConfig, Config{QKCConfig: testMNTConfig()})
	evm.SetTxContext(TxContext{Origin: caller, TransferTokenID: qkccommon.DefaultTokenID})

	for tokenID, want := range map[uint64]uint64{qkccommon.DefaultTokenID: 100, 0: 7} {
		input := make([]byte, 64)
		copy(input[12:32], account.Bytes())
		new(uint256.Int).SetUint64(tokenID).WriteToSlice(input[32:64])
		output, _, err := evm.Call(caller, balanceMNTAddr, input, NewGasBudget(1000), new(uint256.Int))
		require.NoError(t, err)
		require.Equal(t, uint256.NewInt(want), new(uint256.Int).SetBytes(output))
	}
}

func TestTransferMntRoutesDefaultAndTokenZero(t *testing.T) {
	for _, tokenID := range []uint64{qkccommon.DefaultTokenID, 0} {
		t.Run(new(big.Int).SetUint64(tokenID).String(), func(t *testing.T) {
			statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
			caller := common.HexToAddress("0xCA11E2")
			recipient := common.HexToAddress("0xBEEF")
			if tokenID == qkccommon.DefaultTokenID {
				statedb.AddBalance(caller, uint256.NewInt(100), 0)
			} else {
				statedb.AddMntBalance(caller, uint256.NewInt(100), tokenID)
			}

			blockCtx := BlockContext{
				CanTransfer: testCanTransfer,
				Transfer:    testTransfer,
				BlockNumber: big.NewInt(1),
				Time:        1,
			}
			evm := NewEVM(blockCtx, statedb, params.TestChainConfig, Config{QKCConfig: testMNTConfig()})
			evm.SetTxContext(TxContext{Origin: caller, TransferTokenID: qkccommon.DefaultTokenID})

			input := make([]byte, 96)
			copy(input[12:32], recipient.Bytes())
			new(uint256.Int).SetUint64(tokenID).WriteToSlice(input[32:64])
			uint256.NewInt(40).WriteToSlice(input[64:96])
			_, _, err := evm.Call(caller, transferMntAddr, input, NewGasBudget(1_000_000), new(uint256.Int))
			require.NoError(t, err)

			if tokenID == qkccommon.DefaultTokenID {
				require.Equal(t, uint256.NewInt(60), statedb.GetBalance(caller))
				require.Equal(t, uint256.NewInt(40), statedb.GetBalance(recipient))
			} else {
				require.Equal(t, uint256.NewInt(60), statedb.GetMntBalance(caller, tokenID))
				require.Equal(t, uint256.NewInt(40), statedb.GetMntBalance(recipient, tokenID))
			}
		})
	}
}

func TestDeploySystemContractsAtFixedAddresses(t *testing.T) {
	zero := uint64(0)
	config := &qkcconfig.QuarkChainConfig{EnableEvmTimeStamp: zero, EnableNonReservedNativeTokenTimestamp: zero, EnableGeneralNativeTokenTimestamp: zero}

	statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	caller := common.HexToAddress("0xCA11E2")
	blockCtx := BlockContext{
		CanTransfer: testCanTransfer,
		Transfer:    testTransfer,
		BlockNumber: big.NewInt(1),
		Time:        1,
	}
	evm := NewEVM(blockCtx, statedb, params.TestChainConfig, Config{QKCConfig: config})
	evm.SetTxContext(TxContext{Origin: caller, TransferTokenID: qkccommon.DefaultTokenID})

	for index, target := range map[uint64]common.Address{
		0: rootChainPoSWAddr,
		2: nonReservedNativeTokenAddr,
		3: generalNativeTokenAddr,
	} {
		input := make([]byte, 32)
		new(uint256.Int).SetUint64(index).WriteToSlice(input)
		output, _, err := evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(10_000_000), new(uint256.Int))
		require.NoError(t, err, "deploy system contract %d", index)
		require.Equal(t, target.Bytes(), output)
		require.NotEmpty(t, statedb.GetCode(target), "system contract %d not installed at fixed address", index)
	}
}

func TestDeploySystemContractRejectsExistingContract(t *testing.T) {
	config := &qkcconfig.QuarkChainConfig{}
	statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	caller := common.HexToAddress("0xCA11E2")
	evm := NewEVM(BlockContext{
		CanTransfer: testCanTransfer,
		Transfer:    testTransfer,
		BlockNumber: big.NewInt(1),
		Time:        1,
	}, statedb, params.TestChainConfig, Config{QKCConfig: config})
	evm.SetTxContext(TxContext{Origin: caller, TransferTokenID: qkccommon.DefaultTokenID})

	input := make([]byte, 32) // Index 0 defaults to RootChainPoSW (index 1).
	_, _, err := evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(10_000_000), new(uint256.Int))
	require.NoError(t, err)
	code := append([]byte(nil), statedb.GetCode(rootChainPoSWAddr)...)
	deployerNonce := statedb.GetNonce(deploySystemContractAddr)

	_, _, err = evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(10_000_000), new(uint256.Int))
	require.ErrorIs(t, err, ErrContractAddressCollision)
	require.Equal(t, code, statedb.GetCode(rootChainPoSWAddr))
	require.Equal(t, deployerNonce, statedb.GetNonce(deploySystemContractAddr))
}

func TestMintMNTOnlyOnChainZero(t *testing.T) {
	for _, fullShardKey := range []uint32{0, 1 << 16} {
		statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
		recipient := common.HexToAddress("0xBEEF")
		blockCtx := BlockContext{
			CanTransfer: testCanTransfer,
			Transfer:    testTransfer,
			BlockNumber: big.NewInt(1),
			Time:        1,
		}
		evm := NewEVM(blockCtx, statedb, params.TestChainConfig, Config{QKCConfig: testMNTConfig()})
		evm.SetTxContext(TxContext{
			Origin:          nonReservedNativeTokenAddr,
			TransferTokenID: qkccommon.DefaultTokenID,
			FullShardKey:    fullShardKey,
		})

		input := make([]byte, 96)
		copy(input[12:32], recipient.Bytes())
		uint256.NewInt(123).WriteToSlice(input[32:64])
		uint256.NewInt(5).WriteToSlice(input[64:96])
		_, _, err := evm.Call(nonReservedNativeTokenAddr, mintMNTAddr, input, NewGasBudget(100_000), new(uint256.Int))
		if fullShardKey == 0 {
			require.NoError(t, err)
			require.Equal(t, uint256.NewInt(5), statedb.GetMntBalance(recipient, 123))
		} else {
			require.Error(t, err)
			require.True(t, statedb.GetMntBalance(recipient, 123).IsZero())
		}
	}
}

func TestDeploySystemContractScopeAndEnableTime(t *testing.T) {
	zero, enableTime := uint64(0), uint64(2)
	config := &qkcconfig.QuarkChainConfig{EnableEvmTimeStamp: zero, EnableNonReservedNativeTokenTimestamp: enableTime}
	caller := common.HexToAddress("0xCA11E2")
	input := make([]byte, 32)
	new(uint256.Int).SetUint64(2).WriteToSlice(input)

	newEVM := func(timestamp uint64, fullShardKey uint32) (*EVM, *state.StateDB) {
		statedb, _ := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
		blockCtx := BlockContext{
			CanTransfer: testCanTransfer,
			Transfer:    testTransfer,
			BlockNumber: big.NewInt(1),
			Time:        timestamp,
		}
		evm := NewEVM(blockCtx, statedb, params.TestChainConfig, Config{QKCConfig: config})
		evm.SetTxContext(TxContext{Origin: caller, TransferTokenID: qkccommon.DefaultTokenID, FullShardKey: fullShardKey})
		return evm, statedb
	}

	evm, statedb := newEVM(1, 0)
	_, _, err := evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(10_000_000), new(uint256.Int))
	require.Error(t, err)
	require.Empty(t, statedb.GetCode(nonReservedNativeTokenAddr))

	evm, statedb = newEVM(2, 1<<16)
	_, _, err = evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(10_000_000), new(uint256.Int))
	require.Error(t, err)
	require.Empty(t, statedb.GetCode(nonReservedNativeTokenAddr))

	evm, statedb = newEVM(2, 0)
	_, _, err = evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(10_000_000), new(uint256.Int))
	require.NoError(t, err)
	require.NotEmpty(t, statedb.GetCode(nonReservedNativeTokenAddr))
}
