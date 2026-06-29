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
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

// testCanTransfer / testTransfer mirror core/evm.go's CanTransfer/Transfer so the
// vm package (which cannot import core) can exercise the real value-transfer path
// inside evm.Call. They route on tokenID exactly as the production functions do.
func testCanTransfer(db StateDB, addr common.Address, amount *uint256.Int, tokenID uint64) bool {
	if tokenID == 0 || tokenID == types.DefaultTokenID {
		return db.GetBalance(addr).Cmp(amount) >= 0
	}
	return db.GetMntBalance(addr, tokenID).Cmp(amount) >= 0
}

func testTransfer(db StateDB, sender, recipient common.Address, amount *uint256.Int, _ *params.Rules, tokenID uint64) {
	if tokenID == 0 || tokenID == types.DefaultTokenID {
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
	}
	evm := NewEVM(blockCtx, statedb, params.TestChainConfig, Config{})
	evm.SetTxContext(TxContext{Origin: caller, TransferTokenID: types.DefaultTokenID})

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
	}
	evm := NewEVM(blockCtx, statedb, params.TestChainConfig, Config{})
	evm.SetTxContext(TxContext{Origin: caller, TransferTokenID: types.DefaultTokenID})

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

