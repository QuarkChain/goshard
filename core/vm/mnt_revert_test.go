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
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestCreate2AfterRevertedTransferPythonRoot(t *testing.T) {
	for _, finalise := range []bool{false, true} {
		name := "same transaction"
		if finalise {
			name = "after transaction finalisation"
		}
		t.Run(name, func(t *testing.T) {
			s, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
			require.NoError(t, err)
			caller := common.HexToAddress("0x1234")
			code := common.FromHex("0x60006000f3") // Return empty runtime code.
			address := crypto.CreateAddress2(caller, [32]byte{}, crypto.Keccak256(code))
			transfer := func(db StateDB, from, to common.Address, value *uint256.Int, _ *params.Rules) {
				db.SubBalance(from, value, tracing.BalanceChangeTransfer)
				db.AddBalance(to, value, tracing.BalanceChangeTransfer)
			}
			s.SetBalance(caller, uint256.NewInt(10), tracing.BalanceChangeUnspecified)
			snapshot := s.Snapshot()
			transfer(s, caller, address, uint256.NewInt(1), nil)
			s.RevertToSnapshot(snapshot)
			require.False(t, s.Exist(address))
			if finalise {
				transfer(s, caller, address, new(uint256.Int), nil)
				s.Finalise(true)
			}
			evm := NewEVM(BlockContext{
				CanTransfer: func(db StateDB, from common.Address, value *uint256.Int) bool {
					return db.GetBalance(from).Cmp(value) >= 0
				},
				Transfer: transfer, BlockNumber: big.NewInt(0), Difficulty: big.NewInt(1),
			}, s, petersburgOnlyChainConfig(), Config{})
			defer evm.Release()
			_, deployed, _, err := evm.Create2(caller, code, NewGasBudget(1000000), new(uint256.Int), new(uint256.Int))
			require.NoError(t, err)
			require.Equal(t, address, deployed)
			require.Equal(t, uint64(1), s.GetNonce(address))
			root, err := s.Commit(0, true, false)
			require.NoError(t, err)
			// Pyquarkchain ce274f3d: fund caller with 10, transfer 1 to the
			// precomputed address, revert, then create_contract with salt=bytes(32).
			require.Equal(t, common.HexToHash("e9a2b7786349ceff389629102326eb838379feee5b79302aad46bc5cbfa6b9bb"), root)
		})
	}
}
