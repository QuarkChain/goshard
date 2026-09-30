// Copyright 2026-2027, QuarkChain.

package core

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
	qkcparams "github.com/ethereum/go-ethereum/qkc/params"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestApplyMessageCreateUsesChainConfig(t *testing.T) {
	for _, qkc := range []bool{false, true} {
		name := "ethereum"
		var cfg *qkcconfig.QuarkChainConfig
		if qkc {
			name = "quarkchain"
			cfg = qkcconfig.NewQuarkChainConfig()
		}
		t.Run(name, func(t *testing.T) {
			statedb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
			require.NoError(t, err)
			sender := common.HexToAddress("0x7701")
			statedb.SetBalance(sender, uint256.NewInt(10), tracing.BalanceChangeUnspecified)
			evm := vm.NewEVM(vm.BlockContext{
				CanTransfer: CanTransfer, Transfer: Transfer, BlockNumber: big.NewInt(1),
			}, statedb, &qkcparams.DefaultConstantinople, vm.Config{QKCConfig: cfg})
			t.Cleanup(evm.Release)
			for nonce := uint64(0); nonce < 2; nonce++ {
				result, err := ApplyMessage(evm, &Message{
					From: sender, Nonce: nonce, Value: new(uint256.Int),
					GasLimit: 100_000, GasPrice: new(uint256.Int),
					Data: common.FromHex("0x60006000f3"),
				}, nil)
				require.NoError(t, err)
				require.NoError(t, result.Err)
				want := crypto.CreateAddress(sender, nonce)
				if qkc {
					want = vm.QKCContractAddress(sender, 0, nonce)
				}
				require.Equal(t, uint64(1), statedb.GetNonce(want))
				require.Equal(t, nonce+1, statedb.GetNonce(sender))
			}
		})
	}
}
