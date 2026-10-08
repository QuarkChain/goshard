// Copyright 2026-2027, QuarkChain.

package core

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
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

type qkcOracleAllocation struct {
	Address      string            `json:"address"`
	FullShardKey uint32            `json:"full_shard_key"`
	Balances     map[string]string `json:"balances"`
	Code         string            `json:"code"`
	Storage      map[string]string `json:"storage"`
}

type qkcOracleAccount struct {
	Address      string            `json:"address"`
	Exists       bool              `json:"exists"`
	Nonce        uint64            `json:"nonce"`
	Code         string            `json:"code"`
	FullShardKey uint32            `json:"full_shard_key"`
	Balances     map[string]string `json:"balances"`
	Storage      map[string]string `json:"storage"`
}

type qkcOracleLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

type qkcOracleMessageCase struct {
	Name    string `json:"name"`
	Context struct {
		Timestamp     uint64 `json:"timestamp"`
		BlockGasLimit uint64 `json:"block_gas_limit"`
		Coinbase      string `json:"coinbase"`
	} `json:"context"`
	PreAlloc []qkcOracleAllocation `json:"pre_alloc"`
	Message  struct {
		From            string  `json:"from"`
		To              *string `json:"to"`
		Nonce           uint64  `json:"nonce"`
		Value           string  `json:"value"`
		GasLimit        uint64  `json:"gas_limit"`
		GasPrice        string  `json:"gas_price"`
		GasTokenID      uint64  `json:"gas_token_id"`
		TransferTokenID uint64  `json:"transfer_token_id"`
		ToFullShardKey  *uint32 `json:"to_full_shard_key"`
		Data            string  `json:"data"`
	} `json:"message"`
	Result struct {
		Accepted         bool               `json:"accepted"`
		ErrorType        string             `json:"error_type"`
		Success          bool               `json:"success"`
		Output           string             `json:"output"`
		UsedGas          uint64             `json:"used_gas"`
		GasPoolRemaining uint64             `json:"gas_pool_remaining"`
		FullShardKey     uint32             `json:"full_shard_key"`
		Logs             []qkcOracleLog     `json:"logs"`
		PostStateRoot    string             `json:"post_state_root"`
		Accounts         []qkcOracleAccount `json:"accounts"`
	} `json:"result"`
}

func qkcOracleRejectionType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrNonceTooHigh), errors.Is(err, ErrNonceTooLow), errors.Is(err, ErrNonceMax):
		return "InvalidNonce"
	case errors.Is(err, ErrIntrinsicGas):
		return "InsufficientStartGas"
	case errors.Is(err, ErrInsufficientFunds):
		return "InsufficientBalance"
	case errors.Is(err, ErrGasLimitReached):
		return "BlockGasLimitReached"
	case errors.Is(err, ErrQKCInvalidToken), errors.Is(err, ErrQKCGasConversion):
		return "InvalidNativeToken"
	default:
		return "Go:" + err.Error()
	}
}

func qkcOracleLogs(logs []*types.Log) []qkcOracleLog {
	result := make([]qkcOracleLog, 0, len(logs))
	for _, log := range logs {
		topics := make([]string, len(log.Topics))
		for i, topic := range log.Topics {
			topics[i] = topic.Hex()
		}
		result = append(result, qkcOracleLog{
			Address: log.Address.Hex(),
			Topics:  topics,
			Data:    "0x" + hex.EncodeToString(log.Data),
		})
	}
	return result
}

func qkcOracleUint256(t *testing.T, decimal string) *uint256.Int {
	t.Helper()
	value, ok := new(big.Int).SetString(decimal, 10)
	require.True(t, ok, decimal)
	converted, overflow := uint256.FromBig(value)
	require.False(t, overflow, decimal)
	return converted
}

func TestQKCLocalMessageOracleVectors(t *testing.T) {
	var vectors struct {
		Source struct {
			Commit string `json:"pyquarkchain_commit"`
		} `json:"source"`
		Cases []qkcOracleMessageCase `json:"cases"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "qkc", "testdata", "exec_golden", "local_message_level.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &vectors))
	require.Equal(t, "ce274f3dea984da64dc66ff253ee6696a3f4b5fb", vectors.Source.Commit)

	for _, test := range vectors.Cases {
		t.Run(test.Name, func(t *testing.T) {
			disk := state.NewDatabaseForTesting()
			db, err := state.New(types.EmptyRootHash, disk)
			require.NoError(t, err)
			for _, allocation := range test.PreAlloc {
				address := common.HexToAddress(allocation.Address)
				db.SetFullShardKey(allocation.FullShardKey)
				for token, decimal := range allocation.Balances {
					tokenID, err := strconv.ParseUint(token, 10, 64)
					require.NoError(t, err)
					db.AddBalanceByTokenID(address, qkcOracleUint256(t, decimal), tokenID, tracing.BalanceChangeUnspecified)
				}
				if code := common.FromHex(allocation.Code); len(code) != 0 {
					db.SetCode(address, code, tracing.CodeChangeUnspecified)
					db.SetNonce(address, 1, tracing.NonceChangeUnspecified)
				}
				for key, value := range allocation.Storage {
					db.SetState(address, common.HexToHash(key), common.HexToHash(value))
				}
			}
			root, err := db.Commit(0, true, false)
			require.NoError(t, err)
			db, err = state.New(root, disk)
			require.NoError(t, err)
			if len(test.PreAlloc) != 0 {
				db.SetFullShardKey(test.PreAlloc[len(test.PreAlloc)-1].FullShardKey)
			}

			config := qkcconfig.NewQuarkChainConfig()
			config.EnableEvmTimeStamp = 0
			config.EnableNonReservedNativeTokenTimestamp = 0
			config.EnableGeneralNativeTokenTimestamp = 0
			blockContext := vm.BlockContext{
				CanTransfer: CanTransfer,
				Transfer:    Transfer,
				Coinbase:    common.HexToAddress(test.Context.Coinbase),
				BlockNumber: big.NewInt(1),
				Difficulty:  big.NewInt(1),
				Time:        test.Context.Timestamp,
				GasLimit:    test.Context.BlockGasLimit,
			}
			chainConfig := qkcparams.DefaultConstantinople
			db.SetQuarkChainConfig(config)
			evm := vm.NewEVM(blockContext, db, &chainConfig, vm.Config{})
			defer evm.Release()

			var to *common.Address
			if test.Message.To != nil {
				address := common.HexToAddress(*test.Message.To)
				to = &address
			}
			message := &Message{
				From:            common.HexToAddress(test.Message.From),
				To:              to,
				Nonce:           test.Message.Nonce,
				Value:           qkcOracleUint256(t, test.Message.Value),
				GasLimit:        test.Message.GasLimit,
				GasPrice:        qkcOracleUint256(t, test.Message.GasPrice),
				Data:            common.FromHex(test.Message.Data),
				GasTokenID:      test.Message.GasTokenID,
				TransferTokenID: test.Message.TransferTokenID,
				ToFullShardKey:  test.Message.ToFullShardKey,
			}
			gasPool := NewGasPool(test.Context.BlockGasLimit)
			result, applyErr := ApplyMessage(evm, message, gasPool)
			require.Equal(t, test.Result.ErrorType, qkcOracleRejectionType(applyErr))
			if !test.Result.Accepted {
				require.Error(t, applyErr)
				require.Nil(t, result)
			} else {
				require.NoError(t, applyErr)
				require.Equal(t, test.Result.Success, !result.Failed())
				require.Equal(t, test.Result.Output, "0x"+hex.EncodeToString(result.ReturnData))
				require.Equal(t, test.Result.UsedGas, result.UsedGas)
			}
			require.Equal(t, test.Result.GasPoolRemaining, gasPool.Gas())
			require.Equal(t, test.Result.UsedGas, gasPool.CumulativeUsed())
			require.Equal(t, test.Result.Logs, qkcOracleLogs(db.Logs()))

			for _, account := range test.Result.Accounts {
				address := common.HexToAddress(account.Address)
				require.Equal(t, account.Exists, !db.Empty(address), account.Address)
				require.Equal(t, account.Nonce, db.GetNonce(address), account.Address)
				require.Equal(t, account.Code, "0x"+hex.EncodeToString(db.GetCode(address)), account.Address)
				for token, decimal := range account.Balances {
					tokenID, err := strconv.ParseUint(token, 10, 64)
					require.NoError(t, err)
					require.Zero(t,
						db.GetBalanceByTokenID(address, tokenID).Cmp(qkcOracleUint256(t, decimal)),
						account.Address+" token "+token,
					)
				}
				for key, value := range account.Storage {
					require.Equal(t, common.HexToHash(value), db.GetState(address, common.HexToHash(key)), account.Address)
				}
			}

			postRoot, err := db.Commit(1, true, false)
			require.NoError(t, err)
			require.Equal(t, common.HexToHash(test.Result.PostStateRoot), postRoot)
		})
	}
}
