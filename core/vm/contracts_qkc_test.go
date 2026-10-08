// Copyright 2026-2027, QuarkChain.

package vm

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	qkcconfig "github.com/ethereum/go-ethereum/qkc/config"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func newQKCTestEVM(t *testing.T, timestamp uint64, config *qkcconfig.QuarkChainConfig) (*EVM, *state.StateDB) {
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
		Time:        timestamp,
	}
	db.SetQuarkChainConfig(config)
	return NewEVM(blockCtx, db, petersburgOnlyChainConfig(), Config{}), db
}

func TestQKCEVMAwarePrecompileUsesContractToken(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	evm, _ := newQKCTestEVM(t, 1, config)
	defer evm.Release()

	evm.SetTxContext(TxContext{TransferTokenID: 999})
	contract := NewContract(common.Address{1}, currentMntIDAddr, new(uint256.Int), NewGasBudget(10), evm.jumpDests)
	contract.TransferTokenID = 123

	registered, ok := evm.precompile(currentMntIDAddr)
	require.True(t, ok)
	precompile, ok := registered.(PrecompiledContractWithEVM)
	require.True(t, ok)
	output, err := evm.runQKCPrecompiledContract(precompile, nil, contract)
	require.NoError(t, err)
	require.Equal(t, uint64(7), contract.Gas.RegularGas)
	require.Len(t, output, 32)
	require.Equal(t, uint64(123), binary.BigEndian.Uint64(output[24:]))
}

func TestQKCSpecialActivationAndFrameToken(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 5

	for _, tc := range []struct {
		name      string
		timestamp uint64
		wantGas   uint64
		wantData  bool
	}{
		{name: "at boundary disabled", timestamp: 5, wantGas: 10},
		{name: "after boundary enabled", timestamp: 6, wantGas: 7, wantData: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evm, _ := newQKCTestEVM(t, tc.timestamp, config)
			defer evm.Release()
			key := uint32(0)
			evm.SetQKCExecutionFullShardKey(&key)
			evm.SetTxContext(TxContext{
				GasPrice:        new(uint256.Int),
				GasTokenID:      config.GetDefaultChainTokenID(),
				TransferTokenID: 123,
				ToFullShardKey:  &key,
			})
			output, gas, err := evm.Call(common.Address{1}, currentMntIDAddr, nil, NewGasBudget(10), new(uint256.Int))
			require.NoError(t, err)
			require.Equal(t, tc.wantGas, gas.RegularGas)
			if tc.wantData {
				require.Len(t, output, 32)
				require.Equal(t, uint64(123), binary.BigEndian.Uint64(output[24:]))
			} else {
				require.Empty(t, output)
			}
		})
	}
}

func TestNewEVMAlwaysUsesQKCExecution(t *testing.T) {
	db, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	require.NoError(t, err)
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	db.SetQuarkChainConfig(config)
	blockContext := BlockContext{
		BlockNumber: big.NewInt(1),
		Difficulty:  big.NewInt(1),
		Time:        1,
	}
	evm := NewEVM(blockContext, db, petersburgOnlyChainConfig(), Config{})
	defer evm.Release()
	evm.SetTxContext(TxContext{
		GasPrice:        new(uint256.Int),
		GasTokenID:      config.GetDefaultChainTokenID(),
		TransferTokenID: 123,
	})

	output, gas, err := evm.Call(common.Address{1}, currentMntIDAddr, nil, NewGasBudget(10), new(uint256.Int))
	require.NoError(t, err)
	require.Len(t, output, 32)
	require.Equal(t, uint64(123), binary.BigEndian.Uint64(output[24:]))
	require.Equal(t, uint64(7), gas.RegularGas)
}

func TestQKCTokenAcknowledgementIsPerFrame(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	config.EnableNonReservedNativeTokenTimestamp = 0
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()

	caller := common.HexToAddress("0x1000")
	target := common.HexToAddress("0x2000")
	tokenID := uint64(1)
	db.AddBalanceByTokenID(caller, uint256.NewInt(10), tokenID, tracing.BalanceChangeUnspecified)
	db.SetCode(target, []byte{byte(STOP)}, tracing.CodeChangeUnspecified)
	evm.SetTxContext(TxContext{
		GasPrice:        new(uint256.Int),
		GasTokenID:      config.GetDefaultChainTokenID(),
		TransferTokenID: tokenID,
	})

	_, gas, err := evm.Call(caller, target, nil, NewGasBudget(100_000), uint256.NewInt(1))
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, uint64(100_000), gas.RegularGas)
	require.Equal(t, uint64(10), db.GetBalanceByTokenID(caller, tokenID).Uint64())
	require.True(t, db.GetBalanceByTokenID(target, tokenID).IsZero())

	// CALL currentMntID from the target. The opcode marks the target frame,
	// while the special itself returns the inherited transfer token.
	code := common.FromHex("0x6000600060006000600073")
	code = append(code, currentMntIDAddr.Bytes()...)
	code = append(code, byte(GAS), byte(CALL), byte(STOP))
	db.SetCode(target, code, tracing.CodeChangeUnspecified)
	_, _, err = evm.Call(caller, target, nil, NewGasBudget(100_000), uint256.NewInt(1))
	require.NoError(t, err)
	require.Equal(t, uint64(9), db.GetBalanceByTokenID(caller, tokenID).Uint64())
	require.Equal(t, uint64(1), db.GetBalanceByTokenID(target, tokenID).Uint64())
}

func TestQKCSystemContractBytecodesMatchPinnedOracle(t *testing.T) {
	tests := []struct {
		name   string
		code   []byte
		length int
		sha256 common.Hash
	}{
		{name: "non-reserved native token", code: nonReservedNativeTokenBytecode, length: 7173, sha256: common.HexToHash("5d8ffead89bd579c83016d8ff1dc6da810366aa3dc02882930071cd0a55e0508")},
		{name: "general native token", code: generalNativeTokenBytecode, length: 6508, sha256: common.HexToHash("c019c6cae10e46007435f35e44d2ae375129b31bf4ba511150513819c8b1976c")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Len(t, test.code, test.length)
			digest := sha256.Sum256(test.code)
			require.Equal(t, test.sha256, common.BytesToHash(digest[:]))
		})
	}
}

func TestQKCSystemContractSelectionIsMNTOnly(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	evm, _ := newQKCTestEVM(t, 1, config)
	defer evm.Release()

	for _, index := range []uint64{0, 1, 4} {
		_, _, _, _, ok := evm.qkcSystemContract(index)
		require.False(t, ok, index)
	}
	address, code, enableTime, localChainZero, ok := evm.qkcSystemContract(2)
	require.True(t, ok)
	require.Equal(t, nonReservedNativeTokenAddr, address)
	require.Equal(t, nonReservedNativeTokenBytecode, code)
	require.Equal(t, config.EnableNonReservedNativeTokenTimestamp, enableTime)
	require.True(t, localChainZero)

	address, code, enableTime, localChainZero, ok = evm.qkcSystemContract(3)
	require.True(t, ok)
	require.Equal(t, generalNativeTokenAddr, address)
	require.Equal(t, generalNativeTokenBytecode, code)
	require.Equal(t, config.EnableGeneralNativeTokenTimestamp, enableTime)
	require.False(t, localChainZero)
}

func TestQKCStaticSpecialExceptions(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	config.EnableNonReservedNativeTokenTimestamp = 0
	config.EnableGeneralNativeTokenTimestamp = 0

	t.Run("mint mutates in static context", func(t *testing.T) {
		evm, db := newQKCTestEVM(t, 1, config)
		defer evm.Release()
		key := uint32(0)
		evm.SetQKCExecutionFullShardKey(&key)
		evm.SetTxContext(TxContext{Origin: nonReservedNativeTokenAddr, GasPrice: new(uint256.Int), ToFullShardKey: &key})
		recipient := common.HexToAddress("0x1000")
		db.CreateAccount(recipient) // Existing-but-blank is still a new account in pyquarkchain.
		input := make([]byte, 96)
		copy(input[12:32], recipient.Bytes())
		binary.BigEndian.PutUint64(input[56:64], 1)
		binary.BigEndian.PutUint64(input[88:96], 7)

		_, gas, err := evm.StaticCall(nonReservedNativeTokenAddr, mintMNTAddr, input, NewGasBudget(50_000))
		require.NoError(t, err)
		require.Equal(t, uint64(16_000), gas.RegularGas)
		require.Equal(t, uint64(7), db.GetBalanceByTokenID(recipient, 1).Uint64())
	})

	t.Run("deploy creates a non-static child", func(t *testing.T) {
		evm, db := newQKCTestEVM(t, 1, config)
		defer evm.Release()
		key := uint32(0)
		caller := common.HexToAddress("0x2000")
		evm.SetQKCExecutionFullShardKey(&key)
		evm.SetTxContext(TxContext{
			Origin:          caller,
			GasPrice:        new(uint256.Int),
			GasTokenID:      config.GetDefaultChainTokenID(),
			TransferTokenID: config.GetDefaultChainTokenID(),
			ToFullShardKey:  &key,
		})
		input := make([]byte, 32)
		input[31] = 2

		output, _, err := evm.StaticCall(caller, deploySystemContractAddr, input, NewGasBudget(2_000_000))
		require.NoError(t, err)
		require.Equal(t, nonReservedNativeTokenAddr.Bytes(), output)
		require.NotEmpty(t, db.GetCode(nonReservedNativeTokenAddr))
	})

	t.Run("general manager deploys outside chain zero", func(t *testing.T) {
		evm, db := newQKCTestEVM(t, 1, config)
		defer evm.Release()
		key := uint32(1 << 16)
		caller := common.HexToAddress("0x2100")
		evm.SetQKCExecutionFullShardKey(&key)
		evm.SetTxContext(TxContext{
			Origin:          caller,
			GasPrice:        new(uint256.Int),
			GasTokenID:      config.GetDefaultChainTokenID(),
			TransferTokenID: config.GetDefaultChainTokenID(),
			ToFullShardKey:  &key,
		})
		input := make([]byte, 32)
		input[31] = 3

		output, _, err := evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(2_000_000), new(uint256.Int))
		require.NoError(t, err)
		require.Equal(t, generalNativeTokenAddr.Bytes(), output)
		require.NotEmpty(t, db.GetCode(generalNativeTokenAddr))
	})

	t.Run("transfer remains forbidden", func(t *testing.T) {
		evm, _ := newQKCTestEVM(t, 1, config)
		defer evm.Release()
		evm.SetTxContext(TxContext{GasPrice: new(uint256.Int)})
		_, gas, err := evm.StaticCall(common.HexToAddress("0x3000"), transferMntAddr, make([]byte, 96), NewGasBudget(50_000))
		require.Error(t, err)
		require.Zero(t, gas.RegularGas)
	})
}

func TestQKCDeploySystemContractRejectsNonDefaultTransferToken(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	config.EnableNonReservedNativeTokenTimestamp = 0
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	key := uint32(0)
	caller := common.HexToAddress("0x2200")
	evm.SetQKCExecutionFullShardKey(&key)
	evm.SetTxContext(TxContext{
		Origin:          caller,
		GasPrice:        new(uint256.Int),
		GasTokenID:      config.GetDefaultChainTokenID(),
		TransferTokenID: config.GetDefaultChainTokenID() + 1,
		ToFullShardKey:  &key,
	})
	input := make([]byte, 32)
	input[31] = 2

	_, gas, err := evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(2_000_000), new(uint256.Int))
	require.ErrorIs(t, err, ErrExecutionReverted)
	require.Equal(t, uint64(1_999_997), gas.RegularGas)
	require.Empty(t, db.GetCode(nonReservedNativeTokenAddr))
}

func TestQKCLocalChainAuthorizationRequiresExplicitShardKey(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	config.EnableNonReservedNativeTokenTimestamp = 0
	config.EnableGeneralNativeTokenTimestamp = 0

	t.Run("mint uses execution shard instead of nil frame shard", func(t *testing.T) {
		evm, db := newQKCTestEVM(t, 1, config)
		defer evm.Release()
		evm.SetTxContext(TxContext{Origin: nonReservedNativeTokenAddr, GasPrice: new(uint256.Int)})
		chainOne := uint32(1 << 16)
		evm.SetQKCExecutionFullShardKey(&chainOne)
		recipient := common.HexToAddress("0x4100")
		input := make([]byte, 96)
		copy(input[12:32], recipient.Bytes())
		binary.BigEndian.PutUint64(input[56:64], 1)
		binary.BigEndian.PutUint64(input[88:96], 7)

		_, _, err := evm.Call(nonReservedNativeTokenAddr, mintMNTAddr, input, NewGasBudget(50_000), new(uint256.Int))
		require.Error(t, err)
		require.True(t, db.GetBalanceByTokenID(recipient, 1).IsZero())

		key := uint32(0)
		evm.SetQKCExecutionFullShardKey(&key)
		_, _, err = evm.Call(nonReservedNativeTokenAddr, mintMNTAddr, input, NewGasBudget(50_000), new(uint256.Int))
		require.NoError(t, err)
		require.Equal(t, uint64(7), db.GetBalanceByTokenID(recipient, 1).Uint64())
	})

	t.Run("local system deploy uses execution shard instead of nil frame shard", func(t *testing.T) {
		evm, db := newQKCTestEVM(t, 1, config)
		defer evm.Release()
		caller := common.HexToAddress("0x4200")
		chainOne := uint32(1 << 16)
		evm.SetQKCExecutionFullShardKey(&chainOne)
		evm.SetTxContext(TxContext{
			Origin:          caller,
			GasPrice:        new(uint256.Int),
			GasTokenID:      config.GetDefaultChainTokenID(),
			TransferTokenID: config.GetDefaultChainTokenID(),
		})
		input := make([]byte, 32)
		input[31] = 2

		_, _, err := evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(2_000_000), new(uint256.Int))
		require.Error(t, err)
		require.Empty(t, db.GetCode(nonReservedNativeTokenAddr))

		key := uint32(0)
		evm.SetQKCExecutionFullShardKey(&key)
		_, _, err = evm.Call(caller, deploySystemContractAddr, input, NewGasBudget(2_000_000), new(uint256.Int))
		require.NoError(t, err)
		require.NotEmpty(t, db.GetCode(nonReservedNativeTokenAddr))
	})
}

func TestQKCTransferMNTKeepsExecutionChain(t *testing.T) {
	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	config.EnableNonReservedNativeTokenTimestamp = 0
	evm, db := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	key := uint32(0)
	evm.SetQKCExecutionFullShardKey(&key)
	evm.SetTxContext(TxContext{
		Origin:          nonReservedNativeTokenAddr,
		GasPrice:        new(uint256.Int),
		GasTokenID:      config.GetDefaultChainTokenID(),
		TransferTokenID: config.GetDefaultChainTokenID(),
		ToFullShardKey:  &key,
	})
	recipient := common.HexToAddress("0x4000")
	input := make([]byte, 96+96)
	copy(input[12:32], mintMNTAddr.Bytes())
	binary.BigEndian.PutUint64(input[56:64], 1)
	copy(input[96+12:96+32], recipient.Bytes())
	binary.BigEndian.PutUint64(input[96+56:96+64], 1)
	binary.BigEndian.PutUint64(input[96+88:96+96], 9)

	_, _, err := evm.Call(nonReservedNativeTokenAddr, transferMntAddr, input, NewGasBudget(100_000), new(uint256.Int))
	require.NoError(t, err)
	require.Equal(t, uint64(9), db.GetBalanceByTokenID(recipient, 1).Uint64())
}

func TestQKCVMOracleVectors(t *testing.T) {
	type oracleAllocation struct {
		Address      string            `json:"address"`
		FullShardKey uint32            `json:"full_shard_key"`
		Balances     map[string]string `json:"balances"`
		Code         string            `json:"code"`
		Storage      map[string]string `json:"storage"`
	}
	type oracleAccount struct {
		Address      string            `json:"address"`
		Exists       bool              `json:"exists"`
		Nonce        uint64            `json:"nonce"`
		Code         string            `json:"code"`
		FullShardKey uint32            `json:"full_shard_key"`
		Balances     map[string]string `json:"balances"`
		Storage      map[string]string `json:"storage"`
	}
	type oracleLog struct {
		Address string   `json:"address"`
		Topics  []string `json:"topics"`
		Data    string   `json:"data"`
	}
	type executionCase struct {
		Name    string `json:"name"`
		Context struct {
			Timestamp                                 uint64 `json:"timestamp"`
			FullShardKey                              uint32 `json:"full_shard_key"`
			EnableEVMTimeStamp                        uint64 `json:"enable_evm_timestamp"`
			EnableNonReservedNativeTokenTimestamp     uint64 `json:"enable_non_reserved_native_token_timestamp"`
			EnableGeneralNativeTokenContractTimestamp uint64 `json:"enable_general_native_token_timestamp"`
		} `json:"context"`
		PreAlloc []oracleAllocation `json:"pre_alloc"`
		Message  struct {
			From            string  `json:"from"`
			To              string  `json:"to"`
			Value           string  `json:"value"`
			Gas             uint64  `json:"gas"`
			GasTokenID      uint64  `json:"gas_token_id"`
			TransferTokenID uint64  `json:"transfer_token_id"`
			ToFullShardKey  *uint32 `json:"to_full_shard_key"`
			Data            string  `json:"data"`
		} `json:"message"`
		Result struct {
			Success       bool            `json:"success"`
			RemainingGas  uint64          `json:"remaining_gas"`
			Output        string          `json:"output"`
			Logs          []oracleLog     `json:"logs"`
			PostStateRoot string          `json:"post_state_root"`
			Accounts      []oracleAccount `json:"accounts"`
		} `json:"result"`
	}
	type createAddressCase struct {
		Name            string `json:"name"`
		Sender          string `json:"sender"`
		Nonce           uint64 `json:"nonce"`
		ExpectedAddress string `json:"address"`
	}
	type create2Case struct {
		Sender          string `json:"sender"`
		Salt            string `json:"salt"`
		InitCodeHash    string `json:"init_code_hash"`
		ExpectedAddress string `json:"address"`
	}
	type currentMNTCase struct {
		Name            string `json:"name"`
		InputGas        uint64 `json:"input_gas"`
		TransferTokenID uint64 `json:"transfer_token_id"`
		Success         bool   `json:"success"`
		RemainingGas    uint64 `json:"remaining_gas"`
		Output          string `json:"output"`
		TokenIDQueried  bool   `json:"token_id_queried"`
	}
	type systemContractCase struct {
		Name   string `json:"name"`
		Length int    `json:"length"`
		SHA256 string `json:"sha256"`
	}
	var vector struct {
		Source struct {
			Commit string `json:"pyquarkchain_commit"`
		} `json:"source"`
		Cases struct {
			Execution       []executionCase      `json:"execution"`
			CreateAddresses []createAddressCase  `json:"create_addresses"`
			Create2         create2Case          `json:"create2"`
			CurrentMNT      []currentMNTCase     `json:"current_mnt"`
			SystemContracts []systemContractCase `json:"system_contracts"`
		} `json:"cases"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "qkc", "testdata", "exec_golden", "vm_level.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &vector))
	require.Equal(t, "ce274f3dea984da64dc66ff253ee6696a3f4b5fb", vector.Source.Commit)
	parseAmount := func(t *testing.T, decimal string) *uint256.Int {
		t.Helper()
		value, ok := new(big.Int).SetString(decimal, 10)
		require.True(t, ok, decimal)
		converted, overflow := uint256.FromBig(value)
		require.False(t, overflow, decimal)
		return converted
	}

	for _, test := range vector.Cases.Execution {
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
					db.AddBalanceByTokenID(address, parseAmount(t, decimal), tokenID, tracing.BalanceChangeUnspecified)
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
			db.SetFullShardKey(test.Context.FullShardKey)
			config := qkcconfig.NewQuarkChainConfig()
			config.EnableEvmTimeStamp = test.Context.EnableEVMTimeStamp
			config.EnableNonReservedNativeTokenTimestamp = test.Context.EnableNonReservedNativeTokenTimestamp
			config.EnableGeneralNativeTokenTimestamp = test.Context.EnableGeneralNativeTokenContractTimestamp
			blockCtx := BlockContext{
				CanTransfer: func(db StateDB, addr common.Address, amount *uint256.Int) bool {
					return db.GetBalance(addr).Cmp(amount) >= 0
				},
				Transfer: func(db StateDB, from, to common.Address, amount *uint256.Int, _ *params.Rules) {
					db.SubBalance(from, amount, tracing.BalanceChangeTransfer)
					db.AddBalance(to, amount, tracing.BalanceChangeTransfer)
				},
				BlockNumber: big.NewInt(1),
				Difficulty:  big.NewInt(1),
				Time:        test.Context.Timestamp,
				GasLimit:    12_000_000,
			}
			db.SetQuarkChainConfig(config)
			evm := NewEVM(blockCtx, db, petersburgOnlyChainConfig(), Config{})
			defer evm.Release()
			executionKey := test.Context.FullShardKey
			evm.SetQKCExecutionFullShardKey(&executionKey)
			evm.SetTxContext(TxContext{
				Origin:          common.HexToAddress(test.Message.From),
				GasPrice:        new(uint256.Int),
				GasTokenID:      test.Message.GasTokenID,
				TransferTokenID: test.Message.TransferTokenID,
				ToFullShardKey:  test.Message.ToFullShardKey,
			})
			output, remaining, callErr := evm.Call(
				common.HexToAddress(test.Message.From),
				common.HexToAddress(test.Message.To),
				common.FromHex(test.Message.Data),
				NewGasBudget(test.Message.Gas),
				parseAmount(t, test.Message.Value),
			)
			require.Equal(t, test.Result.Success, callErr == nil)
			require.Equal(t, test.Result.RemainingGas, remaining.RegularGas)
			require.Equal(t, test.Result.Output, "0x"+hex.EncodeToString(output))
			db.Finalise(true)

			actualLogs := db.Logs()
			require.Len(t, actualLogs, len(test.Result.Logs))
			for i, expected := range test.Result.Logs {
				require.Equal(t, common.HexToAddress(expected.Address), actualLogs[i].Address)
				require.Len(t, actualLogs[i].Topics, len(expected.Topics))
				for j, topic := range expected.Topics {
					require.Equal(t, common.HexToHash(topic), actualLogs[i].Topics[j])
				}
				require.Equal(t, expected.Data, "0x"+hex.EncodeToString(actualLogs[i].Data))
			}
			for _, account := range test.Result.Accounts {
				address := common.HexToAddress(account.Address)
				require.Equal(t, account.Exists, !db.Empty(address), account.Address)
				require.Equal(t, account.Nonce, db.GetNonce(address), account.Address)
				require.Equal(t, account.Code, "0x"+hex.EncodeToString(db.GetCode(address)), account.Address)
				for token, decimal := range account.Balances {
					tokenID, err := strconv.ParseUint(token, 10, 64)
					require.NoError(t, err)
					require.Zero(t, db.GetBalanceByTokenID(address, tokenID).Cmp(parseAmount(t, decimal)), account.Address)
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

	for _, test := range vector.Cases.CreateAddresses {
		t.Run(test.Name, func(t *testing.T) {
			sender := common.HexToAddress(test.Sender)
			actual := ethcrypto.CreateAddress(sender, test.Nonce)
			require.Equal(t, common.HexToAddress(test.ExpectedAddress), actual)
		})
	}

	salt := [32]byte(common.HexToHash(vector.Cases.Create2.Salt))
	require.Equal(t,
		common.HexToAddress(vector.Cases.Create2.ExpectedAddress),
		ethcrypto.CreateAddress2(
			common.HexToAddress(vector.Cases.Create2.Sender),
			salt,
			common.FromHex(vector.Cases.Create2.InitCodeHash),
		),
	)

	config := qkcconfig.NewQuarkChainConfig()
	config.EnableEvmTimeStamp = 0
	evm, _ := newQKCTestEVM(t, 1, config)
	defer evm.Release()
	for _, test := range vector.Cases.CurrentMNT {
		t.Run(test.Name, func(t *testing.T) {
			evm.SetTxContext(TxContext{
				Origin:          common.HexToAddress("0x1234"),
				GasPrice:        new(uint256.Int),
				GasTokenID:      config.GetDefaultChainTokenID(),
				TransferTokenID: test.TransferTokenID,
			})
			contract := NewContract(
				common.HexToAddress("0x1234"),
				currentMntIDAddr,
				new(uint256.Int),
				NewGasBudget(test.InputGas),
				evm.jumpDests,
			)
			contract.TransferTokenID = test.TransferTokenID
			registered, ok := evm.precompile(currentMntIDAddr)
			require.True(t, ok)
			precompile, ok := registered.(PrecompiledContractWithEVM)
			require.True(t, ok)
			_, _ = evm.runQKCPrecompiledContract(precompile, nil, contract)
			require.Equal(t, test.TokenIDQueried, contract.TokenIDQueried)

			output, remaining, err := evm.Call(
				common.HexToAddress("0x1234"),
				currentMntIDAddr,
				nil,
				NewGasBudget(test.InputGas),
				new(uint256.Int),
			)
			require.Equal(t, test.Success, err == nil)
			require.Equal(t, test.RemainingGas, remaining.RegularGas)
			require.Equal(t, test.Output, "0x"+hex.EncodeToString(output))
		})
	}

	contractCode := map[string][]byte{
		"non_reserved_native_token": nonReservedNativeTokenBytecode,
		"general_native_token":      generalNativeTokenBytecode,
	}
	for _, test := range vector.Cases.SystemContracts {
		code, ok := contractCode[test.Name]
		require.True(t, ok, test.Name)
		digest := sha256.Sum256(code)
		require.Len(t, code, test.Length)
		require.Equal(t, common.HexToHash(test.SHA256), common.BytesToHash(digest[:]))
	}
}
