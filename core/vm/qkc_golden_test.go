// Copyright 2026-2027, QuarkChain.

package vm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

const qkcVMGoldenPath = "../../qkc/testdata/exec_golden/vm_level.json"

type qkcVMGoldenAllocation struct {
	Balances map[string]string `json:"balances"`
	Code     string            `json:"code"`
	Storage  map[string]string `json:"storage"`
}

type qkcVMGoldenAccount struct {
	Nonce        uint64            `json:"nonce"`
	CodeHash     string            `json:"code_hash"`
	FullShardKey uint32            `json:"full_shard_key"`
	Exists       bool              `json:"exists"`
	Balances     map[string]string `json:"balances"`
	Storage      map[string]string `json:"storage"`
}

type qkcVMGoldenCase struct {
	Name    string `json:"name"`
	Comment string `json:"comment"`
	Network string `json:"network"`
	Context struct {
		Timestamp    uint64 `json:"timestamp"`
		FullShardKey uint32 `json:"full_shard_key"`
	} `json:"context"`
	PreAlloc map[string]qkcVMGoldenAllocation `json:"pre_alloc"`
	Message  struct {
		Sender string `json:"sender"`
		To     string `json:"to"`
		Gas    uint64 `json:"gas"`
		Value  string `json:"value"`
		Input  string `json:"input"`
	} `json:"message"`
	Result struct {
		Success      bool   `json:"success"`
		Output       string `json:"output"`
		GasRemaining uint64 `json:"gas_remaining"`
	} `json:"result"`
	Logs          []qkcVMGoldenLog              `json:"logs"`
	PostStateRoot string                        `json:"post_state_root"`
	Accounts      map[string]qkcVMGoldenAccount `json:"accounts"`
}

type qkcVMGoldenLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

func loadQKCVMGolden(t *testing.T) []qkcVMGoldenCase {
	t.Helper()
	raw, err := os.ReadFile(qkcVMGoldenPath)
	require.NoError(t, err)
	var file struct {
		Cases []qkcVMGoldenCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &file))
	require.NotEmpty(t, file.Cases)
	return file.Cases
}

func qkcVMGoldenAddress(t *testing.T, encoded string) (common.Address, uint32) {
	t.Helper()
	raw := common.FromHex(encoded)
	require.Len(t, raw, common.AddressLength+4)
	return common.BytesToAddress(raw[:common.AddressLength]), binary.BigEndian.Uint32(raw[common.AddressLength:])
}

func qkcVMGoldenValue(t *testing.T, value string) *uint256.Int {
	t.Helper()
	parsed, ok := new(big.Int).SetString(value, 10)
	require.True(t, ok, "invalid decimal %q", value)
	converted, overflow := uint256.FromBig(parsed)
	require.False(t, overflow, "value %q overflows uint256", value)
	return converted
}

func applyQKCVMGoldenAlloc(t *testing.T, statedb *state.StateDB, alloc map[string]qkcVMGoldenAllocation) {
	t.Helper()
	addresses := make([]string, 0, len(alloc))
	for encoded := range alloc {
		addresses = append(addresses, encoded)
	}
	sort.Strings(addresses)
	for _, encoded := range addresses {
		entry := alloc[encoded]
		address, shardKey := qkcVMGoldenAddress(t, encoded)
		statedb.SetFullShardKey(shardKey)
		if entry.Code != "" {
			statedb.SetCode(address, common.FromHex(entry.Code), tracing.CodeChangeUnspecified)
			statedb.SetNonce(address, 1, tracing.NonceChangeUnspecified)
		}
		for slot, value := range entry.Storage {
			statedb.SetState(address, common.HexToHash(slot), common.HexToHash(value))
		}
		for token, balance := range entry.Balances {
			if token != "QKC" {
				t.Fatalf("allocation %s: unsupported token %q", encoded, token)
			}
			statedb.SetBalance(address, qkcVMGoldenValue(t, balance), tracing.BalanceChangeUnspecified)
		}
	}
}

func checkQKCVMGoldenAccounts(t *testing.T, statedb *state.StateDB, accounts map[string]qkcVMGoldenAccount) {
	t.Helper()
	for encoded, expected := range accounts {
		address := common.HexToAddress(encoded)
		require.Equal(t, expected.Nonce, statedb.GetNonce(address), "%s nonce", encoded)
		codeHash := statedb.GetCodeHash(address)
		if codeHash == (common.Hash{}) && !statedb.Exist(address) {
			codeHash = types.EmptyCodeHash
		}
		require.Equal(t, common.HexToHash(expected.CodeHash), codeHash, "%s code hash", encoded)
		require.Equal(t, expected.FullShardKey, statedb.GetFullShardKey(address), "%s full shard key", encoded)
		require.Equal(t, expected.Exists, statedb.Exist(address), "%s existence", encoded)

		balances := statedb.GetTokenBalances(address)
		require.Len(t, balances, len(expected.Balances), "%s token balance count", encoded)
		for token, want := range expected.Balances {
			tokenID, err := strconv.ParseUint(token, 10, 64)
			require.NoError(t, err)
			if tokenID == qkccommon.DefaultTokenID {
				require.Equal(t, want, statedb.GetBalance(address).Dec(), "%s QKC balance", encoded)
			} else {
				require.Equal(t, want, statedb.GetMntBalance(address, tokenID).Dec(), "%s token %s balance", encoded, token)
			}
		}
		for slot, want := range expected.Storage {
			require.Equal(t, common.HexToHash(want), statedb.GetState(address, common.HexToHash(slot)), "%s storage %s", encoded, slot)
		}
	}
}

func checkQKCVMGoldenLogs(t *testing.T, statedb *state.StateDB, expected []qkcVMGoldenLog) {
	t.Helper()
	got := statedb.Logs()
	require.Len(t, got, len(expected))
	for i, want := range expected {
		require.Equal(t, common.HexToAddress(want.Address), got[i].Address, "log %d address", i)
		require.Equal(t, common.FromHex(want.Data), got[i].Data, "log %d data", i)
		require.Len(t, got[i].Topics, len(want.Topics), "log %d topic count", i)
		for j, topic := range want.Topics {
			require.Equal(t, common.HexToHash(topic), got[i].Topics[j], "log %d topic %d", i, j)
		}
	}
}

// TestQKCVMGolden replays pyquarkchain-generated direct EVM messages. It pins
// precompile dispatch, CALL, CREATE2, out-of-gas, return data, logs, nested
// state changes, and the final account trie on both shipped network settings.
func TestQKCVMGolden(t *testing.T) {
	for _, tc := range loadQKCVMGolden(t) {
		t.Run(tc.Name, func(t *testing.T) {
			statedb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
			require.NoError(t, err)
			applyQKCVMGoldenAlloc(t, statedb, tc.PreAlloc)
			root, err := statedb.Commit(0, false, false)
			require.NoError(t, err)
			statedb, err = state.New(root, statedb.Database())
			require.NoError(t, err)

			evm := NewEVM(qkcTestBlockContext(BlockContext{BlockNumber: big.NewInt(1), Time: tc.Context.Timestamp}), statedb, petersburgOnlyChainConfig(), Config{})
			t.Cleanup(evm.Release)
			output, gas, err := evm.QKCApplyMessage(
				common.HexToAddress(tc.Message.Sender),
				common.HexToAddress(tc.Message.To),
				common.FromHex(tc.Message.Input),
				NewGasBudget(tc.Message.Gas),
				qkcVMGoldenValue(t, tc.Message.Value),
				qkccommon.DefaultTokenID,
				tc.Context.FullShardKey,
			)
			if tc.Result.Success {
				require.NoError(t, err, tc.Comment)
			} else {
				require.ErrorIs(t, err, ErrOutOfGas, tc.Comment)
			}
			require.True(t, bytes.Equal(common.FromHex(tc.Result.Output), output), tc.Comment)
			require.Equal(t, tc.Result.GasRemaining, gas.RegularGas, tc.Comment)
			checkQKCVMGoldenLogs(t, statedb, tc.Logs)
			statedb.Finalise(true)

			root, err = statedb.Commit(0, false, false)
			require.NoError(t, err)
			require.Equal(t, common.HexToHash(tc.PostStateRoot), root, tc.Comment)
			checkQKCVMGoldenAccounts(t, statedb, tc.Accounts)
		})
	}
}
