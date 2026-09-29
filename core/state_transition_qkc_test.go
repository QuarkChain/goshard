// Copyright 2026-2027, QuarkChain.

package core

import (
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	qkcparams "github.com/ethereum/go-ethereum/qkc/params"
	qkctypes "github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

type goldenAllocation struct {
	Balances map[string]string `json:"balances"`
	Code     string            `json:"code"`
	Storage  map[string]string `json:"storage"`
}

type goldenTransaction struct {
	Nonce            uint64 `json:"nonce"`
	GasPrice         string `json:"gas_price"`
	StartGas         uint64 `json:"start_gas"`
	To               string `json:"to"`
	Value            string `json:"value"`
	Data             string `json:"data"`
	NetworkID        uint32 `json:"network_id"`
	FromFullShardKey uint32 `json:"from_full_shard_key"`
	ToFullShardKey   uint32 `json:"to_full_shard_key"`
	GasTokenID       uint64 `json:"gas_token_id"`
	TransferTokenID  uint64 `json:"transfer_token_id"`
	Version          uint32 `json:"version"`
	V                string `json:"v"`
	R                string `json:"r"`
	S                string `json:"s"`
	Sender           string `json:"sender"`
	Hash             string `json:"hash"`
}

type goldenCase struct {
	Name        string `json:"name"`
	Network     string `json:"network"`
	FullShardID uint32 `json:"full_shard_id"`
	Expect      string `json:"expect"`
	Context     struct {
		Timestamp     uint64 `json:"timestamp"`
		GasLimit      uint64 `json:"gas_limit"`
		BlockNumber   uint64 `json:"block_number"`
		BlockCoinbase string `json:"block_coinbase"`
	} `json:"context"`
	PreAlloc map[string]goldenAllocation `json:"pre_alloc"`
	Inputs   []struct {
		Kind        string            `json:"kind"`
		Transaction goldenTransaction `json:"transaction"`
	} `json:"inputs"`
	PostStateRoot string            `json:"post_state_root"`
	GasUsed       uint64            `json:"gas_used"`
	BlockFees     map[string]string `json:"block_fee_tokens"`
	Result        struct {
		Success bool   `json:"success"`
		Output  string `json:"output"`
	} `json:"result"`
	Receipts []struct {
		Success              bool   `json:"success"`
		CumulativeGasUsed    uint64 `json:"cumulative_gas_used"`
		Bloom                string `json:"bloom"`
		ContractAddress      string `json:"contract_address"`
		ContractFullShardKey uint32 `json:"contract_full_shard_key"`
		Logs                 []struct {
			Address string   `json:"address"`
			Topics  []string `json:"topics"`
			Data    string   `json:"data"`
		} `json:"logs"`
	} `json:"receipts"`
}

func goldenBig(t *testing.T, decimal string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(decimal, 10)
	require.True(t, ok, "invalid decimal %q", decimal)
	return n
}

func goldenTX(t *testing.T, input goldenTransaction) *qkctypes.Transaction {
	t.Helper()
	fromKey, toKey := qkccommon.Uint32(input.FromFullShardKey), qkccommon.Uint32(input.ToFullShardKey)
	var recipient *account.Recipient
	if input.To != "0x" {
		to := common.HexToAddress(input.To)
		recipient = &to
	}
	return qkctypes.NewTransaction(&qkctypes.EvmTx{
		AccountNonce: input.Nonce, Price: goldenBig(t, input.GasPrice), GasLimit: input.StartGas,
		Recipient: recipient, Amount: goldenBig(t, input.Value), Payload: common.FromHex(input.Data),
		NetworkID: input.NetworkID, FromFullShardKey: &fromKey, ToFullShardKey: &toKey,
		GasTokenID: input.GasTokenID, TransferTokenID: input.TransferTokenID, Version: input.Version,
		V: goldenBig(t, input.V), R: goldenBig(t, input.R), S: goldenBig(t, input.S),
	})
}

func applyGoldenAlloc(t *testing.T, s *state.StateDB, alloc map[string]goldenAllocation) {
	t.Helper()
	keys := make([]string, 0, len(alloc))
	for key := range alloc {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		addr, err := account.CreatAddressFromBytes(common.FromHex(key))
		require.NoError(t, err)
		s.SetFullShardKey(addr.FullShardKey)
		entry := alloc[key]
		if entry.Code != "" {
			s.SetCode(addr.Recipient, common.FromHex(entry.Code), tracing.CodeChangeUnspecified)
			s.SetNonce(addr.Recipient, 1, tracing.NonceChangeUnspecified)
		}
		for slot, value := range entry.Storage {
			s.SetState(addr.Recipient, common.HexToHash(slot), common.HexToHash(value))
		}
		for token, balance := range entry.Balances {
			id, err := qkccommon.TokenIDEncodeChecked(token)
			require.NoError(t, err)
			require.NoError(t, s.DeltaTokenBalance(addr.Recipient, id, goldenBig(t, balance)))
		}
	}
}

func TestQKCIntraShardGoldenMessages(t *testing.T) {
	raw, err := os.ReadFile("../qkc/testdata/exec_golden/message_level.json")
	require.NoError(t, err)
	var file struct {
		Cases []goldenCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &file))
	// Cross-shard inputs and active MNT precompiles are covered by later stages.
	wanted := map[string]bool{
		"in_shard_transfer": true, "in_shard_transfer_nonce_too_high": true,
		"in_shard_transfer_insufficient_balance": true, "eip155_transfer": true,
		"eip155_before_enable_timestamp": true, "eip155_non_default_token": true,
		"eip155_non_zero_shard_key": true, "native_token_transfer_with_default_gas": true,
		"create_under_a_shard_key": true, "contract_creation": true,
		"contract_call_returns_value": true, "contract_revert": true,
		"contract_out_of_gas": true, "contract_selfdestruct": true,
		"contract_selfdestruct_reverted_with_parent": true, "contract_logs_and_bloom": true,
		"contract_create2": true, "create2_word_gas_oog_without_growing": true,
		"create2_word_gas_covered": true, "create2_word_gas_oog_while_growing": true,
		"log_byte_gas_oog_without_growing": true, "log_byte_gas_covered": true,
		"log_byte_gas_oog_while_growing": true, "nested_call": true,
		"sstore_legacy_pricing": true, "returndatacopy_within_the_answer": true,
		"returndatacopy_past_the_answer": true, "create_init_code_reverts": true,
		"create_code_store_out_of_gas": true, "create_code_too_large": true,
	}
	for _, tc := range file.Cases {
		if !wanted[tc.Name] {
			continue
		}
		delete(wanted, tc.Name)
		t.Run(tc.Name, func(t *testing.T) {
			cfg, err := config.LoadClusterConfig("../qkc/config/singularity/" + tc.Network + ".json")
			require.NoError(t, err)
			shard := cfg.Quarkchain.GetShardConfigByFullShardID(tc.FullShardID)
			require.NotNil(t, shard)
			db := state.NewQKCDatabase(rawdb.NewMemoryDatabase())
			statedb, err := state.NewQKC(coretypes.EmptyRootHash, db)
			require.NoError(t, err)
			applyGoldenAlloc(t, statedb, tc.PreAlloc)
			root, err := statedb.Commit(0, true, false)
			require.NoError(t, err)
			statedb, err = state.NewQKC(root, db)
			require.NoError(t, err)
			vmctx := vm.BlockContext{
				CanTransfer: CanTransfer, Transfer: Transfer,
				GetHash:     func(uint64) common.Hash { return common.Hash{} },
				Coinbase:    common.HexToAddress(tc.Context.BlockCoinbase),
				BlockNumber: new(big.Int).SetUint64(tc.Context.BlockNumber),
				Time:        tc.Context.Timestamp, GasLimit: tc.Context.GasLimit,
			}
			evm := vm.NewEVM(vmctx, statedb, &qkcparams.DefaultConstantinople, vm.Config{})
			t.Cleanup(evm.Release)
			gp := NewGasPool(tc.Context.GasLimit)
			require.Len(t, tc.Inputs, 1)
			input := tc.Inputs[0].Transaction
			require.Equal(t, "transaction", tc.Inputs[0].Kind)
			tx := goldenTX(t, input)
			receipt, output, err := ApplyQKCTransaction(&QKCExecutionContext{cfg.Quarkchain, shard}, evm, gp, statedb, tx, 0)
			if tc.Name == "native_token_transfer_with_default_gas" {
				require.ErrorIs(t, err, vm.ErrQKCUnsupportedMNT)
				require.Nil(t, receipt)
				require.Nil(t, output)
				require.Equal(t, tc.Context.GasLimit, gp.Gas())
				after, err := statedb.Commit(tc.Context.BlockNumber, true, false)
				require.NoError(t, err)
				require.Equal(t, root, after)
				return
			}
			if tc.Expect == "rejected" {
				require.Error(t, err)
				require.Nil(t, receipt)
				require.Nil(t, output)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.Result.Success, receipt.Status == qkctypes.ReceiptStatusSuccessful)
				require.Equal(t, tc.Result.Output, "0x"+common.Bytes2Hex(output))
				require.Len(t, tc.Receipts, 1)
				want := tc.Receipts[0]
				require.Equal(t, want.Success, receipt.Status == qkctypes.ReceiptStatusSuccessful)
				require.Equal(t, want.CumulativeGasUsed, receipt.CumulativeGasUsed)
				require.Equal(t, want.ContractFullShardKey, receipt.ContractFullShardKey)
				require.Equal(t, common.HexToAddress(want.ContractAddress), common.Address(receipt.ContractAddress))
				require.Equal(t, common.FromHex(want.Bloom), receipt.Bloom.Bytes())
				require.Len(t, receipt.Logs, len(want.Logs))
				for i, log := range receipt.Logs {
					require.Equal(t, common.HexToAddress(want.Logs[i].Address), log.Address)
					require.Equal(t, common.FromHex(want.Logs[i].Data), log.Data)
					require.Len(t, log.Topics, len(want.Logs[i].Topics))
					for j, topic := range log.Topics {
						require.Equal(t, common.HexToHash(want.Logs[i].Topics[j]), topic)
					}
				}
			}
			require.Equal(t, tc.GasUsed, gp.CumulativeUsed())
			root, err = statedb.Commit(tc.Context.BlockNumber, true, false)
			require.NoError(t, err)
			require.Equal(t, common.HexToHash(tc.PostStateRoot), root)
			if fee := tc.BlockFees["35760"]; fee != "" {
				require.Equal(t, goldenBig(t, fee).String(), statedb.GetBalance(vmctx.Coinbase).ToBig().String())
			}
		})
	}
	require.Empty(t, wanted)
}

func TestQKCValidationKeepsNewSenderShardKey(t *testing.T) {
	cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
	require.NoError(t, err)
	ctx := &QKCExecutionContext{cfg.Quarkchain, cfg.Quarkchain.GetShardConfigByFullShardID(1)}
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	tx, err := qkctypes.SignTx(qkctypes.NewEvmTransaction(0, common.HexToAddress("0x1234"), big.NewInt(0), 21000, big.NewInt(0), 0, 1, cfg.Quarkchain.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID), qkctypes.MakeSigner(cfg.Quarkchain.NetworkID, ctx.ShardConfig.EthChainID), key)
	require.NoError(t, err)

	expected, err := state.NewQKC(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
	require.NoError(t, err)
	expected.SetFullShardKey(0)
	expected.SetNonce(sender, 1, tracing.NonceChangeUnspecified)
	wantRoot, err := expected.Commit(1, true, false)
	require.NoError(t, err)

	for _, prevalidate := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "prevalidated"}[prevalidate], func(t *testing.T) {
			statedb, err := state.NewQKC(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
			require.NoError(t, err)
			statedb.SetFullShardKey(0)
			gp := NewGasPool(50000)
			vmctx := vm.BlockContext{CanTransfer: CanTransfer, Transfer: Transfer, BlockNumber: big.NewInt(1), Time: 1, GasLimit: 50000}
			evm := vm.NewEVM(vmctx, statedb, &qkcparams.DefaultConstantinople, vm.Config{})
			t.Cleanup(evm.Release)
			if prevalidate {
				got, err := ValidateQKCTransaction(ctx, statedb, gp, tx, vmctx.Time)
				require.NoError(t, err)
				require.Equal(t, sender, got)
			}
			receipt, _, err := ApplyQKCTransaction(ctx, evm, gp, statedb, tx, 0)
			require.NoError(t, err)
			require.Equal(t, qkctypes.ReceiptStatusSuccessful, receipt.Status)
			require.Equal(t, uint64(1), statedb.GetNonce(sender))
			root, err := statedb.Commit(1, true, false)
			require.NoError(t, err)
			require.Equal(t, wantRoot, root)
		})
	}
}

func TestQKCActiveMNTPrecompileRollsBackTransaction(t *testing.T) {
	cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
	require.NoError(t, err)
	ctx := &QKCExecutionContext{cfg.Quarkchain, cfg.Quarkchain.GetShardConfigByFullShardID(1)}
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x000000000000000000000000000000514b430005")
	tx, err := qkctypes.SignTx(qkctypes.NewEvmTransaction(0, to, big.NewInt(7), 30000, big.NewInt(1), 1, 1, cfg.Quarkchain.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID), qkctypes.MakeSigner(cfg.Quarkchain.NetworkID, ctx.ShardConfig.EthChainID), key)
	require.NoError(t, err)

	db := state.NewQKCDatabase(rawdb.NewMemoryDatabase())
	statedb, err := state.NewQKC(coretypes.EmptyRootHash, db)
	require.NoError(t, err)
	statedb.SetFullShardKey(1)
	statedb.SetBalance(sender, uint256.NewInt(100000), tracing.BalanceChangeUnspecified)
	before, err := statedb.Commit(1, true, false)
	require.NoError(t, err)
	statedb, err = state.NewQKC(before, db)
	require.NoError(t, err)
	gp := NewGasPool(80000)
	require.NoError(t, gp.SubGas(1000))
	require.NoError(t, gp.ReturnGas(0, 1000))
	gasBefore, usedBefore := gp.Gas(), gp.CumulativeUsed()
	vmctx := vm.BlockContext{CanTransfer: CanTransfer, Transfer: Transfer, BlockNumber: big.NewInt(1), Time: 1, GasLimit: 80000}
	evm := vm.NewEVM(vmctx, statedb, &qkcparams.DefaultConstantinople, vm.Config{})
	t.Cleanup(evm.Release)
	_, err = ValidateQKCTransaction(ctx, statedb, gp, tx, vmctx.Time)
	require.NoError(t, err)

	receipt, output, err := ApplyQKCTransaction(ctx, evm, gp, statedb, tx, 0)
	require.ErrorIs(t, err, vm.ErrQKCUnsupportedMNT)
	require.Nil(t, receipt)
	require.Nil(t, output)
	require.Equal(t, uint64(0), statedb.GetNonce(sender))
	require.Equal(t, uint64(100000), statedb.GetBalance(sender).Uint64())
	require.Equal(t, gasBefore, gp.Gas())
	require.Equal(t, usedBefore, gp.CumulativeUsed())
	after, err := statedb.Commit(1, true, false)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestQKCIntrinsicGasMatchesLegacySchedule(t *testing.T) {
	for _, tc := range []struct {
		data   []byte
		create bool
		want   uint64
	}{
		{nil, false, 21000},
		{[]byte{0, 1}, false, 21072},
		{[]byte{0, 1}, true, 53072},
	} {
		cost, err := IntrinsicGas(tc.data, nil, nil, tc.create, true, false, false, false)
		require.NoError(t, err)
		require.Equal(t, tc.want, cost.RegularGas)
	}
}

func TestS3AdmissionBoundaries(t *testing.T) {
	cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
	require.NoError(t, err)
	shard := cfg.Quarkchain.GetShardConfigByFullShardID(1)
	sender := common.HexToAddress("0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a")
	to := common.HexToAddress("0x1563915e194d8cfba1943570603f7606a3115508")
	for _, tc := range []struct {
		name  string
		gas   uint64
		value int64
		key   uint32
		token uint64
		pool  uint64
		nonce uint64
		want  error
	}{
		{"nonce", 30000, 100, 1, qkccommon.DefaultTokenID, 50000, 1, ErrQKCInvalidNonce},
		{"intrinsic", 20000, 100, 1, qkccommon.DefaultTokenID, 50000, 0, ErrQKCInsufficientStartGas},
		{"balance", 30000, 100000, 1, qkccommon.DefaultTokenID, 50000, 0, ErrQKCInsufficientBalance},
		{"block gas", 30000, 100, 1, qkccommon.DefaultTokenID, 29999, 0, ErrQKCBlockGasLimitReached},
		{"branch", 30000, 100, 0x10001, qkccommon.DefaultTokenID, 50000, 0, ErrQKCInvalidTransaction},
		{"token", 30000, 100, 1, qkccommon.DefaultTokenID + 1, 50000, 0, vm.ErrQKCUnsupportedMNT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statedb, err := state.NewQKC(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
			require.NoError(t, err)
			statedb.SetFullShardKey(1)
			statedb.SetBalance(sender, uint256.NewInt(50000), tracing.BalanceChangeUnspecified)
			gp := NewGasPool(tc.pool)
			tx := qkctypes.NewEvmTransaction(tc.nonce, to, big.NewInt(tc.value), tc.gas, big.NewInt(1), 1, tc.key, 255, 0, nil, tc.token, qkccommon.DefaultTokenID)
			err = validateTransaction(&QKCExecutionContext{cfg.Quarkchain, shard}, statedb, gp, tx, sender, 1)
			require.True(t, errors.Is(err, tc.want), "error %v, want %v", err, tc.want)
			require.Equal(t, uint64(0), statedb.GetNonce(sender))
			require.Equal(t, tc.pool, gp.Gas())
		})
	}
}

func TestS3ActivationGates(t *testing.T) {
	cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
	require.NoError(t, err)
	shard := cfg.Quarkchain.GetShardConfigByFullShardID(1)
	sender := common.HexToAddress("0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a")
	to := common.HexToAddress("0x1563915e194d8cfba1943570603f7606a3115508")
	statedb, err := state.NewQKC(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
	require.NoError(t, err)
	statedb.SetFullShardKey(1)
	statedb.SetBalance(sender, uint256.NewInt(50000), tracing.BalanceChangeUnspecified)
	gp := NewGasPool(50000)
	ctx := &QKCExecutionContext{cfg.Quarkchain, shard}
	tx := qkctypes.NewEvmTransaction(0, to, big.NewInt(100), 30000, big.NewInt(1), 1, 1, 255, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)

	cfg.Quarkchain.EnableTxTimeStamp = 2
	require.ErrorIs(t, validateTransaction(ctx, statedb, gp, tx, sender, 1), ErrQKCInvalidTransaction)
	cfg.Quarkchain.TxWhitelistSenders = []string{sender.Hex()[2:]}
	require.NoError(t, validateTransaction(ctx, statedb, gp, tx, sender, 1))
	cfg.Quarkchain.EnableEvmTimeStamp = 2
	create := qkctypes.NewEvmContractCreation(0, big.NewInt(0), 60000, big.NewInt(0), 1, 1, 255, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
	require.ErrorIs(t, validateTransaction(ctx, statedb, gp, create, sender, 1), ErrQKCInvalidTransaction)
	require.NoError(t, validateTransaction(ctx, statedb, gp, tx, sender, 2))
}
