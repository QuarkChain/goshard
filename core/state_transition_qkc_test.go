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
	"github.com/ethereum/go-ethereum/qkc/serialize"
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

type goldenDeposit struct {
	TxHash           string `json:"tx_hash"`
	From             string `json:"from"`
	FromFullShardKey uint32 `json:"from_full_shard_key"`
	To               string `json:"to"`
	ToFullShardKey   uint32 `json:"to_full_shard_key"`
	Value            string `json:"value"`
	GasPrice         string `json:"gas_price"`
	GasTokenID       uint64 `json:"gas_token_id"`
	TransferTokenID  uint64 `json:"transfer_token_id"`
	GasRemained      string `json:"gas_remained"`
	MessageData      string `json:"message_data"`
	CreateContract   bool   `json:"create_contract"`
	IsFromRootChain  bool   `json:"is_from_root_chain"`
	RefundRate       uint8  `json:"refund_rate"`
}

type goldenReceipt struct {
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
		Deposit     goldenDeposit     `json:"deposit"`
	} `json:"inputs"`
	PostStateRoot string            `json:"post_state_root"`
	GasUsed       uint64            `json:"gas_used"`
	BlockFees     map[string]string `json:"block_fee_tokens"`
	Result        struct {
		Success bool   `json:"success"`
		Output  string `json:"output"`
	} `json:"result"`
	Receipts              []goldenReceipt `json:"receipts"`
	XShardDepositReceipts []goldenReceipt `json:"xshard_deposit_receipts"`
	XShardList            []struct {
		From             string `json:"from"`
		FromFullShardKey uint32 `json:"from_full_shard_key"`
		To               string `json:"to"`
		ToFullShardKey   uint32 `json:"to_full_shard_key"`
		Value            string `json:"value"`
		GasPrice         string `json:"gas_price"`
		GasTokenID       uint64 `json:"gas_token_id"`
		TransferTokenID  uint64 `json:"transfer_token_id"`
		GasRemained      string `json:"gas_remained"`
		MessageData      string `json:"message_data"`
		CreateContract   bool   `json:"create_contract"`
		IsFromRootChain  bool   `json:"is_from_root_chain"`
		RefundRate       uint8  `json:"refund_rate"`
	} `json:"xshard_list"`
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

func goldenXShardDeposit(t *testing.T, input goldenDeposit) *qkctypes.CrossShardTransactionDeposit {
	t.Helper()
	return &qkctypes.CrossShardTransactionDeposit{
		TxHash:          common.HexToHash(input.TxHash),
		From:            account.NewAddress(common.HexToAddress(input.From), input.FromFullShardKey),
		To:              account.NewAddress(common.HexToAddress(input.To), input.ToFullShardKey),
		Value:           &serialize.Uint256{Value: goldenBig(t, input.Value)},
		GasPrice:        &serialize.Uint256{Value: goldenBig(t, input.GasPrice)},
		GasTokenID:      input.GasTokenID,
		TransferTokenID: input.TransferTokenID,
		GasRemained:     &serialize.Uint256{Value: goldenBig(t, input.GasRemained)},
		MessageData:     common.FromHex(input.MessageData),
		CreateContract:  input.CreateContract,
		IsFromRootChain: input.IsFromRootChain,
		RefundRate:      input.RefundRate,
	}
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

func TestQKCTransactionGoldenMessages(t *testing.T) {
	raw, err := os.ReadFile("../qkc/testdata/exec_golden/message_level.json")
	require.NoError(t, err)
	var file struct {
		Cases []goldenCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &file))
	// Incoming deposits and active MNT precompiles are covered by later stages.
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
		"xshard_source_transfer": true, "xshard_source_contract_creation": true,
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
			statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, db)
			require.NoError(t, err)
			applyGoldenAlloc(t, statedb, tc.PreAlloc)
			root, err := statedb.Commit(0, true, false)
			require.NoError(t, err)
			statedb, err = state.NewQKCStateDB(root, db)
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
			receipt, deposit, output, err := ApplyQKCTransactionWithDeposit(&QKCExecutionContext{
				QKCConfig: cfg.Quarkchain, ShardConfig: shard,
				RootHeight: 1, XShardGasLimit: tc.Context.GasLimit / 2,
			}, evm, gp, statedb, tx, 0)
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
				if len(tc.XShardList) == 0 {
					require.Nil(t, deposit)
				} else {
					require.Len(t, tc.XShardList, 1)
					want := tc.XShardList[0]
					require.NotNil(t, deposit)
					// Block execution passes the QKC wrapper hash; the message-level
					// oracle passes the raw EVM hash instead.
					require.Equal(t, tx.Hash(), deposit.TxHash)
					require.Equal(t, account.NewAddress(common.HexToAddress(want.From), want.FromFullShardKey), deposit.From)
					require.Equal(t, account.NewAddress(common.HexToAddress(want.To), want.ToFullShardKey), deposit.To)
					require.Equal(t, goldenBig(t, want.Value), deposit.Value.Value)
					require.Equal(t, goldenBig(t, want.GasPrice), deposit.GasPrice.Value)
					require.Equal(t, want.GasTokenID, deposit.GasTokenID)
					require.Equal(t, want.TransferTokenID, deposit.TransferTokenID)
					require.Equal(t, goldenBig(t, want.GasRemained), deposit.GasRemained.Value)
					require.Equal(t, common.FromHex(want.MessageData), deposit.MessageData)
					require.Equal(t, want.CreateContract, deposit.CreateContract)
					require.Equal(t, want.IsFromRootChain, deposit.IsFromRootChain)
					require.Equal(t, want.RefundRate, deposit.RefundRate)
				}
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

func TestQKCXShardDepositGoldenMessages(t *testing.T) {
	raw, err := os.ReadFile("../qkc/testdata/exec_golden/message_level.json")
	require.NoError(t, err)
	var file struct {
		Cases []goldenCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &file))
	wanted := map[string]bool{
		"xshard_deposit_to_empty_account":                        true,
		"xshard_deposit_failed_message_leaves_funds_with_sender": true,
		"xshard_pre_evm_credits_code_account":                    true,
		"xshard_evm_boundary_at_enable":                          true,
		"xshard_evm_boundary_after_enable":                       true,
		"xshard_deposit_contract_creation":                       true,
		"xshard_root_coinbase_for_other_shard":                   true,
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
			statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, db)
			require.NoError(t, err)
			applyGoldenAlloc(t, statedb, tc.PreAlloc)
			root, err := statedb.Commit(0, true, false)
			require.NoError(t, err)
			statedb, err = state.NewQKCStateDB(root, db)
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
			require.Equal(t, "deposit", tc.Inputs[0].Kind)
			deposit := goldenXShardDeposit(t, tc.Inputs[0].Deposit)
			receipt, output, err := RunOneXShardTx(&QKCExecutionContext{
				QKCConfig: cfg.Quarkchain, ShardConfig: shard,
			}, evm, gp, statedb, deposit, true, 0)
			require.NoError(t, err)
			if !deposit.CreateContract {
				require.Equal(t, tc.Result.Output, "0x"+common.Bytes2Hex(output))
			}
			require.Empty(t, tc.Receipts)
			require.Equal(t, tc.GasUsed, gp.CumulativeUsed())
			if len(tc.XShardDepositReceipts) == 0 {
				require.Nil(t, receipt)
			} else {
				require.Len(t, tc.XShardDepositReceipts, 1)
				require.NotNil(t, receipt)
				want := tc.XShardDepositReceipts[0]
				require.Equal(t, tc.Result.Success, receipt.Status == qkctypes.ReceiptStatusSuccessful)
				require.Equal(t, want.Success, receipt.Status == qkctypes.ReceiptStatusSuccessful)
				require.Equal(t, want.CumulativeGasUsed, receipt.CumulativeGasUsed)
				require.Equal(t, want.ContractFullShardKey, receipt.ContractFullShardKey)
				require.Equal(t, common.HexToAddress(want.ContractAddress), common.Address(receipt.ContractAddress))
				if deposit.CreateContract {
					require.Equal(t, common.HexToAddress(want.ContractAddress).Bytes(), output)
				}
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
			root, err = statedb.Commit(tc.Context.BlockNumber, true, false)
			require.NoError(t, err)
			require.Equal(t, common.HexToHash(tc.PostStateRoot), root)
			if fee := tc.BlockFees["35760"]; fee != "" {
				require.Zero(t, goldenBig(t, fee).Cmp(statedb.GetBalance(vmctx.Coinbase).ToBig()))
			}
		})
	}
	require.Empty(t, wanted)
}

func TestQKCXShardDepositStartingGas(t *testing.T) {
	cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
	require.NoError(t, err)
	ctx := &QKCExecutionContext{QKCConfig: cfg.Quarkchain, ShardConfig: cfg.Quarkchain.GetShardConfigByFullShardID(1)}
	for _, tc := range []struct {
		name          string
		checkFromRoot bool
		fromRoot      bool
		price         int64
		wantGas       uint64
	}{
		{"legacy root deposit with price", false, true, 1, 9000},
		{"fixed root deposit", true, true, 1, 0},
		{"legacy free shard deposit", false, false, 0, 0},
		{"fixed free shard deposit", true, false, 0, 9000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
			require.NoError(t, err)
			gp := NewGasPool(100000)
			evm := vm.NewEVM(vm.BlockContext{
				CanTransfer: CanTransfer, Transfer: Transfer,
				BlockNumber: big.NewInt(1), Time: 1, GasLimit: 100000,
			}, statedb, &qkcparams.DefaultConstantinople, vm.Config{})
			t.Cleanup(evm.Release)
			deposit := &qkctypes.CrossShardTransactionDeposit{
				From:       account.NewAddress(common.HexToAddress("0xaaaa"), 1),
				To:         account.NewAddress(common.HexToAddress("0xbbbb"), 1),
				Value:      &serialize.Uint256{Value: big.NewInt(100)},
				GasPrice:   &serialize.Uint256{Value: big.NewInt(tc.price)},
				GasTokenID: qkccommon.DefaultTokenID, TransferTokenID: qkccommon.DefaultTokenID,
				GasRemained:     &serialize.Uint256{Value: big.NewInt(0)},
				IsFromRootChain: tc.fromRoot, RefundRate: 100,
			}
			receipt, _, err := RunOneXShardTx(ctx, evm, gp, statedb, deposit, tc.checkFromRoot, 0)
			require.NoError(t, err)
			require.NotNil(t, receipt)
			require.Equal(t, tc.wantGas, receipt.GasUsed)
			require.Equal(t, tc.wantGas, gp.CumulativeUsed())
			require.Equal(t, uint64(100), statedb.GetBalance(deposit.To.Recipient).Uint64())
		})
	}
}

func TestQKCXShardDepositAbandonsWithoutMutation(t *testing.T) {
	cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
	require.NoError(t, err)
	ctx := &QKCExecutionContext{QKCConfig: cfg.Quarkchain, ShardConfig: cfg.Quarkchain.GetShardConfigByFullShardID(1)}
	for _, tc := range []struct {
		name       string
		pool       uint64
		token      uint64
		refundRate uint8
		toShardKey uint32
		fromRoot   bool
		value      int64
		want       error
	}{
		{"gas limit", 8999, qkccommon.DefaultTokenID, 100, 1, false, 100, ErrGasLimitReached},
		{"nondefault token", 100000, qkccommon.DefaultTokenID + 1, 100, 1, false, 100, vm.ErrQKCUnsupportedMNT},
		{"partial refund with default tokens", 100000, qkccommon.DefaultTokenID, 10, 1, false, 100, vm.ErrQKCUnsupportedMNT},
		{"foreign shard deposit", 100000, qkccommon.DefaultTokenID, 100, 0x00010001, false, 0, ErrQKCInvalidTransaction},
		{"foreign shard nonzero root deposit", 100000, qkccommon.DefaultTokenID, 100, 0x00010001, true, 100, ErrQKCInvalidTransaction},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := state.NewQKCDatabase(rawdb.NewMemoryDatabase())
			statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, db)
			require.NoError(t, err)
			gp := NewGasPool(tc.pool)
			evm := vm.NewEVM(vm.BlockContext{
				CanTransfer: CanTransfer, Transfer: Transfer,
				BlockNumber: big.NewInt(1), Time: 1, GasLimit: tc.pool,
			}, statedb, &qkcparams.DefaultConstantinople, vm.Config{})
			t.Cleanup(evm.Release)
			deposit := &qkctypes.CrossShardTransactionDeposit{
				From:       account.NewAddress(common.HexToAddress("0xaaaa"), 1),
				To:         account.NewAddress(common.HexToAddress("0xbbbb"), tc.toShardKey),
				Value:      &serialize.Uint256{Value: big.NewInt(tc.value)},
				GasPrice:   &serialize.Uint256{Value: big.NewInt(1)},
				GasTokenID: qkccommon.DefaultTokenID, TransferTokenID: tc.token,
				GasRemained: &serialize.Uint256{Value: big.NewInt(0)}, IsFromRootChain: tc.fromRoot, RefundRate: tc.refundRate,
			}
			receipt, output, err := RunOneXShardTx(ctx, evm, gp, statedb, deposit, true, 0)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, receipt)
			require.Nil(t, output)
			require.Equal(t, tc.pool, gp.Gas())
			require.Zero(t, gp.CumulativeUsed())
			root, err := statedb.Commit(1, true, false)
			require.NoError(t, err)
			require.Equal(t, coretypes.EmptyRootHash, root)
		})
	}
}

func TestQKCValidationKeepsNewSenderShardKey(t *testing.T) {
	cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
	require.NoError(t, err)
	ctx := &QKCExecutionContext{QKCConfig: cfg.Quarkchain, ShardConfig: cfg.Quarkchain.GetShardConfigByFullShardID(1)}
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	tx, err := qkctypes.SignTx(qkctypes.NewEvmTransaction(0, common.HexToAddress("0x1234"), big.NewInt(0), 21000, big.NewInt(0), 0, 1, cfg.Quarkchain.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID), qkctypes.MakeSigner(cfg.Quarkchain.NetworkID, ctx.ShardConfig.EthChainID), key)
	require.NoError(t, err)

	expected, err := state.NewQKCStateDB(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
	require.NoError(t, err)
	expected.SetFullShardKey(0)
	expected.SetNonce(sender, 1, tracing.NonceChangeUnspecified)
	wantRoot, err := expected.Commit(1, true, false)
	require.NoError(t, err)

	for _, prevalidate := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "prevalidated"}[prevalidate], func(t *testing.T) {
			statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
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
	ctx := &QKCExecutionContext{QKCConfig: cfg.Quarkchain, ShardConfig: cfg.Quarkchain.GetShardConfigByFullShardID(1)}
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x000000000000000000000000000000514b430005")
	tx, err := qkctypes.SignTx(qkctypes.NewEvmTransaction(0, to, big.NewInt(7), 30000, big.NewInt(1), 1, 1, cfg.Quarkchain.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID), qkctypes.MakeSigner(cfg.Quarkchain.NetworkID, ctx.ShardConfig.EthChainID), key)
	require.NoError(t, err)

	db := state.NewQKCDatabase(rawdb.NewMemoryDatabase())
	statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, db)
	require.NoError(t, err)
	statedb.SetFullShardKey(1)
	statedb.SetBalance(sender, uint256.NewInt(100000), tracing.BalanceChangeUnspecified)
	before, err := statedb.Commit(1, true, false)
	require.NoError(t, err)
	statedb, err = state.NewQKCStateDB(before, db)
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
		{"destination", 30000, 100, 0xffffffff, qkccommon.DefaultTokenID, 50000, 0, ErrQKCInvalidTransaction},
		{"cross-shard intrinsic", 29999, 100, 0x10001, qkccommon.DefaultTokenID, 50000, 0, ErrQKCInsufficientStartGas},
		{"token", 30000, 100, 1, qkccommon.DefaultTokenID + 1, 50000, 0, vm.ErrQKCUnsupportedMNT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
			require.NoError(t, err)
			statedb.SetFullShardKey(1)
			statedb.SetBalance(sender, uint256.NewInt(50000), tracing.BalanceChangeUnspecified)
			gp := NewGasPool(tc.pool)
			tx := qkctypes.NewEvmTransaction(tc.nonce, to, big.NewInt(tc.value), tc.gas, big.NewInt(1), 1, tc.key, 255, 0, nil, tc.token, qkccommon.DefaultTokenID)
			err = validateTransaction(&QKCExecutionContext{
				QKCConfig: cfg.Quarkchain, ShardConfig: shard, RootHeight: 1, XShardGasLimit: 50000,
			}, statedb, gp, tx, sender, 1)
			require.True(t, errors.Is(err, tc.want), "error %v, want %v", err, tc.want)
			require.Equal(t, uint64(0), statedb.GetNonce(sender))
			require.Equal(t, tc.pool, gp.Gas())
		})
	}
}

func TestQKCCrossShardSourceBeforeEVMAndPoSW(t *testing.T) {
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x1234")
	for _, tc := range []struct {
		name    string
		locked  uint64
		blocked bool
	}{
		{"before EVM", 0, false},
		{"PoSW lock", 80000, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
			require.NoError(t, err)
			cfg.Quarkchain.EnableEvmTimeStamp = 2
			ctx := &QKCExecutionContext{
				QKCConfig: cfg.Quarkchain, ShardConfig: cfg.Quarkchain.GetShardConfigByFullShardID(1),
				RootHeight: 1, XShardGasLimit: 100000,
			}
			statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
			require.NoError(t, err)
			statedb.SetFullShardKey(1)
			statedb.SetBalance(sender, uint256.NewInt(200000), tracing.BalanceChangeUnspecified)
			gp := NewGasPool(100000)
			vmctx := vm.BlockContext{
				CanTransfer: CanTransfer, Transfer: Transfer,
				BlockNumber: big.NewInt(1), Time: 1, GasLimit: 100000,
				Coinbase: common.HexToAddress("0xcafe"),
			}
			if tc.blocked {
				vmctx.SenderDisallowMap = map[common.Address]*uint256.Int{sender: uint256.NewInt(tc.locked)}
			}
			evm := vm.NewEVM(vmctx, statedb, &qkcparams.DefaultConstantinople, vm.Config{})
			t.Cleanup(evm.Release)
			tx, err := qkctypes.SignTx(qkctypes.NewEvmTransaction(0, to, big.NewInt(1000), 60000, big.NewInt(2), 1, 0x10001, cfg.Quarkchain.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID), qkctypes.MakeSigner(cfg.Quarkchain.NetworkID, ctx.ShardConfig.EthChainID), key)
			require.NoError(t, err)

			_, _, err = ApplyQKCTransaction(ctx, evm, gp, statedb, tx, 0)
			require.ErrorIs(t, err, ErrQKCInvalidTransaction)
			require.Equal(t, uint64(0), statedb.GetNonce(sender))
			require.Equal(t, uint64(100000), gp.Gas())

			receipt, deposit, output, err := ApplyQKCTransactionWithDeposit(ctx, evm, gp, statedb, tx, 0)
			require.NoError(t, err)
			require.Empty(t, output)
			require.Equal(t, uint64(1), statedb.GetNonce(sender))
			if tc.blocked {
				require.Nil(t, deposit)
				require.Equal(t, qkctypes.ReceiptStatusFailed, receipt.Status)
				require.Equal(t, uint64(60000), gp.CumulativeUsed())
				require.Equal(t, uint64(80000), statedb.GetBalance(sender).Uint64())
				require.Equal(t, uint64(60000), statedb.GetBalance(vmctx.Coinbase).Uint64())
			} else {
				require.NotNil(t, deposit)
				require.Zero(t, deposit.GasRemained.Value.Sign())
				require.Equal(t, qkctypes.ReceiptStatusSuccessful, receipt.Status)
				require.Equal(t, uint64(30000), gp.CumulativeUsed())
				require.Equal(t, uint64(139000), statedb.GetBalance(sender).Uint64())
				require.Equal(t, uint64(21000), statedb.GetBalance(vmctx.Coinbase).Uint64())
			}
		})
	}
}

func TestQKCLegacyEntryRejectsIncompleteTransaction(t *testing.T) {
	for _, tx := range []*qkctypes.Transaction{nil, &qkctypes.Transaction{}, qkctypes.NewTransaction(&qkctypes.EvmTx{})} {
		_, _, err := ApplyQKCTransaction(nil, nil, nil, nil, tx, 0)
		require.ErrorIs(t, err, ErrQKCInvalidTransaction)
	}
}

func TestQKCCrossShardAdmissionUsesConfiguredShards(t *testing.T) {
	cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
	require.NoError(t, err)
	cfg.Quarkchain.Update(4, 16, 10, 10)
	ctx := &QKCExecutionContext{
		QKCConfig: cfg.Quarkchain, ShardConfig: cfg.Quarkchain.GetShardConfigByFullShardID(16),
		RootHeight: 1, XShardGasLimit: 6_000_000,
	}
	sender := common.HexToAddress("0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a")
	to := common.HexToAddress("0x1563915e194d8cfba1943570603f7606a3115508")
	for _, tc := range []struct {
		name   string
		toKey  uint32
		gas    uint64
		height uint32
		want   error
	}{
		{"same-chain intrinsic", 1, 29999, 0, ErrQKCInsufficientStartGas},
		{"cross-shard gas limit", 1, 7_000_000, 0, ErrQKCInvalidTransaction},
		{"uninitialized destination", 1, 30000, 1, ErrQKCInvalidTransaction},
		{"non-neighbor destination", 0x10001, 30000, 0, ErrQKCInvalidTransaction},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx.QKCConfig.GetShardConfigByFullShardID(17).Genesis.RootHeight = tc.height
			statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
			require.NoError(t, err)
			statedb.SetFullShardKey(0)
			statedb.SetBalance(sender, uint256.NewInt(20_000_000), tracing.BalanceChangeUnspecified)
			gp := NewGasPool(12_000_000)
			tx := qkctypes.NewEvmTransaction(0, to, big.NewInt(100), tc.gas, big.NewInt(1), 0, tc.toKey, cfg.Quarkchain.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID)
			err = validateTransaction(ctx, statedb, gp, tx, sender, 1)
			require.ErrorIs(t, err, tc.want)
			require.Equal(t, uint64(0), statedb.GetNonce(sender))
			require.Equal(t, uint64(12_000_000), gp.Gas())
		})
	}
	ctx.QKCConfig.GetShardConfigByFullShardID(17).Genesis.RootHeight = 0
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	require.NoError(t, err)
	sender = crypto.PubkeyToAddress(key.PublicKey)
	tx, err := qkctypes.SignTx(qkctypes.NewEvmTransaction(0, to, big.NewInt(100), 60000, big.NewInt(1), 0, 1, cfg.Quarkchain.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID), qkctypes.MakeSigner(cfg.Quarkchain.NetworkID, ctx.ShardConfig.EthChainID), key)
	require.NoError(t, err)
	statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
	require.NoError(t, err)
	statedb.SetFullShardKey(0)
	statedb.SetBalance(sender, uint256.NewInt(20_000_000), tracing.BalanceChangeUnspecified)
	gp := NewGasPool(12_000_000)
	evm := vm.NewEVM(vm.BlockContext{CanTransfer: CanTransfer, Transfer: Transfer, BlockNumber: big.NewInt(1), Time: 1, GasLimit: 12_000_000}, statedb, &qkcparams.DefaultConstantinople, vm.Config{})
	t.Cleanup(evm.Release)
	_, _, err = ApplyQKCTransaction(ctx, evm, gp, statedb, tx, 0)
	require.ErrorIs(t, err, ErrQKCInvalidTransaction)
	receipt, deposit, _, err := ApplyQKCTransactionWithDeposit(ctx, evm, gp, statedb, tx, 0)
	require.NoError(t, err)
	require.Equal(t, qkctypes.ReceiptStatusSuccessful, receipt.Status)
	require.Equal(t, uint64(21000), gp.CumulativeUsed())
	require.Equal(t, uint64(0), statedb.GetBalance(to).Uint64())
	require.Equal(t, uint64(1), statedb.GetNonce(sender))
	require.NotNil(t, deposit)
	require.Equal(t, uint32(1), deposit.To.FullShardKey)
	require.Equal(t, big.NewInt(100), deposit.Value.Value)
}

func TestQKCCrossShardRejectsInvalidFeeRateBeforeMutation(t *testing.T) {
	key, err := crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	for _, tc := range []struct {
		name string
		rate *big.Rat
	}{
		{"nil", nil}, {"too large", big.NewRat(2, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
			require.NoError(t, err)
			cfg.Quarkchain.LocalFeeRate = tc.rate
			ctx := &QKCExecutionContext{
				QKCConfig: cfg.Quarkchain, ShardConfig: cfg.Quarkchain.GetShardConfigByFullShardID(1),
				RootHeight: 1, XShardGasLimit: 100000,
			}
			statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
			require.NoError(t, err)
			statedb.SetFullShardKey(1)
			statedb.SetBalance(sender, uint256.NewInt(200000), tracing.BalanceChangeUnspecified)
			gp := NewGasPool(100000)
			evm := vm.NewEVM(vm.BlockContext{CanTransfer: CanTransfer, Transfer: Transfer, BlockNumber: big.NewInt(1), Time: 1, GasLimit: 100000}, statedb, &qkcparams.DefaultConstantinople, vm.Config{})
			t.Cleanup(evm.Release)
			tx, err := qkctypes.SignTx(qkctypes.NewEvmTransaction(0, common.HexToAddress("0x1234"), big.NewInt(100), 60000, big.NewInt(1), 1, 0x10001, cfg.Quarkchain.NetworkID, 0, nil, qkccommon.DefaultTokenID, qkccommon.DefaultTokenID), qkctypes.MakeSigner(cfg.Quarkchain.NetworkID, ctx.ShardConfig.EthChainID), key)
			require.NoError(t, err)
			receipt, deposit, _, err := ApplyQKCTransactionWithDeposit(ctx, evm, gp, statedb, tx, 0)
			require.Error(t, err)
			require.Nil(t, receipt)
			require.Nil(t, deposit)
			require.Equal(t, uint64(0), statedb.GetNonce(sender))
			require.Equal(t, uint64(200000), statedb.GetBalance(sender).Uint64())
			require.Equal(t, uint64(100000), gp.Gas())
		})
	}
}

func TestS3ActivationGates(t *testing.T) {
	cfg, err := config.LoadClusterConfig("../qkc/config/singularity/devnet.json")
	require.NoError(t, err)
	shard := cfg.Quarkchain.GetShardConfigByFullShardID(1)
	sender := common.HexToAddress("0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a")
	to := common.HexToAddress("0x1563915e194d8cfba1943570603f7606a3115508")
	statedb, err := state.NewQKCStateDB(coretypes.EmptyRootHash, state.NewQKCDatabase(rawdb.NewMemoryDatabase()))
	require.NoError(t, err)
	statedb.SetFullShardKey(1)
	statedb.SetBalance(sender, uint256.NewInt(50000), tracing.BalanceChangeUnspecified)
	gp := NewGasPool(50000)
	ctx := &QKCExecutionContext{QKCConfig: cfg.Quarkchain, ShardConfig: shard}
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
