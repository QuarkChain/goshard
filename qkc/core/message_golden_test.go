// Copyright 2026-2027, QuarkChain.

package core

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/holiman/uint256"
)

const messageGoldenPath = "../testdata/exec_golden/message_level.json"

type goldenAllocation struct {
	Balances map[string]string `json:"balances"`
	Code     *string           `json:"code"`
	Storage  map[string]string `json:"storage"`
}

type goldenTx struct {
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

type goldenLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

type goldenReceipt struct {
	Success              bool        `json:"success"`
	CumulativeGasUsed    uint64      `json:"cumulative_gas_used"`
	Bloom                string      `json:"bloom"`
	ContractAddress      string      `json:"contract_address"`
	ContractFullShardKey uint32      `json:"contract_full_shard_key"`
	Logs                 []goldenLog `json:"logs"`
}

type goldenAccount struct {
	Nonce        uint64            `json:"nonce"`
	CodeHash     string            `json:"code_hash"`
	FullShardKey uint32            `json:"full_shard_key"`
	Exists       bool              `json:"exists"`
	Balances     map[string]string `json:"balances"`
	Storage      map[string]string `json:"storage"`
}

type goldenMessageCase struct {
	Name        string `json:"name"`
	Comment     string `json:"comment"`
	Network     string `json:"network"`
	FullShardID uint32 `json:"full_shard_id"`
	Expect      string `json:"expect"`
	Context     struct {
		Timestamp       uint64 `json:"timestamp"`
		GasLimit        uint64 `json:"gas_limit"`
		BlockNumber     uint64 `json:"block_number"`
		BlockCoinbase   string `json:"block_coinbase"`
		BlockDifficulty uint64 `json:"block_difficulty"`
	} `json:"context"`
	PreAlloc map[string]goldenAllocation `json:"pre_alloc"`
	Inputs   []struct {
		Kind        string         `json:"kind"`
		Transaction *goldenTx      `json:"transaction"`
		Deposit     *goldenDeposit `json:"deposit"`
	} `json:"inputs"`
	GasUsedStart uint64 `json:"gas_used_start"`
	Result       struct {
		Success   bool   `json:"success"`
		Output    string `json:"output"`
		Rejected  bool   `json:"rejected"`
		ErrorType string `json:"error_type"`
	} `json:"result"`
	PostStateRoot         string                   `json:"post_state_root"`
	GasUsed               uint64                   `json:"gas_used"`
	XShardReceiveGasUsed  uint64                   `json:"xshard_receive_gas_used"`
	BlockFeeTokens        map[string]string        `json:"block_fee_tokens"`
	Receipts              []goldenReceipt          `json:"receipts"`
	XShardDepositReceipts []goldenReceipt          `json:"xshard_deposit_receipts"`
	XShardList            []goldenDeposit          `json:"xshard_list"`
	Accounts              map[string]goldenAccount `json:"accounts"`
}

func loadMessageGolden(t *testing.T) []goldenMessageCase {
	t.Helper()
	raw, err := os.ReadFile(messageGoldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var file struct {
		Cases []goldenMessageCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if len(file.Cases) == 0 {
		t.Fatal("golden has no cases")
	}
	return file.Cases
}

var loadedNetworks = map[string]*config.ClusterConfig{}

func network(t *testing.T, name string) *config.ClusterConfig {
	t.Helper()
	if cfg, ok := loadedNetworks[name]; ok {
		return cfg
	}
	cfg, err := config.LoadClusterConfig(filepath.Join("..", "config", "singularity", name+".json"))
	if err != nil {
		t.Fatalf("load %s config: %v", name, err)
	}
	loadedNetworks[name] = cfg
	return cfg
}

func mustBigString(t *testing.T, decimal string) *big.Int {
	t.Helper()
	value, ok := new(big.Int).SetString(decimal, 10)
	if !ok {
		t.Fatalf("%q is not a decimal integer", decimal)
	}
	return value
}

func recipientOf(t *testing.T, hex string) account.Recipient {
	t.Helper()
	raw := common.FromHex(hex)
	if len(raw) != account.RecipientLength {
		t.Fatalf("recipient %q is %d bytes, want %d", hex, len(raw), account.RecipientLength)
	}
	return account.BytesToIdentityRecipient(raw)
}

// applyGoldenAlloc is quarkchain/genesis.py:55-86, the same shape the
// state-level consumer uses: the shard key is stamped per address before the
// account exists, code arrives with nonce 1, and balances are deltas.
func applyGoldenAlloc(t *testing.T, state *qkcstate.EvmState, alloc map[string]goldenAllocation) {
	t.Helper()
	addresses := make([]string, 0, len(alloc))
	for addr := range alloc {
		addresses = append(addresses, addr)
	}
	sort.Strings(addresses)

	for _, addrHex := range addresses {
		entry := alloc[addrHex]
		addr, err := account.CreatAddressFromBytes(common.FromHex(addrHex))
		if err != nil {
			t.Fatalf("allocation key %q: %v", addrHex, err)
		}
		state.SetFullShardKey(addr.FullShardKey)
		if entry.Code != nil {
			state.SetCode(addr.Recipient, common.FromHex(*entry.Code))
			state.SetNonce(addr.Recipient, 1)
		}
		for slot, value := range entry.Storage {
			state.SetState(addr.Recipient, common.HexToHash(slot), common.HexToHash(value))
		}
		for token, amount := range entry.Balances {
			tokenID, err := qkccommon.TokenIDEncodeChecked(token)
			if err != nil {
				t.Fatalf("allocation %q: %v", addrHex, err)
			}
			state.DeltaTokenBalance(addr.Recipient, tokenID, mustBigString(t, amount))
		}
	}
}

// buildTx reassembles the signed transaction from its dumped fields, including
// the signature, so the Go side recovers the same sender pyquarkchain did
// rather than being told who it was.
func buildTx(t *testing.T, spec *goldenTx) *types.Transaction {
	t.Helper()
	var tx *types.Transaction
	if spec.To == "0x" || spec.To == "" {
		tx = types.NewEvmContractCreation(spec.Nonce, mustBigString(t, spec.Value), spec.StartGas,
			mustBigString(t, spec.GasPrice), spec.FromFullShardKey, spec.ToFullShardKey,
			spec.NetworkID, spec.Version, common.FromHex(spec.Data), spec.GasTokenID, spec.TransferTokenID)
	} else {
		tx = types.NewEvmTransaction(spec.Nonce, recipientOf(t, spec.To), mustBigString(t, spec.Value),
			spec.StartGas, mustBigString(t, spec.GasPrice), spec.FromFullShardKey, spec.ToFullShardKey,
			spec.NetworkID, spec.Version, common.FromHex(spec.Data), spec.GasTokenID, spec.TransferTokenID)
	}
	tx.SetVRS(mustBigString(t, spec.V), mustBigString(t, spec.R), mustBigString(t, spec.S))
	return tx
}

func buildDeposit(t *testing.T, spec *goldenDeposit) *types.CrossShardTransactionDeposit {
	t.Helper()
	return &types.CrossShardTransactionDeposit{
		TxHash:          common.HexToHash(spec.TxHash),
		From:            account.NewAddress(recipientOf(t, spec.From), spec.FromFullShardKey),
		To:              account.NewAddress(recipientOf(t, spec.To), spec.ToFullShardKey),
		Value:           &serialize.Uint256{Value: mustBigString(t, spec.Value)},
		GasPrice:        &serialize.Uint256{Value: mustBigString(t, spec.GasPrice)},
		GasTokenID:      spec.GasTokenID,
		TransferTokenID: spec.TransferTokenID,
		GasRemained:     &serialize.Uint256{Value: mustBigString(t, spec.GasRemained)},
		MessageData:     common.FromHex(spec.MessageData),
		CreateContract:  spec.CreateContract,
		IsFromRootChain: spec.IsFromRootChain,
		RefundRate:      spec.RefundRate,
	}
}

// newCaseState opens an empty state configured for the case's network and
// shard, applies the allocation and commits it, exactly as the generator does.
// Executing against a still-dirty allocation would leave every allocated
// account marked, and a marked account is one commit reconsiders — so the two
// arrangements do not produce the same root.
func newCaseState(t *testing.T, tc *goldenMessageCase) (*qkcstate.EvmState, *ExecutionContext) {
	t.Helper()
	cluster := network(t, tc.Network)
	shard := cluster.Quarkchain.GetShardConfigByFullShardID(tc.FullShardID)
	if shard == nil {
		t.Fatalf("network %s has no shard %#x", tc.Network, tc.FullShardID)
	}

	db := rawdb.NewMemoryDatabase()
	state, err := qkcstate.New(coretypes.EmptyRootHash, db, qkcstate.NewDatabase(db))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	state.SetConfig(cluster.Quarkchain, shard)
	applyGoldenAlloc(t, state, tc.PreAlloc)
	if _, err := state.Commit(0); err != nil {
		t.Fatalf("commit allocation: %v", err)
	}

	state.SetTimestamp(tc.Context.Timestamp)
	state.SetGasLimit(tc.Context.GasLimit)
	state.SetBlockNumber(tc.Context.BlockNumber)
	state.SetBlockCoinbase(recipientOf(t, tc.Context.BlockCoinbase))
	state.SetBlockDifficulty(new(big.Int).SetUint64(tc.Context.BlockDifficulty))

	ctx := &ExecutionContext{
		QKCConfig:   cluster.Quarkchain,
		ShardConfig: shard,
	}
	return state, ctx
}

// TestMessageGolden replays every message-level vector: the same allocation,
// the same signed transaction or deposit, then pyquarkchain's post state root
// and every consensus output alongside it. A rejected case asserts only that
// execution was refused — the wording of pyquarkchain's exception carries no
// consensus meaning — and that nothing was written.
func TestMessageGolden(t *testing.T) {
	for _, tc := range loadMessageGolden(t) {
		t.Run(tc.Name, func(t *testing.T) {
			state, ctx := newCaseState(t, &tc)
			preRoot := state.Root()

			if len(tc.Inputs) != 1 {
				t.Fatalf("case has %d inputs, want 1", len(tc.Inputs))
			}
			input := tc.Inputs[0]

			var (
				success bool
				output  []byte
				err     error
			)
			switch input.Kind {
			case "transaction":
				tx := buildTx(t, input.Transaction)
				if got, senderErr := TxSender(ctx, tx); senderErr == nil {
					if want := recipientOf(t, input.Transaction.Sender); got != want {
						t.Fatalf("recovered sender %s, golden says %s", got.Hex(), want.Hex())
					}
				}
				// The hash a transaction is applied under is the wrapper's, and
				// pyquarkchain has no wrapper here — it passes the EVM
				// transaction's own hash. Take the golden's rather than
				// recomputing, so the two sides agree on what the receipt and
				// any produced deposit are keyed by.
				success, output, err = ApplyTransaction(ctx, state, tx, common.HexToHash(input.Transaction.Hash), 0)
			case "deposit":
				deposit := buildDeposit(t, input.Deposit)
				success, output, err = ApplyXShardDeposit(ctx, state, deposit, tc.GasUsedStart, 0)
			default:
				t.Fatalf("unknown input kind %q", input.Kind)
			}

			if tc.Expect == "rejected" {
				if err == nil {
					t.Fatalf("expected a rejection, got success=%v output=%x", success, output)
				}
				checkRejectionReason(t, tc.Result.ErrorType, err)
				root, commitErr := state.Commit(tc.Context.BlockNumber)
				if commitErr != nil {
					t.Fatalf("commit after rejection: %v", commitErr)
				}
				if root != preRoot {
					t.Errorf("a rejected message changed the state root: %s, was %s", root, preRoot)
				}
				return
			}

			if err != nil {
				t.Fatalf("execution failed: %v\ncase: %s", err, tc.Comment)
			}
			if success != tc.Result.Success {
				t.Errorf("success = %v, want %v", success, tc.Result.Success)
			}
			if got := "0x" + common.Bytes2Hex(output); got != tc.Result.Output {
				t.Errorf("output = %s, want %s", got, tc.Result.Output)
			}
			if got := state.GasUsed(); got != tc.GasUsed {
				t.Errorf("gas used = %d, want %d", got, tc.GasUsed)
			}
			if got := state.XShardReceiveGasUsed(); got != tc.XShardReceiveGasUsed {
				t.Errorf("cross-shard receive gas used = %d, want %d", got, tc.XShardReceiveGasUsed)
			}
			checkBlockFees(t, state.BlockFeeTokens(), tc.BlockFeeTokens)
			checkReceipts(t, "receipt", state.Receipts(), tc.Receipts)
			checkReceipts(t, "deposit receipt", state.XShardDepositReceipts(), tc.XShardDepositReceipts)
			checkDeposits(t, state.XShardList(), tc.XShardList)

			root, err := state.Commit(tc.Context.BlockNumber)
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			if want := common.HexToHash(tc.PostStateRoot); root != want {
				t.Errorf("state root = %s, want %s\ncase: %s", root, want, tc.Comment)
			}
			checkGoldenAccounts(t, state, tc.Accounts)
		})
	}
}

func checkBlockFees(t *testing.T, got map[uint64]*uint256.Int, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("block fee tokens: %d entries, want %d", len(got), len(want))
	}
	for token, amount := range want {
		id, err := strconv.ParseUint(token, 10, 64)
		if err != nil {
			t.Fatalf("token id %q: %v", token, err)
		}
		have := got[id]
		if have == nil {
			have = new(uint256.Int)
		}
		if have.Dec() != amount {
			t.Errorf("block fee for token %s = %s, want %s", token, have.Dec(), amount)
		}
	}
}

func checkReceipts(t *testing.T, kind string, got []*types.Receipt, want []goldenReceipt) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%ss: %d, want %d", kind, len(got), len(want))
	}
	for i, expected := range want {
		receipt := got[i]
		if (receipt.Status == types.ReceiptStatusSuccessful) != expected.Success {
			t.Errorf("%s %d: success = %v, want %v", kind, i, receipt.Status == types.ReceiptStatusSuccessful, expected.Success)
		}
		if receipt.CumulativeGasUsed != expected.CumulativeGasUsed {
			t.Errorf("%s %d: cumulative gas = %d, want %d", kind, i, receipt.CumulativeGasUsed, expected.CumulativeGasUsed)
		}
		if got := "0x" + common.Bytes2Hex(receipt.Bloom.Bytes()); got != expected.Bloom {
			t.Errorf("%s %d: bloom mismatch", kind, i)
		}
		wantContract := common.FromHex(expected.ContractAddress)
		if len(wantContract) == 0 {
			if receipt.ContractAddress != (account.Recipient{}) {
				t.Errorf("%s %d: contract address = %s, want none", kind, i, receipt.ContractAddress.Hex())
			}
		} else if receipt.ContractAddress != common.BytesToAddress(wantContract) {
			t.Errorf("%s %d: contract address = %s, want %s", kind, i, receipt.ContractAddress.Hex(), expected.ContractAddress)
		}
		if receipt.ContractFullShardKey != expected.ContractFullShardKey {
			t.Errorf("%s %d: contract full shard key = %d, want %d", kind, i, receipt.ContractFullShardKey, expected.ContractFullShardKey)
		}
		checkLogs(t, kind, i, receipt.Logs, expected.Logs)
	}
}

func checkLogs(t *testing.T, kind string, index int, got []*coretypes.Log, want []goldenLog) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s %d: %d logs, want %d", kind, index, len(got), len(want))
	}
	for i, expected := range want {
		log := got[i]
		if log.Address != common.HexToAddress(expected.Address) {
			t.Errorf("%s %d log %d: address = %s, want %s", kind, index, i, log.Address.Hex(), expected.Address)
		}
		if len(log.Topics) != len(expected.Topics) {
			t.Fatalf("%s %d log %d: %d topics, want %d", kind, index, i, len(log.Topics), len(expected.Topics))
		}
		for j, topic := range expected.Topics {
			if log.Topics[j] != common.HexToHash(topic) {
				t.Errorf("%s %d log %d topic %d = %s, want %s", kind, index, i, j, log.Topics[j].Hex(), topic)
			}
		}
		if got := "0x" + common.Bytes2Hex(log.Data); got != expected.Data {
			t.Errorf("%s %d log %d: data = %s, want %s", kind, index, i, got, expected.Data)
		}
	}
}

func checkDeposits(t *testing.T, got []*types.CrossShardTransactionDeposit, want []goldenDeposit) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("produced %d deposits, want %d", len(got), len(want))
	}
	for i, expected := range want {
		deposit := got[i]
		check := func(field, have, wantValue string) {
			if have != wantValue {
				t.Errorf("deposit %d: %s = %s, want %s", i, field, have, wantValue)
			}
		}
		check("tx hash", deposit.TxHash.Hex(), common.HexToHash(expected.TxHash).Hex())
		check("from", deposit.From.Recipient.Hex(), recipientOf(t, expected.From).Hex())
		check("to", deposit.To.Recipient.Hex(), recipientOf(t, expected.To).Hex())
		check("value", deposit.Value.Value.String(), expected.Value)
		check("gas price", deposit.GasPrice.Value.String(), expected.GasPrice)
		check("gas remained", deposit.GasRemained.Value.String(), expected.GasRemained)
		check("message data", "0x"+common.Bytes2Hex(deposit.MessageData), expected.MessageData)
		if deposit.From.FullShardKey != expected.FromFullShardKey {
			t.Errorf("deposit %d: from full shard key = %d, want %d", i, deposit.From.FullShardKey, expected.FromFullShardKey)
		}
		if deposit.To.FullShardKey != expected.ToFullShardKey {
			t.Errorf("deposit %d: to full shard key = %d, want %d", i, deposit.To.FullShardKey, expected.ToFullShardKey)
		}
		if deposit.GasTokenID != expected.GasTokenID || deposit.TransferTokenID != expected.TransferTokenID {
			t.Errorf("deposit %d: tokens = (%d, %d), want (%d, %d)", i,
				deposit.GasTokenID, deposit.TransferTokenID, expected.GasTokenID, expected.TransferTokenID)
		}
		if deposit.CreateContract != expected.CreateContract {
			t.Errorf("deposit %d: create contract = %v, want %v", i, deposit.CreateContract, expected.CreateContract)
		}
		if deposit.IsFromRootChain != expected.IsFromRootChain {
			t.Errorf("deposit %d: is from root chain = %v, want %v", i, deposit.IsFromRootChain, expected.IsFromRootChain)
		}
		if deposit.RefundRate != expected.RefundRate {
			t.Errorf("deposit %d: refund rate = %d, want %d", i, deposit.RefundRate, expected.RefundRate)
		}
	}
}

func checkGoldenAccounts(t *testing.T, state *qkcstate.EvmState, want map[string]goldenAccount) {
	t.Helper()
	for addrHex, expected := range want {
		addr := recipientOf(t, addrHex)
		if got := state.GetNonce(addr); got != expected.Nonce {
			t.Errorf("%s: nonce = %d, want %d", addrHex, got, expected.Nonce)
		}
		if got := state.GetCodeHash(addr); got != common.HexToHash(expected.CodeHash) {
			t.Errorf("%s: code hash = %s, want %s", addrHex, got, expected.CodeHash)
		}
		if got := state.GetFullShardKey(addr); got != expected.FullShardKey {
			t.Errorf("%s: full shard key = %d, want %d", addrHex, got, expected.FullShardKey)
		}
		if got := state.Exists(addr); got != expected.Exists {
			t.Errorf("%s: exists = %v, want %v", addrHex, got, expected.Exists)
		}
		balances := state.GetBalances(addr)
		if len(balances) != len(expected.Balances) {
			t.Errorf("%s: holds %d tokens, want %d", addrHex, len(balances), len(expected.Balances))
		}
		for token, amount := range expected.Balances {
			id, err := strconv.ParseUint(token, 10, 64)
			if err != nil {
				t.Fatalf("%s: token id %q: %v", addrHex, token, err)
			}
			if got := state.GetBalance(addr, id).Dec(); got != amount {
				t.Errorf("%s: token %s = %s, want %s", addrHex, token, got, amount)
			}
		}
		for slot, value := range expected.Storage {
			got := state.GetState(addr, common.HexToHash(slot))
			if want := common.HexToHash(value); got != want {
				t.Errorf("%s: slot %s = %s, want %s", addrHex, slot, got, want)
			}
		}
	}
}

// rejectionReasons maps pyquarkchain's exception classes to this package's
// sentinels. The vectors say the wording carries no consensus meaning, but the
// reason does: a transaction refused for the wrong rule is refused by accident,
// and the next change to that rule will not notice.
var rejectionReasons = map[string]error{
	"UnsignedTransaction":  ErrUnsignedTransaction,
	"InvalidTransaction":   ErrInvalidTransaction,
	"InvalidNonce":         ErrInvalidNonce,
	"InsufficientStartGas": ErrInsufficientStartGas,
	"InsufficientBalance":  ErrInsufficientBalance,
	"InvalidNativeToken":   ErrInvalidNativeToken,
	"BlockGasLimitReached": ErrBlockGasLimitReached,
	// A bare assert, not one of pyquarkchain's typed refusals: the transaction
	// is not rejected for a reason, it cannot be executed at all. Should a
	// second cause ever raise one, this mapping has to grow a way to tell them
	// apart rather than quietly accept the wrong sentinel.
	"AssertionError": ErrGasUnderflow,
}

func checkRejectionReason(t *testing.T, errorType string, err error) {
	t.Helper()
	want, ok := rejectionReasons[errorType]
	if !ok {
		t.Fatalf("no sentinel is mapped to pyquarkchain's %s; add one rather than "+
			"letting the case pass on any error", errorType)
	}
	if !errorsIs(err, want) {
		t.Errorf("refused with %v, want %v (pyquarkchain raised %s)", err, want, errorType)
	}
}
