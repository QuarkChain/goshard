// Copyright 2026-2027, QuarkChain.

package state

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/holiman/uint256"
)

const stateGoldenPath = "../testdata/exec_golden/state_level.json"

type goldenAllocation struct {
	Balances map[string]string `json:"balances"`
	Code     *string           `json:"code"`
	Storage  map[string]string `json:"storage"`
}

// goldenOp mirrors one entry of a case's op list. Value carries whichever shape
// the operation uses — a decimal amount, a hex slot value or a shard key — so it
// stays raw until the operation decides.
type goldenOp struct {
	Op      string          `json:"op"`
	Address string          `json:"address"`
	Token   string          `json:"token"`
	Key     string          `json:"key"`
	Code    string          `json:"code"`
	Value   json.RawMessage `json:"value"`
}

type goldenAccount struct {
	Nonce        uint64            `json:"nonce"`
	CodeHash     string            `json:"code_hash"`
	FullShardKey uint32            `json:"full_shard_key"`
	Exists       bool              `json:"exists"`
	Balances     map[string]string `json:"balances"`
	Storage      map[string]string `json:"storage"`
}

type goldenStateCase struct {
	Name     string                      `json:"name"`
	Comment  string                      `json:"comment"`
	Network  string                      `json:"network"`
	PreAlloc map[string]goldenAllocation `json:"pre_alloc"`
	PreRoot  string                      `json:"pre_state_root"`
	Ops      []goldenOp                  `json:"ops"`
	Root     string                      `json:"post_state_root"`
	Accounts map[string]goldenAccount    `json:"accounts"`
}

func loadStateGolden(t *testing.T) []goldenStateCase {
	t.Helper()
	raw, err := os.ReadFile(stateGoldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var file struct {
		Cases []goldenStateCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if len(file.Cases) == 0 {
		t.Fatal("golden has no cases")
	}
	return file.Cases
}

func newTestState(t *testing.T) *StateDB {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	state, err := New(coretypes.EmptyRootHash, db, NewDatabase(db))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return state
}

func mustRecipient(t *testing.T, hex string) account.Recipient {
	t.Helper()
	raw := common.FromHex(hex)
	if len(raw) != account.RecipientLength {
		t.Fatalf("recipient %q is %d bytes, want %d", hex, len(raw), account.RecipientLength)
	}
	return account.BytesToIdentityRecipient(raw)
}

func mustBig(t *testing.T, raw json.RawMessage) *big.Int {
	t.Helper()
	var decimal string
	if err := json.Unmarshal(raw, &decimal); err != nil {
		t.Fatalf("value %s is not a decimal string: %v", raw, err)
	}
	value, ok := new(big.Int).SetString(decimal, 10)
	if !ok {
		t.Fatalf("value %q is not a decimal integer", decimal)
	}
	return value
}

// applyAlloc is quarkchain/genesis.py:55-86: the shard key is set per address
// before its account is created, code comes with nonce 1, and balances arrive as
// deltas.
func applyAlloc(t *testing.T, state *StateDB, alloc map[string]goldenAllocation) {
	t.Helper()
	addresses := make([]string, 0, len(alloc))
	for addr := range alloc {
		addresses = append(addresses, addr)
	}
	sort.Strings(addresses)

	for _, addrHex := range addresses {
		entry := alloc[addrHex]
		raw := common.FromHex(addrHex)
		if len(raw) != account.RecipientLength+account.FullShardKeyLength {
			t.Fatalf("allocation key %q is %d bytes, want 24", addrHex, len(raw))
		}
		addr, err := account.CreatAddressFromBytes(raw)
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
			tokenID, err := qkcCommon.TokenIDEncodeChecked(token)
			if err != nil {
				t.Fatalf("allocation %q: %v", addrHex, err)
			}
			value, ok := new(big.Int).SetString(amount, 10)
			if !ok {
				t.Fatalf("allocation %q: balance %q is not a decimal integer", addrHex, amount)
			}
			state.DeltaTokenBalance(addr.Recipient, tokenID, value)
		}
	}
}

func runOps(t *testing.T, state *StateDB, ops []goldenOp) {
	t.Helper()
	var snapshots []int
	for i, op := range ops {
		var addr account.Recipient
		if op.Address != "" {
			addr = mustRecipient(t, op.Address)
		}
		var tokenID uint64
		if op.Token != "" {
			id, err := qkcCommon.TokenIDEncodeChecked(op.Token)
			if err != nil {
				t.Fatalf("op %d: %v", i, err)
			}
			tokenID = id
		}

		switch op.Op {
		case "set_full_shard_key":
			var key uint32
			if err := json.Unmarshal(op.Value, &key); err != nil {
				t.Fatalf("op %d: full shard key %s: %v", i, op.Value, err)
			}
			state.SetFullShardKey(key)
		case "delta_token_balance":
			state.DeltaTokenBalance(addr, tokenID, mustBig(t, op.Value))
		case "set_token_balance":
			value, overflow := uint256.FromBig(mustBig(t, op.Value))
			if overflow {
				t.Fatalf("op %d: balance overflows 256 bits", i)
			}
			state.SetTokenBalance(addr, tokenID, value)
		case "set_nonce":
			var nonce uint64
			if err := json.Unmarshal(op.Value, &nonce); err != nil {
				t.Fatalf("op %d: nonce %s: %v", i, op.Value, err)
			}
			state.SetNonce(addr, nonce)
		case "increment_nonce":
			state.IncrementNonce(addr)
		case "set_code":
			state.SetCode(addr, common.FromHex(op.Code))
		case "set_storage":
			var value string
			if err := json.Unmarshal(op.Value, &value); err != nil {
				t.Fatalf("op %d: storage value %s: %v", i, op.Value, err)
			}
			state.SetState(addr, common.HexToHash(op.Key), common.HexToHash(value))
		case "reset_balances":
			state.ResetBalances(addr)
		case "reset_storage":
			state.ResetStorage(addr)
		case "del_account":
			state.DelAccount(addr)
		case "snapshot":
			snapshots = append(snapshots, state.Snapshot())
		case "revert":
			if len(snapshots) == 0 {
				t.Fatalf("op %d: revert without a snapshot", i)
			}
			state.RevertToSnapshot(snapshots[len(snapshots)-1])
			snapshots = snapshots[:len(snapshots)-1]
		case "commit":
			if _, err := state.Commit(); err != nil {
				t.Fatalf("op %d: commit: %v", i, err)
			}
		default:
			t.Fatalf("op %d: unknown op %q", i, op.Op)
		}
	}
}

func checkAccounts(t *testing.T, state *StateDB, want map[string]goldenAccount) {
	t.Helper()
	for addrHex, expected := range want {
		addr := mustRecipient(t, addrHex)
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
			tokenID, err := strconv.ParseUint(token, 10, 64)
			if err != nil {
				t.Fatalf("%s: token id %q: %v", addrHex, token, err)
			}
			if got := state.GetBalance(addr, tokenID).Dec(); got != amount {
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

// TestStateGolden replays every state-level vector: the same allocation, the
// same operations, and then the state root pyquarkchain committed to, plus the
// per-account reads that say where a mismatch came from.
//
// The allocation is committed before the operations run, as the generator does.
// Executing against a still-dirty allocation would leave every allocated
// account touched, and an account nothing touches is skipped by commit — so the
// two arrangements do not answer the same question.
func TestStateGolden(t *testing.T) {
	for _, tc := range loadStateGolden(t) {
		t.Run(tc.Name, func(t *testing.T) {
			state := newTestState(t)
			applyAlloc(t, state, tc.PreAlloc)
			preRoot, err := state.Commit()
			if err != nil {
				t.Fatalf("commit allocation: %v", err)
			}
			if want := common.HexToHash(tc.PreRoot); preRoot != want {
				t.Fatalf("allocation state root = %s, want %s", preRoot, want)
			}

			runOps(t, state, tc.Ops)

			root, err := state.Commit()
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			if want := common.HexToHash(tc.Root); root != want {
				t.Errorf("state root = %s, want %s\ncase: %s", root, want, tc.Comment)
			}
			checkAccounts(t, state, tc.Accounts)
		})
	}
}

// TestGenesisAllocRoundTrip reopens a committed genesis allocation, marks every
// account touched without changing anything, and commits again. The root has to
// come back identical, which it only does if each six-field leaf and each token
// balance blob decodes and re-encodes to the same bytes.
func TestGenesisAllocRoundTrip(t *testing.T) {
	for _, tc := range loadStateGolden(t) {
		if len(tc.Ops) != 0 || len(tc.PreAlloc) == 0 {
			continue
		}
		t.Run(tc.Name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			tdb := NewDatabase(db)
			state, err := New(coretypes.EmptyRootHash, db, tdb)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			applyAlloc(t, state, tc.PreAlloc)
			root, err := state.Commit()
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			if want := common.HexToHash(tc.Root); root != want {
				t.Fatalf("state root = %s, want %s", root, want)
			}

			reopened, err := New(root, db, tdb)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			for addrHex := range tc.PreAlloc {
				addr := mustRecipient(t, addrHex[:2*account.RecipientLength])
				reopened.SetNonce(addr, reopened.GetNonce(addr))
			}
			again, err := reopened.Commit()
			if err != nil {
				t.Fatalf("recommit: %v", err)
			}
			if again != root {
				t.Errorf("root after re-committing unchanged accounts = %s, want %s", again, root)
			}
		})
	}
}

// TestStoragePersistsAcrossReopen: a contract's code and storage survive being
// committed and read back through a fresh trie database, which is what the
// storage-tries-flushed-first convention has to buy.
func TestStoragePersistsAcrossReopen(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	state, err := New(coretypes.EmptyRootHash, db, NewDatabase(db))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	addr := mustRecipient(t, "0x00000000000000000000000000000000000000c0")
	code := []byte{0x60, 0x00, 0x60, 0x01}
	state.SetFullShardKey(7)
	state.SetCode(addr, code)
	state.SetNonce(addr, 1)
	for slot := 1; slot <= 3; slot++ {
		state.SetState(addr, common.BigToHash(big.NewInt(int64(slot))), common.BigToHash(big.NewInt(int64(slot*100))))
	}
	root, err := state.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	reopened, err := New(root, db, NewDatabase(db))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := reopened.GetNonce(addr); got != 1 {
		t.Errorf("nonce = %d, want 1", got)
	}
	if got := reopened.GetFullShardKey(addr); got != 7 {
		t.Errorf("full shard key = %d, want 7", got)
	}
	if got := reopened.GetCode(addr); string(got) != string(code) {
		t.Errorf("code = %x, want %x", got, code)
	}
	for slot := 1; slot <= 3; slot++ {
		got := reopened.GetState(addr, common.BigToHash(big.NewInt(int64(slot))))
		if want := common.BigToHash(big.NewInt(int64(slot * 100))); got != want {
			t.Errorf("slot %d = %s, want %s", slot, got, want)
		}
	}
	if err := reopened.Error(); err != nil {
		t.Fatalf("reads reported %v", err)
	}
}

// TestSnapshotRevertRestoresEverything covers the mutations the golden's revert
// case does not reach: nested snapshots, storage that was only read, and the
// execution counters a revert has to put back with the accounts.
func TestSnapshotRevertRestoresEverything(t *testing.T) {
	state := newTestState(t)
	addr := mustRecipient(t, "0x00000000000000000000000000000000000000a1")
	other := mustRecipient(t, "0x00000000000000000000000000000000000000b2")

	state.SetFullShardKey(1)
	state.DeltaTokenBalance(addr, qkcCommon.DefaultTokenID, big.NewInt(1000))
	state.SetNonce(addr, 3)
	state.SetState(addr, common.HexToHash("0x01"), common.HexToHash("0x11"))
	base, err := state.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	outer := state.Snapshot()
	state.DeltaTokenBalance(addr, qkcCommon.DefaultTokenID, big.NewInt(-500))
	state.AddGasUsed(21000)
	state.AddSuicide(other)

	inner := state.Snapshot()
	state.SetState(addr, common.HexToHash("0x01"), common.HexToHash("0x22"))
	state.SetCode(addr, []byte{0xfe})
	state.DeltaTokenBalance(other, qkcCommon.DefaultTokenID, big.NewInt(7))
	state.AddGasUsed(9000)
	state.RevertToSnapshot(inner)

	if got := state.GetState(addr, common.HexToHash("0x01")); got != common.HexToHash("0x11") {
		t.Errorf("slot after inner revert = %s, want 0x11", got)
	}
	if got := state.GetCodeSize(addr); got != 0 {
		t.Errorf("code size after inner revert = %d, want 0", got)
	}
	if got := state.GasUsed(); got != 21000 {
		t.Errorf("gas used after inner revert = %d, want 21000", got)
	}
	if got := state.GetBalance(addr, qkcCommon.DefaultTokenID).Uint64(); got != 500 {
		t.Errorf("balance after inner revert = %d, want 500", got)
	}

	state.RevertToSnapshot(outer)
	if got := state.GasUsed(); got != 0 {
		t.Errorf("gas used after outer revert = %d, want 0", got)
	}
	if got := len(state.Suicides()); got != 0 {
		t.Errorf("suicides after outer revert = %d, want 0", got)
	}
	if got := state.GetBalance(addr, qkcCommon.DefaultTokenID).Uint64(); got != 1000 {
		t.Errorf("balance after outer revert = %d, want 1000", got)
	}

	root, err := state.Commit()
	if err != nil {
		t.Fatalf("recommit: %v", err)
	}
	if root != base {
		t.Errorf("root after reverting everything = %s, want the committed %s", root, base)
	}
}

// TestCorruptTrieFailsLoudly: a missing trie node has to surface as an error,
// not as an absent account. Reading it through geth's Must* accessors returns
// nothing and logs, which would let a corrupt database read as an empty account,
// silently drop the writes made against it, and still report a committed root —
// the same root as before, since the failed update leaves the trie untouched.
func TestCorruptTrieFailsLoudly(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	state, err := New(coretypes.EmptyRootHash, db, NewDatabase(db))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	addr := mustRecipient(t, "0x00000000000000000000000000000000000000a1")
	other := mustRecipient(t, "0x00000000000000000000000000000000000000b2")
	state.SetFullShardKey(1)
	state.DeltaTokenBalance(addr, qkcCommon.DefaultTokenID, big.NewInt(1000))
	state.DeltaTokenBalance(other, qkcCommon.DefaultTokenID, big.NewInt(2000))
	root, err := state.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Drop every trie node but the root, so the leaves can no longer be resolved.
	it := db.NewIterator(nil, nil)
	var orphaned [][]byte
	for it.Next() {
		if key := common.CopyBytes(it.Key()); len(key) == common.HashLength && common.BytesToHash(key) != root {
			orphaned = append(orphaned, key)
		}
	}
	it.Release()
	if len(orphaned) == 0 {
		t.Fatal("nothing to corrupt: the trie is a single node")
	}
	for _, key := range orphaned {
		if err := db.Delete(key); err != nil {
			t.Fatalf("corrupt: %v", err)
		}
	}

	corrupt, err := New(root, db, NewDatabase(db))
	if err != nil {
		return // Refusing to open at all is a loud enough failure.
	}
	corrupt.GetBalance(addr, qkcCommon.DefaultTokenID)
	if corrupt.Error() == nil {
		t.Error("reading an unresolvable account reported no error")
	}
	corrupt.DeltaTokenBalance(addr, qkcCommon.DefaultTokenID, big.NewInt(500))
	again, err := corrupt.Commit()
	if err == nil {
		t.Errorf("commit of a corrupt state succeeded with root %s (was %s)", again, root)
	}
}

// TestBlankAccountsAreNeverWritten: touching accounts that stay blank leaves the
// trie empty, the rule that keeps a zero-value transfer from creating leaves.
func TestBlankAccountsAreNeverWritten(t *testing.T) {
	state := newTestState(t)
	for i := 0; i < 4; i++ {
		addr := mustRecipient(t, fmt.Sprintf("0x%040x", i+1))
		state.DeltaTokenBalance(addr, qkcCommon.DefaultTokenID, big.NewInt(0))
		state.SetState(addr, common.HexToHash("0x01"), common.HexToHash("0x2a"))
	}
	root, err := state.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if root != coretypes.EmptyRootHash {
		t.Errorf("root = %s, want the empty root %s", root, coretypes.EmptyRootHash)
	}
}
