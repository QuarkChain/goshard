// Copyright 2026-2027, QuarkChain.

package state_test

import (
	"encoding/json"
	"math/big"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	corestate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

const stateGoldenPath = "../../qkc/testdata/exec_golden/state_level.json"

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

func newTestState(t *testing.T) *corestate.StateDB {
	t.Helper()
	state, err := corestate.NewQKC(coretypes.EmptyRootHash, corestate.NewQKCDatabase(rawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatalf("NewQKC: %v", err)
	}
	return state
}

func commitAndReopen(t *testing.T, state *corestate.StateDB, block uint64, fullShardKey uint32) (*corestate.StateDB, common.Hash) {
	t.Helper()
	db := state.Database()
	root, err := state.Commit(block, true, false)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	reopened, err := corestate.NewQKC(root, db)
	if err != nil {
		t.Fatalf("reopen state %s: %v", root, err)
	}
	reopened.SetFullShardKey(fullShardKey)
	return reopened, root
}

func mustDeltaTokenBalance(t *testing.T, state *corestate.StateDB, addr account.Recipient, tokenID uint64, delta *big.Int) {
	t.Helper()
	if err := state.DeltaTokenBalance(addr, tokenID, delta); err != nil {
		t.Fatal(err)
	}
}

func TestNewQKCRejectsPathDatabase(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	db := corestate.NewDatabase(
		triedb.NewDatabase(disk, &triedb.Config{PathDB: pathdb.Defaults}),
		corestate.NewCodeDB(disk),
	)
	if _, err := corestate.NewQKC(coretypes.EmptyRootHash, db); err == nil {
		t.Fatal("NewQKC accepted a path-based state database")
	}
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
func applyAlloc(t *testing.T, state *corestate.StateDB, alloc map[string]goldenAllocation) uint32 {
	t.Helper()
	addresses := make([]string, 0, len(alloc))
	for addr := range alloc {
		addresses = append(addresses, addr)
	}
	sort.Strings(addresses)

	var fullShardKey uint32
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
		fullShardKey = addr.FullShardKey
		state.SetFullShardKey(fullShardKey)
		if entry.Code != nil {
			state.SetCode(addr.Recipient, common.FromHex(*entry.Code), tracing.CodeChangeUnspecified)
			state.SetNonce(addr.Recipient, 1, tracing.NonceChangeUnspecified)
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
			mustDeltaTokenBalance(t, state, addr.Recipient, tokenID, value)
		}
	}
	return fullShardKey
}

func runOps(t *testing.T, state *corestate.StateDB, ops []goldenOp, fullShardKey uint32) (*corestate.StateDB, uint32) {
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
			fullShardKey = key
			state.SetFullShardKey(fullShardKey)
		case "delta_token_balance":
			mustDeltaTokenBalance(t, state, addr, tokenID, mustBig(t, op.Value))
		case "set_token_balance":
			value, overflow := uint256.FromBig(mustBig(t, op.Value))
			if overflow {
				t.Fatalf("op %d: balance overflows 256 bits", i)
			}
			state.SetBalanceByTokenID(addr, value, tokenID, tracing.BalanceChangeUnspecified)
		case "read_account":
			// A read is not inert: it is where an absent account's shard key
			// freezes, so the op has to reach the state rather than be skipped.
			state.GetBalanceByTokenID(addr, qkcCommon.DefaultTokenID)
		case "set_nonce":
			var nonce uint64
			if err := json.Unmarshal(op.Value, &nonce); err != nil {
				t.Fatalf("op %d: nonce %s: %v", i, op.Value, err)
			}
			state.SetNonce(addr, nonce, tracing.NonceChangeUnspecified)
		case "increment_nonce":
			state.SetNonce(addr, state.GetNonce(addr)+1, tracing.NonceChangeUnspecified)
		case "set_code":
			state.SetCode(addr, common.FromHex(op.Code), tracing.CodeChangeUnspecified)
		case "set_storage":
			var value string
			if err := json.Unmarshal(op.Value, &value); err != nil {
				t.Fatalf("op %d: storage value %s: %v", i, op.Value, err)
			}
			state.SetState(addr, common.HexToHash(op.Key), common.HexToHash(value))
		case "snapshot":
			snapshots = append(snapshots, state.Snapshot())
		case "revert":
			if len(snapshots) == 0 {
				t.Fatalf("op %d: revert without a snapshot", i)
			}
			state.RevertToSnapshot(snapshots[len(snapshots)-1])
			snapshots = snapshots[:len(snapshots)-1]
		case "commit":
			state, _ = commitAndReopen(t, state, 0, fullShardKey)
		default:
			t.Fatalf("op %d: unknown op %q", i, op.Op)
		}
	}
	return state, fullShardKey
}

func checkAccounts(t *testing.T, state *corestate.StateDB, want map[string]goldenAccount) {
	t.Helper()
	for addrHex, expected := range want {
		addr := mustRecipient(t, addrHex)
		if got := state.GetNonce(addr); got != expected.Nonce {
			t.Errorf("%s: nonce = %d, want %d", addrHex, got, expected.Nonce)
		}
		codeHash := state.GetCodeHash(addr)
		if codeHash == (common.Hash{}) {
			codeHash = coretypes.EmptyCodeHash
		}
		if codeHash != common.HexToHash(expected.CodeHash) {
			t.Errorf("%s: code hash = %s, want %s", addrHex, codeHash, expected.CodeHash)
		}
		if got := state.GetFullShardKey(addr); got != expected.FullShardKey {
			t.Errorf("%s: full shard key = %d, want %d", addrHex, got, expected.FullShardKey)
		}
		if got := !state.Empty(addr); got != expected.Exists {
			t.Errorf("%s: exists = %v, want %v", addrHex, got, expected.Exists)
		}

		balances := state.GetTokenBalances(addr)
		if len(balances) != len(expected.Balances) {
			t.Errorf("%s: holds %d tokens, want %d", addrHex, len(balances), len(expected.Balances))
		}
		for token, amount := range expected.Balances {
			tokenID, err := strconv.ParseUint(token, 10, 64)
			if err != nil {
				t.Fatalf("%s: token id %q: %v", addrHex, token, err)
			}
			if got := state.GetBalanceByTokenID(addr, tokenID).Dec(); got != amount {
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
			fullShardKey := applyAlloc(t, state, tc.PreAlloc)
			state, preRoot := commitAndReopen(t, state, 0, fullShardKey)
			if want := common.HexToHash(tc.PreRoot); preRoot != want {
				t.Fatalf("allocation state root = %s, want %s", preRoot, want)
			}

			state, fullShardKey = runOps(t, state, tc.Ops, fullShardKey)

			state, root := commitAndReopen(t, state, 0, fullShardKey)
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
			db := corestate.NewQKCDatabase(rawdb.NewMemoryDatabase())
			state, err := corestate.NewQKC(coretypes.EmptyRootHash, db)
			if err != nil {
				t.Fatalf("NewQKC: %v", err)
			}
			applyAlloc(t, state, tc.PreAlloc)
			root, err := state.Commit(0, true, false)
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			if want := common.HexToHash(tc.Root); root != want {
				t.Fatalf("state root = %s, want %s", root, want)
			}

			reopened, err := corestate.NewQKC(root, db)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			for addrHex := range tc.PreAlloc {
				addr := mustRecipient(t, addrHex[:2*account.RecipientLength])
				reopened.SetNonce(addr, reopened.GetNonce(addr), tracing.NonceChangeUnspecified)
			}
			again, err := reopened.Commit(0, true, false)
			if err != nil {
				t.Fatalf("recommit: %v", err)
			}
			if again != root {
				t.Errorf("root after re-committing unchanged accounts = %s, want %s", again, root)
			}
		})
	}
}

// TestSnapshotRevertRestoresEverything covers the mutations the golden's revert
// case does not reach: nested snapshots and storage that was only read.
func TestSnapshotRevertRestoresEverything(t *testing.T) {
	state := newTestState(t)
	addr := mustRecipient(t, "0x00000000000000000000000000000000000000a1")
	other := mustRecipient(t, "0x00000000000000000000000000000000000000b2")

	state.SetFullShardKey(1)
	mustDeltaTokenBalance(t, state, addr, qkcCommon.DefaultTokenID, big.NewInt(1000))
	state.SetNonce(addr, 3, tracing.NonceChangeUnspecified)
	state.SetState(addr, common.HexToHash("0x01"), common.HexToHash("0x11"))
	state, base := commitAndReopen(t, state, 0, 1)

	outer := state.Snapshot()
	mustDeltaTokenBalance(t, state, addr, qkcCommon.DefaultTokenID, big.NewInt(-500))

	inner := state.Snapshot()
	state.SetState(addr, common.HexToHash("0x01"), common.HexToHash("0x22"))
	state.SetCode(addr, []byte{0xfe}, tracing.CodeChangeUnspecified)
	mustDeltaTokenBalance(t, state, other, qkcCommon.DefaultTokenID, big.NewInt(7))
	state.RevertToSnapshot(inner)

	if got := state.GetState(addr, common.HexToHash("0x01")); got != common.HexToHash("0x11") {
		t.Errorf("slot after inner revert = %s, want 0x11", got)
	}
	if got := state.GetCodeSize(addr); got != 0 {
		t.Errorf("code size after inner revert = %d, want 0", got)
	}
	if got := state.GetBalanceByTokenID(addr, qkcCommon.DefaultTokenID).Uint64(); got != 500 {
		t.Errorf("balance after inner revert = %d, want 500", got)
	}

	state.RevertToSnapshot(outer)
	if got := state.GetBalanceByTokenID(addr, qkcCommon.DefaultTokenID).Uint64(); got != 1000 {
		t.Errorf("balance after outer revert = %d, want 1000", got)
	}

	_, root := commitAndReopen(t, state, 0, 1)
	if root != base {
		t.Errorf("root after reverting everything = %s, want the committed %s", root, base)
	}
}

func TestGetTokenBalancesRetainsCachedZeros(t *testing.T) {
	for _, tokenID := range []uint64{qkcCommon.DefaultTokenID, 100} {
		for _, finalise := range []bool{false, true} {
			t.Run(strconv.FormatUint(tokenID, 10)+"/finalise="+strconv.FormatBool(finalise), func(t *testing.T) {
				state := newTestState(t)
				addr := common.HexToAddress("0xa1")
				snapshot := state.Snapshot()
				mustDeltaTokenBalance(t, state, addr, tokenID, big.NewInt(7))
				state.RevertToSnapshot(snapshot)
				if finalise {
					mustDeltaTokenBalance(t, state, addr, tokenID, new(big.Int))
					state.Finalise(true)
				}

				balances := state.GetTokenBalances(addr)
				if value, ok := balances[tokenID]; !ok || !value.IsZero() || len(balances) != 1 {
					t.Fatalf("cached balances = %v, want only token %d at zero", balances, tokenID)
				}
				if state.Exist(addr) {
					t.Fatal("reading cached balances created a live account")
				}
				balances[tokenID].SetUint64(9)
				delete(balances, tokenID)
				state.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
				if value, ok := state.GetTokenBalances(addr)[tokenID]; !ok || !value.IsZero() {
					t.Fatal("mutating returned balances changed the retained zero entry")
				}
				state, _ = commitAndReopen(t, state, 0, 0)
				if balances := state.GetTokenBalances(addr); len(balances) != 0 {
					t.Fatalf("balances after commit = %v, want no cached zeros", balances)
				}
			})
		}
	}
}

func TestNativeAccountLifecycleClearsDestroyedStorage(t *testing.T) {
	for _, tc := range []struct {
		name      string
		recreate  func(*testing.T, *corestate.StateDB, account.Recipient)
		wantNonce uint64
		wantQKC   uint64
	}{
		{
			name: "paid after finalise",
			recreate: func(t *testing.T, state *corestate.StateDB, addr account.Recipient) {
				mustDeltaTokenBalance(t, state, addr, qkcCommon.DefaultTokenID, big.NewInt(777))
			},
			wantQKC: 777,
		},
		{
			name: "contract recreation",
			recreate: func(_ *testing.T, state *corestate.StateDB, addr account.Recipient) {
				state.CreateAccount(addr)
				state.CreateContract(addr)
				state.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
			},
			wantNonce: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestState(t)
			addr := mustRecipient(t, "0x00000000000000000000000000000000000000c0")
			slot := common.HexToHash("0x01")

			state.SetFullShardKey(1)
			state.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
			state.SetCode(addr, []byte{0x00}, tracing.CodeChangeUnspecified)
			state.SetState(addr, slot, common.HexToHash("0x2a"))
			state.SetBalanceByTokenID(addr, uint256.NewInt(5), qkcCommon.DefaultTokenID, tracing.BalanceChangeUnspecified)
			state.SetBalanceByTokenID(addr, uint256.NewInt(7), 100, tracing.BalanceChangeUnspecified)
			state, _ = commitAndReopen(t, state, 0, 1)

			// SELFDESTRUCT transfers the default-token balance before marking the
			// account. Finalise removes the old incarnation at the message boundary.
			state.SetBalanceByTokenID(addr, new(uint256.Int), qkcCommon.DefaultTokenID, tracing.BalanceChangeUnspecified)
			state.SelfDestruct(addr)
			state.Finalise(true)
			state.SetFullShardKey(2)
			if got := state.GetFullShardKey(addr); got != 1 {
				t.Errorf("shard key before recreation = %d, want 1", got)
			}
			if balances := state.GetTokenBalances(addr); len(balances) != 0 {
				t.Errorf("destroyed balances = %v, want empty", balances)
			}
			tc.recreate(t, state, addr)
			if got := state.GetFullShardKey(addr); got != 1 {
				t.Errorf("shard key after recreation = %d, want 1", got)
			}
			state.Finalise(true)
			state, _ = commitAndReopen(t, state, 1, 2)

			if got := state.GetState(addr, slot); got != (common.Hash{}) {
				t.Errorf("old storage = %s, want empty", got)
			}
			if got := state.GetNonce(addr); got != tc.wantNonce {
				t.Errorf("nonce = %d, want %d", got, tc.wantNonce)
			}
			if got := state.GetBalanceByTokenID(addr, qkcCommon.DefaultTokenID).Uint64(); got != tc.wantQKC {
				t.Errorf("QKC balance = %d, want %d", got, tc.wantQKC)
			}
			if got := state.GetBalanceByTokenID(addr, 100); !got.IsZero() {
				t.Errorf("old MNT balance = %s, want zero", got)
			}
		})
	}
}
