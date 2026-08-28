// Copyright 2026-2027, QuarkChain.

package state

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newQKCTestState returns a state and the trie database behind it, so a test can
// read the committed leaf back and compare its bytes rather than only its root.
func newQKCTestState(t *testing.T) (*StateDB, *triedb.Database) {
	t.Helper()
	tdb := triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)
	s, err := New(types.EmptyRootHash, NewDatabase(tdb, nil))
	require.NoError(t, err)
	return s, tdb
}

// leafAt returns the raw account trie leaf, or nil when the account is absent.
func leafAt(t *testing.T, tdb *triedb.Database, root common.Hash, addr common.Address) []byte {
	t.Helper()
	tr, err := trie.NewStateTrie(trie.StateTrieID(root), tdb)
	require.NoError(t, err)
	blob, err := tr.MustGet(addr.Bytes()), error(nil)
	require.NoError(t, err)
	return blob
}

func tokenBalOf(t *testing.T, leaf []byte) []byte {
	t.Helper()
	var acct types.StateAccount
	require.NoError(t, rlp.DecodeBytes(leaf, &acct))
	// The wire field itself, not the decoded view: only the raw bytes
	// distinguish a drained account from one that never held the token.
	var wire struct {
		Nonce        uint64
		TokenBal     []byte
		Root         common.Hash
		CodeHash     []byte
		FullShardKey qkccommon.Uint32
		Optional     []byte
	}
	require.NoError(t, rlp.DecodeBytes(leaf, &wire))
	return wire.TokenBal
}

// An account created and drained inside one block has no leaf to seed the
// presence marker from, so the marker has to be set when the balance is written.
// Without it the account encodes like one that never held the token, which is a
// different leaf and a different state root.
func TestQKCFreshAccountDrainedToZeroKeepsEntry(t *testing.T) {
	s, tdb := newQKCTestState(t)
	addr := common.HexToAddress("0xaa")

	s.SetFullShardKey(1)
	s.AddBalance(addr, uint256.NewInt(1000), tracing.BalanceChangeUnspecified)
	s.SetNonce(addr, 1, tracing.NonceChangeUnspecified) // keeps it out of the empty sweep
	s.SubBalance(addr, uint256.NewInt(1000), tracing.BalanceChangeUnspecified)

	root, err := s.Commit(0, true, false)
	require.NoError(t, err)

	leaf := leafAt(t, tdb, root, addr)
	require.NotNil(t, leaf, "an account with a nonce survives the empty sweep")
	assert.Equal(t, []byte{0x00, 0xc0}, tokenBalOf(t, leaf),
		"a drained account keeps its zero entry, so the blob is an empty pair list")
}

// The same account, never credited at all, encodes the other form. Together with
// the case above this is what the state root distinguishes.
func TestQKCUntouchedBalanceHasNoEntry(t *testing.T) {
	s, tdb := newQKCTestState(t)
	addr := common.HexToAddress("0xaa")

	s.SetFullShardKey(1)
	s.SetNonce(addr, 1, tracing.NonceChangeUnspecified)

	root, err := s.Commit(0, true, false)
	require.NoError(t, err)

	leaf := leafAt(t, tdb, root, addr)
	require.NotNil(t, leaf)
	assert.Empty(t, tokenBalOf(t, leaf), "an account that never held a token serializes empty bytes")
}

// Existence is decided once, at the end of the block, over every account the
// block touched — not per transaction. An account emptied by an early
// transaction and never seen again must still be dropped.
func TestQKCEmptyAccountPrunedAtBlockEnd(t *testing.T) {
	s, tdb := newQKCTestState(t)
	drained := common.HexToAddress("0xaa")
	other := common.HexToAddress("0xbb")

	// "tx1": credit and then fully drain an account, leaving it blank.
	s.SetFullShardKey(1)
	s.AddBalance(drained, uint256.NewInt(1000), tracing.BalanceChangeUnspecified)
	s.SubBalance(drained, uint256.NewInt(1000), tracing.BalanceChangeUnspecified)

	// "tx2": unrelated work. Crucially no Finalise in between — that is the
	// discipline the QuarkChain semantics depend on.
	s.AddBalance(other, uint256.NewInt(7), tracing.BalanceChangeUnspecified)

	root, err := s.Commit(0, true, false)
	require.NoError(t, err)

	assert.Nil(t, leafAt(t, tdb, root, drained), "a blank account leaves no leaf behind")
	assert.NotNil(t, leafAt(t, tdb, root, other))
}

// The shard key freezes when an absent address is first looked up, not when it
// is first written: pyquarkchain caches a blank account stamped with the shard
// key current at that moment, and the cache outlives the transaction.
func TestQKCShardKeyFreezesAtFirstLookup(t *testing.T) {
	s, _ := newQKCTestState(t)
	addr := common.HexToAddress("0xaa")

	// "tx1" only reads the address.
	s.SetFullShardKey(1)
	require.True(t, s.GetBalance(addr).IsZero())

	// "tx2", running against a different destination shard key, is the first to
	// write it.
	s.SetFullShardKey(2)
	s.AddBalance(addr, uint256.NewInt(5), tracing.BalanceChangeUnspecified)

	assert.EqualValues(t, 1, s.getStateObject(addr).data.FullShardKey,
		"the key from the first lookup wins, not the one current at the write")
}

func TestQKCShardKeyOfNeverReadAccountIsCurrent(t *testing.T) {
	s, _ := newQKCTestState(t)
	addr := common.HexToAddress("0xaa")

	s.SetFullShardKey(3)
	s.AddBalance(addr, uint256.NewInt(5), tracing.BalanceChangeUnspecified)

	assert.EqualValues(t, 3, s.getStateObject(addr).data.FullShardKey)
}

// del_account strips the account and deletes its leaf rather than writing a
// blank one.
func TestQKCDelAccountRemovesLeaf(t *testing.T) {
	s, tdb := newQKCTestState(t)
	addr := common.HexToAddress("0xaa")

	s.SetFullShardKey(1)
	s.AddBalance(addr, uint256.NewInt(9), tracing.BalanceChangeUnspecified)
	s.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
	s.SetCode(addr, []byte{0x60, 0x01}, tracing.CodeChangeUnspecified)
	s.SetState(addr, common.HexToHash("0x01"), common.HexToHash("0x2a"))
	root, err := s.Commit(0, true, false)
	require.NoError(t, err)
	require.NotNil(t, leafAt(t, tdb, root, addr))

	s, err = New(root, NewDatabase(tdb, nil))
	require.NoError(t, err)
	s.DelAccount(addr)
	root, err = s.Commit(1, true, false)
	require.NoError(t, err)

	assert.Nil(t, leafAt(t, tdb, root, addr))
}

// Reverting del_account unwinds every step it journalled, so the account is not
// dirty at the end of the block and its stored leaf is left exactly as it was —
// a selfdestruct inside a reverted frame.
func TestQKCRevertUndoesDelAccount(t *testing.T) {
	s, tdb := newQKCTestState(t)
	addr := common.HexToAddress("0xaa")

	s.SetFullShardKey(1)
	s.AddBalance(addr, uint256.NewInt(5), tracing.BalanceChangeUnspecified)
	s.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
	s.SetCode(addr, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified)
	root, err := s.Commit(0, true, false)
	require.NoError(t, err)
	before := leafAt(t, tdb, root, addr)

	s, err = New(root, NewDatabase(tdb, nil))
	require.NoError(t, err)
	snap := s.Snapshot()
	s.DelAccount(addr)
	s.RevertToSnapshot(snap)

	// The revert does not put the balances back — del_account drops them through
	// reset_balances, whose undo is the misspelled no-op. What saves the account
	// is that unwinding the journal also un-dirties it, so commit never writes
	// the drained version and the stored leaf is left alone.
	assert.True(t, s.GetBalance(addr).IsZero(), "in memory the account stays drained")

	root, err = s.Commit(1, true, false)
	require.NoError(t, err)
	assert.Equal(t, before, leafAt(t, tdb, root, addr), "the leaf must survive the reverted frame")

	reopened, err := New(root, NewDatabase(tdb, nil))
	require.NoError(t, err)
	assert.Equal(t, uint256.NewInt(5), reopened.GetBalance(addr), "and reads back whole")
}

// reset_balances journals its undo onto a misspelled attribute upstream
// (state.py:195), so a revert does not bring the balances back. Reverting
// "correctly" here would compute a different state root than the reference
// clients. The opening credit is what makes it observable: reset_balances marks
// nothing dirty on its own.
func TestQKCRevertDoesNotRestoreResetBalances(t *testing.T) {
	s, tdb := newQKCTestState(t)
	addr := common.HexToAddress("0xaa")

	s.SetFullShardKey(1)
	s.AddBalance(addr, uint256.NewInt(5), tracing.BalanceChangeUnspecified)
	s.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
	root, err := s.Commit(0, true, false)
	require.NoError(t, err)

	s, err = New(root, NewDatabase(tdb, nil))
	require.NoError(t, err)
	s.AddBalance(addr, uint256.NewInt(1), tracing.BalanceChangeUnspecified)
	snap := s.Snapshot()
	s.ResetBalances(addr)
	s.RevertToSnapshot(snap)

	assert.True(t, s.GetBalance(addr).IsZero(), "the revert leaves the account drained")

	root, err = s.Commit(1, true, false)
	require.NoError(t, err)
	leaf := leafAt(t, tdb, root, addr)
	require.NotNil(t, leaf, "the nonce keeps the account alive")
	assert.Empty(t, tokenBalOf(t, leaf), "reset_balances drops the entry as well as the value")
}
