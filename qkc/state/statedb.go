// Copyright 2026-2027, QuarkChain.

// Package state is the mutable QuarkChain state: the Go counterpart of
// pyquarkchain's quarkchain/evm/state.py, over geth's trie and triedb.
//
// It is written rather than adapted from geth's core/state, because four things
// differ structurally: balances are a table indexed by token id instead of a
// scalar, the address's full shard key lives in the leaf, existence and deletion
// follow QuarkChain's own is_blank rule, and the journal has to cover direct
// overwrites of the storage trie's root that geth has no entry for.
//
// # Commit convention
//
// Storage tries are flushed as roots in their own right, before the account trie
// that names them, and the account trie is committed with collectLeaf=false.
// This is not a preference: triedb/hashdb builds its account -> storage-root
// references by decoding account leaves as geth's four-field StateAccount
// (triedb/hashdb/database.go:574), which a six-field QuarkChain account is not.
// Collecting leaves would hand it bytes it cannot read. The same convention is
// already proven by qkc.commitGenesisAlloc.
//
// The price is that this path loses hashdb's reference counting; it does not
// affect consensus, since the root is fixed by trie.Hash() before triedb sees
// anything, and a mistake there surfaces as a local missing trie node rather
// than as a different root. Restoring the references needs no geth change:
// commit with collectLeaf=true, decode the storage roots out of the QuarkChain
// leaves, clear NodeSet.Leaves so hashdb skips its own decoding, and call
// triedb.Reference immediately after Update.
//
// Only hashdb is supported. pathdb decodes accounts on its read and rollback
// paths, not in one skippable place.
package state

import (
	"fmt"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// NewDatabase opens the trie database goshard runs on. It pins hashdb, which
// the commit convention documented above depends on.
func NewDatabase(db ethdb.Database) *triedb.Database {
	return triedb.NewDatabase(db, triedb.HashDefaults)
}

// hashKey is the keying pyquarkchain's SecureTrie applies to every key
// (quarkchain/evm/securetrie.py). geth's StateTrie wraps the same hashing, but
// only behind Must* accessors that log a missing node and carry on returning
// nothing — which would turn a corrupt database into an absent account. The
// plain trie is used instead so every read and write reports its errors.
func hashKey(key []byte) []byte { return crypto.Keccak256(key) }

// executionContext is what pyquarkchain keeps on State outside the trie
// (STATE_DEFAULTS, state.py:45). A snapshot copies it wholesale and a revert
// puts it back, so everything mutable during one transaction belongs here.
type executionContext struct {
	// fullShardKey is stamped into accounts created from now on. Callers set it
	// before applying each transaction or deposit.
	fullShardKey uint32

	gasUsed              uint64
	xShardReceiveGasUsed uint64
	refunds              uint64

	suicides              []account.Recipient
	logs                  []*coretypes.Log
	receipts              []*types.Receipt
	xShardDepositReceipts []*types.Receipt
	xShardList            []*types.CrossShardTransactionDeposit
	blockFeeTokens        map[uint64]*uint256.Int
}

func (c executionContext) clone() executionContext {
	out := c
	out.suicides = append([]account.Recipient(nil), c.suicides...)
	out.logs = append([]*coretypes.Log(nil), c.logs...)
	out.receipts = append([]*types.Receipt(nil), c.receipts...)
	out.xShardDepositReceipts = append([]*types.Receipt(nil), c.xShardDepositReceipts...)
	out.xShardList = append([]*types.CrossShardTransactionDeposit(nil), c.xShardList...)
	out.blockFeeTokens = make(map[uint64]*uint256.Int, len(c.blockFeeTokens))
	for token, value := range c.blockFeeTokens {
		out.blockFeeTokens[token] = new(uint256.Int).Set(value)
	}
	return out
}

// StateDB is one shard's mutable state at one point in its chain.
type StateDB struct {
	db     ethdb.Database
	triedb *triedb.Database
	trie   *trie.Trie
	root   common.Hash

	stateObjects map[account.Recipient]*stateObject
	journal      []journalEntry
	revisions    []revision

	ctx executionContext

	// dbErr holds the first database failure. Reads cannot report errors without
	// giving every EVM opcode an error path, so they record and carry on; Commit
	// refuses to produce a root once one is set.
	dbErr error
}

type revision struct {
	journalIndex int
	root         common.Hash
	ctx          executionContext
}

// New opens the state a root names.
func New(root common.Hash, db ethdb.Database, tdb *triedb.Database) (*StateDB, error) {
	tr, err := trie.New(trie.StateTrieID(root), tdb)
	if err != nil {
		return nil, fmt.Errorf("open state trie %s: %w", root, err)
	}
	return &StateDB{
		db:           db,
		triedb:       tdb,
		trie:         tr,
		root:         root,
		stateObjects: make(map[account.Recipient]*stateObject),
		ctx:          executionContext{blockFeeTokens: make(map[uint64]*uint256.Int)},
	}, nil
}

// Root is the root the state was opened at, or last committed to.
func (s *StateDB) Root() common.Hash { return s.root }

// Error reports the first database failure seen by a read.
func (s *StateDB) Error() error { return s.dbErr }

func (s *StateDB) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

// getStateObject is get_and_cache_account (state.py:362): a miss caches a blank
// account rather than nothing, so a later write finds it already there. An
// account cached but never touched is not committed, which is why caching a
// blank one is harmless.
func (s *StateDB) getStateObject(addr account.Recipient) *stateObject {
	if obj, ok := s.stateObjects[addr]; ok {
		return obj
	}
	obj := newObject(s, addr, s.ctx.fullShardKey)
	enc, err := s.trie.Get(hashKey(addr.Bytes()))
	if err != nil {
		// A missing node is a corrupt database, not an absent account. Recording
		// it here is what stops Commit from later writing a root derived from a
		// state it could not read.
		s.setError(fmt.Errorf("account %s: read leaf: %w", addr.Hex(), err))
	} else if len(enc) > 0 {
		acct := new(Account)
		if err := rlp.DecodeBytes(enc, acct); err != nil {
			s.setError(fmt.Errorf("account %s: decode leaf: %w", addr.Hex(), err))
		} else if stored, err := newObjectFromAccount(s, addr, acct); err != nil {
			s.setError(err)
		} else {
			obj = stored
		}
	}
	s.stateObjects[addr] = obj
	return obj
}

func (s *StateDB) append(entry journalEntry) { s.journal = append(s.journal, entry) }

// setTouched is the set_and_journal(acct, "touched", True) that follows nearly
// every mutation. reset_balances and reset_storage are the two that do not.
func (s *StateDB) setTouched(obj *stateObject) {
	s.append(touchChange{addr: obj.address, prev: obj.touched})
	obj.touched = true
}

// Exists is account_exists (state.py:540): the negation of is_blank.
func (s *StateDB) Exists(addr account.Recipient) bool {
	return !s.getStateObject(addr).empty()
}

func (s *StateDB) GetNonce(addr account.Recipient) uint64 {
	return s.getStateObject(addr).nonce
}

func (s *StateDB) SetNonce(addr account.Recipient, nonce uint64) {
	obj := s.getStateObject(addr)
	s.append(nonceChange{addr: addr, prev: obj.nonce})
	obj.nonce = nonce
	s.setTouched(obj)
}

func (s *StateDB) IncrementNonce(addr account.Recipient) {
	s.SetNonce(addr, s.getStateObject(addr).nonce+1)
}

func (s *StateDB) GetCode(addr account.Recipient) []byte {
	return s.getStateObject(addr).getCode()
}

func (s *StateDB) GetCodeHash(addr account.Recipient) common.Hash {
	return s.getStateObject(addr).codeHash
}

func (s *StateDB) GetCodeSize(addr account.Recipient) int {
	return len(s.GetCode(addr))
}

func (s *StateDB) SetCode(addr account.Recipient, code []byte) {
	obj := s.getStateObject(addr)
	s.append(codeChange{addr: addr, prevCode: obj.getCode(), prevHash: obj.codeHash})
	obj.setCode(code)
	s.setTouched(obj)
}

// GetFullShardKey is the shard key frozen into the account when it was created.
func (s *StateDB) GetFullShardKey(addr account.Recipient) uint32 {
	return s.getStateObject(addr).fullShardKey
}

// FullShardKey is the key accounts created from now on will carry.
func (s *StateDB) FullShardKey() uint32 { return s.ctx.fullShardKey }

func (s *StateDB) SetFullShardKey(key uint32) { s.ctx.fullShardKey = key }

func (s *StateDB) GetBalance(addr account.Recipient, tokenID uint64) *uint256.Int {
	return s.getStateObject(addr).balances.GetTokenBalance(tokenID)
}

// GetBalances returns every token the account holds, for callers that report
// state rather than execute against it.
func (s *StateDB) GetBalances(addr account.Recipient) map[uint64]*uint256.Int {
	return s.getStateObject(addr).balances.GetBalanceMap()
}

func (s *StateDB) setTokenBalance(obj *stateObject, tokenID uint64, value *uint256.Int) {
	s.append(balanceChange{addr: obj.address, tokenID: tokenID, prev: obj.balances.GetTokenBalance(tokenID)})
	obj.balances.SetValue(value, tokenID)
}

// SetTokenBalance mirrors set_token_balance (state.py:443), including its early
// return: writing the balance an account already holds only marks it touched.
func (s *StateDB) SetTokenBalance(addr account.Recipient, tokenID uint64, value *uint256.Int) {
	obj := s.getStateObject(addr)
	if obj.balances.GetTokenBalance(tokenID).Eq(value) {
		s.setTouched(obj)
		return
	}
	s.setTokenBalance(obj, tokenID, value)
	s.setTouched(obj)
}

// DeltaTokenBalance adds a signed amount, as delta_token_balance (state.py:461).
// A zero delta only touches the account — it does not create a zero entry, which
// is what keeps a zero-value transfer from changing the recipient's leaf.
//
// pyquarkchain has no underflow check here; its callers go through deduct_value
// or transfer_value, which test the balance first. A negative result is a caller
// bug, and is recorded rather than wrapped around.
func (s *StateDB) DeltaTokenBalance(addr account.Recipient, tokenID uint64, delta *big.Int) {
	obj := s.getStateObject(addr)
	if delta.Sign() == 0 {
		s.setTouched(obj)
		return
	}
	next := new(big.Int).Add(obj.balances.GetTokenBalance(tokenID).ToBig(), delta)
	if next.Sign() < 0 {
		s.setError(fmt.Errorf("account %s: token %d balance underflow by %s", addr.Hex(), tokenID, new(big.Int).Neg(next)))
		return
	}
	value, overflow := uint256.FromBig(next)
	if overflow {
		s.setError(fmt.Errorf("account %s: token %d balance overflows 256 bits", addr.Hex(), tokenID))
		return
	}
	s.setTokenBalance(obj, tokenID, value)
	s.setTouched(obj)
}

// DeductValue is deduct_value (state.py:552): the debit happens only if the
// balance covers it, and the caller learns whether it did.
func (s *StateDB) DeductValue(addr account.Recipient, tokenID uint64, value *uint256.Int) bool {
	if s.GetBalance(addr, tokenID).Lt(value) {
		return false
	}
	s.DeltaTokenBalance(addr, tokenID, new(big.Int).Neg(value.ToBig()))
	return true
}

// TransferValue is transfer_value (state.py:544).
func (s *StateDB) TransferValue(from, to account.Recipient, tokenID uint64, value *uint256.Int) bool {
	if !s.DeductValue(from, tokenID, value) {
		return false
	}
	s.DeltaTokenBalance(to, tokenID, value.ToBig())
	return true
}

// ResetBalances drops every token balance.
//
// It deliberately restores nothing on revert, and deliberately leaves the
// account untouched. pyquarkchain journals the restore onto a misspelled
// attribute (state.py:195 writes _balance, not _balances), so reverting brings
// back only the token trie — which this implementation does not have — and the
// balances stay dropped. The golden case revert_does_not_restore_reset_balances
// pins that behavior against pyquarkchain itself.
func (s *StateDB) ResetBalances(addr account.Recipient) {
	s.getStateObject(addr).balances = qkcCommon.NewEmptyTokenBalances()
}

func (s *StateDB) GetState(addr account.Recipient, key common.Hash) common.Hash {
	return s.getStateObject(addr).getState(key)
}

func (s *StateDB) SetState(addr account.Recipient, key, value common.Hash) {
	obj := s.getStateObject(addr)
	s.append(storageChange{addr: addr, key: key, prev: obj.getState(key)})
	obj.storage[key] = value
	s.setTouched(obj)
}

// ResetStorage empties the storage trie (state.py:613). Like ResetBalances it
// does not mark the account touched; whatever else deleted it must.
func (s *StateDB) ResetStorage(addr account.Recipient) {
	obj := s.getStateObject(addr)
	s.append(storageResetChange{addr: addr, prev: obj.storage, prevRoot: obj.storageRoot})
	obj.storage = make(map[common.Hash]common.Hash)
	obj.setStorageRoot(coretypes.EmptyRootHash)
}

// DelAccount is del_account (state.py:596), six steps whose order and final
// flags decide what commit does: the trailing touched=false with deleted=true is
// what turns the account into a trie deletion instead of a blank leaf.
func (s *StateDB) DelAccount(addr account.Recipient) {
	s.ResetBalances(addr)
	s.SetNonce(addr, 0)
	s.SetCode(addr, nil)
	s.ResetStorage(addr)

	obj := s.getStateObject(addr)
	s.append(deletedChange{addr: addr, prev: obj.deleted})
	obj.deleted = true
	s.append(touchChange{addr: addr, prev: obj.touched})
	obj.touched = false
}

// Gas and fee counters. gas_used is the block's running total across both the
// cross-shard and the in-shard halves; xshard_receive_gas_used tracks only what
// the cross-shard half consumed, and both are compared against the block.

func (s *StateDB) GasUsed() uint64              { return s.ctx.gasUsed }
func (s *StateDB) SetGasUsed(gas uint64)        { s.ctx.gasUsed = gas }
func (s *StateDB) AddGasUsed(gas uint64)        { s.ctx.gasUsed += gas }
func (s *StateDB) XShardReceiveGasUsed() uint64 { return s.ctx.xShardReceiveGasUsed }
func (s *StateDB) SetXShardReceiveGasUsed(gas uint64) {
	s.ctx.xShardReceiveGasUsed = gas
}

func (s *StateDB) Refunds() uint64      { return s.ctx.refunds }
func (s *StateDB) AddRefund(gas uint64) { s.ctx.refunds += gas }
func (s *StateDB) ResetRefunds()        { s.ctx.refunds = 0 }

// BlockFeeTokens accumulates the fees credited to the coinbase, which the block
// compares as part of its coinbase amount map.
func (s *StateDB) BlockFeeTokens() map[uint64]*uint256.Int { return s.ctx.blockFeeTokens }

func (s *StateDB) AddBlockFee(tokenID uint64, amount *uint256.Int) {
	total := s.ctx.blockFeeTokens[tokenID]
	if total == nil {
		total = new(uint256.Int)
	}
	s.ctx.blockFeeTokens[tokenID] = new(uint256.Int).Add(total, amount)
}

// Suicides, logs and the two receipt lists are per-transaction state the caller
// clears between transactions, as apply_transaction does (messages.py:419-421).

func (s *StateDB) Suicides() []account.Recipient { return s.ctx.suicides }
func (s *StateDB) AddSuicide(addr account.Recipient) {
	s.ctx.suicides = append(s.ctx.suicides, addr)
}
func (s *StateDB) ResetSuicides() { s.ctx.suicides = nil }

func (s *StateDB) Logs() []*coretypes.Log    { return s.ctx.logs }
func (s *StateDB) AddLog(log *coretypes.Log) { s.ctx.logs = append(s.ctx.logs, log) }
func (s *StateDB) ResetLogs()                { s.ctx.logs = nil }

// Receipts and XShardDepositReceipts are kept apart because the block's receipt
// root is the two lists concatenated, in that order (shard_state.py:957).
func (s *StateDB) Receipts() []*types.Receipt { return s.ctx.receipts }
func (s *StateDB) AddReceipt(r *types.Receipt) {
	s.ctx.receipts = append(s.ctx.receipts, r)
}

func (s *StateDB) XShardDepositReceipts() []*types.Receipt { return s.ctx.xShardDepositReceipts }
func (s *StateDB) AddXShardDepositReceipt(r *types.Receipt) {
	s.ctx.xShardDepositReceipts = append(s.ctx.xShardDepositReceipts, r)
}

// XShardList collects the deposits this block sends to other shards.
func (s *StateDB) XShardList() []*types.CrossShardTransactionDeposit {
	return s.ctx.xShardList
}

func (s *StateDB) AddXShardDeposit(d *types.CrossShardTransactionDeposit) {
	s.ctx.xShardList = append(s.ctx.xShardList, d)
}

// Snapshot records a point to revert to.
func (s *StateDB) Snapshot() int {
	s.revisions = append(s.revisions, revision{
		journalIndex: len(s.journal),
		root:         s.root,
		ctx:          s.ctx.clone(),
	})
	return len(s.revisions) - 1
}

// RevertToSnapshot unwinds the journal and restores the execution context,
// as State.revert (state.py:520).
func (s *StateDB) RevertToSnapshot(id int) {
	if id < 0 || id >= len(s.revisions) {
		s.setError(fmt.Errorf("revert to snapshot %d out of %d", id, len(s.revisions)))
		return
	}
	rev := s.revisions[id]
	if rev.root != s.root {
		// pyquarkchain allows this only with an empty journal, where reverting is
		// just moving the trie back to an earlier root. Committing inside a
		// snapshot is not something this layer's callers do.
		s.setError(fmt.Errorf("cannot revert across a commit: snapshot root %s, current %s", rev.root, s.root))
		return
	}
	for i := len(s.journal) - 1; i >= rev.journalIndex; i-- {
		s.journal[i].revert(s)
	}
	s.journal = s.journal[:rev.journalIndex]
	s.ctx = rev.ctx
	s.revisions = s.revisions[:id]
}

// Commit writes every touched or deleted account into the trie and persists the
// result, returning the new state root. It is State.commit (state.py:562) plus
// the flush pyethereum's trie did on every update.
func (s *StateDB) Commit() (common.Hash, error) {
	if s.dbErr != nil {
		return common.Hash{}, s.dbErr
	}
	addrs := make([]account.Recipient, 0, len(s.stateObjects))
	for addr := range s.stateObjects {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i].Cmp(addrs[j]) < 0 })

	nodes := trienode.NewMergedNodeSet()
	var storageRoots []common.Hash
	batch := s.db.NewBatch()

	for _, addr := range addrs {
		obj := s.stateObjects[addr]
		if !obj.touched && !obj.deleted {
			continue
		}
		set, err := obj.commitStorage()
		if err != nil {
			return common.Hash{}, err
		}
		if set != nil {
			if err := nodes.Merge(set); err != nil {
				return common.Hash{}, err
			}
			if obj.storageRoot != coretypes.EmptyRootHash {
				storageRoots = append(storageRoots, obj.storageRoot)
			}
		}
		if obj.dirtyCode {
			rawdb.WriteCode(batch, obj.codeHash, obj.code)
			obj.dirtyCode = false
		}
		if obj.empty() {
			if err := s.trie.Delete(hashKey(addr.Bytes())); err != nil {
				return common.Hash{}, fmt.Errorf("account %s: delete leaf: %w", addr.Hex(), err)
			}
			continue
		}
		blob, err := obj.encode()
		if err != nil {
			return common.Hash{}, err
		}
		if err := s.trie.Update(hashKey(addr.Bytes()), blob); err != nil {
			return common.Hash{}, fmt.Errorf("account %s: write leaf: %w", addr.Hex(), err)
		}
	}

	// A read that failed above left the objects incomplete, and hashing them
	// would produce a root for a state that was never read.
	if s.dbErr != nil {
		return common.Hash{}, s.dbErr
	}

	root, set := s.trie.Commit(false)
	if root != s.root {
		if set != nil {
			if err := nodes.Merge(set); err != nil {
				return common.Hash{}, err
			}
		}
		if err := s.triedb.Update(root, s.root, 0, nodes, nil); err != nil {
			return common.Hash{}, fmt.Errorf("update trie nodes: %w", err)
		}
		// Storage roots are only reachable through references hashdb cannot build
		// from QuarkChain leaves, so each is flushed as a root of its own, before
		// the account trie that names it.
		for _, storageRoot := range storageRoots {
			if err := s.triedb.Commit(storageRoot, false); err != nil {
				return common.Hash{}, fmt.Errorf("commit storage %s: %w", storageRoot, err)
			}
		}
		if err := s.triedb.Commit(root, false); err != nil {
			return common.Hash{}, fmt.Errorf("commit state %s: %w", root, err)
		}
	}
	if err := batch.Write(); err != nil {
		return common.Hash{}, fmt.Errorf("write code: %w", err)
	}

	tr, err := trie.New(trie.StateTrieID(root), s.triedb)
	if err != nil {
		return common.Hash{}, fmt.Errorf("reopen state trie %s: %w", root, err)
	}
	s.trie, s.root = tr, root
	s.stateObjects = make(map[account.Recipient]*stateObject)
	s.journal, s.revisions = nil, nil
	return root, nil
}
