// Copyright 2026-2027, QuarkChain.

// Package state is the mutable QuarkChain account state: the Go counterpart of
// the account half of pyquarkchain's quarkchain/evm/state.py.
//
// The rules themselves — leaf encoding, the balance table, existence and
// deletion, the journal — live in geth's core/state, which this fork has taught
// QuarkChain's rules (see core/state/statedb_qkc.go). This package adds the
// token-indexed spelling of the account API and the commit that closes a block.
//
// Of the per-block values pyquarkchain also keeps on State (STATE_DEFAULTS,
// state.py:45), only logs, refunds and suicides change between a snapshot and
// its revert, and geth's journal reverts all three. The rest — block
// parameters, gas counters, receipts, the cross-shard list, block fees — belong
// to the block executor, as they do in geth's StateProcessor.
//
// # Driving rule
//
// A block must Commit exactly once, at its end, and must not Finalise in
// between; core/state/statedb_qkc.go explains why.
package state

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// NewDatabase opens the state database goshard runs on: geth's hash-keyed trie
// database, with contract code in the same key-value store.
func NewDatabase(db ethdb.Database) state.Database {
	return state.NewDatabase(triedb.NewDatabase(db, triedb.HashDefaults), state.NewCodeDB(db))
}

// EvmState is one shard's mutable state at one point in its chain.
//
// It embeds geth's StateDB and shadows the handful of methods whose QuarkChain
// meaning differs: balances are indexed by token id (GetBalance), nonce and code
// writes drop the tracing reason QuarkChain has no use for (SetNonce, SetCode),
// and Commit reopens the state at the root it produced. Reach the geth spelling
// through the embedded field when one is genuinely wanted.
type EvmState struct {
	*state.StateDB
}

// New opens the state a root names. The states of one shard share db, as geth's
// blockchain shares one state database across blocks: a root committed through
// it can be reopened before anything is flushed to disk.
func New(root common.Hash, db state.Database) (*EvmState, error) {
	sdb, err := state.New(root, db)
	if err != nil {
		return nil, fmt.Errorf("open state %s: %w", root, err)
	}
	return &EvmState{StateDB: sdb}, nil
}

// Exists is account_exists (state.py:540): the negation of is_blank.
func (s *EvmState) Exists(addr account.Recipient) bool { return !s.Empty(addr) }

func (s *EvmState) SetNonce(addr account.Recipient, nonce uint64) {
	s.StateDB.SetNonce(addr, nonce, tracing.NonceChangeUnspecified)
}

func (s *EvmState) IncrementNonce(addr account.Recipient) {
	s.SetNonce(addr, s.GetNonce(addr)+1)
}

func (s *EvmState) SetCode(addr account.Recipient, code []byte) {
	s.StateDB.SetCode(addr, code, tracing.CodeChangeUnspecified)
}

// GetCodeHash reports the hash of the empty code for an account that does not
// exist, where geth reports the zero hash. pyquarkchain has no absent case to
// report: a miss caches a blank account, whose code_hash is keccak(b"").
func (s *EvmState) GetCodeHash(addr account.Recipient) common.Hash {
	if hash := s.StateDB.GetCodeHash(addr); hash != (common.Hash{}) {
		return hash
	}
	return coretypes.EmptyCodeHash
}

func (s *EvmState) GetBalance(addr account.Recipient, tokenID uint64) *uint256.Int {
	return s.GetBalanceByTokenID(addr, tokenID)
}

// GetBalances returns every token the account holds, for callers that report
// state rather than execute against it.
func (s *EvmState) GetBalances(addr account.Recipient) map[uint64]*uint256.Int {
	return s.GetTokenBalances(addr)
}

// SetTokenBalance is set_token_balance (state.py:443). Its early return —
// writing the balance an account already holds changes nothing but still marks
// the account — is in core/state, where the write itself is.
func (s *EvmState) SetTokenBalance(addr account.Recipient, tokenID uint64, value *uint256.Int) {
	s.SetBalanceByTokenID(addr, value, tokenID, tracing.BalanceChangeUnspecified)
}

// DeltaTokenBalance adds a signed amount, as delta_token_balance (state.py:461).
// A zero delta reaches the credit path, which marks the account without creating
// an entry — that is what keeps a zero-value transfer from changing the
// recipient's leaf.
//
// pyquarkchain has no underflow check here; its callers test the balance first.
// A negative result is a caller bug, and is recorded rather than wrapped around.
func (s *EvmState) DeltaTokenBalance(addr account.Recipient, tokenID uint64, delta *big.Int) {
	if delta.Sign() >= 0 {
		amount, overflow := uint256.FromBig(delta)
		if overflow {
			s.SetError(fmt.Errorf("account %s: token %d credit overflows 256 bits", addr.Hex(), tokenID))
			return
		}
		s.AddBalanceByTokenID(addr, amount, tokenID, tracing.BalanceChangeUnspecified)
		return
	}
	amount, overflow := uint256.FromBig(new(big.Int).Neg(delta))
	if overflow {
		s.SetError(fmt.Errorf("account %s: token %d debit overflows 256 bits", addr.Hex(), tokenID))
		return
	}
	if s.GetBalance(addr, tokenID).Lt(amount) {
		s.SetError(fmt.Errorf("account %s: token %d balance underflow", addr.Hex(), tokenID))
		return
	}
	s.SubBalanceByTokenID(addr, amount, tokenID, tracing.BalanceChangeUnspecified)
}

// Commit writes the block's accounts and returns the new state root. It is
// State.commit (state.py:562), and must be called once per block — see the
// package comment.
//
// The new trie nodes stay in the trie database's dirty cache. Flushing them is
// the caller's decision, as it is geth's blockchain's: pyquarkchain persists an
// applied block but not a block template (shard_state.py:1338).
func (s *EvmState) Commit(block uint64) (common.Hash, error) {
	if err := s.Error(); err != nil {
		return common.Hash{}, err
	}
	root, err := s.StateDB.Commit(block, true, false)
	if err != nil {
		return common.Hash{}, err
	}
	// pyquarkchain drops its account cache here (state.py:587), and geth's tries
	// stop working once committed. Reopening covers both without depending on how
	// much of geth's caching survives a commit — including that a balance entry
	// left at zero is gone once the leaf has been through the trie, since the pair
	// list carries no zeros.
	sdb, err := state.New(root, s.Database())
	if err != nil {
		return common.Hash{}, fmt.Errorf("reopen state %s: %w", root, err)
	}
	sdb.SetFullShardKey(s.FullShardKey())
	s.StateDB = sdb
	return root, nil
}
