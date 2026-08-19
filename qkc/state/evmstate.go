// Copyright 2026-2027, QuarkChain.

// Package state is the mutable QuarkChain state: the Go counterpart of
// pyquarkchain's quarkchain/evm/state.py.
//
// The account half of that file — leaf encoding, the balance table, existence
// and deletion, the journal — lives in geth's core/state, which this fork has
// taught QuarkChain's rules (see core/state/statedb_qkc.go). What is left here
// is the execution half: the counters, lists and receipts that pyquarkchain
// keeps on State outside the trie (STATE_DEFAULTS, state.py:45) and that a
// snapshot has to capture along with the account changes.
//
// # Driving rule
//
// A block must Commit exactly once, at its end, and must not Finalise in
// between. QuarkChain decides which accounts exist once, over everything the
// block touched (state.py:562); geth decides in Finalise, over what the journal
// has dirtied since the last call. Finalising per transaction empties that set,
// and an account emptied early in a block and never touched again would survive
// in the trie where pyquarkchain drops it.
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
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// NewDatabase opens the trie database goshard runs on.
func NewDatabase(db ethdb.Database) *triedb.Database {
	return triedb.NewDatabase(db, triedb.HashDefaults)
}

// executionContext is what pyquarkchain keeps on State outside the trie
// (STATE_DEFAULTS, state.py:45). A snapshot copies it wholesale and a revert
// puts it back, so everything mutable during one transaction belongs here.
type executionContext struct {
	gasUsed              uint64
	xShardReceiveGasUsed uint64

	receipts              []*types.Receipt
	xShardDepositReceipts []*types.Receipt
	xShardList            []*types.CrossShardTransactionDeposit
	blockFeeTokens        map[uint64]*uint256.Int

	// The block-level parameters are in STATE_DEFAULTS too, so a revert puts
	// them back along with the counters. Only full_shard_key actually moves
	// during a block — it is restamped before every transaction and every
	// deposit — but restoring the rest costs nothing and keeps the set honest.
	gasLimit        uint64
	blockNumber     uint64
	blockCoinbase   account.Recipient
	blockDifficulty *big.Int
	timestamp       uint64
	fullShardKey    uint32
	xShardTxCursor  *types.XShardTxCursorInfo
	prevHeaders     []*types.MinorBlockHeader
}

func (c executionContext) clone() executionContext {
	out := c
	out.receipts = append([]*types.Receipt(nil), c.receipts...)
	out.xShardDepositReceipts = append([]*types.Receipt(nil), c.xShardDepositReceipts...)
	out.xShardList = append([]*types.CrossShardTransactionDeposit(nil), c.xShardList...)
	out.blockFeeTokens = make(map[uint64]*uint256.Int, len(c.blockFeeTokens))
	for token, value := range c.blockFeeTokens {
		out.blockFeeTokens[token] = new(uint256.Int).Set(value)
	}
	out.prevHeaders = append([]*types.MinorBlockHeader(nil), c.prevHeaders...)
	if c.blockDifficulty != nil {
		out.blockDifficulty = new(big.Int).Set(c.blockDifficulty)
	}
	if c.xShardTxCursor != nil {
		cursor := *c.xShardTxCursor
		out.xShardTxCursor = &cursor
	}
	return out
}

// EvmState is one shard's mutable state at one point in its chain.
//
// It embeds geth's StateDB and shadows the handful of methods whose QuarkChain
// meaning differs: balances are indexed by token id (GetBalance, SetBalance),
// nonce and code writes drop the tracing reason QuarkChain has no use for
// (SetNonce, SetCode), logs are a flat per-transaction list rather than a
// per-transaction-hash map (AddLog), and Snapshot/RevertToSnapshot/Commit have
// to cover the execution context as well as the accounts. Reach the geth
// spelling through the embedded field when one is genuinely wanted.
type EvmState struct {
	*state.StateDB

	db     ethdb.Database
	triedb *triedb.Database
	root   common.Hash

	ctx       executionContext
	revisions []revision

	// Outside STATE_DEFAULTS, and so outside the snapshot: the configuration a
	// State is built with (state.py:317) and the POSW map handed to it by the
	// shard (shard_state.py:597). None of it changes while a block runs.
	qkcConfig         *config.QuarkChainConfig
	shardConfig       *config.ShardConfig
	senderDisallowMap map[account.Recipient]*big.Int

	// Where the current message's logs start, see BeginMessage.
	messageHash     common.Hash
	messageLogsFrom int
}

type revision struct {
	stateID int
	ctx     executionContext
}

// New opens the state a root names.
func New(root common.Hash, db ethdb.Database, tdb *triedb.Database) (*EvmState, error) {
	sdb, err := state.New(root, state.NewDatabase(tdb, state.NewCodeDB(db)))
	if err != nil {
		return nil, fmt.Errorf("open state %s: %w", root, err)
	}
	return &EvmState{
		StateDB: sdb,
		db:      db,
		triedb:  tdb,
		root:    root,
		ctx: executionContext{
			blockFeeTokens:  make(map[uint64]*uint256.Int),
			gasLimit:        defaultGasLimit,
			blockDifficulty: big.NewInt(1),
		},
		senderDisallowMap: make(map[account.Recipient]*big.Int),
	}, nil
}

// defaultGasLimit is STATE_DEFAULTS["gas_limit"] (state.py:47). Every real block
// overwrites it; it only matters for states built outside block execution.
const defaultGasLimit = 3141592

// SetConfig attaches the network and shard configuration the execution rules are
// read from. pyquarkchain passes both to the State constructor.
func (s *EvmState) SetConfig(qkcConfig *config.QuarkChainConfig, shardConfig *config.ShardConfig) {
	s.qkcConfig, s.shardConfig = qkcConfig, shardConfig
}

func (s *EvmState) QKCConfig() *config.QuarkChainConfig { return s.qkcConfig }
func (s *EvmState) ShardConfig() *config.ShardConfig    { return s.shardConfig }

// GenesisTokenID is state.genesis_token: the network's genesis token, which is
// the only token gas is ever denominated in.
func (s *EvmState) GenesisTokenID() uint64 {
	if s.qkcConfig == nil {
		return qkcCommon.DefaultTokenID
	}
	return s.qkcConfig.GetDefaultChainTokenID()
}

// DefaultChainTokenID is shard_config.default_chain_token, the token a balance
// query without an explicit id means. It is per chain, so it is not necessarily
// the genesis token.
func (s *EvmState) DefaultChainTokenID() uint64 {
	if s.shardConfig == nil || s.shardConfig.DefaultChainToken == "" {
		return s.GenesisTokenID()
	}
	id, err := qkcCommon.TokenIDEncodeChecked(s.shardConfig.DefaultChainToken)
	if err != nil {
		s.SetError(fmt.Errorf("shard default chain token %q: %w", s.shardConfig.DefaultChainToken, err))
		return s.GenesisTokenID()
	}
	return id
}

// LocalFeeRate is 1 - reward_tax_rate: the share of a transaction fee that stays
// with the shard rather than going to the root chain.
func (s *EvmState) LocalFeeRate() *big.Rat {
	if s.qkcConfig == nil || s.qkcConfig.LocalFeeRate == nil {
		return big.NewRat(1, 1)
	}
	return s.qkcConfig.LocalFeeRate
}

// Root is the root the state was opened at, or last committed to.
func (s *EvmState) Root() common.Hash { return s.root }

// Block parameters. pyquarkchain sets these on the State before running a block
// (shard_state.py:802-812) and reads them back through VMExt.

func (s *EvmState) GasLimit() uint64          { return s.ctx.gasLimit }
func (s *EvmState) SetGasLimit(limit uint64)  { s.ctx.gasLimit = limit }
func (s *EvmState) BlockNumber() uint64       { return s.ctx.blockNumber }
func (s *EvmState) SetBlockNumber(num uint64) { s.ctx.blockNumber = num }
func (s *EvmState) Timestamp() uint64         { return s.ctx.timestamp }
func (s *EvmState) SetTimestamp(ts uint64)    { s.ctx.timestamp = ts }

func (s *EvmState) BlockCoinbase() account.Recipient { return s.ctx.blockCoinbase }
func (s *EvmState) SetBlockCoinbase(addr account.Recipient) {
	s.ctx.blockCoinbase = addr
}

func (s *EvmState) BlockDifficulty() *big.Int { return s.ctx.blockDifficulty }
func (s *EvmState) SetBlockDifficulty(d *big.Int) {
	s.ctx.blockDifficulty = new(big.Int).Set(d)
}

// SenderDisallowMap is the POSW map: how much of each recent block producer's
// balance is locked against being spent (shard_state.py:1988). An empty map
// means POSW is off for this shard, which is not the same as an absent one —
// the check reads it either way.
func (s *EvmState) SenderDisallowMap() map[account.Recipient]*big.Int {
	return s.senderDisallowMap
}

func (s *EvmState) SetSenderDisallowMap(m map[account.Recipient]*big.Int) {
	if m == nil {
		m = make(map[account.Recipient]*big.Int)
	}
	s.senderDisallowMap = m
}

// XShardTxCursor is the position the block's cross-shard traversal stopped at.
// It is part of the block's meta and so part of consensus.
func (s *EvmState) XShardTxCursor() *types.XShardTxCursorInfo { return s.ctx.xShardTxCursor }
func (s *EvmState) SetXShardTxCursor(info *types.XShardTxCursorInfo) {
	s.ctx.xShardTxCursor = info
}

// AddBlockHeader pushes a header onto the BLOCKHASH window, which pyquarkchain
// caps at 256 (state.py:352).
func (s *EvmState) AddBlockHeader(header *types.MinorBlockHeader) {
	s.ctx.prevHeaders = append([]*types.MinorBlockHeader{header}, s.ctx.prevHeaders...)
	if len(s.ctx.prevHeaders) > 256 {
		s.ctx.prevHeaders = s.ctx.prevHeaders[:256]
	}
}

func (s *EvmState) SetPrevHeaders(headers []*types.MinorBlockHeader) {
	if len(headers) > 256 {
		headers = headers[:256]
	}
	s.ctx.prevHeaders = append([]*types.MinorBlockHeader(nil), headers...)
}

// GetBlockHash is get_block_hash (state.py:346): the n'th header back, or the
// zero hash once the window runs out. BLOCKHASH passes block_number - n - 1.
func (s *EvmState) GetBlockHash(n uint64) common.Hash {
	if n >= uint64(len(s.ctx.prevHeaders)) || n >= 256 {
		return common.Hash{}
	}
	return s.ctx.prevHeaders[n].Hash()
}

// Bloom is the block's bloom: the union over both receipt lists (state.py:325).
func (s *EvmState) Bloom() types.Bloom {
	var out types.Bloom
	or := func(receipts []*types.Receipt) {
		for _, r := range receipts {
			for i := range out {
				out[i] |= r.Bloom[i]
			}
		}
	}
	or(s.ctx.receipts)
	or(s.ctx.xShardDepositReceipts)
	return out
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

// GetFullShardKey is the shard key frozen into the account when it was created.
func (s *EvmState) GetFullShardKey(addr account.Recipient) uint32 {
	return s.StateDB.GetFullShardKey(addr)
}

// SetFullShardKey restamps the key new accounts are created with. It is part of
// the snapshotted context, so it is mirrored here as well as on the StateDB.
func (s *EvmState) SetFullShardKey(key uint32) {
	s.ctx.fullShardKey = key
	s.StateDB.SetFullShardKey(key)
}

// FullShardKey is the key accounts created from now on will carry — the
// destination shard key of the transaction being applied.
func (s *EvmState) FullShardKey() uint32 { return s.ctx.fullShardKey }

func (s *EvmState) GetBalance(addr account.Recipient, tokenID uint64) *uint256.Int {
	return s.GetBalanceByTokenID(addr, tokenID)
}

// GetBalances returns every token the account holds, for callers that report
// state rather than execute against it.
func (s *EvmState) GetBalances(addr account.Recipient) map[uint64]*uint256.Int {
	balances := s.GetMntBalances(addr)
	if qkc := s.StateDB.GetBalance(addr); !qkc.IsZero() {
		balances[qkcCommon.DefaultTokenID] = new(uint256.Int).Set(qkc)
	}
	return balances
}

// SetTokenBalance mirrors set_token_balance (state.py:443), including its early
// return: writing the balance an account already holds changes nothing but
// still marks the account, so a blank account written this way is created and
// then dropped again at commit.
func (s *EvmState) SetTokenBalance(addr account.Recipient, tokenID uint64, value *uint256.Int) {
	if s.GetBalance(addr, tokenID).Eq(value) {
		s.touch(addr)
		return
	}
	s.SetBalanceByTokenID(addr, tokenID, value, tracing.BalanceChangeUnspecified)
}

// DeltaTokenBalance adds a signed amount, as delta_token_balance (state.py:461).
// A zero delta only marks the account — it does not create an entry, which is
// what keeps a zero-value transfer from changing the recipient's leaf.
//
// pyquarkchain has no underflow check here; its callers go through DeductValue
// or TransferValue, which test the balance first. A negative result is a caller
// bug, and is recorded rather than wrapped around.
func (s *EvmState) DeltaTokenBalance(addr account.Recipient, tokenID uint64, delta *big.Int) {
	if delta.Sign() == 0 {
		s.touch(addr)
		return
	}
	if delta.Sign() > 0 {
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

// DeductValue is deduct_value (state.py:552): the debit happens only if the
// balance covers it, and the caller learns whether it did.
func (s *EvmState) DeductValue(addr account.Recipient, tokenID uint64, value *uint256.Int) bool {
	if s.GetBalance(addr, tokenID).Lt(value) {
		return false
	}
	s.SubBalanceByTokenID(addr, value, tokenID, tracing.BalanceChangeUnspecified)
	return true
}

// TransferValue is transfer_value (state.py:544).
func (s *EvmState) TransferValue(from, to account.Recipient, tokenID uint64, value *uint256.Int) bool {
	if !s.DeductValue(from, tokenID, value) {
		return false
	}
	s.AddBalanceByTokenID(to, value, tokenID, tracing.BalanceChangeUnspecified)
	return true
}

// touch marks an account without changing it, as the set_and_journal(acct,
// "touched", True) that trails nearly every mutation upstream does.
func (s *EvmState) touch(addr account.Recipient) {
	s.AddBalanceByTokenID(addr, new(uint256.Int), qkcCommon.DefaultTokenID, tracing.BalanceChangeUnspecified)
}

// Gas and fee counters. gas_used is the block's running total across both the
// cross-shard and the in-shard halves; xshard_receive_gas_used tracks only what
// the cross-shard half consumed, and both are compared against the block.

func (s *EvmState) GasUsed() uint64       { return s.ctx.gasUsed }
func (s *EvmState) SetGasUsed(gas uint64) { s.ctx.gasUsed = gas }
func (s *EvmState) AddGasUsed(gas uint64) { s.ctx.gasUsed += gas }

// SubGasUsed gives gas back to the block. Only the cross-shard source path uses
// it, to hand the deposit surcharge to the target shard (messages.py:571).
func (s *EvmState) SubGasUsed(gas uint64) {
	if gas > s.ctx.gasUsed {
		s.SetError(fmt.Errorf("gas used %d cannot give back %d", s.ctx.gasUsed, gas))
		return
	}
	s.ctx.gasUsed -= gas
}
func (s *EvmState) XShardReceiveGasUsed() uint64 { return s.ctx.xShardReceiveGasUsed }
func (s *EvmState) SetXShardReceiveGasUsed(gas uint64) {
	s.ctx.xShardReceiveGasUsed = gas
}

// Refunds is state.refunds. It lives on the embedded StateDB rather than in the
// execution context because that is the counter the interpreter increments from
// SSTORE and SELFDESTRUCT, and it is journalled there — so a reverted frame
// gives its refunds back without any bookkeeping here.
func (s *EvmState) Refunds() uint64      { return s.StateDB.GetRefund() }
func (s *EvmState) AddRefund(gas uint64) { s.StateDB.AddRefund(gas) }
func (s *EvmState) ResetRefunds()        { s.StateDB.SubRefund(s.StateDB.GetRefund()) }

// BlockFeeTokens accumulates the fees credited to the coinbase, which the block
// compares as part of its coinbase amount map.
func (s *EvmState) BlockFeeTokens() map[uint64]*uint256.Int { return s.ctx.blockFeeTokens }

func (s *EvmState) AddBlockFee(tokenID uint64, amount *uint256.Int) {
	total := s.ctx.blockFeeTokens[tokenID]
	if total == nil {
		total = new(uint256.Int)
	}
	s.ctx.blockFeeTokens[tokenID] = new(uint256.Int).Add(total, amount)
}

// The two receipt lists are per-transaction state the caller clears between
// transactions, as apply_transaction does (messages.py:419-421). The suicide
// list is not here: it lives on the EVM's QuarkChain context, where the
// interpreter marks it and a reverted frame drops it.

// BeginMessage marks the start of one transaction or deposit: the logs it emits
// are attributed to txHash, and MessageLogs will return exactly those.
//
// The logs themselves live on the embedded StateDB, where the interpreter
// writes them and where the journal already knows how to unwind them, rather
// than in the execution context. state.logs = [] at the head of
// apply_transaction (messages.py:417) is this watermark.
func (s *EvmState) BeginMessage(txHash common.Hash, index int) {
	s.StateDB.SetTxContext(txHash, index)
	s.messageHash = txHash
	s.messageLogsFrom = len(s.StateDB.GetLogs(txHash, 0, common.Hash{}, 0))
}

// MessageLogs returns the logs emitted since BeginMessage.
func (s *EvmState) MessageLogs() []*coretypes.Log {
	all := s.StateDB.GetLogs(s.messageHash, s.ctx.blockNumber, common.Hash{}, s.ctx.timestamp)
	if s.messageLogsFrom >= len(all) {
		return nil
	}
	return all[s.messageLogsFrom:]
}

// Receipts and XShardDepositReceipts are kept apart because the block's receipt
// root is the two lists concatenated, in that order (shard_state.py:957).
func (s *EvmState) Receipts() []*types.Receipt { return s.ctx.receipts }
func (s *EvmState) AddReceipt(r *types.Receipt) {
	s.ctx.receipts = append(s.ctx.receipts, r)
}

func (s *EvmState) XShardDepositReceipts() []*types.Receipt { return s.ctx.xShardDepositReceipts }
func (s *EvmState) AddXShardDepositReceipt(r *types.Receipt) {
	s.ctx.xShardDepositReceipts = append(s.ctx.xShardDepositReceipts, r)
}

// XShardList collects the deposits this block sends to other shards.
func (s *EvmState) XShardList() []*types.CrossShardTransactionDeposit {
	return s.ctx.xShardList
}

func (s *EvmState) AddXShardDeposit(d *types.CrossShardTransactionDeposit) {
	s.ctx.xShardList = append(s.ctx.xShardList, d)
}

// Snapshot records a point to revert to, covering both the accounts and the
// execution context — State.snapshot copies the whole of STATE_DEFAULTS
// (state.py:508), not just the journal position.
func (s *EvmState) Snapshot() int {
	s.revisions = append(s.revisions, revision{
		stateID: s.StateDB.Snapshot(),
		ctx:     s.ctx.clone(),
	})
	return len(s.revisions) - 1
}

// RevertToSnapshot unwinds both halves, as State.revert (state.py:520).
func (s *EvmState) RevertToSnapshot(id int) {
	if id < 0 || id >= len(s.revisions) {
		s.SetError(fmt.Errorf("revert to snapshot %d out of %d", id, len(s.revisions)))
		return
	}
	rev := s.revisions[id]
	s.StateDB.RevertToSnapshot(rev.stateID)
	s.ctx = rev.ctx
	s.StateDB.SetFullShardKey(s.ctx.fullShardKey)
	s.revisions = s.revisions[:id]
}

// Commit writes the block's accounts and returns the new state root. It is
// State.commit (state.py:562), and must be called once per block — see the
// package comment.
func (s *EvmState) Commit(block uint64) (common.Hash, error) {
	if err := s.Error(); err != nil {
		return common.Hash{}, err
	}
	root, err := s.StateDB.Commit(block, true, false)
	if err != nil {
		return common.Hash{}, err
	}
	// Push the nodes out of hashdb's dirty cache and onto disk. geth leaves that
	// to the blockchain layer, which keeps recent blocks in memory and flushes on
	// its own schedule; pyquarkchain's trie writes through on every update, and a
	// shard that stops here has to be able to reopen the root it just announced.
	if err := s.triedb.Commit(root, false); err != nil {
		return common.Hash{}, fmt.Errorf("flush state %s: %w", root, err)
	}
	// pyquarkchain drops its account cache here (state.py:587). Reopening gives
	// the same guarantee without depending on how much of geth's caching
	// survives a commit.
	sdb, err := state.New(root, state.NewDatabase(s.triedb, state.NewCodeDB(s.db)))
	if err != nil {
		return common.Hash{}, fmt.Errorf("reopen state %s: %w", root, err)
	}
	s.StateDB, s.root = sdb, root
	s.StateDB.SetFullShardKey(s.ctx.fullShardKey)
	s.revisions = nil
	return root, nil
}
