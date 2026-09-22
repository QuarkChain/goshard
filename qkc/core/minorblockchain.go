// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/internal/syncx"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/triedb"
)

// MinorBlockChain owns local minor block storage and execution state. Root-chain
// policy and canonical-head selection remain the responsibility of
// ShardCoordinator.
type MinorBlockChain struct {
	shardConfig *config.ShardConfig
	vmConfig    vm.Config

	db     ethdb.Database
	triedb *triedb.Database
	codedb *state.CodeDB

	processor Processor
	validator MinorBlockValidator

	chainmu *syncx.ClosableMutex
	mu      sync.RWMutex
	// mu protects the in-memory head. Database readers don't need it.

	genesisBlock *types.MinorBlock
	current      *types.MinorBlock
	stopOnce     sync.Once
}

// NewMinorBlockChain opens a local minor chain whose genesis block and state
// have already been materialized in db.
func NewMinorBlockChain(db ethdb.Database, shardConfig *config.ShardConfig, processor Processor, validator MinorBlockValidator, vmConfig vm.Config) (*MinorBlockChain, error) {
	if db == nil {
		return nil, ErrDatabaseUnavailable
	}
	if shardConfig == nil {
		return nil, ErrShardConfigUnavailable
	}
	if processor == nil {
		return nil, ErrExecutorUnavailable
	}
	if validator == nil {
		return nil, ErrValidatorUnavailable
	}
	tdb := triedb.NewDatabase(db, triedb.HashDefaults)
	chain := &MinorBlockChain{
		shardConfig: shardConfig,
		vmConfig:    vmConfig,
		db:          db,
		triedb:      tdb,
		codedb:      state.NewCodeDB(db),
		processor:   processor,
		validator:   validator,
		chainmu:     syncx.NewClosableMutex(),
	}
	genesis := chain.GetBlockByNumber(0)
	if genesis == nil {
		_ = tdb.Close()
		return nil, ErrNoGenesis
	}
	chain.genesisBlock = genesis
	if rawdb.ReadHeadBlockHash(db) == (common.Hash{}) {
		if !chain.HasState(genesis.Root()) {
			_ = tdb.Close()
			return nil, fmt.Errorf("genesis root %s: %w", genesis.Root(), ErrStateUnavailable)
		}
		if err := chain.writeHeadBlock(genesis); err != nil {
			_ = tdb.Close()
			return nil, err
		}
	}
	if err := chain.loadLastState(); err != nil {
		_ = tdb.Close()
		return nil, err
	}
	return chain, nil
}

// writeHeadBlock adds block to the canonical number scheme and updates the
// persistent minor-chain head markers in an owned batch.
func (c *MinorBlockChain) writeHeadBlock(block *types.MinorBlock) error {
	batch := c.db.NewBatch()
	defer batch.Close()
	rawdb.WriteMinorCanonicalHash(batch, block.Hash(), block.NumberU64())
	rawdb.WriteHeadHeaderHash(batch, block.Hash())
	rawdb.WriteHeadBlockHash(batch, block.Hash())
	if err := batch.Write(); err != nil {
		return fmt.Errorf("write minor genesis: %w", err)
	}
	return nil
}

// loadLastState restores the persisted local minor head and verifies that its
// execution state is available before exposing the chain to callers.
func (c *MinorBlockChain) loadLastState() error {
	headHash := rawdb.ReadHeadBlockHash(c.db)
	if headHash == (common.Hash{}) {
		return ErrNoCurrentBlock
	}
	head := rawdb.ReadMinorBlock(c.db, headHash)
	if head == nil {
		return fmt.Errorf("restore minor head %s: %w", headHash, ErrNoCurrentBlock)
	}
	if !c.HasState(head.Root()) {
		return fmt.Errorf("restore minor head %s root %s: %w", headHash, head.Root(), ErrStateUnavailable)
	}
	c.current = head
	return nil
}

// EthChainID returns the EVM chain ID assigned to this shard.
func (c *MinorBlockChain) EthChainID() uint32 {
	return c.shardConfig.EthChainID
}

// GenesisHash returns the configured local minor genesis hash.
func (c *MinorBlockChain) GenesisHash() common.Hash {
	return c.genesisBlock.Hash()
}

// Head returns the current canonical minor block number and hash.
func (c *MinorBlockChain) Head() (uint64, common.Hash) {
	current := c.CurrentBlock()
	if current == nil {
		return 0, common.Hash{}
	}
	return current.NumberU64(), current.Hash()
}

func (c *MinorBlockChain) GetBlock(hash common.Hash) *types.MinorBlock {
	return rawdb.ReadMinorBlock(c.db, hash)
}

func (c *MinorBlockChain) GetBlockByNumber(number uint64) *types.MinorBlock {
	hash := rawdb.ReadMinorCanonicalHash(c.db, number)
	if hash == (common.Hash{}) {
		return nil
	}
	return c.GetBlock(hash)
}

func (c *MinorBlockChain) HasBlock(hash common.Hash) bool {
	return rawdb.HasMinorBlock(c.db, hash)
}

func (c *MinorBlockChain) HasBlockAndState(hash common.Hash) bool {
	block := c.GetBlock(hash)
	return block != nil && c.HasState(block.Root())
}

func (c *MinorBlockChain) CurrentBlock() *types.MinorBlock {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.current
}

func (c *MinorBlockChain) HasState(root common.Hash) bool {
	_, err := c.triedb.NodeReader(root)
	return err == nil
}

func (c *MinorBlockChain) StateAt(root common.Hash) (*state.StateDB, error) {
	return state.New(root, state.NewMPTDatabase(c.triedb, c.codedb))
}

// SetCanonicalHead applies the local canonical indexes for a candidate chosen
// by ShardCoordinator. Root-chain fork choice remains outside MinorBlockChain.
func (c *MinorBlockChain) SetCanonicalHead(hash common.Hash) error {
	if !c.chainmu.TryLock() {
		return ErrChainStopped
	}
	defer c.chainmu.Unlock()
	target := c.GetBlock(hash)
	if target == nil {
		return ErrUnknownBlock
	}
	return c.setHead(target)
}

// setHead assumes chainmu is held.
func (c *MinorBlockChain) setHead(target *types.MinorBlock) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if target == nil {
		return ErrUnknownBlock
	}
	if !c.HasState(target.Root()) {
		return ErrStateUnavailable
	}
	current := c.current
	if current == nil {
		return ErrNoCurrentBlock
	}
	type canonicalBlock struct {
		hash   common.Hash
		number uint64
	}
	newCanonicalBlocks := make([]canonicalBlock, 0)
	oldHead := current
	newHead := target
	for oldHead.NumberU64() > newHead.NumberU64() {
		var err error
		oldHead, err = c.parentBlock(oldHead)
		if err != nil {
			return err
		}
	}
	for newHead.NumberU64() > oldHead.NumberU64() {
		newCanonicalBlocks = append(newCanonicalBlocks, canonicalBlock{hash: newHead.Hash(), number: newHead.NumberU64()})
		var err error
		newHead, err = c.parentBlock(newHead)
		if err != nil {
			return err
		}
	}
	for oldHead.Hash() != newHead.Hash() {
		newCanonicalBlocks = append(newCanonicalBlocks, canonicalBlock{hash: newHead.Hash(), number: newHead.NumberU64()})
		var err error
		oldHead, err = c.parentBlock(oldHead)
		if err != nil {
			return err
		}
		newHead, err = c.parentBlock(newHead)
		if err != nil {
			return err
		}
	}
	batch := c.db.NewBatch()
	defer batch.Close()
	for number := current.NumberU64(); number > target.NumberU64(); number-- {
		rawdb.DeleteMinorCanonicalHash(batch, number)
	}
	for index := len(newCanonicalBlocks) - 1; index >= 0; index-- {
		block := newCanonicalBlocks[index]
		rawdb.WriteMinorCanonicalHash(batch, block.hash, block.number)
	}
	rawdb.WriteHeadHeaderHash(batch, target.Hash())
	rawdb.WriteHeadBlockHash(batch, target.Hash())
	if err := batch.Write(); err != nil {
		return fmt.Errorf("write canonical minor head: %w", err)
	}
	c.current = target
	return nil
}

func (c *MinorBlockChain) parentBlock(block *types.MinorBlock) (*types.MinorBlock, error) {
	if block.NumberU64() == 0 {
		return nil, fmt.Errorf("minor block %s at height 0 has no valid parent: %w", block.Hash(), ErrNonContiguousBlock)
	}
	parent := c.GetBlock(block.ParentHash())
	if parent == nil {
		return nil, fmt.Errorf("minor block %s parent %s: %w", block.Hash(), block.ParentHash(), ErrUnknownParent)
	}
	if parent.NumberU64() != block.NumberU64()-1 {
		return nil, fmt.Errorf("minor block %s at height %d has parent %s at height %d: %w",
			block.Hash(), block.NumberU64(), parent.Hash(), parent.NumberU64(), ErrNonContiguousBlock)
	}
	return parent, nil
}

// Stop closes the chain's state backend. The caller retains ownership of db.
func (c *MinorBlockChain) Stop() {
	c.stopOnce.Do(func() {
		c.chainmu.Close()
		_ = c.triedb.Close()
	})
}
