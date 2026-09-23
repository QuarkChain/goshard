// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"sync"
	"sync/atomic"

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
	validator BlockValidator

	chainmu *syncx.ClosableMutex

	genesisBlock *types.MinorBlock
	currentBlock atomic.Pointer[types.MinorBlock]
	stopOnce     sync.Once
}

// NewMinorBlockChain opens a local minor chain whose genesis block and state
// have already been materialized in db.
func NewMinorBlockChain(db ethdb.Database, shardConfig *config.ShardConfig, processor Processor, validator BlockValidator, vmConfig vm.Config) (*MinorBlockChain, error) {
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
	c.currentBlock.Store(head)
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
	return c.currentBlock.Load()
}

func (c *MinorBlockChain) HasState(root common.Hash) bool {
	_, err := c.triedb.NodeReader(root)
	return err == nil
}

func (c *MinorBlockChain) StateAt(root common.Hash) (*state.StateDB, error) {
	return state.New(root, state.NewMPTDatabase(c.triedb, c.codedb))
}

// Stop closes the chain's state backend. The caller retains ownership of db.
func (c *MinorBlockChain) Stop() {
	c.stopOnce.Do(func() {
		c.chainmu.Close()
		_ = c.triedb.Close()
	})
}
