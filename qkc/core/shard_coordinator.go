// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// XShardBroadcast associates outgoing deposits with the minor block producing them.
type XShardBroadcast struct {
	Block    *types.MinorBlock
	Deposits []*types.CrossShardTransactionDeposit
}

// ShardCoordinator provides Milestone 1 root storage, cross-shard execution input
// and propagation. Every imported candidate is selected directly, without root
// consensus validation or root/minor fork choice.
type ShardCoordinator struct {
	shardConfig     *config.ShardConfig
	branch          account.Branch
	db              ethdb.Database
	minorBlockChain MinorChain
	connManager     ConnManager

	// Imports are serialized separately from lifecycle state. Inbound cross-shard
	// lists must remain writable while an import waits for network acknowledgments.
	importMu sync.Mutex
	mu       sync.RWMutex
	stopped  bool
	rootTip  *types.RootBlock
	stopOnce sync.Once
}

func NewShardCoordinator(qkcConfig *config.QuarkChainConfig, shardConfig *config.ShardConfig, db ethdb.Database, minorChain MinorChain, connManager ConnManager) (*ShardCoordinator, error) {
	if qkcConfig == nil {
		return nil, ErrQuarkChainConfigUnavailable
	}
	if shardConfig == nil || shardConfig.Genesis == nil {
		return nil, ErrShardConfigUnavailable
	}
	if db == nil {
		return nil, ErrDatabaseUnavailable
	}
	if minorChain == nil {
		return nil, ErrMinorChainUnavailable
	}
	if connManager == nil {
		return nil, ErrConnManagerUnavailable
	}
	return &ShardCoordinator{shardConfig: shardConfig, branch: account.NewBranch(shardConfig.GetFullShardId()), db: db, minorBlockChain: minorChain, connManager: connManager}, nil
}

// InitFromRootBlock installs the initial root or restores the persisted root tip.
// Recovery preserves the execution layer's current minor head.
func (c *ShardCoordinator) InitFromRootBlock(root *types.RootBlock) error {
	c.importMu.Lock()
	defer c.importMu.Unlock()
	c.mu.RLock()
	stopped, rootTip := c.stopped, c.rootTip
	c.mu.RUnlock()
	if stopped {
		return ErrChainStopped
	}
	if rootTip != nil {
		return ErrRootChainAlreadyInitialized
	}
	if root == nil {
		return ErrUnknownRootBlock
	}
	genesis := c.minorBlockChain.GetBlockByNumber(0)
	if genesis == nil {
		return ErrNoGenesis
	}
	storedHash := rawdb.ReadRootHeadHash(c.db)
	if storedHash != (common.Hash{}) {
		root = c.RootBlockByHash(storedHash)
		if root == nil {
			return fmt.Errorf("stored root head %s: %w", storedHash, ErrUnknownRootBlock)
		}
	} else {
		// Persist the input before propagation, but publish the head marker only
		// after both genesis notifications succeed. A failed boot can then retry.
		batch := c.db.NewBatch()
		rawdb.WriteRootBlock(batch, root)
		err := batch.Write()
		batch.Close()
		if err != nil {
			return err
		}
		if err := c.connManager.BroadcastXShardTxList(XShardBroadcast{Block: genesis, Deposits: make([]*types.CrossShardTransactionDeposit, 0)}); err != nil {
			return fmt.Errorf("broadcast genesis x-shard transactions: %w", err)
		}
		if err := c.connManager.SendMinorBlockHeaderToMaster(genesis, 0); err != nil {
			return fmt.Errorf("send genesis minor header to master: %w", err)
		}
		if err := c.storeRootHead(root); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.rootTip = root
	c.mu.Unlock()
	return nil
}

// AddRootBlock stores the root and directly selects its head. It never changes minor head.
func (c *ShardCoordinator) AddRootBlock(root *types.RootBlock) (bool, error) {
	if root == nil {
		return false, ErrUnknownRootBlock
	}
	c.importMu.Lock()
	defer c.importMu.Unlock()
	if err := c.running(); err != nil {
		return false, err
	}
	if c.GetRootTip().Hash() == root.Hash() {
		return false, nil
	}
	if err := c.storeRootHead(root); err != nil {
		return false, err
	}
	c.mu.Lock()
	c.rootTip = root
	c.mu.Unlock()
	return true, nil
}

// AddMinorBlock executes a candidate, directly selects it and publishes its output.
// A propagation failure leaves the block persisted and canonical, as in the reference.
func (c *ShardCoordinator) AddMinorBlock(block *types.MinorBlock) error {
	if block == nil {
		return ErrUnknownBlock
	}
	if block.Branch() != c.branch {
		return ErrWrongShard
	}
	if block.NumberU64() == 0 {
		return fmt.Errorf("genesis minor block cannot be imported")
	}
	c.importMu.Lock()
	defer c.importMu.Unlock()
	if err := c.running(); err != nil {
		return err
	}
	previousHead := c.minorBlockChain.CurrentBlock()
	if previousHead != nil && previousHead.Hash() == block.Hash() {
		return nil
	}
	parent := c.minorBlockChain.GetBlock(block.ParentHash())
	if parent == nil || parent.Meta() == nil || parent.Meta().XShardTxCursorInfo == nil {
		return fmt.Errorf("parent %s: %w", block.ParentHash(), ErrUnknownParent)
	}
	cursor, err := newXShardTxCursor(c.db, uint64(c.shardConfig.Genesis.RootHeight), block, parent.Meta().XShardTxCursorInfo)
	if err != nil {
		return err
	}
	outputs, err := c.minorBlockChain.InsertBlockWithXShardInput(block, cursor, InsertOptions{ForceInsert: true})
	if err != nil {
		return err
	}
	if err := c.minorBlockChain.SetCanonicalHead(block.Hash()); err != nil {
		return fmt.Errorf("set canonical minor head %s: %w", block.Hash(), err)
	}
	if err := c.connManager.BroadcastXShardTxList(XShardBroadcast{Block: block, Deposits: outputs}); err != nil {
		return fmt.Errorf("broadcast x-shard transactions: %w", err)
	}
	if err := c.connManager.SendMinorBlockHeaderToMaster(block, uint32(len(outputs))); err != nil {
		return fmt.Errorf("send minor header to master: %w", err)
	}
	if err := c.connManager.BroadcastNewTip([]*types.MinorBlockHeader{block.Header()}, c.GetRootTip().Header(), c.branch.Value); err != nil {
		return fmt.Errorf("broadcast new minor tip: %w", err)
	}
	return nil
}

// AddBlockListForSync is outside the Milestone 1 coordinator scope.
func (c *ShardCoordinator) AddBlockListForSync([]*types.MinorBlock) error {
	panic("AddBlockListForSync is not implemented")
}

// AddXShardTxList stores received deposits independently of block imports.
func (c *ShardCoordinator) AddXShardTxList(hash common.Hash, deposits []*types.CrossShardTransactionDeposit) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.stopped {
		return ErrChainStopped
	}
	batch := c.db.NewBatch()
	defer batch.Close()
	if err := rawdb.WriteCrossShardTxList(batch, hash, types.NewCrossShardTransactionList(deposits)); err != nil {
		return fmt.Errorf("store x-shard transactions: %w", err)
	}
	return batch.Write()
}

func (c *ShardCoordinator) running() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.stopped {
		return ErrChainStopped
	}
	if c.rootTip == nil {
		return ErrRootChainUninitialized
	}
	return nil
}

func (c *ShardCoordinator) RootBlockByHash(hash common.Hash) *types.RootBlock {
	return rawdb.ReadRootBlock(c.db, hash)
}

func (c *ShardCoordinator) GetRootTip() *types.RootBlock {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rootTip
}

// storeRootHead atomically saves the root and selected head hash.
func (c *ShardCoordinator) storeRootHead(block *types.RootBlock) error {
	batch := c.db.NewBatch()
	defer batch.Close()
	rawdb.WriteRootBlock(batch, block)
	rawdb.WriteRootHeadHash(batch, block.Hash())
	if err := batch.Write(); err != nil {
		return fmt.Errorf("store root head: %w", err)
	}
	return nil
}

// Stop drains imports and inbound writes before stopping the execution layer.
// The shard owner closes the database after this returns.
func (c *ShardCoordinator) Stop() {
	c.stopOnce.Do(func() {
		c.importMu.Lock()
		defer c.importMu.Unlock()
		c.mu.Lock()
		c.stopped = true
		c.mu.Unlock()
		c.minorBlockChain.Stop()
	})
}
