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

// XShardBroadcast is the outgoing cross-shard payload produced by one block.
type XShardBroadcast struct {
	Block    *types.MinorBlock
	Deposits []*types.CrossShardTransactionDeposit
}

// ShardCoordinator applies root-chain fork choice and confirmation policy to a
// local minor chain. It does not execute or propagate minor blocks.
type ShardCoordinator struct {
	shardConfig     *config.ShardConfig
	branch          account.Branch
	db              ethdb.Database
	minorBlockChain MinorChain
	connManager     ConnManager

	initialized       bool
	stopped           bool
	rootTip           *types.RootBlock
	confirmedMinorTip *types.MinorBlock

	stopOnce sync.Once
	mu       sync.RWMutex
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
	return &ShardCoordinator{
		shardConfig:     shardConfig,
		branch:          account.NewBranch(shardConfig.GetFullShardId()),
		db:              db,
		minorBlockChain: minorChain,
		connManager:     connManager,
	}, nil
}

// InitFromRootBlock initializes the shard at its genesis root height or restores
// its root and confirmed-minor tips from a later root block.
func (c *ShardCoordinator) InitFromRootBlock(root *types.RootBlock) error {
	if err := c.validateRootBlock(root); err != nil {
		return err
	}
	genesisRootHeight := uint64(c.shardConfig.Genesis.RootHeight)
	if root.NumberU64() < genesisRootHeight {
		return fmt.Errorf("root height %d is below shard genesis root height %d: %w", root.NumberU64(), genesisRootHeight, ErrRootBeforeGenesis)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return ErrChainStopped
	}
	if c.initialized {
		return ErrRootChainAlreadyInitialized
	}
	if root.NumberU64() == genesisRootHeight {
		return c.initFromGenesisRoot(root)
	}
	return c.recoverFromRootBlock(root)
}

func (c *ShardCoordinator) initFromGenesisRoot(root *types.RootBlock) error {
	genesis := c.minorBlockChain.GetBlockByNumber(0)
	if genesis == nil {
		return ErrNoGenesis
	}
	if genesis.PrevRootBlockHash() != root.Hash() {
		return fmt.Errorf("minor genesis previous root %s does not match %s", genesis.PrevRootBlockHash(), root.Hash())
	}
	if !c.minorBlockChain.HasState(genesis.Root()) {
		return fmt.Errorf("minor genesis root %s: %w", genesis.Root(), ErrStateUnavailable)
	}
	if err := c.writeRootBlock(root, nil); err != nil {
		return fmt.Errorf("write genesis root block: %w", err)
	}
	if err := c.setCurrentRootBlock(root); err != nil {
		return fmt.Errorf("set genesis root tip: %w", err)
	}
	c.rootTip = root
	c.confirmedMinorTip = nil
	c.initialized = true
	return nil
}

func (c *ShardCoordinator) recoverFromRootBlock(root *types.RootBlock) error {
	confirmed, err := c.lastConfirmedMinorBlockAtRootBlock(root.Hash())
	if err != nil {
		return err
	}
	if c.RootBlockByHash(root.Hash()) == nil {
		if err := c.validateRootBlockForImport(root); err != nil {
			return err
		}
		confirmed, err = c.deriveConfirmedMinorBlock(root)
		if err != nil {
			return err
		}
		if err := c.writeRootBlock(root, confirmed); err != nil {
			return fmt.Errorf("write recovered root block: %w", err)
		}
	}
	headerTip := confirmed
	if headerTip == nil {
		headerTip = c.minorBlockChain.GetBlockByNumber(0)
	}
	if headerTip == nil {
		return ErrNoGenesis
	}
	if !c.minorBlockChain.HasState(headerTip.Root()) {
		return fmt.Errorf("restore minor block %s root %s: %w", headerTip.Hash(), headerTip.Root(), ErrStateUnavailable)
	}
	if err := c.minorBlockChain.SetCanonicalHead(headerTip.Hash()); err != nil {
		return fmt.Errorf("restore canonical minor head %s: %w", headerTip.Hash(), err)
	}
	if err := c.setCurrentRootBlock(root); err != nil {
		return fmt.Errorf("set recovered root tip: %w", err)
	}
	c.rootTip = root
	c.confirmedMinorTip = confirmed
	c.initialized = true
	return nil
}

// deriveConfirmedMinorBlock validates this shard's headers in a root block and
// returns the newest confirmed local block.
func (c *ShardCoordinator) deriveConfirmedMinorBlock(root *types.RootBlock) (*types.MinorBlock, error) {
	lastConfirmed, err := c.lastConfirmedMinorBlockAtRootBlock(root.ParentHash())
	if err != nil {
		return nil, err
	}
	previous := lastConfirmed
	hasConfirmed := previous != nil
	if !hasConfirmed {
		previous = c.minorBlockChain.GetBlockByNumber(0)
	}
	if previous == nil {
		return nil, ErrNoGenesis
	}
	var localBlockCount uint32
	for _, header := range root.MinorBlockHeaders() {
		if header.Branch != c.branch {
			if err := c.validateRemoteXShardList(header); err != nil {
				return nil, err
			}
			continue
		}
		block := c.minorBlockChain.GetBlock(header.Hash())
		if block == nil {
			return nil, fmt.Errorf("confirmed minor block %s: %w", header.Hash(), ErrConfirmedMinorBlockUnavailable)
		}
		if localBlockCount == 0 && !hasConfirmed {
			if header.Number != 0 || block.Hash() != previous.Hash() {
				return nil, ErrMinorNotConfirmedDescendant
			}
		} else if header.Number != previous.NumberU64()+1 || header.ParentHash != previous.Hash() {
			return nil, ErrMinorNotConfirmedDescendant
		}
		localBlockCount++
		lastConfirmed = block
		previous = block
	}
	if localBlockCount > c.shardConfig.MaxBlocksPerShardInOneRootBlock() {
		return nil, fmt.Errorf("root block contains %d minor blocks for shard, maximum is %d", localBlockCount, c.shardConfig.MaxBlocksPerShardInOneRootBlock())
	}
	if localBlockCount == 0 {
		return lastConfirmed, nil
	}
	confirmedPrevRoot := c.RootBlockByHash(lastConfirmed.PrevRootBlockHash())
	if !c.isRootDescendant(root, confirmedPrevRoot) {
		return nil, ErrMinorRootNotCanonical
	}
	return lastConfirmed, nil
}

func (c *ShardCoordinator) lastConfirmedMinorBlockAtRootBlock(hash common.Hash) (*types.MinorBlock, error) {
	minorHash := rawdb.ReadLastConfirmedMinorBlockHeaderAtRootBlock(c.db, hash)
	if minorHash == (common.Hash{}) {
		return nil, nil
	}
	block := c.minorBlockChain.GetBlock(minorHash)
	if block == nil {
		return nil, fmt.Errorf("confirmed minor block %s at root %s: %w", minorHash, hash, ErrConfirmedMinorBlockUnavailable)
	}
	return block, nil
}

func (c *ShardCoordinator) validateRemoteXShardList(header *types.MinorBlockHeader) error {
	previousRoot := c.RootBlockByHash(header.PrevRootBlockHash)
	expectsList := previousRoot != nil && previousRoot.NumberU64() != uint64(c.shardConfig.Genesis.RootHeight)
	list := rawdb.ReadCrossShardTxList(c.db, header.Hash())
	if expectsList && list == nil {
		return fmt.Errorf("minor block %s: %w", header.Hash(), ErrRemoteXShardTxListUnavailable)
	}
	if !expectsList && list != nil {
		return fmt.Errorf("minor block %s: %w", header.Hash(), ErrUnexpectedRemoteXShardTxList)
	}
	return nil
}

func (c *ShardCoordinator) validateRootBlock(root *types.RootBlock) error {
	if root == nil {
		return ErrUnknownRootBlock
	}
	if root.Version() != 0 {
		return fmt.Errorf("unsupported root block version %d", root.Version())
	}
	if root.MinorHeaderHash() != types.CalculateMerkleRoot(root.MinorBlockHeaders()) {
		return fmt.Errorf("root block %s has invalid minor header root", root.Hash())
	}
	return nil
}

func (c *ShardCoordinator) validateRootBlockForImport(root *types.RootBlock) error {
	if root.NumberU64() <= uint64(c.shardConfig.Genesis.RootHeight) {
		return ErrRootBeforeGenesis
	}
	if c.RootBlockByHash(root.Hash()) != nil {
		return nil
	}
	parent := c.RootBlockByHash(root.ParentHash())
	if parent == nil {
		return fmt.Errorf("root parent %s: %w", root.ParentHash(), ErrUnknownRootBlock)
	}
	if root.NumberU64() != parent.NumberU64()+1 {
		return ErrRootNotContiguous
	}
	return nil
}

// AddRootBlock stores a root block and selects it when its total difficulty is
// greater than the current root tip's total difficulty.
func (c *ShardCoordinator) AddRootBlock(root *types.RootBlock) (bool, error) {
	if err := c.validateRootBlock(root); err != nil {
		return false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return false, ErrChainStopped
	}
	if !c.initialized || c.rootTip == nil {
		return false, ErrRootChainUninitialized
	}
	return c.importRootBlock(root)
}

func (c *ShardCoordinator) importRootBlock(root *types.RootBlock) (bool, error) {
	if err := c.validateRootBlockForImport(root); err != nil {
		return false, err
	}
	// Root storage precedes canonical selection, as in pyquarkchain. If a
	// previous canonical switch failed, reuse the persisted confirmation data
	// so adding the same root again can finish the switch.
	stored := c.RootBlockByHash(root.Hash())
	var confirmed *types.MinorBlock
	var err error
	if stored == nil {
		confirmed, err = c.deriveConfirmedMinorBlock(root)
		if err != nil {
			return false, err
		}
		if err := c.writeRootBlock(root, confirmed); err != nil {
			return false, fmt.Errorf("write root block: %w", err)
		}
	} else {
		root = stored
		confirmed, err = c.lastConfirmedMinorBlockAtRootBlock(root.Hash())
		if err != nil {
			return false, err
		}
	}
	if root.TotalDifficulty().Cmp(c.rootTip.TotalDifficulty()) <= 0 {
		return false, nil
	}
	if err := c.setMinorHeadForCanonicalRoot(root, confirmed); err != nil {
		return false, err
	}
	if err := c.setCurrentRootBlock(root); err != nil {
		return false, fmt.Errorf("set root tip: %w", err)
	}
	c.rootTip = root
	c.confirmedMinorTip = confirmed
	return true, nil
}

func (c *ShardCoordinator) setMinorHeadForCanonicalRoot(root *types.RootBlock, confirmed *types.MinorBlock) error {
	current := c.minorBlockChain.CurrentBlock()
	if current == nil {
		return ErrNoCurrentBlock
	}
	if confirmed != nil {
		canonical := c.minorBlockChain.GetBlockByNumber(confirmed.NumberU64())
		if canonical == nil || canonical.Hash() != confirmed.Hash() {
			if err := c.minorBlockChain.SetCanonicalHead(confirmed.Hash()); err != nil {
				return fmt.Errorf("select confirmed minor head %s: %w", confirmed.Hash(), err)
			}
			current = confirmed
		}
	}
	for !c.isRootDescendant(root, c.RootBlockByHash(current.PrevRootBlockHash())) {
		if current.NumberU64() == 0 {
			return ErrMinorRootNotCanonical
		}
		current = c.minorBlockChain.GetBlock(current.ParentHash())
		if current == nil {
			return ErrUnknownParent
		}
	}
	if current.Hash() != c.minorBlockChain.CurrentBlock().Hash() {
		if err := c.minorBlockChain.SetCanonicalHead(current.Hash()); err != nil {
			return fmt.Errorf("rewind minor head to root chain %s: %w", current.Hash(), err)
		}
	}
	return nil
}

// TODO(next PR): implement minor block replay and propagation.
func (c *ShardCoordinator) AddMinorBlock(block *types.MinorBlock) error {
	panic("implemented in the next PR")
}

// TODO(next PR): implement batched minor block replay and propagation.
func (c *ShardCoordinator) AddBlockListForSync(blocks []*types.MinorBlock) error {
	panic("implemented in the next PR")
}

func (c *ShardCoordinator) isRootDescendant(descendant, ancestor *types.RootBlock) bool {
	if descendant == nil || ancestor == nil || descendant.NumberU64() < ancestor.NumberU64() {
		return false
	}
	current := descendant
	for current.NumberU64() > ancestor.NumberU64() {
		current = c.RootBlockByHash(current.ParentHash())
		if current == nil {
			return false
		}
	}
	return current.Hash() == ancestor.Hash()
}

// TODO(next PR): implement cross-shard transaction list ingestion.
func (c *ShardCoordinator) AddXShardTxList(hash common.Hash, deposits []*types.CrossShardTransactionDeposit) error {
	panic("implemented in the next PR")
}

func (c *ShardCoordinator) RootBlockByHash(hash common.Hash) *types.RootBlock {
	return rawdb.ReadRootBlock(c.db, hash)
}

func (c *ShardCoordinator) CurrentRootBlock() *types.RootBlock {
	hash := rawdb.ReadRootHeadHash(c.db)
	if hash == (common.Hash{}) {
		return nil
	}
	return rawdb.ReadRootBlock(c.db, hash)
}

func (c *ShardCoordinator) writeRootBlock(block *types.RootBlock, confirmed *types.MinorBlock) error {
	batch := c.db.NewBatch()
	defer batch.Close()
	rawdb.WriteRootBlock(batch, block)
	confirmedHash := common.Hash{}
	if confirmed != nil {
		confirmedHash = confirmed.Hash()
	}
	rawdb.WriteLastConfirmedMinorBlockHeaderAtRootBlock(batch, block.Hash(), confirmedHash)
	return batch.Write()
}

// setCurrentRootBlock rewrites the canonical root index and head atomically.
func (c *ShardCoordinator) setCurrentRootBlock(block *types.RootBlock) error {
	current := c.CurrentRootBlock()
	newCanonical := make([]*types.RootBlock, 0)
	if current == nil {
		// Recovery may start from a stored checkpoint before a root head exists.
		// Rebuild every canonical index back to this shard's genesis root so
		// height-based lookups cannot observe a partial chain.
		for root := block; root != nil && root.NumberU64() >= uint64(c.shardConfig.Genesis.RootHeight); root = c.RootBlockByHash(root.ParentHash()) {
			newCanonical = append(newCanonical, root)
			if root.NumberU64() == uint64(c.shardConfig.Genesis.RootHeight) {
				break
			}
		}
		if len(newCanonical) == 0 || newCanonical[len(newCanonical)-1].NumberU64() != uint64(c.shardConfig.Genesis.RootHeight) {
			return ErrUnknownRootBlock
		}
	} else {
		// Align both forks by height, then walk them together to their common
		// ancestor while collecting the replacement canonical segment.
		oldHead, newHead := current, block
		for oldHead.NumberU64() > newHead.NumberU64() {
			oldHead = c.RootBlockByHash(oldHead.ParentHash())
			if oldHead == nil {
				return ErrUnknownRootBlock
			}
		}
		for newHead.NumberU64() > oldHead.NumberU64() {
			newCanonical = append(newCanonical, newHead)
			newHead = c.RootBlockByHash(newHead.ParentHash())
			if newHead == nil {
				return ErrUnknownRootBlock
			}
		}
		for oldHead.Hash() != newHead.Hash() {
			newCanonical = append(newCanonical, newHead)
			oldHead = c.RootBlockByHash(oldHead.ParentHash())
			newHead = c.RootBlockByHash(newHead.ParentHash())
			if oldHead == nil || newHead == nil {
				return ErrUnknownRootBlock
			}
		}
	}
	batch := c.db.NewBatch()
	defer batch.Close()
	if current != nil {
		for number := current.NumberU64(); number > block.NumberU64(); number-- {
			rawdb.DeleteRootCanonicalHash(batch, number)
		}
	}
	for index := len(newCanonical) - 1; index >= 0; index-- {
		root := newCanonical[index]
		rawdb.WriteRootCanonicalHash(batch, root.Hash(), root.NumberU64())
	}
	rawdb.WriteRootHeadHash(batch, block.Hash())
	return batch.Write()
}

func (c *ShardCoordinator) GetConfirmedMinorTip() *types.MinorBlock {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.confirmedMinorTip
}

func (c *ShardCoordinator) GetRootTip() *types.RootBlock {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rootTip
}

func (c *ShardCoordinator) Stop() {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.stopped = true
		c.initialized = false
		minorChain := c.minorBlockChain
		c.mu.Unlock()
		minorChain.Stop()
	})
}
