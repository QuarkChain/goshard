// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// AllowedFutureBlocksTimeBroadcast is the maximum accepted future timestamp skew.
var AllowedFutureBlocksTimeBroadcast = 15

// XShardBroadcast is the outgoing cross-shard payload produced by one block.
type XShardBroadcast struct {
	Block    *types.MinorBlock
	Deposits []*types.CrossShardTransactionDeposit
}

// ShardCoordinator applies root-chain policy to local minor-block replay and
// coordinates propagation with the master, other shards, and peers.
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

// AddMinorBlock executes and propagates one block. External submission is
// intentionally separate from local block/state persistence.
func (c *ShardCoordinator) AddMinorBlock(block *types.MinorBlock) error {
	if block == nil {
		return ErrUnknownBlock
	}
	if block.Branch().Value != c.branch.Value {
		return fmt.Errorf("minor block branch %d does not match shard branch %d: %w", block.Branch().Value, c.branch.Value, ErrWrongShard)
	}
	if block.NumberU64() == 0 {
		return fmt.Errorf("genesis minor block cannot be imported")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return ErrChainStopped
	}
	if !c.initialized {
		return ErrRootChainUninitialized
	}
	if c.minorBlockChain.GetBlock(block.Hash()) != nil {
		return nil
	}
	parentBlock := c.minorBlockChain.GetBlock(block.ParentHash())
	if parentBlock == nil {
		return fmt.Errorf("parent %s: %w", block.ParentHash(), ErrUnknownParent)
	}
	if err := validateMinorBlockTime(block, parentBlock); err != nil {
		return err
	}
	if _, err := c.validateMinorBlockRootChain(block, parentBlock); err != nil {
		return err
	}
	cursor, err := c.xShardExecutionInput(block, parentBlock)
	if err != nil {
		return err
	}
	previousHead := c.minorBlockChain.CurrentBlock()
	_, outputs, err := c.minorBlockChain.InsertChainWithXShardInputs([]*types.MinorBlock{block}, []*XShardTxCursor{cursor}, InsertOptions{ForceInsert: true})
	if err != nil {
		return err
	}
	if len(outputs) != 1 {
		return fmt.Errorf("minor insertion returned %d x-shard output lists for 1 block", len(outputs))
	}
	payload := XShardBroadcast{Block: block, Deposits: outputs[0]}
	if err := c.connManager.BroadcastXShardTxList(payload); err != nil {
		return fmt.Errorf("broadcast x-shard transactions: %w", err)
	}
	if err := c.connManager.SendMinorBlockHeaderToMaster(block, uint32(len(payload.Deposits))); err != nil {
		return fmt.Errorf("send minor block header to master: %w", err)
	}
	current := c.minorBlockChain.CurrentBlock()
	if current != nil && (previousHead == nil || current.Hash() != previousHead.Hash()) {
		if c.rootTip == nil {
			return ErrRootChainUninitialized
		}
		if err := c.connManager.BroadcastNewTip([]*types.MinorBlockHeader{current.Header()}, c.rootTip.Header(), c.branch.Value); err != nil {
			return fmt.Errorf("broadcast new minor tip: %w", err)
		}
	}
	return nil
}

// AddBlockListForSync replays each requested block independently, then propagates
// its x-shard outputs and headers in batches. The input may contain gaps bridged
// by locally known blocks.
func (c *ShardCoordinator) AddBlockListForSync(blocks []*types.MinorBlock) error {
	if len(blocks) == 0 {
		return nil
	}
	for index, block := range blocks {
		if block == nil {
			return fmt.Errorf("validate sync block %d: %w", index, ErrUnknownBlock)
		}
		if block.Branch().Value != c.branch.Value {
			return fmt.Errorf("sync block %d branch %d does not match shard branch %d: %w", index, block.Branch().Value, c.branch.Value, ErrWrongShard)
		}
		if index > 0 && block.NumberU64() <= blocks[index-1].NumberU64() {
			return fmt.Errorf("sync block %d height %d follows height %d: %w", index, block.NumberU64(), blocks[index-1].NumberU64(), ErrMinorBlockOrder)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return ErrChainStopped
	}
	if !c.initialized {
		return ErrRootChainUninitialized
	}
	processedBlocks := make([]*types.MinorBlock, 0, len(blocks))
	payloads := make([]XShardBroadcast, 0, len(blocks))
	for index, block := range blocks {
		parentBlock := c.minorBlockChain.GetBlock(block.ParentHash())
		if parentBlock == nil {
			return fmt.Errorf("validate sync block %d parent %s: %w", index, block.ParentHash(), ErrUnknownParent)
		}
		if err := validateMinorBlockTime(block, parentBlock); err != nil {
			return fmt.Errorf("validate sync block %d time: %w", index, err)
		}
		if _, err := c.validateMinorBlockRootChain(block, parentBlock); err != nil {
			return fmt.Errorf("validate root reference for block %d: %w", index, err)
		}
		cursor, err := c.xShardExecutionInput(block, parentBlock)
		if err != nil {
			return fmt.Errorf("prepare x-shard execution input for block %d: %w", index, err)
		}
		_, outputs, err := c.minorBlockChain.InsertChainWithXShardInputs([]*types.MinorBlock{block}, []*XShardTxCursor{cursor}, InsertOptions{ForceInsert: true})
		if err != nil {
			return fmt.Errorf("insert sync block %d: %w", index, err)
		}
		if len(outputs) != 1 {
			return fmt.Errorf("minor insertion returned %d x-shard output lists for block %d", len(outputs), index)
		}
		processedBlocks = append(processedBlocks, block)
		payloads = append(payloads, XShardBroadcast{Block: block, Deposits: outputs[0]})
	}
	if err := c.connManager.BatchBroadcastXShardTxList(payloads); err != nil {
		return fmt.Errorf("batch broadcast x-shard transactions: %w", err)
	}
	if err := c.connManager.SendMinorBlockHeaderListToMaster(processedBlocks); err != nil {
		return fmt.Errorf("send minor block header list to master: %w", err)
	}
	return nil
}

// validateMinorBlockRootChain resolves block.PrevRootBlockHash and validates its
// relationship with the root referenced by the parent minor block. Local
// minor-chain continuity is owned by MinorBlockChain.
func (c *ShardCoordinator) validateMinorBlockRootChain(block, parentBlock *types.MinorBlock) (*types.RootBlock, error) {
	if block == nil {
		return nil, ErrUnknownBlock
	}
	prevRootBlock := c.RootBlockByHash(block.PrevRootBlockHash())
	if prevRootBlock == nil {
		return nil, fmt.Errorf("previous root %s: %w", block.PrevRootBlockHash(), ErrUnknownRootBlock)
	}
	if parentBlock != nil {
		parentPrevRootBlock := c.RootBlockByHash(parentBlock.PrevRootBlockHash())
		if parentPrevRootBlock == nil {
			return nil, fmt.Errorf("parent previous root %s: %w", parentBlock.PrevRootBlockHash(), ErrUnknownRootBlock)
		}
		if prevRootBlock.NumberU64() < parentPrevRootBlock.NumberU64() {
			return nil, fmt.Errorf("root height %d follows %d: %w", prevRootBlock.NumberU64(), parentPrevRootBlock.NumberU64(), ErrMinorRootOrder)
		}
		confirmed, err := c.lastConfirmedMinorBlockAtRootBlock(prevRootBlock.Hash())
		if err != nil {
			return nil, err
		}
		if confirmed != nil && !c.isMinorDescendant(parentBlock, confirmed) {
			return nil, ErrMinorNotConfirmedDescendant
		}
		if !c.isRootDescendant(prevRootBlock, parentPrevRootBlock) {
			return nil, ErrMinorRootNotCanonical
		}
	}
	if !c.isRootDescendant(c.CurrentRootBlock(), prevRootBlock) {
		return nil, ErrMinorRootNotCanonical
	}
	return prevRootBlock, nil
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

func (c *ShardCoordinator) isMinorDescendant(descendant, ancestor *types.MinorBlock) bool {
	if descendant == nil || ancestor == nil || descendant.NumberU64() < ancestor.NumberU64() {
		return false
	}
	current := descendant
	for current.NumberU64() > ancestor.NumberU64() {
		current = c.minorBlockChain.GetBlock(current.ParentHash())
		if current == nil {
			return false
		}
	}
	return current.Hash() == ancestor.Hash()
}

// AddXShardTxList stores deposits received from another shard for later replay.
func (c *ShardCoordinator) AddXShardTxList(hash common.Hash, deposits []*types.CrossShardTransactionDeposit) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.stopped {
		return ErrChainStopped
	}
	rawdb.WriteCrossShardTxList(c.db, hash, types.NewCrossShardTransactionList(deposits))
	return nil
}

func (c *ShardCoordinator) xShardExecutionInput(block, parent *types.MinorBlock) (*XShardTxCursor, error) {
	if parent == nil || parent.Meta() == nil {
		return nil, ErrUnknownParent
	}
	return newXShardTxCursor(c.db, uint64(c.shardConfig.Genesis.RootHeight), block, parent.Meta().XShardTxCursorInfo)
}

func validateMinorBlockTime(block, parent *types.MinorBlock) error {
	now := uint64(time.Now().Unix())
	if block.Time() > now+uint64(AllowedFutureBlocksTimeBroadcast) {
		return fmt.Errorf("block time %d is too far in the future (now %d)", block.Time(), now)
	}
	if parent != nil && block.Time() <= parent.Time() {
		return fmt.Errorf("block time %d must be after parent time %d", block.Time(), parent.Time())
	}
	return nil
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
