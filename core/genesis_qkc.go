// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

// CommitQKCGenesisBlock installs a pre-derived QKC minor genesis in geth's
// native block/head/config schema. Its state must already be materialized in db.
func CommitQKCGenesisBlock(db ethdb.Database, block *types.Block, chainConfig *params.ChainConfig) error {
	if db == nil || block == nil || chainConfig == nil {
		return errors.New("QKC genesis database, block, and chain config are required")
	}
	minorHeader, err := block.ToMinorBlockHeader()
	if err != nil {
		return fmt.Errorf("project QKC genesis header: %w", err)
	}
	header := block.Header()
	if minorHeader.Number != 0 {
		return fmt.Errorf("QKC genesis has block number %d", minorHeader.Number)
	}
	if !header.IsQKC() {
		return errors.New("genesis block is not a QKC minor block")
	}
	if err := block.Validate(); err != nil {
		return fmt.Errorf("invalid QKC genesis: %w", err)
	}
	// Fail normally instead of letting rawdb.WriteBlock terminate the process
	// on an internal RLP encoding error.
	if _, err := rlp.EncodeToBytes(block); err != nil {
		return fmt.Errorf("encode QKC genesis for storage: %w", err)
	}
	if err := chainConfig.CheckConfigForkOrder(); err != nil {
		return err
	}
	stored := rawdb.ReadCanonicalHash(db, 0)
	if stored != (common.Hash{}) {
		if stored != block.Hash() {
			return &GenesisMismatchError{Stored: stored, New: block.Hash()}
		}
		if existing := rawdb.ReadBlock(db, stored, 0); existing == nil {
			return fmt.Errorf("QKC canonical genesis %s has no block data", stored)
		}
		if rawdb.ReadChainConfig(db, stored) == nil {
			return fmt.Errorf("QKC canonical genesis %s has no chain config", stored)
		}
		return nil
	}
	batch := db.NewBatch()
	defer batch.Close()
	rawdb.WriteGenesisStateSpec(batch, block.Hash(), []byte("{}"))
	rawdb.WriteBlock(batch, block)
	rawdb.WriteReceipts(batch, block.Hash(), block.NumberU64(), nil)
	rawdb.WriteCanonicalHash(batch, block.Hash(), block.NumberU64())
	rawdb.WriteHeadBlockHash(batch, block.Hash())
	rawdb.WriteHeadFastBlockHash(batch, block.Hash())
	rawdb.WriteHeadHeaderHash(batch, block.Hash())
	rawdb.WriteChainConfig(batch, block.Hash(), chainConfig)
	if err := batch.Write(); err != nil {
		return fmt.Errorf("write QKC genesis: %w", err)
	}
	return nil
}
