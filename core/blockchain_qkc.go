// Copyright 2026-2027, QuarkChain.

package core

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	errNilQKCCandidate           = errors.New("nil QKC candidate")
	errNilQKCCandidateSigner     = errors.New("QKC candidate signer is required")
	errQKCGenesisCandidate       = errors.New("QKC candidate insertion does not accept genesis")
	errNonContiguousQKCCandidate = errors.New("QKC candidate does not extend its parent")
	errConflictingQKCCandidate   = errors.New("QKC candidate hash already exists with different block data")
)

// InsertBlockWithoutExecution validates and stores a QKC minor-block candidate
// without executing transactions or changing the canonical chain.
func (bc *BlockChain) InsertBlockWithoutExecution(block *types.Block, signer types.Signer) error {
	if block == nil {
		return errNilQKCCandidate
	}
	if signer == nil {
		return errNilQKCCandidateSigner
	}
	if !bc.chainmu.TryLock() {
		return errChainStopped
	}
	defer bc.chainmu.Unlock()

	if bc.insertStopped() {
		return errInsertionInterrupted
	}
	if err := block.Validate(); err != nil {
		return err
	}
	minorHeader, err := block.ToMinorBlockHeader()
	if err != nil {
		return fmt.Errorf("project QKC candidate header: %w", err)
	}
	if minorHeader.Number == 0 {
		return errQKCGenesisCandidate
	}
	if !block.Header().IsQKC() {
		return errors.New("candidate is not a QKC minor block")
	}
	for index, tx := range block.Transactions() {
		if _, err := types.Sender(signer, tx); err != nil {
			return fmt.Errorf("invalid QKC transaction %d signature: %w", index, err)
		}
	}
	// Pre-encode the complete internal record so rawdb.WriteBlock cannot hit a
	// fatal encoder error after validation. TODO: support trie-backed internal
	// encoding for coinbase maps with more than 16 non-zero tokens.
	encoded, err := rlp.EncodeToBytes(block)
	if err != nil {
		return fmt.Errorf("encode QKC candidate for storage: %w", err)
	}
	if bc.HasBlock(block.Hash(), minorHeader.Number) {
		existing := bc.GetBlock(block.Hash(), minorHeader.Number)
		if existing == nil {
			return errors.New("QKC candidate header exists without its block body")
		}
		existingEncoded, err := rlp.EncodeToBytes(existing)
		if err != nil {
			return fmt.Errorf("encode stored QKC candidate: %w", err)
		}
		if bytes.Equal(existingEncoded, encoded) {
			return nil
		}
		return errConflictingQKCCandidate
	}
	parent := bc.GetBlock(block.ParentHash(), minorHeader.Number-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	if parent.NumberU64()+1 != minorHeader.Number || parent.Hash() != block.ParentHash() {
		return errNonContiguousQKCCandidate
	}
	return bc.writeBlockWithoutState(block)
}
