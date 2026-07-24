// Copyright 2026-2027, QuarkChain.

// qkc/genesis is the GenesisManager analog: pure functions of the parsed config
// that derive the cluster's genesis blocks. Only the root genesis block (the
// anchor every shard genesis links to) is derived here; shard genesis state
// materialization belongs to the separate shard-chain task.

package genesis

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// CreateRootBlock derives the genesis root block header purely from
// QUARKCHAIN.ROOT.GENESIS, mirroring pyquarkchain's
// GenesisManager.create_root_block (quarkchain/genesis.py:28). total_difficulty is
// set equal to difficulty; the evm-state root, coinbase, amount map, extra data,
// mixhash, and signature are all empty/zero, exactly as the Python default header.
// The resulting header.Hash() is byte-identical to pyquarkchain's
// create_root_block().header.get_hash().
func CreateRootBlock(qkc *config.QuarkChainConfig) (*types.RootBlockHeader, error) {
	if qkc == nil || qkc.Root == nil || qkc.Root.Genesis == nil {
		return nil, fmt.Errorf("genesis: QUARKCHAIN.ROOT.GENESIS is missing")
	}
	g := qkc.Root.Genesis

	prev, err := parseRootHash(g.HashPrevBlock)
	if err != nil {
		return nil, fmt.Errorf("genesis: ROOT.GENESIS.HASH_PREV_BLOCK: %w", err)
	}
	merkle, err := parseRootHash(g.HashMerkleRoot)
	if err != nil {
		return nil, fmt.Errorf("genesis: ROOT.GENESIS.HASH_MERKLE_ROOT: %w", err)
	}

	difficulty := new(big.Int)
	if g.Difficulty != nil {
		difficulty.Set(g.Difficulty)
	}
	return &types.RootBlockHeader{
		Version:         g.Version,
		Number:          g.Height,
		ParentHash:      prev,
		MinorHeaderHash: merkle,
		Coinbase:        account.CreatEmptyAddress(0),
		CoinbaseAmount:  types.NewEmptyTokenBalances(),
		Time:            g.Timestamp,
		Difficulty:      difficulty,
		TotalDifficulty: new(big.Int).Set(difficulty),
		Nonce:           g.Nonce,
	}, nil
}

// parseRootHash decodes a config hash field (a hex string such as "0000…0000",
// optionally "0x"-prefixed; empty means all-zero) into a 32-byte hash. Any other
// length is rejected so a malformed config fails loudly here rather than producing
// a silently wrong genesis hash.
func parseRootHash(s string) (common.Hash, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return common.Hash{}, nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return common.Hash{}, fmt.Errorf("invalid hex %q: %w", s, err)
	}
	if len(b) != common.HashLength {
		return common.Hash{}, fmt.Errorf("expected %d bytes, got %d", common.HashLength, len(b))
	}
	return common.BytesToHash(b), nil
}
