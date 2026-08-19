// Copyright 2026-2027, QuarkChain.

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// SenderDisallowMap is _get_sender_disallow_map (shard_state.py:1988): how much
// of each recent block producer's balance is locked against being spent.
//
// This is computed here rather than accepted from the caller, and that is a
// deliberate part of the API. The check that consumes it
// (transfer_failure_by_posw_balance_check, messages.py:627) reads the map and
// nothing else: an empty map does not fail, it simply lets a transfer through
// that should have been refused. A wrong map is therefore a silent consensus
// divergence, the one gap in this package that would not announce itself, so
// the caller supplies the configuration and the header lookup and never the
// finished map.
//
// The configuration read is the *shard's* POSW_CONFIG, not the root chain's.
// On mainnet chain 0 has it off and chains 1-7 have it on from their first
// block, so skipping this would leave seven of the eight chains with no
// replayable range at all.
func SenderDisallowMap(ctx *ExecutionContext, parent *types.MinorBlockHeader, coinbase *account.Recipient) (map[account.Recipient]*big.Int, error) {
	posw := ctx.ShardConfig.PoswConfig
	if posw == nil || !posw.Enabled || parent.Time < posw.EnableTimestamp {
		return map[account.Recipient]*big.Int{}, nil
	}
	counts, err := poswCoinbaseBlockCount(ctx, posw.WindowSize, parent.Hash())
	if err != nil {
		return nil, err
	}
	// A block being produced counts its own coinbase too: the producer is
	// staking against the block it is making, not only against past ones
	// (shard_state.py:1994).
	if coinbase != nil {
		counts[*coinbase]++
	}
	stake := posw.TotalStakePerBlock
	if stake == nil {
		stake = new(big.Int)
	}
	out := make(map[account.Recipient]*big.Int, len(counts))
	for addr, count := range counts {
		out[addr] = new(big.Int).Mul(stake, big.NewInt(int64(count)))
	}
	return out, nil
}

// poswCoinbaseBlockCount is get_posw_coinbase_blockcnt (posw.py:23): how many
// of the last WINDOW_SIZE-1 blocks each address produced, counting back from
// headerHash inclusive and stopping at the genesis block.
func poswCoinbaseBlockCount(ctx *ExecutionContext, windowSize uint64, headerHash common.Hash) (map[account.Recipient]int, error) {
	counts := make(map[account.Recipient]int)
	if windowSize == 0 {
		return counts, nil
	}
	header, err := ctx.MinorHeaderByHash(headerHash)
	if err != nil {
		return nil, fmt.Errorf("posw window: %w", err)
	}
	if header == nil {
		return nil, fmt.Errorf("posw window: block %s not found", headerHash)
	}
	for i := uint64(0); i < windowSize-1; i++ {
		counts[header.Coinbase.Recipient]++
		if header.Number == 0 {
			break
		}
		header, err = ctx.MinorHeaderByHash(header.ParentHash)
		if err != nil {
			return nil, fmt.Errorf("posw window: %w", err)
		}
		if header == nil {
			return nil, fmt.Errorf("posw window: chain broken above genesis")
		}
	}
	return counts, nil
}

// prevHeaders collects the BLOCKHASH window: up to 256 headers ending at the
// parent, nearest first.
func prevHeaders(ctx *ExecutionContext, parent *types.MinorBlockHeader) ([]*types.MinorBlockHeader, error) {
	headers := make([]*types.MinorBlockHeader, 0, 256)
	header := parent
	for len(headers) < 256 {
		headers = append(headers, header)
		if header.Number == 0 {
			break
		}
		next, err := ctx.MinorHeaderByHash(header.ParentHash)
		if err != nil {
			return nil, fmt.Errorf("blockhash window: %w", err)
		}
		if next == nil {
			break
		}
		header = next
	}
	return headers, nil
}
