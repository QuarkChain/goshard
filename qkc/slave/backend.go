// Copyright 2026-2027, QuarkChain.

// Package slave runs one slave identity's shards inside a single process,
// mirroring pyquarkchain's SlaveServer. The slave process itself owns no
// database: all persistent state lives in the per-shard chaindbs, and the shard
// registry is rebuilt from config on every boot. It performs no network I/O.
package slave

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/shard"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// SlaveBackend hosts the shards owned by one slave identity, keyed by branch as
// in pyquarkchain's SlaveServer.
type SlaveBackend struct {
	ID     string
	shards map[account.Branch]*shard.Shard
	order  []account.Branch // boot (config) order, for deterministic iteration and shutdown

	stopOnce sync.Once
	stopErr  error
}

// New boots every shard the context's slave owns, eagerly and in config order.
// Eager construction is interim scaffolding: with no master in the cluster yet it
// is the only way to bring the shards up; the master's PING(root_tip) trigger
// (#5) replaces it (pyquarkchain slave.py:927).
// TODO(#5): move shard creation behind PING(root_tip), preserving rollback and
// blocking shutdown for dynamically created shards.
//
// On any shard failure the shards already started are stopped and their databases
// closed before the error returns, so the datadir stays reopenable.
func New(ctx *config.SlaveContext, rootGenesis *types.RootBlockHeader, opts shard.Options) (*SlaveBackend, error) {
	b := &SlaveBackend{
		ID:     ctx.ID,
		shards: make(map[account.Branch]*shard.Shard, len(ctx.FullShardIDs())),
	}
	for _, id := range ctx.FullShardIDs() {
		branch := account.NewBranch(id)
		s, err := shard.New(ctx, branch, rootGenesis, ctx.DBPathRoot, opts)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("slave %s: %w", b.ID, err), b.Stop())
		}
		b.shards[branch] = s
		b.order = append(b.order, branch)

		height, _ := s.Chain().Head()
		log.Info("shard started", "shard", fmt.Sprintf("0x%08x", id), "genesis", s.Chain().GenesisHash(), "head", height)
	}
	return b, nil
}

// Shard returns the shard hosting branch, or nil if this slave does not own it.
func (b *SlaveBackend) Shard(branch account.Branch) *shard.Shard {
	return b.shards[branch]
}

// Shards returns the hosted shards in boot (config) order.
func (b *SlaveBackend) Shards() []*shard.Shard {
	out := make([]*shard.Shard, 0, len(b.order))
	for _, branch := range b.order {
		out = append(out, b.shards[branch])
	}
	return out
}

// Stop shuts every shard down — chain first, then database — and blocks until all
// are stopped and their databases closed. It is idempotent; a per-shard failure is
// collected rather than aborting the remaining shutdowns.
func (b *SlaveBackend) Stop() error {
	b.stopOnce.Do(func() {
		var errs []error
		for _, branch := range b.order {
			if err := b.shards[branch].Stop(); err != nil {
				errs = append(errs, fmt.Errorf("stop shard 0x%08x: %w", branch.GetFullShardID(), err))
			}
		}
		b.stopErr = errors.Join(errs...)
	})
	return b.stopErr
}
