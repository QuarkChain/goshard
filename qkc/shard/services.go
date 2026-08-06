// Copyright 2026-2027, QuarkChain.

package shard

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// ShardChain is the named boundary between the slave skeleton and the geth-core
// shard chain (a geth core.BlockChain, the same component as
// qkc/core.MinorBlockChain), which is delivered by a separate task. It exposes
// only what the slave needs and is expected to co-evolve with that task.
type ShardChain interface {
	// GenesisHash is the hash of the genesis block the chain stands on.
	GenesisHash() common.Hash
	// Head is the current chain head.
	Head() (height uint64, hash common.Hash)
	// Stop shuts the chain down, blocking until its background work has drained.
	Stop()
}

// ChainService constructs the ShardChain for one shard from its isolated chaindb,
// the shard's genesis block (whose state is already materialized in that db), and
// the EVM rule set.
//
// An implementation must fail rather than open a chain whose head state is missing
// from db, as geth's NewBlockChain does. The caller checks only the genesis state,
// which is all it can know about; every state above genesis belongs to the chain.
//
// This is the seam that survives: the implementation becomes a NewBlockChain call
// over the same arguments — qkc derives the genesis from config and root linkage,
// geth owns the chain.
type ChainService interface {
	New(db ethdb.Database, genesis *types.MinorBlock, chainConfig *params.ChainConfig) (ShardChain, error)
}

// Options is the injection-point surface downstream issues plug into. It carries
// a field only once a concrete consumer exists: an engine, master connection,
// miner and synchronizer all belong here eventually, but each arrives with the
// task that defines its interface, not as an empty placeholder.
type Options struct {
	Chain ChainService // nil means the stub; the geth-core shard-chain task supplies the real one
}

func (o Options) chainService() ChainService {
	if o.Chain != nil {
		return o.Chain
	}
	// TODO: production wiring must inject the real chain service (a geth
	// core.BlockChain over QKC minor blocks) once it exists; the stub is a test
	// seam until then.
	return StubChainService{}
}

// StubChainService satisfies the ShardChain seam without any execution, so the
// slave skeleton boots and is testable before the real chain lands. It runs no
// EVM and imports no blocks: it stands at the genesis block it was handed, whose
// state is already in the database, and stops cleanly.
type StubChainService struct{}

func (StubChainService) New(_ ethdb.Database, genesis *types.MinorBlock, _ *params.ChainConfig) (ShardChain, error) {
	return &stubChain{genesis: genesis.Hash()}, nil
}

type stubChain struct {
	genesis common.Hash
}

func (c *stubChain) GenesisHash() common.Hash    { return c.genesis }
func (c *stubChain) Head() (uint64, common.Hash) { return 0, c.genesis }
func (c *stubChain) Stop()                       {}
