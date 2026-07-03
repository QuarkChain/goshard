// Copyright 2026-2027, QuarkChain.

package shard

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

// ShardChain is the named boundary between the slave skeleton and the geth-core
// shard chain (a geth core.BlockChain, the same component as
// qkc/core.MinorBlockChain), which is delivered by a separate task. It exposes
// only what the slave needs and is expected to co-evolve with that task.
type ShardChain interface {
	// GenesisHash is the chain's genesis block hash, recorded in the genesis
	// metadata and compared on reopen.
	GenesisHash() common.Hash
	// Head is the current chain head.
	Head() (height uint64, hash common.Hash)
	// Stop shuts the chain down, blocking until its background work has drained.
	Stop()
}

// ChainService constructs the ShardChain for one shard from its isolated chaindb
// and genesis descriptor.
type ChainService interface {
	New(db ethdb.Database, genesis *Genesis) (ShardChain, error)
}

// Placeholder injection points for downstream issues. Each is an empty interface
// today; the concrete surface arrives with the referenced issue.
type (
	Engine       interface{} // filled by #4 (real PoW / PoSW); the concrete interface is chosen by the chain task
	MasterConn   interface{} // filled by #5 (cluster wire protocol)
	Miner        interface{} // filled by #4 (miner)
	Synchronizer interface{} // filled by later sync work
)

// Options is the injection-point surface downstream issues plug into.
type Options struct {
	Chain        ChainService // nil means the stub; the geth-core shard-chain task supplies the real one
	Engine       Engine       // nil today; #4
	MasterConn   MasterConn   // nil today; #5
	Miner        Miner        // nil today; #4
	Synchronizer Synchronizer // nil today
}

func (o Options) chainService() ChainService {
	if o.Chain != nil {
		return o.Chain
	}
	return StubChainService{}
}

// StubChainService satisfies the ShardChain seam without any execution, so the
// slave skeleton boots and is testable before the real chain lands. The stub
// builds no state: it reports the genesis descriptor's identity at head height 0
// and stops cleanly.
type StubChainService struct{}

func (StubChainService) New(_ ethdb.Database, genesis *Genesis) (ShardChain, error) {
	return &stubChain{genesis: genesis.Fingerprint()}, nil
}

type stubChain struct {
	genesis common.Hash
}

func (c *stubChain) GenesisHash() common.Hash    { return c.genesis }
func (c *stubChain) Head() (uint64, common.Hash) { return 0, c.genesis }
func (c *stubChain) Stop()                       {}
