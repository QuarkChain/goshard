package cluster

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
)

// FullShardID identifies a shard by its chain and shard index.
type FullShardID struct {
	ChainID uint32
	ShardID uint32
}

// XshardPool manages a pool of connections to other slave nodes.
// It is indexed by FullShardID to route xshard traffic to the correct target.
//
// When the master sends CONNECT_TO_SLAVES_REQUEST, the slave populates this pool.
type XshardPool struct {
	mu    sync.RWMutex
	conns map[FullShardID][]*XshardConn
	log   log.Logger
}

// NewXshardPool creates a new connection pool.
func NewXshardPool(logger log.Logger) *XshardPool {
	return &XshardPool{
		conns: make(map[FullShardID][]*XshardConn),
		log:   logger,
	}
}

// Add adds a connection to the pool, indexed by the target shard.
func (p *XshardPool) Add(target FullShardID, conn *XshardConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.conns[target] = append(p.conns[target], conn)
	p.log.Info("added xshard connection", "target", target, "remote_addr", conn.RemoteAddr())
}

// Get returns all connections to a specific target shard.
func (p *XshardPool) Get(target FullShardID) []*XshardConn {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.conns[target]
}

// Remove removes a specific connection from the pool.
func (p *XshardPool) Remove(target FullShardID, conn *XshardConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	conns := p.conns[target]
	for i, c := range conns {
		if c == conn {
			p.conns[target] = append(conns[:i], conns[i+1:]...)
			p.log.Info("removed xshard connection", "target", target)
			return
		}
	}
}

// RemoveTarget removes all connections to a specific target shard.
func (p *XshardPool) RemoveTarget(target FullShardID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.conns, target)
	p.log.Info("removed all connections to target", "target", target)
}

// SendXshardTx sends xshard transactions to the target shard.
func (p *XshardPool) SendXshardTx(target FullShardID, branch uint32, payload []byte) error {
	conns := p.Get(target)
	if len(conns) == 0 {
		return fmt.Errorf("no connections to target %v", target)
	}
	return conns[0].SendXshardTxList(branch, payload)
}

// Close closes all connections in the pool.
func (p *XshardPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for target, conns := range p.conns {
		for _, conn := range conns {
			conn.Close()
		}
		p.log.Info("closed connections to target", "target", target)
	}
	p.conns = nil
}

// Size returns the number of connections in the pool.
func (p *XshardPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	total := 0
	for _, conns := range p.conns {
		total += len(conns)
	}
	return total
}

// Targets returns all target shards in the pool.
func (p *XshardPool) Targets() []FullShardID {
	p.mu.RLock()
	defer p.mu.RUnlock()

	targets := make([]FullShardID, 0, len(p.conns))
	for target := range p.conns {
		targets = append(targets, target)
	}
	return targets
}
