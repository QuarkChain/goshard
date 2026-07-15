// Copyright 2026-2027, QuarkChain.

package slave

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// Dispatcher routes intra-cluster frames that carry a non-zero cluster_peer_id
// to the corresponding virtual PeerConn. Frames with cluster_peer_id == 0 are
// left for MasterConn to handle.
//
// It owns the registry of PeerConns as a two-layer map:
//
//	cluster_peer_id -> branch -> *PeerConn
//
// This matches Python's MasterConnection.v_conn_map and shard.peers layout.
type Dispatcher struct {
	mu    sync.RWMutex
	peers map[uint64]map[uint32]*PeerConn
	log   log.Logger
}

// NewDispatcher creates an empty dispatcher.
func NewDispatcher(logger log.Logger) *Dispatcher {
	if logger == nil {
		logger = log.Root()
	}
	return &Dispatcher{
		peers: make(map[uint64]map[uint32]*PeerConn),
		log:   logger,
	}
}

// Register adds an already-created PeerConn to the registry. It returns an
// error if a PeerConn for the same cluster_peer_id and branch already exists.
func (d *Dispatcher) Register(pc *PeerConn) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	branchMap, ok := d.peers[pc.ClusterPeerID()]
	if !ok {
		branchMap = make(map[uint32]*PeerConn)
		d.peers[pc.ClusterPeerID()] = branchMap
	}
	if _, exists := branchMap[pc.Branch()]; exists {
		return fmt.Errorf("peer connection already exists for cluster_peer_id %d branch %d", pc.ClusterPeerID(), pc.Branch())
	}
	branchMap[pc.Branch()] = pc
	return nil
}

// Unregister removes a single PeerConn from the registry. It returns the
// removed PeerConn (if any) without closing it.
func (d *Dispatcher) Unregister(clusterPeerID uint64, branch uint32) *PeerConn {
	d.mu.Lock()
	defer d.mu.Unlock()

	branchMap, ok := d.peers[clusterPeerID]
	if !ok {
		return nil
	}
	pc := branchMap[branch]
	delete(branchMap, branch)
	if len(branchMap) == 0 {
		delete(d.peers, clusterPeerID)
	}
	return pc
}

// CreatePeerConns creates and starts one PeerConn per branch for the given
// cluster_peer_id, using masterConn as the transport. Existing branch entries
// are skipped (logged as duplicates), matching Python's behavior.
func (d *Dispatcher) CreatePeerConns(clusterPeerID uint64, branches []uint32, masterConn *MasterConn, logger log.Logger) {
	if clusterPeerID == ReservedClusterPeerID {
		d.log.Error("refusing to create peer connection with reserved cluster_peer_id", "cluster_peer_id", clusterPeerID)
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	branchMap, ok := d.peers[clusterPeerID]
	if !ok {
		branchMap = make(map[uint32]*PeerConn)
		d.peers[clusterPeerID] = branchMap
	}

	for _, branch := range branches {
		if _, exists := branchMap[branch]; exists {
			d.log.Warn("duplicate create cluster peer connection", "cluster_peer_id", clusterPeerID, "branch", branch)
			continue
		}
		pc := NewPeerConn(clusterPeerID, branch, masterConn, logger)
		pc.Start()
		branchMap[branch] = pc
	}
}

// DestroyPeerConns removes all PeerConns for clusterPeerID from the registry
// and closes them. Missing entries are silently ignored.
func (d *Dispatcher) DestroyPeerConns(clusterPeerID uint64) {
	d.mu.Lock()
	branchMap, ok := d.peers[clusterPeerID]
	if ok {
		delete(d.peers, clusterPeerID)
	}
	d.mu.Unlock()

	if !ok {
		return
	}
	for _, pc := range branchMap {
		pc.Close()
	}
}

// RouteFrame is the forwarder callback installed on MasterConn. It returns
// false for master-local traffic (cluster_peer_id == 0) so MasterConn handles
// the frame normally. For peer traffic it looks up the PeerConn, enqueues the
// frame if found, or drops it (matching Python's NULL_CONNECTION) and logs a
// warning if not found.
func (d *Dispatcher) RouteFrame(frame *wire.Frame) bool {
	if frame.Meta.ClusterPeerID == 0 {
		return false
	}

	d.mu.RLock()
	branchMap, ok := d.peers[frame.Meta.ClusterPeerID]
	if !ok {
		d.mu.RUnlock()
		d.log.Warn("no peer connection for cluster_peer_id", "cluster_peer_id", frame.Meta.ClusterPeerID)
		return true
	}
	pc, ok := branchMap[frame.Meta.Branch]
	d.mu.RUnlock()

	if !ok {
		d.log.Warn("no peer connection for branch", "cluster_peer_id", frame.Meta.ClusterPeerID, "branch", frame.Meta.Branch)
		return true
	}

	if err := pc.HandleFrame(frame); err != nil {
		d.log.Warn("failed to deliver frame to peer connection", "cluster_peer_id", frame.Meta.ClusterPeerID, "branch", frame.Meta.Branch, "err", err)
	}
	return true
}

// Close closes all registered PeerConns and clears the registry.
func (d *Dispatcher) Close() error {
	d.mu.Lock()
	all := make([]*PeerConn, 0)
	for _, branchMap := range d.peers {
		for _, pc := range branchMap {
			all = append(all, pc)
		}
	}
	d.peers = make(map[uint64]map[uint32]*PeerConn)
	d.mu.Unlock()

	for _, pc := range all {
		pc.Close()
	}
	return nil
}
