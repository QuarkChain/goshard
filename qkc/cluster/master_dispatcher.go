package cluster

import (
	"sync"

	"github.com/ethereum/go-ethereum/log"
)

// Dispatcher routes inbound frames from the master connection by cluster_peer_id.
//
// One physical TCP connection to master carries two logical traffic types:
//   - cluster_peer_id == 0 → cluster RPC (master commands)
//   - cluster_peer_id != 0 → peer-shard P2P (virtual connections)
//
// The Dispatcher is wired into MasterConn.OnFrame so every frame read from the
// TCP connection goes through Dispatch() before reaching any handler.
type Dispatcher struct {
	mu         sync.RWMutex
	masterConn *MasterConn
	peerConns  map[uint64]map[uint32]*PeerConn // cluster_peer_id → branch → PeerConn
	log        log.Logger
}

// NewDispatcher creates a new dispatcher for the given master connection.
func NewDispatcher(masterConn *MasterConn, logger log.Logger) *Dispatcher {
	return &Dispatcher{
		masterConn: masterConn,
		peerConns:  make(map[uint64]map[uint32]*PeerConn),
		log:        logger,
	}
}

// AddPeerConn registers a PeerConn for a specific cluster_peer_id and branch.
func (d *Dispatcher) AddPeerConn(conn *PeerConn) {
	d.mu.Lock()
	defer d.mu.Unlock()

	clusterPeerID := conn.ClusterPeerID()
	branch := conn.Branch()

	// Initialize inner map if needed
	if d.peerConns[clusterPeerID] == nil {
		d.peerConns[clusterPeerID] = make(map[uint32]*PeerConn)
	}

	// Close old connection if replacing
	if old, exists := d.peerConns[clusterPeerID][branch]; exists {
		old.Close()
		d.log.Warn("replacing existing peer connection",
			"cluster_peer_id", clusterPeerID,
			"branch", branch)
	}

	d.peerConns[clusterPeerID][branch] = conn
	d.log.Info("added peer connection",
		"cluster_peer_id", clusterPeerID,
		"branch", branch)
}

// RemovePeerConn removes all PeerConns for a specific cluster_peer_id.
func (d *Dispatcher) RemovePeerConn(clusterPeerID uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	branches, ok := d.peerConns[clusterPeerID]
	if !ok {
		return
	}

	for branch := range branches {
		d.log.Info("removed peer connection", "cluster_peer_id", clusterPeerID, "branch", branch)
	}
	delete(d.peerConns, clusterPeerID)
}

// Dispatch routes an inbound frame to the appropriate handler.
// This is set as the MasterConn.OnFrame callback.
func (d *Dispatcher) Dispatch(frame *Frame) {
	if frame.Meta.ClusterPeerID == 0 {
		// Master command → go to MasterConn handlers
		d.masterConn.Handle(frame)
		return
	}

	// Peer-shard P2P → go to PeerConn
	d.mu.RLock()
	branches, ok := d.peerConns[frame.Meta.ClusterPeerID]
	if !ok {
		d.mu.RUnlock()
		d.log.Warn("no peer connection for cluster_peer_id",
			"cluster_peer_id", frame.Meta.ClusterPeerID)
		return
	}

	conn, ok := branches[frame.Meta.Branch]
	d.mu.RUnlock()

	if !ok {
		d.log.Warn("no peer connection for cluster_peer_id and branch",
			"cluster_peer_id", frame.Meta.ClusterPeerID,
			"branch", frame.Meta.Branch)
		return
	}

	// HandleFrame's closeMu check provides safety; the PeerConn object
	// remains valid (Go GC) even if the map entry is concurrently replaced.
	conn.HandleFrame(frame)
}

// Close closes all peer connections.
func (d *Dispatcher) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for clusterPeerID, branches := range d.peerConns {
		for branch, conn := range branches {
			conn.Close()
			d.log.Info("closed peer connection", "cluster_peer_id", clusterPeerID, "branch", branch)
		}
	}
	d.peerConns = nil
}

// PeerConnCount returns the number of active peer connections.
func (d *Dispatcher) PeerConnCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	total := 0
	for _, branches := range d.peerConns {
		total += len(branches)
	}
	return total
}
