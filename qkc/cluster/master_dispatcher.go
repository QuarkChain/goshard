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
	peerConns  map[uint64]*PeerConn // cluster_peer_id → PeerConn
	log        log.Logger
}

// NewDispatcher creates a new dispatcher for the given master connection.
func NewDispatcher(masterConn *MasterConn, logger log.Logger) *Dispatcher {
	return &Dispatcher{
		masterConn: masterConn,
		peerConns:  make(map[uint64]*PeerConn),
		log:        logger,
	}
}

// AddPeerConn registers a PeerConn for a specific cluster_peer_id.
func (d *Dispatcher) AddPeerConn(conn *PeerConn) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.peerConns[conn.ClusterPeerID()] = conn
	d.log.Info("added peer connection",
		"cluster_peer_id", conn.ClusterPeerID(),
		"branch", conn.Branch())
}

// RemovePeerConn removes a PeerConn from the dispatcher.
func (d *Dispatcher) RemovePeerConn(clusterPeerID uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.peerConns, clusterPeerID)
	d.log.Info("removed peer connection", "cluster_peer_id", clusterPeerID)
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
	conn, ok := d.peerConns[frame.Meta.ClusterPeerID]
	d.mu.RUnlock()

	if !ok {
		d.log.Warn("no peer connection for cluster_peer_id",
			"cluster_peer_id", frame.Meta.ClusterPeerID)
		return
	}

	if conn.Branch() != frame.Meta.Branch {
		d.log.Warn("branch mismatch in peer frame",
			"conn_branch", conn.Branch(), "frame_branch", frame.Meta.Branch)
		return
	}

	conn.HandleFrame(frame)
}

// Close closes all peer connections.
func (d *Dispatcher) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, conn := range d.peerConns {
		conn.Close()
	}
	d.peerConns = nil
}

// PeerConnCount returns the number of active peer connections.
func (d *Dispatcher) PeerConnCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.peerConns)
}
