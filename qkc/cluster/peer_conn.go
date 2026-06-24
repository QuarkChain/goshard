package cluster

import (
	"sync"

	"github.com/ethereum/go-ethereum/log"
)

// PeerConn represents a virtual connection to an external cluster peer
// for a specific shard. It multiplexes P2P traffic over the master connection,
// identified by cluster_peer_id in the frame metadata.
//
// When an external peer wants to communicate with a shard:
//  1. Master assigns a cluster_peer_id (derived from peer's node ID hash)
//  2. Master sends CREATE_CLUSTER_PEER_CONNECTION_REQUEST to each slave
//  3. Slave creates a PeerConn for each local shard
//  4. Traffic is routed by cluster_peer_id in the frame metadata
type PeerConn struct {
	clusterPeerID uint64
	branch        uint32
	masterConn    *MasterConn // backing master connection (for sending)

	handlersMu sync.RWMutex
	handlers   map[byte]func(*Frame) ([]byte, error)

	log     log.Logger
	closed  bool
	closeMu sync.Mutex
}

// NewPeerConn creates a new virtual peer connection.
func NewPeerConn(clusterPeerID uint64, branch uint32, masterConn *MasterConn, logger log.Logger) *PeerConn {
	return &PeerConn{
		clusterPeerID: clusterPeerID,
		branch:        branch,
		masterConn:    masterConn,
		handlers:      make(map[byte]func(*Frame) ([]byte, error)),
		log:           logger,
	}
}

// RegisterHandler registers a handler for a specific peer command opcode.
func (p *PeerConn) RegisterHandler(opcode byte, handler func(*Frame) ([]byte, error)) {
	p.handlersMu.Lock()
	p.handlers[opcode] = handler
	p.handlersMu.Unlock()
}

// HandleFrame processes an inbound frame from this peer.
func (p *PeerConn) HandleFrame(frame *Frame) {
	// Check if connection is closed
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return
	}
	p.closeMu.Unlock()

	if frame.Meta.ClusterPeerID != p.clusterPeerID {
		p.log.Warn("frame cluster_peer_id mismatch",
			"expected", p.clusterPeerID, "got", frame.Meta.ClusterPeerID)
		return
	}

	p.handlersMu.RLock()
	handler, ok := p.handlers[frame.Opcode]
	p.handlersMu.RUnlock()

	if !ok {
		p.log.Warn("no handler for peer opcode", "opcode", frame.Opcode)
		p.sendEmptyResponse(frame, "no-handler")
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				p.log.Error("peer handler panic recovered", "opcode", frame.Opcode, "panic", r)
				p.sendEmptyResponse(frame, "panic")
			}
		}()

		respPayload, err := handler(frame)
		if err != nil {
			p.log.Error("peer handler failed", "opcode", frame.Opcode, "err", err)
			p.sendEmptyResponse(frame, "error")
			return
		}
		if frame.RPCID != 0 {
			resp := &Frame{
				Meta:    Metadata{Branch: p.branch, ClusterPeerID: p.clusterPeerID},
				Opcode:  frame.Opcode + 1, // response opcode = request opcode + 1
				RPCID:   frame.RPCID,
				Payload: respPayload,
			}
			if err := p.SendFrame(resp); err != nil {
				p.log.Error("failed to send peer response", "err", err)
			}
		}
	}()
}

// sendEmptyResponse sends an empty (nil-payload) response for error/no-handler/panic cases.
func (p *PeerConn) sendEmptyResponse(frame *Frame, reason string) {
	if frame.RPCID == 0 {
		return
	}
	resp := &Frame{
		Meta:    Metadata{Branch: p.branch, ClusterPeerID: p.clusterPeerID},
		Opcode:  frame.Opcode + 1,
		RPCID:   frame.RPCID,
		Payload: nil,
	}
	if err := p.SendFrame(resp); err != nil {
		p.log.Error("failed to send response", "reason", reason, "err", err)
	}
}

// SendFrame sends a frame to the peer via the master connection.
func (p *PeerConn) SendFrame(frame *Frame) error {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return ErrConnectionClosed
	}
	p.closeMu.Unlock()

	frame.Meta.Branch = p.branch
	frame.Meta.ClusterPeerID = p.clusterPeerID
	return p.masterConn.WriteFrame(frame)
}

// SendCommand sends a fire-and-forget command to the peer.
func (p *PeerConn) SendCommand(opcode byte, payload []byte) error {
	return p.SendFrame(&Frame{
		Meta:    Metadata{Branch: p.branch, ClusterPeerID: p.clusterPeerID},
		Opcode:  opcode,
		RPCID:   0,
		Payload: payload,
	})
}

// Close closes the virtual connection.
func (p *PeerConn) Close() {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	p.closed = true
}

// ClusterPeerID returns the peer identifier.
func (p *PeerConn) ClusterPeerID() uint64 { return p.clusterPeerID }

// Branch returns the shard branch this connection serves.
func (p *PeerConn) Branch() uint32 { return p.branch }
