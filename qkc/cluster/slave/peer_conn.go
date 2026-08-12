// Copyright 2026-2027, QuarkChain.

package slave

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// virtualTransport implements conn.FrameTransport for PeerConn. It has no TCP
// socket; inbound frames are pushed by the Dispatcher via receive(), and
// outbound frames are forwarded through the associated MasterConn.
type virtualTransport struct {
	clusterPeerID uint64
	branch        uint32
	masterConn    *MasterConn

	inbound    chan *wire.Frame
	closedChan chan struct{}
	closeOnce  sync.Once
	remoteAddr string
}

func newVirtualTransport(clusterPeerID uint64, branch uint32, masterConn *MasterConn) *virtualTransport {
	return &virtualTransport{
		clusterPeerID: clusterPeerID,
		branch:        branch,
		masterConn:    masterConn,
		inbound:       make(chan *wire.Frame, 64),
		closedChan:    make(chan struct{}),
		remoteAddr:    fmt.Sprintf("virtual://peer/%d/%d", clusterPeerID, branch),
	}
}

func (vt *virtualTransport) ReadFrame() (*wire.Frame, error) {
	select {
	case frame := <-vt.inbound:
		return frame, nil
	case <-vt.closedChan:
		return nil, conn.ErrConnectionClosed
	}
}

func (vt *virtualTransport) WriteFrame(f *wire.Frame) error {
	// PeerShardConnection in Python always writes with the shard branch and its
	// own cluster_peer_id so the master can route the frame back to the peer.
	f.Meta = wire.ClusterMetadata{
		Branch:        vt.branch,
		ClusterPeerID: vt.clusterPeerID,
	}
	return vt.masterConn.ForwardFrame(f)
}

func (vt *virtualTransport) Close() error {
	vt.closeOnce.Do(func() { close(vt.closedChan) })
	return nil
}

func (vt *virtualTransport) RemoteAddr() string {
	return vt.remoteAddr
}

// receive pushes a frame into the inbound queue. It returns false if the
// transport is already closed.
func (vt *virtualTransport) receive(frame *wire.Frame) bool {
	select {
	case vt.inbound <- frame:
		return true
	case <-vt.closedChan:
		return false
	}
}

// PeerConn is a virtual RPC channel representing the slave-side endpoint of a
// forwarded external peer connection. It does not own a TCP socket; all wire
// traffic is tunneled through the slave's MasterConn.
//
// It corresponds to Python's PeerShardConnection and shares the same
// responsibilities: independent RPC ID namespace, CommandOp handler dispatch,
// and lifecycle tied to master commands.
type PeerConn struct {
	*conn.BaseConn

	clusterPeerID uint64
	branch        uint32
	vt            *virtualTransport
}

// NewPeerConn creates a virtual peer connection for the given cluster_peer_id
// and branch, tunneling outbound frames through masterConn.
func NewPeerConn(clusterPeerID uint64, branch uint32, masterConn *MasterConn, logger log.Logger) *PeerConn {
	vt := newVirtualTransport(clusterPeerID, branch, masterConn)
	pc := &PeerConn{
		BaseConn:      conn.NewBaseConn(vt, logger),
		clusterPeerID: clusterPeerID,
		branch:        branch,
		vt:            vt,
	}
	pc.registerOpSerializers()
	pc.registerHandlers()
	return pc
}

// ReservedClusterPeerID is the reserved cluster_peer_id used by the master for
// its own control traffic. PeerConn must not use this value.
const ReservedClusterPeerID = 0

// registerOpSerializers registers serializers for the CommandOps that
// PeerShardConnection handles. Only shard-level opcodes are registered;
// master-only (root-level) opcodes are handled by Peer on the Master side and
// never reach PeerShardConnection.
//
// Python reference: PeerShardConnection uses OP_SERIALIZER_MAP for
// serialization but only OP_NONRPC_MAP + OP_RPC_MAP define what it actually
// handles. See quarkchain/cluster/shard.py.
func (pc *PeerConn) registerOpSerializers() {
	pc.BaseConn.RegisterOpSerializers(map[byte]*conn.OpSerializer{
		// Non-RPC commands (fire-and-forget). Response opcode mirrors the
		// command opcode (same convention as DestroyClusterPeerConnectionCommand).
		byte(wire.CommandOpNewMinorBlockHeaderList): conn.OpSerializerFor[wire.NewMinorBlockHeaderListCommand, wire.NewMinorBlockHeaderListCommand](byte(wire.CommandOpNewMinorBlockHeaderList)),
		byte(wire.CommandOpNewTransactionList):      conn.OpSerializerFor[wire.NewTransactionListCommand, wire.NewTransactionListCommand](byte(wire.CommandOpNewTransactionList)),
		byte(wire.CommandOpNewBlockMinor):           conn.OpSerializerFor[wire.NewBlockMinorCommand, wire.NewBlockMinorCommand](byte(wire.CommandOpNewBlockMinor)),

		// RPC request/response pairs. Matches PeerShardConnection.OP_RPC_MAP.
		byte(wire.CommandOpGetMinorBlockListRequest):               conn.OpSerializerFor[wire.GetMinorBlockListRequest, wire.GetMinorBlockListResponse](byte(wire.CommandOpGetMinorBlockListResponse)),
		byte(wire.CommandOpGetMinorBlockHeaderListRequest):         conn.OpSerializerFor[wire.GetMinorBlockHeaderListRequest, wire.GetMinorBlockHeaderListResponse](byte(wire.CommandOpGetMinorBlockHeaderListResponse)),
		byte(wire.CommandOpGetMinorBlockHeaderListWithSkipRequest): conn.OpSerializerFor[wire.GetMinorBlockHeaderListWithSkipRequest, wire.GetMinorBlockHeaderListResponse](byte(wire.CommandOpGetMinorBlockHeaderListWithSkipResponse)),
	})
}

// registerHandlers registers handlers for the shard-level CommandOps that
// PeerShardConnection handles. Master-only (root-level) opcodes (PING,
// GET_PEER_LIST_REQUEST, GET_ROOT_BLOCK_HEADER_LIST_REQUEST, etc.) are handled
// by Peer on the Master side and never reach PeerShardConnection — they are not
// registered here.
//
// Python reference: PeerShardConnection.OP_NONRPC_MAP + OP_RPC_MAP in
// quarkchain/cluster/shard.py.
func (pc *PeerConn) registerHandlers() {
	pc.BaseConn.RegisterTypedHandlers(map[byte]conn.TypedHandler{
		// Non-RPC commands (fire-and-forget). Python: OP_NONRPC_MAP.
		byte(wire.CommandOpNewMinorBlockHeaderList): pc.handleNewMinorBlockHeaderList,
		byte(wire.CommandOpNewTransactionList):      pc.handleNewTransactionList,
		byte(wire.CommandOpNewBlockMinor):           pc.handleNewBlockMinor,

		// RPC request handlers. Python: OP_RPC_MAP.
		byte(wire.CommandOpGetMinorBlockListRequest):               pc.handleGetMinorBlockListRequest,
		byte(wire.CommandOpGetMinorBlockHeaderListRequest):         pc.handleGetMinorBlockHeaderListRequest,
		byte(wire.CommandOpGetMinorBlockHeaderListWithSkipRequest): pc.handleGetMinorBlockHeaderListWithSkipRequest,
	})

	pc.BaseConn.RegisterNonRPCOps([]byte{
		byte(wire.CommandOpNewMinorBlockHeaderList),
		byte(wire.CommandOpNewTransactionList),
		byte(wire.CommandOpNewBlockMinor),
	})
}

// HandleFrame receives a frame routed by the Dispatcher. It enqueues the frame
// for the PeerConn read loop. Frames received after close are dropped.
func (pc *PeerConn) HandleFrame(frame *wire.Frame) error {
	if pc.Closed() {
		return conn.ErrConnectionClosed
	}
	if !pc.vt.receive(frame) {
		return conn.ErrConnectionClosed
	}
	return nil
}

// ClusterPeerID returns the peer's cluster-scoped identifier.
func (pc *PeerConn) ClusterPeerID() uint64 { return pc.clusterPeerID }

// Branch returns the shard branch this virtual connection serves.
func (pc *PeerConn) Branch() uint32 { return pc.branch }

// ── Non-RPC stubs ────────────────────────────────────────────────────────────

func (pc *PeerConn) handleNewMinorBlockHeaderList(req any) (any, error) {
	_ = req.(*wire.NewMinorBlockHeaderListCommand)
	// TODO: delegate to shard synchronizer once Shard Runtime is ported.
	return nil, conn.ErrHandlerNotImplemented
}

func (pc *PeerConn) handleNewTransactionList(req any) (any, error) {
	_ = req.(*wire.NewTransactionListCommand)
	// TODO: delegate to shard tx pool once Shard Runtime is ported.
	return nil, conn.ErrHandlerNotImplemented
}

func (pc *PeerConn) handleNewBlockMinor(req any) (any, error) {
	_ = req.(*wire.NewBlockMinorCommand)
	// TODO: delegate to shard block processing once Shard Runtime is ported.
	return nil, conn.ErrHandlerNotImplemented
}

// ── Shard-level RPC stubs ────────────────────────────────────────────────────

func (pc *PeerConn) handleGetMinorBlockListRequest(req any) (any, error) {
	_ = req.(*wire.GetMinorBlockListRequest)
	// TODO: fetch blocks from shard state db once Shard Runtime is ported.
	return nil, conn.ErrHandlerNotImplemented
}

func (pc *PeerConn) handleGetMinorBlockHeaderListRequest(req any) (any, error) {
	_ = req.(*wire.GetMinorBlockHeaderListRequest)
	// TODO: fetch headers from shard state db once Shard Runtime is ported.
	return nil, conn.ErrHandlerNotImplemented
}

func (pc *PeerConn) handleGetMinorBlockHeaderListWithSkipRequest(req any) (any, error) {
	_ = req.(*wire.GetMinorBlockHeaderListWithSkipRequest)
	// TODO: fetch headers from shard state db once Shard Runtime is ported.
	return nil, conn.ErrHandlerNotImplemented
}
