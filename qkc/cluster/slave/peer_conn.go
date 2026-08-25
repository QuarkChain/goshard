// Copyright 2026-2027, QuarkChain.

package slave

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// PeerRuntime is MasterConn's required dependency on the runtime that owns the
// shards and the peer registry (Python: MasterConnection.slave_server,
// slave.py:108). "No shards created yet" is expressed by an empty shard set
// inside the runtime, never by a nil PeerRuntime. A future SlaveService
// implements it; tests use a fake.
type PeerRuntime interface {
	// CreatePeerConns establishes PeerConns on the runtime's created shards.
	CreatePeerConns(clusterPeerID uint64)
	// DestroyPeerConns removes and closes every PeerConn of clusterPeerID.
	DestroyPeerConns(clusterPeerID uint64)
	// LookupPeer returns the active PeerConn for (cid, branch), or nil.
	LookupPeer(clusterPeerID uint64, branch uint32) *PeerConn
	// CloseAllPeers closes every PeerConn (master shutdown cascade).
	CloseAllPeers()
}

// PeerHandler is the business boundary between PeerConn and the Shard layer,
// mirroring XshardHandler (xshard_conn.go): PeerConn carries no business logic.
// A nil handler returns ErrHandlerNotImplemented; a future Shard runtime
// implements it. Outbound broadcasts use BaseConn.SendCommandMeta/SendRPCMeta
// directly and are not part of this interface.
type PeerHandler interface {
	// Non-RPC commands (fire-and-forget, rpc_id = 0)
	NewMinorBlockHeaderList(req *wire.NewMinorBlockHeaderListCommand) error
	NewTransactionList(req *wire.NewTransactionListCommand) error
	NewBlockMinor(req *wire.NewBlockMinorCommand) error
	// RPC requests (return a response)
	GetMinorBlockHeaderList(req *wire.GetMinorBlockHeaderListRequest) (*wire.GetMinorBlockHeaderListResponse, error)
	GetMinorBlockList(req *wire.GetMinorBlockListRequest) (*wire.GetMinorBlockListResponse, error)
	GetMinorBlockHeaderListWithSkip(req *wire.GetMinorBlockHeaderListWithSkipRequest) (*wire.GetMinorBlockHeaderListResponse, error)
}

// virtualTransport implements conn.FrameTransport for PeerConn: no real socket;
// inbound frames are pushed by MasterConn's reader via receive() into an
// unbounded FIFO (Python read_deque+read_event), outbound frames are stamped
// with this peer's (branch, cluster_peer_id) and forwarded through MasterConn.
type virtualTransport struct {
	clusterPeerID uint64
	branch        uint32
	masterConn    *MasterConn
	remoteAddr    string

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []*wire.Frame
	closed bool
}

func newVirtualTransport(clusterPeerID uint64, branch uint32, masterConn *MasterConn) *virtualTransport {
	vt := &virtualTransport{
		clusterPeerID: clusterPeerID,
		branch:        branch,
		masterConn:    masterConn,
		remoteAddr:    fmt.Sprintf("virtual://peer/%d/%d", clusterPeerID, branch),
	}
	vt.cond = sync.NewCond(&vt.mu)
	return vt
}

// ReadFrame blocks until a frame is queued or the transport is closed.
func (vt *virtualTransport) ReadFrame() (*wire.Frame, error) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	for len(vt.queue) == 0 && !vt.closed {
		vt.cond.Wait()
	}
	if len(vt.queue) == 0 && vt.closed {
		return nil, conn.ErrConnectionClosed
	}

	f := vt.queue[0]
	vt.queue[0] = nil // release the reference
	vt.queue = vt.queue[1:]
	return f, nil
}

// WriteFrame stamps the peer metadata and forwards through the master, sharing
// MasterConn's writeMu with all other writers.
func (vt *virtualTransport) WriteFrame(f *wire.Frame) error {
	f.Meta = wire.ClusterMetadata{
		Branch:        vt.branch,
		ClusterPeerID: vt.clusterPeerID,
	}
	return vt.masterConn.WriteFrame(f)
}

// Close unblocks any pending ReadFrame and drops further receive() calls.
func (vt *virtualTransport) Close() error {
	vt.mu.Lock()
	if !vt.closed {
		vt.closed = true
		vt.cond.Broadcast()
	}
	vt.mu.Unlock()
	return nil
}

func (vt *virtualTransport) RemoteAddr() string {
	return vt.remoteAddr
}

// receive enqueues a frame without blocking; returns false if already closed.
func (vt *virtualTransport) receive(frame *wire.Frame) bool {
	vt.mu.Lock()
	if vt.closed {
		vt.mu.Unlock()
		return false
	}
	vt.queue = append(vt.queue, frame)
	vt.cond.Signal()
	vt.mu.Unlock()
	return true
}

// PeerConn is the slave-side virtual endpoint of a forwarded peer connection
// (Python: PeerShardConnection). All wire traffic tunnels through MasterConn;
// it keeps an independent RPC ID namespace and carries no business logic —
// business handling is injected via PeerHandler.
type PeerConn struct {
	*conn.BaseConn

	clusterPeerID uint64
	branch        uint32
	vt            *virtualTransport
}

// NewPeerConn is the exported construction API (used by the future runtime and
// tests). handler is the injected business boundary; nil keeps the agreed
// behavior of returning conn.ErrHandlerNotImplemented when a business command
// arrives.
func NewPeerConn(clusterPeerID uint64, branch uint32, masterConn *MasterConn, handler PeerHandler, logger log.Logger) *PeerConn {
	vt := newVirtualTransport(clusterPeerID, branch, masterConn)
	pc := &PeerConn{
		clusterPeerID: clusterPeerID,
		branch:        branch,
		vt:            vt,
	}

	pc.BaseConn = conn.NewBaseConn(conn.Config{
		Transport: vt,
		// Only shard-level CommandOps are registered here; root-level opcodes
		// are handled by Peer on the Master side (Python: OP_SERIALIZER_MAP).
		Serializers: map[byte]*conn.OpSerializer{
			// Non-RPC commands (fire-and-forget). Response opcode mirrors the
			// command opcode (same convention as DestroyClusterPeerConnectionCommand).
			byte(wire.CommandOpNewMinorBlockHeaderList): conn.OpSerializerFor[wire.NewMinorBlockHeaderListCommand, wire.NewMinorBlockHeaderListCommand](byte(wire.CommandOpNewMinorBlockHeaderList)),
			byte(wire.CommandOpNewTransactionList):      conn.OpSerializerFor[wire.NewTransactionListCommand, wire.NewTransactionListCommand](byte(wire.CommandOpNewTransactionList)),
			byte(wire.CommandOpNewBlockMinor):           conn.OpSerializerFor[wire.NewBlockMinorCommand, wire.NewBlockMinorCommand](byte(wire.CommandOpNewBlockMinor)),
			// RPC request/response pairs. Matches PeerShardConnection.OP_RPC_MAP.
			byte(wire.CommandOpGetMinorBlockListRequest):               conn.OpSerializerFor[wire.GetMinorBlockListRequest, wire.GetMinorBlockListResponse](byte(wire.CommandOpGetMinorBlockListResponse)),
			byte(wire.CommandOpGetMinorBlockHeaderListRequest):         conn.OpSerializerFor[wire.GetMinorBlockHeaderListRequest, wire.GetMinorBlockHeaderListResponse](byte(wire.CommandOpGetMinorBlockHeaderListResponse)),
			byte(wire.CommandOpGetMinorBlockHeaderListWithSkipRequest): conn.OpSerializerFor[wire.GetMinorBlockHeaderListWithSkipRequest, wire.GetMinorBlockHeaderListResponse](byte(wire.CommandOpGetMinorBlockHeaderListWithSkipResponse)),
		},
		Handlers: map[byte]conn.TypedHandler{
			byte(wire.CommandOpNewMinorBlockHeaderList): newNonRPCHandler(handler, func(h PeerHandler, req *wire.NewMinorBlockHeaderListCommand) error {
				return h.NewMinorBlockHeaderList(req)
			}),
			byte(wire.CommandOpNewTransactionList): newNonRPCHandler(handler, func(h PeerHandler, req *wire.NewTransactionListCommand) error { return h.NewTransactionList(req) }),
			byte(wire.CommandOpNewBlockMinor):      newNonRPCHandler(handler, func(h PeerHandler, req *wire.NewBlockMinorCommand) error { return h.NewBlockMinor(req) }),
			byte(wire.CommandOpGetMinorBlockListRequest): newRPCHandler(handler, func(h PeerHandler, req *wire.GetMinorBlockListRequest) (*wire.GetMinorBlockListResponse, error) {
				return h.GetMinorBlockList(req)
			}),
			byte(wire.CommandOpGetMinorBlockHeaderListRequest): newRPCHandler(handler, func(h PeerHandler, req *wire.GetMinorBlockHeaderListRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
				return h.GetMinorBlockHeaderList(req)
			}),
			byte(wire.CommandOpGetMinorBlockHeaderListWithSkipRequest): newRPCHandler(handler, func(h PeerHandler, req *wire.GetMinorBlockHeaderListWithSkipRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
				return h.GetMinorBlockHeaderListWithSkip(req)
			}),
		},
		NonRPCOps: map[byte]struct{}{
			byte(wire.CommandOpNewMinorBlockHeaderList): {},
			byte(wire.CommandOpNewTransactionList):      {},
			byte(wire.CommandOpNewBlockMinor):           {},
		},
		Logger: logger,
	})
	return pc
}

// newNonRPCHandler / newRPCHandler adapt a PeerHandler method to a BaseConn
// handler; a nil handler yields ErrHandlerNotImplemented.
func newNonRPCHandler[R any](h PeerHandler, fn func(PeerHandler, R) error) conn.TypedHandler {
	return func(req any) (any, error) {
		if h == nil {
			return nil, conn.ErrHandlerNotImplemented
		}
		return nil, fn(h, req.(R))
	}
}

func newRPCHandler[R, S any](h PeerHandler, fn func(PeerHandler, R) (S, error)) conn.TypedHandler {
	return func(req any) (any, error) {
		if h == nil {
			return nil, conn.ErrHandlerNotImplemented
		}
		return fn(h, req.(R))
	}
}

// ReservedClusterPeerID is the reserved cluster_peer_id used by the master for
// its own control traffic. PeerConn must not use this value.
const ReservedClusterPeerID = 0

// HandleFrame enqueues a frame routed by the master for the PeerConn read loop;
// frames received after close are dropped.
func (pc *PeerConn) HandleFrame(frame *wire.Frame) error {
	if pc.IsClosed() {
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
