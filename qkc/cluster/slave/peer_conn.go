// Copyright 2026-2027, QuarkChain.

package slave

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// virtualTransport implements frameTransport for PeerConn. It has no TCP
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

func (vt *virtualTransport) readFrame() (*wire.Frame, error) {
	select {
	case frame := <-vt.inbound:
		return frame, nil
	case <-vt.closedChan:
		return nil, ErrConnectionClosed
	}
}

func (vt *virtualTransport) writeFrame(f *wire.Frame) error {
	// PeerShardConnection in Python always writes with the shard branch and its
	// own cluster_peer_id so the master can route the frame back to the peer.
	f.Meta = wire.ClusterMetadata{
		Branch:        vt.branch,
		ClusterPeerID: vt.clusterPeerID,
	}
	return vt.masterConn.ForwardFrame(f)
}

func (vt *virtualTransport) close() error {
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
	*rpcConn

	clusterPeerID uint64
	branch        uint32
	vt            *virtualTransport
}

// NewPeerConn creates a virtual peer connection for the given cluster_peer_id
// and branch, tunneling outbound frames through masterConn.
func NewPeerConn(clusterPeerID uint64, branch uint32, masterConn *MasterConn, logger log.Logger) *PeerConn {
	vt := newVirtualTransport(clusterPeerID, branch, masterConn)
	pc := &PeerConn{
		rpcConn:       newRPCConn(vt, logger),
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

// registerOpSerializers registers serializers for every CommandOp so that both
// inbound requests and outbound responses can be (de)serialized.
func (pc *PeerConn) registerOpSerializers() {
	pc.rpcConn.RegisterOpSerializers(map[byte]*OpSerializer{
		// §1 Hello / master-only
		byte(wire.CommandOpHello):                          OpSerializerFor[wire.HelloCommand, wire.HelloCommand](),
		byte(wire.CommandOpNewMinorBlockHeaderList):        OpSerializerFor[wire.NewMinorBlockHeaderListCommand, wire.NewMinorBlockHeaderListCommand](),
		byte(wire.CommandOpNewTransactionList):             OpSerializerFor[wire.NewTransactionListCommand, wire.NewTransactionListCommand](),
		byte(wire.CommandOpGetPeerListRequest):             OpSerializerFor[wire.GetPeerListRequest, wire.GetPeerListResponse](),
		byte(wire.CommandOpGetPeerListResponse):            OpSerializerFor[wire.GetPeerListResponse, wire.GetPeerListRequest](),
		byte(wire.CommandOpGetRootBlockHeaderListRequest):  OpSerializerFor[wire.GetRootBlockHeaderListRequest, wire.GetRootBlockHeaderListResponse](),
		byte(wire.CommandOpGetRootBlockHeaderListResponse): OpSerializerFor[wire.GetRootBlockHeaderListResponse, wire.GetRootBlockHeaderListRequest](),
		byte(wire.CommandOpGetRootBlockListRequest):        OpSerializerFor[wire.GetRootBlockListRequest, wire.GetRootBlockListResponse](),
		byte(wire.CommandOpGetRootBlockListResponse):       OpSerializerFor[wire.GetRootBlockListResponse, wire.GetRootBlockListRequest](),

		// §2 Slave RPC request/response pairs
		byte(wire.CommandOpGetMinorBlockListRequest):        OpSerializerFor[wire.GetMinorBlockListRequest, wire.GetMinorBlockListResponse](),
		byte(wire.CommandOpGetMinorBlockListResponse):       OpSerializerFor[wire.GetMinorBlockListResponse, wire.GetMinorBlockListRequest](),
		byte(wire.CommandOpGetMinorBlockHeaderListRequest):  OpSerializerFor[wire.GetMinorBlockHeaderListRequest, wire.GetMinorBlockHeaderListResponse](),
		byte(wire.CommandOpGetMinorBlockHeaderListResponse): OpSerializerFor[wire.GetMinorBlockHeaderListResponse, wire.GetMinorBlockHeaderListRequest](),

		// §3 More master-only / root-chain peer opcodes
		byte(wire.CommandOpNewBlockMinor):                           OpSerializerFor[wire.NewBlockMinorCommand, wire.NewBlockMinorCommand](),
		byte(wire.CommandOpPing):                                    OpSerializerFor[wire.PingPongCommand, wire.PingPongCommand](),
		byte(wire.CommandOpPong):                                    OpSerializerFor[wire.PingPongCommand, wire.PingPongCommand](),
		byte(wire.CommandOpGetRootBlockHeaderListWithSkipRequest):   OpSerializerFor[wire.GetRootBlockHeaderListWithSkipRequest, wire.GetRootBlockHeaderListResponse](),
		byte(wire.CommandOpGetRootBlockHeaderListWithSkipResponse):  OpSerializerFor[wire.GetRootBlockHeaderListResponse, wire.GetRootBlockHeaderListWithSkipRequest](),
		byte(wire.CommandOpNewRootBlock):                            OpSerializerFor[wire.NewRootBlockCommand, wire.NewRootBlockCommand](),
		byte(wire.CommandOpGetMinorBlockHeaderListWithSkipRequest):  OpSerializerFor[wire.GetMinorBlockHeaderListWithSkipRequest, wire.GetMinorBlockHeaderListResponse](),
		byte(wire.CommandOpGetMinorBlockHeaderListWithSkipResponse): OpSerializerFor[wire.GetMinorBlockHeaderListResponse, wire.GetMinorBlockHeaderListWithSkipRequest](),
	})
}

// registerHandlers registers the shard-level peer handlers. These are stubs
// because PR6 does not implement shard runtime / block processing.
func (pc *PeerConn) registerHandlers() {
	pc.rpcConn.RegisterTypedHandlers(map[byte]TypedHandler{
		// Non-RPC commands (fire-and-forget).
		byte(wire.CommandOpNewMinorBlockHeaderList): pc.handleNewMinorBlockHeaderList,
		byte(wire.CommandOpNewTransactionList):      pc.handleNewTransactionList,
		byte(wire.CommandOpNewBlockMinor):           pc.handleNewBlockMinor,

		// RPC requests; responses use opcode+1.
		byte(wire.CommandOpGetMinorBlockListRequest):               pc.handleGetMinorBlockListRequest,
		byte(wire.CommandOpGetMinorBlockHeaderListRequest):         pc.handleGetMinorBlockHeaderListRequest,
		byte(wire.CommandOpGetMinorBlockHeaderListWithSkipRequest): pc.handleGetMinorBlockHeaderListWithSkipRequest,
	})

	pc.rpcConn.RegisterNonRPCOps([]byte{
		byte(wire.CommandOpNewMinorBlockHeaderList),
		byte(wire.CommandOpNewTransactionList),
		byte(wire.CommandOpNewBlockMinor),
	})
}

// HandleFrame receives a frame routed by the Dispatcher. It enqueues the frame
// for the PeerConn read loop. Frames received after close are dropped.
func (pc *PeerConn) HandleFrame(frame *wire.Frame) error {
	if pc.Closed() {
		return ErrConnectionClosed
	}
	if !pc.vt.receive(frame) {
		return ErrConnectionClosed
	}
	return nil
}

// ClusterPeerID returns the peer's cluster-scoped identifier.
func (pc *PeerConn) ClusterPeerID() uint64 { return pc.clusterPeerID }

// Branch returns the shard branch this virtual connection serves.
func (pc *PeerConn) Branch() uint32 { return pc.branch }

// ── stub handlers ────────────────────────────────────────────────────────────

func (pc *PeerConn) handleNewMinorBlockHeaderList(req any) (any, error) {
	_ = req.(*wire.NewMinorBlockHeaderListCommand)
	// TODO: delegate to shard synchronizer once Shard Runtime is ported.
	return nil, nil
}

func (pc *PeerConn) handleNewTransactionList(req any) (any, error) {
	_ = req.(*wire.NewTransactionListCommand)
	// TODO: delegate to shard tx pool once Shard Runtime is ported.
	return nil, nil
}

func (pc *PeerConn) handleNewBlockMinor(req any) (any, error) {
	_ = req.(*wire.NewBlockMinorCommand)
	// TODO: delegate to shard block processing once Shard Runtime is ported.
	return nil, nil
}

func (pc *PeerConn) handleGetMinorBlockListRequest(req any) (any, error) {
	_ = req.(*wire.GetMinorBlockListRequest)
	// TODO: fetch blocks from shard state db once Shard Runtime is ported.
	return &wire.GetMinorBlockListResponse{MinorBlockList: []*wire.RawBytes{}}, nil
}

func (pc *PeerConn) handleGetMinorBlockHeaderListRequest(req any) (any, error) {
	_ = req.(*wire.GetMinorBlockHeaderListRequest)
	// TODO: fetch headers from shard state db once Shard Runtime is ported.
	return &wire.GetMinorBlockHeaderListResponse{
		RootTip:         nil,
		ShardTip:        nil,
		BlockHeaderList: []*wire.RawBytes{},
	}, nil
}

func (pc *PeerConn) handleGetMinorBlockHeaderListWithSkipRequest(req any) (any, error) {
	_ = req.(*wire.GetMinorBlockHeaderListWithSkipRequest)
	// TODO: fetch headers from shard state db once Shard Runtime is ported.
	return &wire.GetMinorBlockHeaderListResponse{
		RootTip:         nil,
		ShardTip:        nil,
		BlockHeaderList: []*wire.RawBytes{},
	}, nil
}
