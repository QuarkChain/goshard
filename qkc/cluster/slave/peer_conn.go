// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// PeerHandler processes PeerConn's inbound commands and RPC requests. It is
// implemented by the business layer and injected at construction. Outbound
// sends are not part of this interface.
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

// WriteFrame stamps the routing metadata of this virtual peer connection
// and forwards the frame through the master's TCP connection. The virtual
// connection is created with a fixed branch and cluster peer ID, and every
// frame sent through it must be routed using that identity. This mirrors
// Python's PeerShardConnection.get_metadata_to_write(), which derives the
// metadata from the connection's branch and cluster peer ID.
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

// RemoteAddr returns the transport's remote address. virtualTransport has no
// real network peer (frames tunnel through MasterConn), so it reports an empty
// address.
func (vt *virtualTransport) RemoteAddr() string {
	return ""
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
	handler       PeerHandler
}

// NewPeerConn creates a PeerConn for peer clusterPeerID on branch, tunnelling
// all frames through masterConn. clusterPeerID must not be 0 because 0 is
// reserved for master-local traffic and a PeerConn represents peer traffic.
// The caller is responsible for calling Start().
func NewPeerConn(clusterPeerID uint64, branch uint32, masterConn *MasterConn, handler PeerHandler, logger log.Logger) (*PeerConn, error) {
	if masterConn == nil {
		return nil, errors.New("master connection must not be nil")
	}
	if handler == nil {
		return nil, errors.New("peer handler must not be nil")
	}
	if clusterPeerID == 0 {
		return nil, errors.New("cluster peer id must not be 0")
	}

	vt := newVirtualTransport(clusterPeerID, branch, masterConn)
	pc := &PeerConn{
		clusterPeerID: clusterPeerID,
		branch:        branch,
		vt:            vt,
		handler:       handler,
	}

	pc.BaseConn = conn.NewBaseConn(conn.Config{
		Transport: vt,
		// Only shard-level CommandOps are registered here; root-level opcodes
		// are handled by Peer on the Master side (Python: OP_SERIALIZER_MAP).
		Serializers: map[byte]*conn.OpSerializer{
			// Non-RPC commands (fire-and-forget, no response). The response
			// opcode argument is a placeholder: 0, since these commands never
			// produce a response (the opcode is only used for RPC pairs).
			byte(wire.CommandOpNewMinorBlockHeaderList): conn.OpSerializerFor[wire.NewMinorBlockHeaderListCommand, wire.NewMinorBlockHeaderListCommand](0),
			byte(wire.CommandOpNewTransactionList):      conn.OpSerializerFor[wire.NewTransactionListCommand, wire.NewTransactionListCommand](0),
			byte(wire.CommandOpNewBlockMinor):           conn.OpSerializerFor[wire.NewBlockMinorCommand, wire.NewBlockMinorCommand](0),
			// RPC request/response pairs. Matches PeerShardConnection.OP_RPC_MAP.
			byte(wire.CommandOpGetMinorBlockListRequest):               conn.OpSerializerFor[wire.GetMinorBlockListRequest, wire.GetMinorBlockListResponse](byte(wire.CommandOpGetMinorBlockListResponse)),
			byte(wire.CommandOpGetMinorBlockHeaderListRequest):         conn.OpSerializerFor[wire.GetMinorBlockHeaderListRequest, wire.GetMinorBlockHeaderListResponse](byte(wire.CommandOpGetMinorBlockHeaderListResponse)),
			byte(wire.CommandOpGetMinorBlockHeaderListWithSkipRequest): conn.OpSerializerFor[wire.GetMinorBlockHeaderListWithSkipRequest, wire.GetMinorBlockHeaderListResponse](byte(wire.CommandOpGetMinorBlockHeaderListWithSkipResponse)),
		},
		Handlers: map[byte]conn.TypedHandler{
			byte(wire.CommandOpNewMinorBlockHeaderList):                pc.handleNewMinorBlockHeaderList,
			byte(wire.CommandOpNewTransactionList):                     pc.handleNewTransactionList,
			byte(wire.CommandOpNewBlockMinor):                          pc.handleNewBlockMinor,
			byte(wire.CommandOpGetMinorBlockListRequest):               pc.handleGetMinorBlockList,
			byte(wire.CommandOpGetMinorBlockHeaderListRequest):         pc.handleGetMinorBlockHeaderList,
			byte(wire.CommandOpGetMinorBlockHeaderListWithSkipRequest): pc.handleGetMinorBlockHeaderListWithSkip,
		},
		NonRPCOps: map[byte]struct{}{
			byte(wire.CommandOpNewMinorBlockHeaderList): {},
			byte(wire.CommandOpNewTransactionList):      {},
			byte(wire.CommandOpNewBlockMinor):           {},
		},
		Logger: logger,
	})
	return pc, nil
}

// HandleFrame enqueues a frame routed by the master for the PeerConn read loop;
// frames received after close are dropped. It is a pure injection entry: it
// runs no business logic, does not block, and never closes MasterConn. Inbound
// frames are processed asynchronously by the PeerConn's own reader loop, where
// PeerHandler panics are recovered and close only this PeerConn.
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

// ── Outbound typed helpers ────────────────────────────────────────────────
//
// Each helper serializes a typed message and sends it with the corresponding
// opcode via BaseConn.SendCommandMeta (rpc_id=0) or SendRPCMeta. The frame is
// stamped with this peer's (branch, cluster_peer_id) by virtualTransport.

// SendNewBlock sends a minor block to the peer (CommandOp.NEW_BLOCK_MINOR).
// Python: PeerShardConnection.send_new_block (block passed in by caller).
func (pc *PeerConn) SendNewBlock(cmd *wire.NewBlockMinorCommand) error {
	payload, err := serialize.SerializeToBytes(cmd)
	if err != nil {
		return fmt.Errorf("serialize NewBlockMinorCommand: %w", err)
	}
	return pc.SendCommandMeta(byte(wire.CommandOpNewBlockMinor), payload, wire.ClusterMetadata{})
}

// SendNewMinorBlockHeaderList sends a caller-constructed new-tip header list
// to the peer (CommandOp.NEW_MINOR_BLOCK_HEADER_LIST, rpc_id=0).
func (pc *PeerConn) SendNewMinorBlockHeaderList(cmd *wire.NewMinorBlockHeaderListCommand) error {
	payload, err := serialize.SerializeToBytes(cmd)
	if err != nil {
		return fmt.Errorf("serialize NewMinorBlockHeaderListCommand: %w", err)
	}
	return pc.SendCommandMeta(byte(wire.CommandOpNewMinorBlockHeaderList), payload, wire.ClusterMetadata{})
}

// SendTransactionList sends a constructed transaction list to the peer
// (CommandOp.NEW_TRANSACTION_LIST). The list is supplied by the caller;
// Python: PeerShardConnection.broadcast_tx_list (tx_list passed in by caller).
func (pc *PeerConn) SendTransactionList(cmd *wire.NewTransactionListCommand) error {
	payload, err := serialize.SerializeToBytes(cmd)
	if err != nil {
		return fmt.Errorf("serialize NewTransactionListCommand: %w", err)
	}
	return pc.SendCommandMeta(byte(wire.CommandOpNewTransactionList), payload, wire.ClusterMetadata{})
}

// GetMinorBlockList issues an active RPC to the peer
// (CommandOp.GET_MINOR_BLOCK_LIST_REQUEST) and returns the parsed response.
// Python: PeerShardConnection.write_rpc_request(GET_MINOR_BLOCK_LIST_REQUEST).
func (pc *PeerConn) GetMinorBlockList(ctx context.Context, req *wire.GetMinorBlockListRequest) (*wire.GetMinorBlockListResponse, error) {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return nil, fmt.Errorf("serialize GetMinorBlockListRequest: %w", err)
	}
	resp, err := pc.SendRPCMeta(ctx, byte(wire.CommandOpGetMinorBlockListRequest), payload, wire.ClusterMetadata{})
	if err != nil {
		return nil, err
	}
	r, ok := resp.(*wire.GetMinorBlockListResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected GetMinorBlockList response %T", resp)
	}
	return r, nil
}

// GetMinorBlockHeaderList issues an active RPC to the peer
// (CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST) and returns the parsed
// response. Python: SyncTask.__download_block_headers
// (shard_conn.write_rpc_request, shard.py:441-451).
func (pc *PeerConn) GetMinorBlockHeaderList(ctx context.Context, req *wire.GetMinorBlockHeaderListRequest) (*wire.GetMinorBlockHeaderListResponse, error) {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return nil, fmt.Errorf("serialize GetMinorBlockHeaderListRequest: %w", err)
	}
	resp, err := pc.SendRPCMeta(ctx, byte(wire.CommandOpGetMinorBlockHeaderListRequest), payload, wire.ClusterMetadata{})
	if err != nil {
		return nil, err
	}
	r, ok := resp.(*wire.GetMinorBlockHeaderListResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected GetMinorBlockHeaderList response %T", resp)
	}
	return r, nil
}

// ── Inbound protocol handlers ──────────────────────────────────────────
//
// Each handler delegates the deserialized request of its opcode to the
// injected PeerHandler.

// handleNewMinorBlockHeaderList handles CommandOp.NEW_MINOR_BLOCK_HEADER_LIST.
// Python: handle_new_minor_block_header_list_command (OP_SERIALIZER_MAP).
func (pc *PeerConn) handleNewMinorBlockHeaderList(req any) (any, error) {
	return nil, pc.handler.NewMinorBlockHeaderList(req.(*wire.NewMinorBlockHeaderListCommand))
}

// handleNewTransactionList handles CommandOp.NEW_TRANSACTION_LIST.
// Python: handle_new_transaction_list_command (OP_SERIALIZER_MAP).
func (pc *PeerConn) handleNewTransactionList(req any) (any, error) {
	return nil, pc.handler.NewTransactionList(req.(*wire.NewTransactionListCommand))
}

// handleNewBlockMinor handles CommandOp.NEW_BLOCK_MINOR.
// Python: handle_new_block_minor_command (OP_SERIALIZER_MAP).
func (pc *PeerConn) handleNewBlockMinor(req any) (any, error) {
	return nil, pc.handler.NewBlockMinor(req.(*wire.NewBlockMinorCommand))
}

// handleGetMinorBlockList dispatches a GET_MINOR_BLOCK_LIST_REQUEST RPC to the
// business layer and returns its response.
// Python: OP_RPC_MAP[GET_MINOR_BLOCK_LIST_REQUEST] → PeerShardConnection.GetMinorBlockList.
func (pc *PeerConn) handleGetMinorBlockList(req any) (any, error) {
	return pc.handler.GetMinorBlockList(req.(*wire.GetMinorBlockListRequest))
}

// handleGetMinorBlockHeaderList dispatches a
// GET_MINOR_BLOCK_HEADER_LIST_REQUEST RPC to the business layer and returns its
// response.
// Python: OP_RPC_MAP[GET_MINOR_BLOCK_HEADER_LIST_REQUEST] → PeerShardConnection.GetMinorBlockHeaderList.
func (pc *PeerConn) handleGetMinorBlockHeaderList(req any) (any, error) {
	return pc.handler.GetMinorBlockHeaderList(req.(*wire.GetMinorBlockHeaderListRequest))
}

// handleGetMinorBlockHeaderListWithSkip dispatches a
// GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST RPC to the business layer and
// returns its response.
// Python: OP_RPC_MAP[GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST] → PeerShardConnection.GetMinorBlockHeaderListWithSkip.
func (pc *PeerConn) handleGetMinorBlockHeaderListWithSkip(req any) (any, error) {
	return pc.handler.GetMinorBlockHeaderListWithSkip(req.(*wire.GetMinorBlockHeaderListWithSkipRequest))
}
