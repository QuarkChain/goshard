// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

const defaultDialTimeout = 10 * time.Second

// XshardConn is a direct TCP connection to another slave node for cross-shard
// traffic. It uses 0-byte metadata (slave↔slave mode) and corresponds to Python's
// SlaveConnection.
//
// Architecture:
//
//	XshardConn embeds *conn.BaseConn, which uses the TCP frame transport.
//
// No forwarder — all frames are dispatched locally. RPC ID validation is
// global monotonic (the default in conn.BaseConn).
type XshardConn struct {
	*conn.BaseConn

	// local identity of this slave, used in PONG responses.
	localID              []byte
	localFullShardIDList []uint32

	// peer identity state, protected by its own mutex.
	stateMu               sync.Mutex
	remoteID              []byte
	remoteFullShardIDList []uint32
	pingReceived          chan struct{}
	pingOnce              sync.Once
}

// NewXshardConn dials another slave and returns an XshardConn.
// Call Start before using the connection.
// maxPayloadSize controls frame payload size limit; 0 disables the limit.
// localID and localFullShardIDList identify this slave and are used in PONG responses.
func NewXshardConn(addr string, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, logger log.Logger) (*XshardConn, error) {
	nc, err := net.DialTimeout("tcp", addr, defaultDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial xshard slave %s: %w", addr, err)
	}
	return newXshardConn(nc, maxPayloadSize, localID, localFullShardIDList, logger), nil
}

// NewXshardConnFromConn wraps an accepted net.Conn as an XshardConn.
// maxPayloadSize controls frame payload size limit; 0 disables the limit.
// localID and localFullShardIDList identify this slave and are used in PONG responses.
func NewXshardConnFromConn(nc net.Conn, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, logger log.Logger) *XshardConn {
	return newXshardConn(nc, maxPayloadSize, localID, localFullShardIDList, logger)
}

func newXshardConn(nc net.Conn, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, logger log.Logger) *XshardConn {
	readFrame := func(r io.Reader) (*wire.Frame, error) {
		return wire.ReadFrameNoMeta(r, maxPayloadSize)
	}
	xc := &XshardConn{
		BaseConn:             conn.NewBaseConnFromConn(nc, readFrame, wire.WriteFrameNoMeta, logger),
		localID:              append([]byte(nil), localID...),
		localFullShardIDList: append([]uint32(nil), localFullShardIDList...),
		pingReceived:         make(chan struct{}),
	}

	// Register serializers for all slave-to-slave RPC opcodes. Each serializer
	// is registered under both its request opcode and response opcode so BaseConn
	// can deserialize inbound response payloads.
	xc.BaseConn.RegisterOpSerializers(map[byte]*conn.OpSerializer{
		byte(wire.ClusterOpPing):                        conn.OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
		byte(wire.ClusterOpAddXshardTxListRequest):      conn.OpSerializerFor[wire.AddXshardTxListRequest, wire.AddXshardTxListResponse](byte(wire.ClusterOpAddXshardTxListResponse)),
		byte(wire.ClusterOpBatchAddXshardTxListRequest): conn.OpSerializerFor[wire.BatchAddXshardTxListRequest, wire.BatchAddXshardTxListResponse](byte(wire.ClusterOpBatchAddXshardTxListResponse)),
	})

	// Register handlers for all slave-to-slave RPCs.
	// PING/PONG is the slave-to-slave identity exchange.
	// ADD_XSHARD_TX_LIST and BATCH_ADD_XSHARD_TX_LIST are fail-fast stubs.
	// If invoked, the connection is closed to expose the unimplemented path.
	xc.BaseConn.RegisterTypedHandlers(map[byte]conn.TypedHandler{
		// ── Permanent connection handler ───────────────────────────────
		// PING/PONG is the slave-to-slave identity exchange.

		byte(wire.ClusterOpPing): xc.handlePing,

		// ── Migration stubs ─────────────────────────────────────────────────
		// Wire messages and serializers are registered for protocol opcode coverage.
		// Handlers return ErrHandlerNotImplemented to trigger connection close.
		// This is intentional fail-fast: if any of these opcodes are invoked
		// before their implementation is migrated, the connection dies to
		// prevent silent data loss.

		byte(wire.ClusterOpAddXshardTxListRequest):      xc.handleAddXshardTxList,
		byte(wire.ClusterOpBatchAddXshardTxListRequest): xc.handleBatchAddXshardTxList,
	})

	return xc
}

// handlePing is the built-in PING handler. It records peer identity, validates
// the shard list, and returns a PONG with this slave's identity.
func (x *XshardConn) handlePing(req any) (any, error) {
	ping := req.(*wire.PingRequest)

	// Reject empty slave ID — a peer without a valid identity cannot be used
	// for routing or deduplication.
	if len(ping.ID) == 0 {
		return nil, fmt.Errorf("empty slave ID in PING")
	}

	// Record peer identity (only on first ping, matches Python's "if not self.id")
	x.stateMu.Lock()
	if len(x.remoteID) == 0 {
		x.remoteID = append([]byte(nil), ping.ID...)
		x.remoteFullShardIDList = append([]uint32(nil), ping.FullShardIDList...)
	}
	// Check stored shard list (matches Python's self.full_shard_id_list check)
	storedShardList := x.remoteFullShardIDList
	x.stateMu.Unlock()

	if len(storedShardList) == 0 {
		// Returning error causes BaseConn to close connection (Python's close_with_error)
		return nil, fmt.Errorf("empty shard list from slave %s", ping.ID)
	}

	// Signal ping received AFTER check passes (matches Python's ping_received_event.set())
	if !x.BaseConn.Closed() {
		x.pingOnce.Do(func() { close(x.pingReceived) })
	}

	return &wire.PongResponse{
		ID:              append([]byte(nil), x.localID...),
		FullShardIDList: append([]uint32(nil), x.localFullShardIDList...),
	}, nil
}

// handleAddXshardTxList is the ADD_XSHARD_TX_LIST_REQUEST stub.
//
// Business logic is not migrated yet. This handler intentionally
// returns ErrHandlerNotImplemented so that invoking an unsupported
// migration path fails fast instead of silently accepting requests.
func (x *XshardConn) handleAddXshardTxList(req any) (any, error) {
	_ = req.(*wire.AddXshardTxListRequest)

	// TODO(xshard): implement xshard transaction processing.
	x.Logger().Warn("AddXshardTxList stub invoked — closing connection (not implemented)", "remote", x.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleBatchAddXshardTxList is the BATCH_ADD_XSHARD_TX_LIST_REQUEST stub.
//
// Business logic is not migrated yet. This handler intentionally
// returns ErrHandlerNotImplemented so that invoking an unsupported
// migration path fails fast instead of silently accepting requests.
func (x *XshardConn) handleBatchAddXshardTxList(req any) (any, error) {
	_ = req.(*wire.BatchAddXshardTxListRequest)

	// TODO(xshard): implement batch xshard transaction processing.
	x.Logger().Warn("BatchAddXshardTxList stub invoked — closing connection (not implemented)", "remote", x.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// SetRemoteIdentity sets the peer identity for outbound xshard connections that
// completed PING-based verification without receiving a PING from the peer.
// This matches Python's SlaveConnection, whose remote id is known at creation
// time for outbound connections.
func (x *XshardConn) SetRemoteIdentity(id []byte, shardList []uint32) {
	x.stateMu.Lock()
	defer x.stateMu.Unlock()
	x.remoteID = append([]byte(nil), id...)
	x.remoteFullShardIDList = append([]uint32(nil), shardList...)
}

// RemoteID returns the peer's slave ID, populated after the first PING.
func (x *XshardConn) RemoteID() []byte {
	x.stateMu.Lock()
	defer x.stateMu.Unlock()
	return append([]byte(nil), x.remoteID...)
}

// RemoteFullShardIDList returns the peer's full shard ID list, populated after
// the first PING.
func (x *XshardConn) RemoteFullShardIDList() []uint32 {
	x.stateMu.Lock()
	defer x.stateMu.Unlock()
	return append([]uint32(nil), x.remoteFullShardIDList...)
}

// WaitUntilPingReceived blocks until the first PING is received or the
// connection is closed. It returns true if the connection is still alive.
func (x *XshardConn) WaitUntilPingReceived() bool {
	select {
	case <-x.pingReceived:
		return !x.BaseConn.Closed()
	case <-x.BaseConn.WaitUntilClosed():
		return false
	}
}

// SendPing sends a PING request and waits for PONG response. It returns the
// peer's id and full_shard_id_list from the PONG response.
// This is the outbound half of the slave-to-slave identity exchange,
// corresponding to Python's SlaveConnection.send_ping().
// The connection must have been started (Start() called).
func (x *XshardConn) SendPing(ctx context.Context) (id []byte, shardList []uint32, err error) {
	payload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              x.localID,
		FullShardIDList: x.localFullShardIDList,
		// TODO: Port RootBlock wire type.
		// Slave-to-slave PING does not consume root tip currently.
		// Python still serializes an empty RootBlockHeader for this field,
		// but RootBlock wire representation is not migrated yet.
		// Keep nil until the RootBlock type and encoding are implemented.
		RootTip: nil,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("serialize ping: %w", err)
	}

	frame, err := x.BaseConn.SendRPC(ctx, byte(wire.ClusterOpPing), payload)
	if err != nil {
		return nil, nil, fmt.Errorf("send ping: %w", err)
	}
	if frame.Opcode != byte(wire.ClusterOpPong) {
		return nil, nil, fmt.Errorf("unexpected ping response opcode: got 0x%x, want 0x%x",
			frame.Opcode, byte(wire.ClusterOpPong))
	}

	var pong wire.PongResponse
	if err := serialize.DeserializeFromBytes(frame.Payload, &pong); err != nil {
		return nil, nil, fmt.Errorf("deserialize pong: %w", err)
	}

	if len(pong.ID) == 0 {
		return nil, nil, fmt.Errorf("empty slave ID in PONG")
	}

	if len(pong.FullShardIDList) == 0 {
		return nil, nil, fmt.Errorf("empty shard list in PONG")
	}
	return pong.ID, pong.FullShardIDList, nil
}

// SendXshardTxList sends an AddXshardTxListRequest via RPC and returns the response.
// Python's ADD_XSHARD_TX_LIST_REQUEST is an RPC (in SLAVE_OP_RPC_MAP), not fire-and-forget.
func (x *XshardConn) SendXshardTxList(ctx context.Context, payload []byte) (*wire.Frame, error) {
	return x.BaseConn.SendRPC(ctx, byte(wire.ClusterOpAddXshardTxListRequest), payload)
}

// SendBatchXshardTxList sends a BatchAddXshardTxListRequest via RPC and returns the response.
// Python's BATCH_ADD_XSHARD_TX_LIST_REQUEST is an RPC (in SLAVE_OP_RPC_MAP).
func (x *XshardConn) SendBatchXshardTxList(ctx context.Context, payload []byte) (*wire.Frame, error) {
	return x.BaseConn.SendRPC(ctx, byte(wire.ClusterOpBatchAddXshardTxListRequest), payload)
}

// ParseAddXshardTxListResponse decodes and validates an
// AddXshardTxListResponse frame.
//
// A non-zero error_code indicates that the remote side rejected the
// operation and is returned as an error.
func ParseAddXshardTxListResponse(frame *wire.Frame) (*wire.AddXshardTxListResponse, error) {
	if frame == nil {
		return nil, fmt.Errorf("nil xshard response frame")
	}
	if frame.Opcode != byte(wire.ClusterOpAddXshardTxListResponse) {
		return nil, fmt.Errorf("unexpected xshard response opcode: got 0x%x, want 0x%x",
			frame.Opcode, byte(wire.ClusterOpAddXshardTxListResponse))
	}
	var resp wire.AddXshardTxListResponse
	if err := serialize.DeserializeFromBytes(frame.Payload, &resp); err != nil {
		return nil, fmt.Errorf("deserialize AddXshardTxListResponse: %w", err)
	}
	if resp.ErrorCode != 0 {
		return &resp, fmt.Errorf("AddXshardTxList failed: error_code=%d", resp.ErrorCode)
	}
	return &resp, nil
}
