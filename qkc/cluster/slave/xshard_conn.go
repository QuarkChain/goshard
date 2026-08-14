// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// xshardConn is a direct TCP connection to another slave, using 0-byte metadata
// (slave↔slave mode). It is package-private: callers reach it only through
// XshardPool, which owns construction, handshake, and lifecycle.
type xshardConn struct {
	*conn.BaseConn

	localID              []byte // this slave's identity, sent in PONG
	localFullShardIDList []uint32

	stateMu             sync.RWMutex // guards peerID / peerFullShardIDList
	peerID              []byte
	peerFullShardIDList []uint32
	pingReceived        chan struct{}
	pingOnce            sync.Once
}

// newXshardConn wraps an established net.Conn as an xshardConn, registering the
// serializers and handlers. It does not dial, accept, ping, or register with a
// pool; net.Conn ownership belongs to the caller.
func newXshardConn(nc net.Conn, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, logger log.Logger) *xshardConn {
	readFrame := func(r io.Reader) (*wire.Frame, error) {
		return wire.ReadFrameNoMeta(r, maxPayloadSize)
	}
	xc := &xshardConn{
		BaseConn:             conn.NewBaseConnFromConn(nc, readFrame, wire.WriteFrameNoMeta, logger),
		localID:              append([]byte(nil), localID...),
		localFullShardIDList: append([]uint32(nil), localFullShardIDList...),
		pingReceived:         make(chan struct{}),
	}

	xc.BaseConn.RegisterOpSerializers(map[byte]*conn.OpSerializer{
		byte(wire.ClusterOpPing):                        conn.OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
		byte(wire.ClusterOpAddXshardTxListRequest):      conn.OpSerializerFor[wire.AddXshardTxListRequest, wire.AddXshardTxListResponse](byte(wire.ClusterOpAddXshardTxListResponse)),
		byte(wire.ClusterOpBatchAddXshardTxListRequest): conn.OpSerializerFor[wire.BatchAddXshardTxListRequest, wire.BatchAddXshardTxListResponse](byte(wire.ClusterOpBatchAddXshardTxListResponse)),
	})

	xc.BaseConn.RegisterTypedHandlers(map[byte]conn.TypedHandler{
		byte(wire.ClusterOpPing): xc.handlePing,

		// Fail-fast stubs: invoking them closes the connection until migrated.
		byte(wire.ClusterOpAddXshardTxListRequest):      xc.handleAddXshardTxList,
		byte(wire.ClusterOpBatchAddXshardTxListRequest): xc.handleBatchAddXshardTxList,
	})

	return xc
}

// handlePing records peer identity and replies with a PONG.
func (x *xshardConn) handlePing(req any) (any, error) {
	ping := req.(*wire.PingRequest)

	// An empty ID is accepted; only an empty shard list is rejected.
	x.stateMu.Lock()
	if len(x.peerID) == 0 {
		x.peerID = append([]byte(nil), ping.ID...)
		x.peerFullShardIDList = append([]uint32(nil), ping.FullShardIDList...)
	}
	emptyShardList := len(x.peerFullShardIDList) == 0
	x.stateMu.Unlock()

	if emptyShardList {
		return nil, fmt.Errorf("empty shard list from slave %s", ping.ID)
	}

	if !x.BaseConn.IsClosed() {
		x.pingOnce.Do(func() { close(x.pingReceived) })
	}

	return &wire.PongResponse{
		ID:              append([]byte(nil), x.localID...),
		FullShardIDList: append([]uint32(nil), x.localFullShardIDList...),
	}, nil
}

// handleAddXshardTxList is a fail-fast stub until the business logic is migrated.
func (x *xshardConn) handleAddXshardTxList(req any) (any, error) {
	_ = req.(*wire.AddXshardTxListRequest)

	// TODO(xshard): implement xshard transaction processing.
	x.Logger().Warn("AddXshardTxList stub invoked — closing connection (not implemented)", "remote", x.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleBatchAddXshardTxList is a fail-fast stub until the business logic is migrated.
func (x *xshardConn) handleBatchAddXshardTxList(req any) (any, error) {
	_ = req.(*wire.BatchAddXshardTxListRequest)

	// TODO(xshard): implement batch xshard transaction processing.
	x.Logger().Warn("BatchAddXshardTxList stub invoked — closing connection (not implemented)", "remote", x.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// setRemoteIdentity records the peer identity for outbound connections, which
// never receive a PING.
func (x *xshardConn) setRemoteIdentity(id []byte, shardList []uint32) {
	x.stateMu.Lock()
	defer x.stateMu.Unlock()
	x.peerID = append([]byte(nil), id...)
	x.peerFullShardIDList = append([]uint32(nil), shardList...)
}

// remoteID returns the peer's slave ID.
func (x *xshardConn) remoteID() []byte {
	x.stateMu.RLock()
	defer x.stateMu.RUnlock()
	return append([]byte(nil), x.peerID...)
}

// remoteFullShardIDList returns the peer's full shard ID list.
func (x *xshardConn) remoteFullShardIDList() []uint32 {
	x.stateMu.RLock()
	defer x.stateMu.RUnlock()
	return append([]uint32(nil), x.peerFullShardIDList...)
}

// waitUntilPingReceived blocks until the first PING or connection close. It
// returns false on close (an intentional divergence from Python, which blocks
// forever).
func (x *xshardConn) waitUntilPingReceived() bool {
	select {
	case <-x.pingReceived:
		return !x.BaseConn.IsClosed()
	case <-x.BaseConn.WaitUntilClosed():
		return false
	}
}

// sendPing sends PING and returns the peer's id and shard list from PONG.
func (x *xshardConn) sendPing(ctx context.Context) (id []byte, shardList []uint32, err error) {
	payload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              x.localID,
		FullShardIDList: x.localFullShardIDList,
		// RootTip stays nil until the RootBlock wire type is ported; the
		// handshake does not consume it.
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

// sendXshardTxList sends an AddXshardTxListRequest RPC.
func (x *xshardConn) sendXshardTxList(ctx context.Context, payload []byte) (*wire.Frame, error) {
	return x.BaseConn.SendRPC(ctx, byte(wire.ClusterOpAddXshardTxListRequest), payload)
}

// sendBatchXshardTxList sends a BatchAddXshardTxListRequest RPC.
func (x *xshardConn) sendBatchXshardTxList(ctx context.Context, payload []byte) (*wire.Frame, error) {
	return x.BaseConn.SendRPC(ctx, byte(wire.ClusterOpBatchAddXshardTxListRequest), payload)
}

// parseAddXshardTxListResponse decodes an AddXshardTxListResponse; a non-zero
// error_code is returned as an error.
func parseAddXshardTxListResponse(frame *wire.Frame) (*wire.AddXshardTxListResponse, error) {
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

// parseBatchAddXshardTxListResponse decodes a BatchAddXshardTxListResponse; a
// non-zero error_code is returned as an error.
func parseBatchAddXshardTxListResponse(frame *wire.Frame) (*wire.BatchAddXshardTxListResponse, error) {
	if frame == nil {
		return nil, fmt.Errorf("nil xshard response frame")
	}
	if frame.Opcode != byte(wire.ClusterOpBatchAddXshardTxListResponse) {
		return nil, fmt.Errorf("unexpected xshard response opcode: got 0x%x, want 0x%x",
			frame.Opcode, byte(wire.ClusterOpBatchAddXshardTxListResponse))
	}
	var resp wire.BatchAddXshardTxListResponse
	if err := serialize.DeserializeFromBytes(frame.Payload, &resp); err != nil {
		return nil, fmt.Errorf("deserialize BatchAddXshardTxListResponse: %w", err)
	}
	if resp.ErrorCode != 0 {
		return &resp, fmt.Errorf("BatchAddXshardTxList failed: error_code=%d", resp.ErrorCode)
	}
	return &resp, nil
}
