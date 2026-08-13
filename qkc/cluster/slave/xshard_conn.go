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

// XshardConn is a direct TCP connection to another slave for cross-shard
// traffic, using 0-byte metadata (slave↔slave mode). Corresponds to Python's
// SlaveConnection.
type XshardConn struct {
	*conn.BaseConn

	localID              []byte // this slave's identity, sent in PONG
	localFullShardIDList []uint32

	stateMu               sync.Mutex // guards remoteID / remoteFullShardIDList
	remoteID              []byte
	remoteFullShardIDList []uint32
	pingReceived          chan struct{}
	pingOnce              sync.Once
}

// NewXshardConn dials another slave. It is the low-level dial primitive:
// production callers must use XshardPool.DialToSlave, which adds the pre-dial
// dedup. Call Start before use; maxPayloadSize 0 disables the payload limit.
func NewXshardConn(addr string, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, logger log.Logger) (*XshardConn, error) {
	nc, err := net.DialTimeout("tcp", addr, defaultDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial xshard slave %s: %w", addr, err)
	}
	return newXshardConn(nc, maxPayloadSize, localID, localFullShardIDList, logger), nil
}

// NewXshardConnFromConn wraps an accepted net.Conn as an XshardConn.
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

	// Register serializers for all slave-to-slave RPC opcodes.
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

// handlePing records peer identity and returns a PONG with this slave's identity.
func (x *XshardConn) handlePing(req any) (any, error) {
	ping := req.(*wire.PingRequest)

	// First PING records identity (Python's "if not self.id"). An empty slave
	// ID is accepted — Python only rejects an empty shard list.
	x.stateMu.Lock()
	if len(x.remoteID) == 0 {
		x.remoteID = append([]byte(nil), ping.ID...)
		x.remoteFullShardIDList = append([]uint32(nil), ping.FullShardIDList...)
	}
	storedShardList := x.remoteFullShardIDList
	x.stateMu.Unlock()

	if len(storedShardList) == 0 {
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
func (x *XshardConn) handleAddXshardTxList(req any) (any, error) {
	_ = req.(*wire.AddXshardTxListRequest)

	// TODO(xshard): implement xshard transaction processing.
	x.Logger().Warn("AddXshardTxList stub invoked — closing connection (not implemented)", "remote", x.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// handleBatchAddXshardTxList is a fail-fast stub until the business logic is migrated.
func (x *XshardConn) handleBatchAddXshardTxList(req any) (any, error) {
	_ = req.(*wire.BatchAddXshardTxListRequest)

	// TODO(xshard): implement batch xshard transaction processing.
	x.Logger().Warn("BatchAddXshardTxList stub invoked — closing connection (not implemented)", "remote", x.RemoteAddr())
	return nil, conn.ErrHandlerNotImplemented
}

// SetRemoteIdentity records the peer identity for outbound connections, which
// never receive a PING from the peer (Python sets it at creation).
func (x *XshardConn) SetRemoteIdentity(id []byte, shardList []uint32) {
	x.stateMu.Lock()
	defer x.stateMu.Unlock()
	x.remoteID = append([]byte(nil), id...)
	x.remoteFullShardIDList = append([]uint32(nil), shardList...)
}

// RemoteID returns the peer's slave ID.
func (x *XshardConn) RemoteID() []byte {
	x.stateMu.Lock()
	defer x.stateMu.Unlock()
	return append([]byte(nil), x.remoteID...)
}

// RemoteFullShardIDList returns the peer's full shard ID list.
func (x *XshardConn) RemoteFullShardIDList() []uint32 {
	x.stateMu.Lock()
	defer x.stateMu.Unlock()
	return append([]uint32(nil), x.remoteFullShardIDList...)
}

// WaitUntilPingReceived blocks until the first PING or connection close; it
// returns false on close. Python blocks forever here — returning false is an
// intentional divergence from Python's leak.
func (x *XshardConn) WaitUntilPingReceived() bool {
	select {
	case <-x.pingReceived:
		return !x.BaseConn.IsClosed()
	case <-x.BaseConn.WaitUntilClosed():
		return false
	}
}

// SendPing sends PING and returns the peer's id and shard list from PONG.
// Corresponds to Python's SlaveConnection.send_ping.
func (x *XshardConn) SendPing(ctx context.Context) (id []byte, shardList []uint32, err error) {
	payload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              x.localID,
		FullShardIDList: x.localFullShardIDList,
		// TODO: Port RootBlock wire type. nil differs from Python's empty
		// RootTip is intentionally nil for the current migration scope.
		// Python's slave-to-slave PING serializes a non-nil empty RootBlock,
		// whereas this Go migration has not yet ported the RootBlock wire type.
		// This means the current Go PING is not byte-for-byte compatible with
		// Python for this field, but the slave-to-slave handshake does not consume
		// RootTip. Do not introduce a fake RootBlock type here; port the real
		// RootBlock wire representation when RootBlock migration is implemented.
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

// SendXshardTxList sends an AddXshardTxListRequest RPC.
func (x *XshardConn) SendXshardTxList(ctx context.Context, payload []byte) (*wire.Frame, error) {
	return x.BaseConn.SendRPC(ctx, byte(wire.ClusterOpAddXshardTxListRequest), payload)
}

// SendBatchXshardTxList sends a BatchAddXshardTxListRequest RPC.
func (x *XshardConn) SendBatchXshardTxList(ctx context.Context, payload []byte) (*wire.Frame, error) {
	return x.BaseConn.SendRPC(ctx, byte(wire.ClusterOpBatchAddXshardTxListRequest), payload)
}

// ParseAddXshardTxListResponse decodes an AddXshardTxListResponse; a non-zero
// error_code is returned as an error.
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
