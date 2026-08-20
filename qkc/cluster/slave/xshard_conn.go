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

	localID              []byte // this slave's identity, sent in PING/PONG
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
	xc := &xshardConn{
		localID:              append([]byte(nil), localID...),
		localFullShardIDList: append([]uint32(nil), localFullShardIDList...),
		pingReceived:         make(chan struct{}),
	}
	xc.BaseConn = conn.NewBaseConn(conn.Config{
		Transport: conn.NewTCPTransport(
			nc,
			func(r io.Reader) (*wire.Frame, error) { return wire.ReadFrameNoMeta(r, maxPayloadSize) },
			wire.WriteFrameNoMeta,
		),
		Serializers: map[byte]*conn.OpSerializer{
			byte(wire.ClusterOpPing):                        conn.OpSerializerFor[wire.PingRequest, wire.PongResponse](byte(wire.ClusterOpPong)),
			byte(wire.ClusterOpAddXshardTxListRequest):      conn.OpSerializerFor[wire.AddXshardTxListRequest, wire.AddXshardTxListResponse](byte(wire.ClusterOpAddXshardTxListResponse)),
			byte(wire.ClusterOpBatchAddXshardTxListRequest): conn.OpSerializerFor[wire.BatchAddXshardTxListRequest, wire.BatchAddXshardTxListResponse](byte(wire.ClusterOpBatchAddXshardTxListResponse)),
		},
		Handlers: map[byte]conn.TypedHandler{
			byte(wire.ClusterOpPing): xc.handlePing,

			// Fail-fast stubs: invoking them closes the connection until migrated.
			byte(wire.ClusterOpAddXshardTxListRequest):      xc.handleAddXshardTxList,
			byte(wire.ClusterOpBatchAddXshardTxListRequest): xc.handleBatchAddXshardTxList,
		},
		Logger: logger,
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

	// Matches Python's close_with_error: a handler error closes the connection
	// and the pending PING completes with ErrConnectionClosed.
	if emptyShardList {
		return nil, fmt.Errorf("empty shard list from slave %s", ping.ID)
	}

	x.pingOnce.Do(func() { close(x.pingReceived) })

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

// sendRPCAs serializes req, sends it as an RPC under opcode, and returns the
// response decoded as S. The response is deserialized once by BaseConn; a wrong
// response opcode surfaces as a type mismatch here and does not close the
// connection.
func sendRPCAs[S any](x *xshardConn, ctx context.Context, opcode byte, req any) (*S, error) {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return nil, fmt.Errorf("serialize request: %w", err)
	}
	resp, err := x.BaseConn.SendRPC(ctx, opcode, payload)
	if err != nil {
		return nil, err
	}
	typed, ok := resp.(*S)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T for opcode 0x%x", resp, opcode)
	}
	return typed, nil
}

// sendPing sends PING and returns the peer's id and shard list from PONG.
func (x *xshardConn) sendPing(ctx context.Context) (id []byte, shardList []uint32, err error) {
	pong, err := sendRPCAs[wire.PongResponse](x, ctx, byte(wire.ClusterOpPing), &wire.PingRequest{
		ID:              x.localID,
		FullShardIDList: x.localFullShardIDList,
		// TODO: RootTip stays nil until the RootBlock wire type is ported; the
		// handshake does not consume it.
		RootTip: nil,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("send ping: %w", err)
	}

	if len(pong.ID) == 0 {
		return nil, nil, fmt.Errorf("empty slave ID in PONG")
	}
	if len(pong.FullShardIDList) == 0 {
		return nil, nil, fmt.Errorf("empty shard list in PONG")
	}
	return append([]byte(nil), pong.ID...), append([]uint32(nil), pong.FullShardIDList...), nil
}

// sendXshardTxList sends an AddXshardTxListRequest and verifies its error code.
func (x *xshardConn) sendXshardTxList(ctx context.Context, req *wire.AddXshardTxListRequest) error {
	resp, err := sendRPCAs[wire.AddXshardTxListResponse](x, ctx, byte(wire.ClusterOpAddXshardTxListRequest), req)
	if err != nil {
		return err
	}
	if resp.ErrorCode != 0 {
		return fmt.Errorf("AddXshardTxList failed: error_code=%d", resp.ErrorCode)
	}
	return nil
}

// sendBatchXshardTxList sends a BatchAddXshardTxListRequest and verifies its
// error code.
func (x *xshardConn) sendBatchXshardTxList(ctx context.Context, req *wire.BatchAddXshardTxListRequest) error {
	resp, err := sendRPCAs[wire.BatchAddXshardTxListResponse](x, ctx, byte(wire.ClusterOpBatchAddXshardTxListRequest), req)
	if err != nil {
		return err
	}
	if resp.ErrorCode != 0 {
		return fmt.Errorf("BatchAddXshardTxList failed: error_code=%d", resp.ErrorCode)
	}
	return nil
}
