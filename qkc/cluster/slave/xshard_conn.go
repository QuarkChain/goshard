// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// XshardHandler serves inbound xshard requests. It is implemented by the
// business layer and injected at construction.
type XshardHandler interface {
	AddXshardTxList(req *wire.AddXshardTxListRequest) (*wire.AddXshardTxListResponse, error)

	BatchAddXshardTxList(req *wire.BatchAddXshardTxListRequest) (*wire.BatchAddXshardTxListResponse, error)
}

// xshardConn is a direct TCP connection to another slave, using 0-byte
// metadata (slave↔slave mode). Callers reach it only through XshardPool.
type xshardConn struct {
	*conn.BaseConn

	handler XshardHandler

	localID              []byte // this slave's identity, sent in PING/PONG
	localFullShardIDList []uint32

	stateMu             sync.RWMutex // guards peerID / peerFullShardIDList
	peerID              []byte
	peerFullShardIDList []uint32
	pingReceived        chan struct{}
	pingOnce            sync.Once
}

// newXshardConn wraps an established net.Conn as an xshardConn and registers
// the serializers and handlers. handler must not be nil.
func newXshardConn(nc net.Conn, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, handler XshardHandler, logger log.Logger) (*xshardConn, error) {
	if handler == nil {
		return nil, errors.New("xshard handler must not be nil")
	}
	xc := &xshardConn{
		handler:              handler,
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
			byte(wire.ClusterOpPing):                        xc.handlePing,
			byte(wire.ClusterOpAddXshardTxListRequest):      xc.handleAddXshardTxList,
			byte(wire.ClusterOpBatchAddXshardTxListRequest): xc.handleBatchAddXshardTxList,
		},
		Logger: logger,
	})
	return xc, nil
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

	// A handler error closes the connection.
	if emptyShardList {
		return nil, fmt.Errorf("empty shard list from slave %s", ping.ID)
	}

	x.pingOnce.Do(func() { close(x.pingReceived) })

	return &wire.PongResponse{
		ID:              append([]byte(nil), x.localID...),
		FullShardIDList: append([]uint32(nil), x.localFullShardIDList...),
	}, nil
}

// handleAddXshardTxList delegates to the business handler.
func (x *xshardConn) handleAddXshardTxList(req any) (any, error) {
	return x.handler.AddXshardTxList(req.(*wire.AddXshardTxListRequest))
}

// handleBatchAddXshardTxList delegates to the business handler.
func (x *xshardConn) handleBatchAddXshardTxList(req any) (any, error) {
	return x.handler.BatchAddXshardTxList(req.(*wire.BatchAddXshardTxListRequest))
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

// waitUntilPingReceived blocks until the first PING or connection close,
// returning false on close.
func (x *xshardConn) waitUntilPingReceived() bool {
	select {
	case <-x.pingReceived:
		return !x.BaseConn.IsClosed()
	case <-x.BaseConn.WaitUntilClosed():
		return false
	}
}

// sendRPCAs sends req as an RPC under opcode and returns the response decoded
// as S.
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
		// TODO: RootTip stays nil until the RootBlock wire type is ported.
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
