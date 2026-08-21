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

	stateMu sync.RWMutex // guards peerID / peerFullShardIDList
	// Peer identity: injected at construction for outbound connections
	// (master-advertised SlaveInfo, mirroring Python's SlaveConnection
	// constructor); recorded from the first PING for inbound connections.
	peerID              []byte
	peerFullShardIDList []uint32
	pingReceived        chan struct{}
	pingOnce            sync.Once
}

// newXshardConn creates a slave-to-slave connection. Outbound callers inject
// the master-advertised peer identity (peerID/peerShardList); inbound callers
// pass nil and identity is recorded from the first PING.
func newXshardConn(nc net.Conn, maxPayloadSize uint32, localID []byte, localFullShardIDList []uint32, peerID []byte, peerShardList []uint32, handler XshardHandler, logger log.Logger) (*xshardConn, error) {
	if handler == nil {
		return nil, errors.New("xshard handler must not be nil")
	}
	xc := &xshardConn{
		handler:              handler,
		localID:              append([]byte(nil), localID...),
		localFullShardIDList: append([]uint32(nil), localFullShardIDList...),
		peerID:               append([]byte(nil), peerID...),
		peerFullShardIDList:  append([]uint32(nil), peerShardList...),
		pingReceived:         make(chan struct{}),
	}
	xc.BaseConn = conn.NewBaseConn(conn.Config{
		Transport: conn.NewTCPTransport(
			nc,
			func(r io.Reader) (*wire.Frame, error) {
				return wire.ReadFrameNoMeta(r, maxPayloadSize)
			},
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

// handlePing performs slave identity handshake.
func (x *xshardConn) handlePing(req any) (any, error) {
	ping := req.(*wire.PingRequest)

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

func (x *xshardConn) remoteID() []byte {
	x.stateMu.RLock()
	defer x.stateMu.RUnlock()
	return append([]byte(nil), x.peerID...)
}

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
		return !x.IsClosed()
	case <-x.WaitUntilClosed():
		return false
	}
}

// sendPing sends PING and returns the peer's id and shard list from PONG.
func (x *xshardConn) sendPing(ctx context.Context) ([]byte, []uint32, error) {
	req := &wire.PingRequest{
		ID:              x.localID,
		FullShardIDList: x.localFullShardIDList,
		RootTip:         nil,
	}
	resp, err := x.sendRPC(ctx, byte(wire.ClusterOpPing), req)
	if err != nil {
		return nil, nil, err
	}

	pong, ok := resp.(*wire.PongResponse)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected ping response %T", resp)
	}
	if len(pong.ID) == 0 || len(pong.FullShardIDList) == 0 {
		return nil, nil, errors.New("invalid pong")
	}
	return pong.ID, pong.FullShardIDList, nil
}

// --------------------
// outbound protocol send
// --------------------

func (x *xshardConn) sendAddXshardTxList(ctx context.Context, req *wire.AddXshardTxListRequest) error {
	resp, err := x.sendRPC(ctx, byte(wire.ClusterOpAddXshardTxListRequest), req)
	if err != nil {
		return err
	}

	r, ok := resp.(*wire.AddXshardTxListResponse)
	if !ok {
		return fmt.Errorf("unexpected response %T", resp)
	}
	if r.ErrorCode != 0 {
		return fmt.Errorf("AddXshardTxList failed: %d", r.ErrorCode)
	}

	return nil
}

func (x *xshardConn) sendBatchAddXshardTxList(ctx context.Context, req *wire.BatchAddXshardTxListRequest) error {
	resp, err := x.sendRPC(ctx, byte(wire.ClusterOpBatchAddXshardTxListRequest), req)
	if err != nil {
		return err
	}

	r, ok := resp.(*wire.BatchAddXshardTxListResponse)
	if !ok {
		return fmt.Errorf("unexpected response %T", resp)
	}

	if r.ErrorCode != 0 {
		return fmt.Errorf("BatchAddXshardTxList failed: %d", r.ErrorCode)
	}

	return nil
}

// sendRPC is xshard protocol helper.
// BaseConn stays payload-oriented.
func (x *xshardConn) sendRPC(ctx context.Context, opcode byte, req any) (any, error) {
	payload, err := serialize.SerializeToBytes(req)
	if err != nil {
		return nil, err
	}
	return x.BaseConn.SendRPC(ctx, opcode, payload)
}
