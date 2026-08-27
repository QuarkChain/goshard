// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"errors"
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

// XshardHandler serves inbound xshard requests. It is implemented by the
// business layer and injected at construction.
//
// Handler implementations must be safe for concurrent calls.
//
// The error return is reserved for connection-level failures. Returning an
// error causes the connection to be closed by BaseConn. Business-level
// failures must be encoded in the response ErrorCode field.
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

	// Peer identity. Immutable: injected at construction for outbound
	// connections (master-advertised SlaveInfo), and for inbound set exactly
	// once by the first PING inside pingOnce.Do. Read via remoteID()/... with
	// no lock; pingOnce close(pingReceived) publishes the initialization.
	peerID              []byte
	peerFullShardIDList []uint32
	// pingReceived is closed on the first PING (py: ping_received_event);
	// pingOnce makes the close exactly-once under concurrent PING dispatch.
	pingReceived chan struct{}
	pingOnce     sync.Once
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

// handlePing performs the slave identity handshake.
//
// peer metadata is initialized at most once by pingOnce and is immutable
// afterwards. Outbound connections have metadata pre-filled at construction;
// inbound connections initialize it from the first PING with a non-empty shard
// list. An empty inbound PING does not publish metadata or complete the
// handshake and causes the connection to be rejected below. The Once
// synchronization makes the published metadata safe for subsequent lock-free
// reads.
func (x *xshardConn) handlePing(req any) (any, error) {
	ping := req.(*wire.PingRequest)

	x.pingOnce.Do(func() {
		// Outbound connections already have peer metadata from construction.
		// Inbound connections initialize it from the first valid PING.
		if len(x.peerID) == 0 {
			if len(ping.FullShardIDList) == 0 {
				// Do not publish an invalid inbound identity or complete the
				// handshake. The handler error below will close the connection.
				return
			}

			x.peerID = append([]byte(nil), ping.ID...)
			x.peerFullShardIDList = append([]uint32(nil), ping.FullShardIDList...)
		}

		close(x.pingReceived)
	})

	// sync.Once.Do provides the synchronization boundary for inbound
	// metadata initialization. After Do returns, peer metadata is immutable.
	if len(x.peerFullShardIDList) == 0 {
		return nil, fmt.Errorf("empty shard list from slave %s", ping.ID)
	}

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

// remoteID returns a copy of the peer's id. Metadata is immutable after its
// one-time publication (construction for outbound; the first PING inside
// pingOnce for inbound), so the read needs no lock.
func (x *xshardConn) remoteID() []byte {
	return append([]byte(nil), x.peerID...)
}

func (x *xshardConn) remoteFullShardIDList() []uint32 {
	return append([]uint32(nil), x.peerFullShardIDList...)
}

// waitUntilPingReceived blocks until the first PING, connection close, or
// handshake timeout, returning false on close or timeout.
func (x *xshardConn) waitUntilPingReceived() bool {
	timer := time.NewTimer(xshardHandshakeTimeout)
	defer timer.Stop()

	select {
	case <-x.pingReceived:
		return !x.IsClosed()
	case <-x.WaitUntilClosed():
		return false
	case <-timer.C:
		// Close is non-blocking; it shuts the connection down and drains
		// any pending RPCs, so the peer cannot hold resources hostage.
		x.Close()
		return false
	}
}

// sendPing sends PING and returns the peer's id and shard list from PONG.
func (x *xshardConn) sendPing(ctx context.Context) ([]byte, []uint32, error) {
	req := &wire.PingRequest{
		ID:              x.localID,
		FullShardIDList: x.localFullShardIDList,
		RootTip:         nil, // TODO: RootTip stays nil until the RootBlock wire type is ported.
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
