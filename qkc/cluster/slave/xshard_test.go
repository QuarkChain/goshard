// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// ── TCP test pair helpers ─────────────────────────────────────────────────────

// newTestConnPair creates a pair of xshardConns connected over a local TCP
// socket. The caller is responsible for calling cleanup.
func newTestConnPair(t *testing.T) (client, server *xshardConn, cleanup func()) {
	t.Helper()
	return newTestConnPairWithIdentity(t, []byte("client-slave"), []uint32{0x00010001}, []byte("server-slave"), []uint32{0x00030004})
}

func newTestConnPairWithIdentity(t *testing.T, clientID []byte, clientShards []uint32, serverID []byte, serverShards []uint32) (client, server *xshardConn, cleanup func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var serverConn net.Conn
	var acceptErr error
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		serverConn, acceptErr = ln.Accept()
		ln.Close()
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	<-accepted
	if acceptErr != nil {
		t.Fatalf("accept: %v", acceptErr)
	}

	logger := log.New()
	client = newXshardConn(clientConn, 0, clientID, clientShards, logger) // 0 = no limit (matches Python)
	server = newXshardConn(serverConn, 0, serverID, serverShards, logger)
	cleanup = func() {
		client.Close()
		server.Close()
	}
	return
}

// newRawConnPair establishes a TCP connection and returns both ends. It is used
// to drive the pool's HandleInbound with a raw accepted net.Conn.
func newRawConnPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- c
	}()

	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case server = <-accepted:
	case err := <-acceptErr:
		t.Fatalf("accept: %v", err)
	}
	return client, server
}

// establishInbound drives one inbound connection through HandleInbound and the
// PING handshake: the client side acts as the remote slave sending PING, the
// server side is handed to the pool. It returns once the connection is indexed.
func establishInbound(t *testing.T, pool *XshardPool, remoteID []byte, remoteShards []uint32) {
	t.Helper()
	clientConn, serverConn := newRawConnPair(t)

	done := make(chan struct{})
	go func() {
		pool.HandleInbound(serverConn)
		close(done)
	}()

	client := newXshardConn(clientConn, 0, remoteID, remoteShards, log.New())
	client.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := client.sendPing(ctx); err != nil {
		client.Close()
		t.Fatalf("inbound ping: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("HandleInbound did not return after PING")
	}
	client.Close()
}

// ── xshardConn layer tests ────────────────────────────────────────────────────

// TestXshardConn_BuiltinPingHandler verifies that the PING handler
// auto-registered by newXshardConn records peer identity and returns a PONG
// with the server's own identity.
func TestXshardConn_BuiltinPingHandler(t *testing.T) {
	clientID := []byte("client-slave")
	clientShards := []uint32{0x00010001}
	serverID := []byte("server-slave")
	serverShards := []uint32{0x00030004}

	client, server, cleanup := newTestConnPairWithIdentity(t, clientID, clientShards, serverID, serverShards)
	defer cleanup()

	server.Start()
	client.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              clientID,
		FullShardIDList: clientShards,
		RootTip:         nil,
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.SendRPC(ctx, byte(wire.ClusterOpPing), pingPayload)
	if err != nil {
		t.Fatalf("send ping rpc: %v", err)
	}
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected opcode 0x%x, got 0x%x", wire.ClusterOpPong, resp.Opcode)
	}

	var pong wire.PongResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &pong); err != nil {
		t.Fatalf("deserialize pong: %v", err)
	}
	if string(pong.ID) != string(serverID) {
		t.Fatalf("pong id mismatch: got %s, expected %s", pong.ID, serverID)
	}
	if len(pong.FullShardIDList) != len(serverShards) {
		t.Fatalf("pong shard list mismatch: got %v", pong.FullShardIDList)
	}

	if !server.waitUntilPingReceived() {
		t.Fatal("server did not receive ping")
	}
	if string(server.remoteID()) != string(clientID) {
		t.Fatalf("server remote id mismatch: got %s", server.remoteID())
	}
}

func TestXshardConn_RPCRoundTrip(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	clientID := []byte("client-slave")
	clientShards := []uint32{0x00010001, 0x00010002}
	serverID := []byte("server-slave")

	server.Start()
	client.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              clientID,
		FullShardIDList: clientShards,
		RootTip:         nil,
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.SendRPC(ctx, byte(wire.ClusterOpPing), pingPayload)
	if err != nil {
		t.Fatalf("send ping rpc: %v", err)
	}
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected opcode 0x%x, got 0x%x", wire.ClusterOpPong, resp.Opcode)
	}

	var pong wire.PongResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &pong); err != nil {
		t.Fatalf("deserialize pong: %v", err)
	}
	if string(pong.ID) != string(serverID) {
		t.Fatalf("pong id mismatch: got %s", pong.ID)
	}

	if !server.waitUntilPingReceived() {
		t.Fatal("server did not receive ping")
	}
	if string(server.remoteID()) != string(clientID) {
		t.Fatalf("server remote id mismatch: got %s", server.remoteID())
	}
	if len(server.remoteFullShardIDList()) != len(clientShards) {
		t.Fatalf("server remote shard list mismatch: got %v", server.remoteFullShardIDList())
	}
}

// TestXshardConn_XshardRPCStubClosesConnection verifies the ADD_XSHARD_TX_LIST
// stub closes the connection (ErrHandlerNotImplemented → connection-fatal).
func TestXshardConn_XshardRPCStubClosesConnection(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()
	server.Start()
	client.Start()

	txList := wire.RawBytes{}
	payload, err := serialize.SerializeToBytes(&wire.AddXshardTxListRequest{
		Branch: 1,
		TxList: &txList,
	})
	if err != nil {
		t.Fatalf("serialize xshard request: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.sendXshardTxList(ctx, payload)
	if err != conn.ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
	<-client.WaitUntilClosed()
	<-server.WaitUntilClosed()
}

// TestXshardConn_SendPingRejectsWrongResponseOpcode verifies a wrong-opcode
// PONG is rejected by sendPing's opcode check but does not close the connection.
func TestXshardConn_SendPingRejectsWrongResponseOpcode(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := newXshardConn(clientConn, 0, []byte("client"), []uint32{1}, log.New())
	client.Start()
	peerDone := make(chan error, 1)
	go func() {
		request, err := wire.ReadFrameNoMeta(serverConn, 0)
		if err != nil {
			peerDone <- err
			return
		}
		payload, err := serialize.SerializeToBytes(&wire.AddXshardTxListResponse{
			ErrorCode: 0,
		})
		if err == nil {
			err = wire.WriteFrameNoMeta(serverConn, &wire.Frame{
				Opcode:  byte(wire.ClusterOpAddXshardTxListResponse),
				RPCID:   request.RPCID,
				Payload: payload,
			})
		}
		peerDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := client.sendPing(ctx)
	if err == nil {
		t.Fatal("expected wrong PING response opcode error")
	}
	if err := <-peerDone; err != nil {
		t.Fatalf("raw peer failed: %v", err)
	}
	if client.IsClosed() {
		t.Fatal("wrong PING response opcode should not close the connection")
	}
}

func TestXshardConn_WaitUntilPingReceivedReturnsAfterClose(t *testing.T) {
	_, server, cleanup := newTestConnPair(t)
	defer cleanup()

	result := make(chan bool, 1)
	go func() {
		result <- server.waitUntilPingReceived()
	}()
	if err := server.Close(); err != nil {
		t.Fatalf("close server: %v", err)
	}
	select {
	case got := <-result:
		if got {
			t.Fatal("expected false after close before PING")
		}
	case <-time.After(time.Second):
		t.Fatal("waitUntilPingReceived did not return after close")
	}
}

func TestXshardConn_RejectEmptyShardList(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("bad-slave"),
		FullShardIDList: []uint32{}, // empty list
		RootTip:         nil,
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.SendRPC(ctx, byte(wire.ClusterOpPing), pingPayload)
	if err == nil {
		t.Fatal("expected error due to connection close, got nil")
	}
	if string(server.remoteID()) != "bad-slave" {
		t.Fatalf("expected remote ID 'bad-slave', got %v", server.remoteID())
	}
}

// TestXshardConn_RecordPingOnlyOnce verifies the first PING records identity
// and later PINGs do not overwrite it (matches Python's handle_ping).
func TestXshardConn_RecordPingOnlyOnce(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ping1, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client1"),
		FullShardIDList: []uint32{0x00010001, 0x00010002},
	})
	if _, err := client.SendRPC(ctx, byte(wire.ClusterOpPing), ping1); err != nil {
		t.Fatalf("first ping failed: %v", err)
	}

	firstID := server.remoteID()
	firstShards := server.remoteFullShardIDList()

	ping2, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client2"),
		FullShardIDList: []uint32{0x00030004},
	})
	if _, err := client.SendRPC(ctx, byte(wire.ClusterOpPing), ping2); err != nil {
		t.Fatalf("second ping failed: %v", err)
	}

	if string(server.remoteID()) != string(firstID) {
		t.Fatalf("remote ID changed: got %s, expected %s", server.remoteID(), firstID)
	}
	if len(server.remoteFullShardIDList()) != len(firstShards) {
		t.Fatalf("remote shard list changed: got %v, expected %v", server.remoteFullShardIDList(), firstShards)
	}
}

// TestXshardConn_AcceptEmptyPingID verifies a PING with an empty slave ID is
// accepted (Python only rejects an empty shard list).
func TestXshardConn_AcceptEmptyPingID(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	client.Start()
	server.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte{},
		FullShardIDList: []uint32{0x00010001},
		RootTip:         nil,
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err = client.SendRPC(ctx, byte(wire.ClusterOpPing), pingPayload); err != nil {
		t.Fatalf("expected PING with empty ID to be accepted, got %v", err)
	}
	if len(server.remoteID()) != 0 {
		t.Fatalf("expected empty remote ID, got %s", server.remoteID())
	}
	if server.IsClosed() {
		t.Fatal("server connection should remain open after empty-ID PING")
	}
}

// ── XshardPool indexing / broadcast tests ─────────────────────────────────────

func TestXshardPool_AddGet(t *testing.T) {
	pool := NewXshardPool(nil, nil, 0, log.New())
	defer pool.Close()

	_, conn1, cleanup1 := newTestConnPair(t)
	defer cleanup1()
	_, conn2, cleanup2 := newTestConnPair(t)
	defer cleanup2()

	pool.add(0x00010001, conn1)
	pool.add(0x00010001, conn2)
	pool.add(0x00020001, conn1)

	if got := pool.outboundSize(); got != 2 {
		t.Fatalf("expected pool outbound size 2 (unique conns), got %d", got)
	}

	if conns := pool.get(0x00010001); len(conns) != 2 {
		t.Fatalf("expected 2 conns for shard 0x00010001, got %d", len(conns))
	}

	if targets := pool.targets(); len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(targets))
	}
}

// TestXshardPool_ClosedConnectionStaysIndexed verifies Python parity: a CLOSED
// connection is never evicted from the routing index or slave ID registry.
func TestXshardPool_ClosedConnectionStaysIndexed(t *testing.T) {
	client, server, cleanup := newTestConnPairWithIdentity(
		t,
		[]byte("client-slave"),
		[]uint32{0x00010001},
		[]byte("server-slave"),
		[]uint32{0x00030004, 0x00030005},
	)
	defer cleanup()

	server.Start()
	client.Start()
	pool := NewXshardPool(nil, nil, 0, log.New())
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.verifyAndAddToShards(ctx, client, []byte("server-slave"), []uint32{0x00030004, 0x00030005}); err != nil {
		t.Fatalf("verify and add: %v", err)
	}

	client.Close()

	for _, shardID := range []uint32{0x00030004, 0x00030005} {
		if conns := pool.get(shardID); len(conns) != 1 || conns[0] != client {
			t.Fatalf("route 0x%x no longer contains the closed connection: %v", shardID, conns)
		}
	}
	if !pool.hasSlaveID([]byte("server-slave")) {
		t.Fatal("slave ID was removed after connection close")
	}
	if len(pool.targets()) != 2 {
		t.Fatalf("expected both shard targets to remain, got %v", pool.targets())
	}
}

// TestXshardPool_SendXshardTxToClosedConnectionFails verifies broadcast attempts
// a CLOSED connection (Python never filters it out) and fails.
func TestXshardPool_SendXshardTxToClosedConnectionFails(t *testing.T) {
	client, server, cleanup := newTestConnPairWithIdentity(
		t,
		[]byte("client-slave"),
		[]uint32{0x00010001},
		[]byte("server-slave"),
		[]uint32{0x00030004, 0x00030005},
	)
	defer cleanup()

	server.Start()
	client.Start()
	pool := NewXshardPool(nil, nil, 0, log.New())
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.verifyAndAddToShards(ctx, client, []byte("server-slave"), []uint32{0x00030004, 0x00030005}); err != nil {
		t.Fatalf("verify and add: %v", err)
	}

	client.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	req := &wire.AddXshardTxListRequest{Branch: 0x00030004, TxList: &wire.RawBytes{}}
	if err := pool.SendXshardTx(ctx2, 0x00030004, req); err == nil {
		t.Fatal("expected SendXshardTx to fail on a CLOSED connection (Python parity)")
	}
}

func TestXshardPool_SendXshardTxNoConnection(t *testing.T) {
	pool := NewXshardPool(nil, nil, 0, log.New())
	defer pool.Close()

	// Empty target set is a silent no-op (Python's broadcast succeeds on an
	// empty future list).
	req := &wire.AddXshardTxListRequest{Branch: 0x00010001, TxList: &wire.RawBytes{}}
	if err := pool.SendXshardTx(context.Background(), 0x00010001, req); err != nil {
		t.Fatalf("expected silent success on empty target, got error: %v", err)
	}
}

// ── inbound tests ─────────────────────────────────────────────────────────────

// TestXshardPool_HandleInboundAllowsMultipleInboundConnections verifies two
// inbound connections from the same remote are both accepted (Python's
// handle_new_connection does not check slave_ids).
func TestXshardPool_HandleInboundAllowsMultipleInboundConnections(t *testing.T) {
	pool := NewXshardPool([]byte("local-slave"), []uint32{0x00030004}, 0, log.New())
	defer pool.Close()

	establishInbound(t, pool, []byte("same-slave"), []uint32{0x00010001})
	establishInbound(t, pool, []byte("same-slave"), []uint32{0x00010001})

	if conns := pool.get(0x00010001); len(conns) != 2 {
		t.Fatalf("expected 2 connections for shard, got %d", len(conns))
	}
	if !pool.hasSlaveID([]byte("same-slave")) {
		t.Fatal("slaveID not tracked")
	}
}

// TestXshardPool_OutboundAndInboundCoexist verifies an outbound and an inbound
// connection to the same remote coexist (Python's bidirectional model).
func TestXshardPool_OutboundAndInboundCoexist(t *testing.T) {
	client1, server1, cleanup1 := newTestConnPairWithIdentity(
		t, []byte("local"), []uint32{0x00030004}, []byte("remote-slave"), []uint32{0x00010001},
	)
	defer cleanup1()
	client1.Start()
	server1.Start()

	pool := NewXshardPool([]byte("local"), []uint32{0x00030004}, 0, log.New())
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Outbound (S1 → S2).
	if err := pool.verifyAndAddToShards(ctx, client1, []byte("remote-slave"), []uint32{0x00010001}); err != nil {
		t.Fatalf("outbound verify and add: %v", err)
	}

	// Inbound (S2 → S1).
	establishInbound(t, pool, []byte("remote-slave"), []uint32{0x00010001})

	if conns := pool.get(0x00010001); len(conns) != 2 {
		t.Fatalf("expected 2 connections, got %d", len(conns))
	}
	if !pool.hasSlaveID([]byte("remote-slave")) {
		t.Fatal("slaveID not tracked")
	}
}

// TestXshardPool_InboundFirstOutboundSkipped verifies that when inbound
// registers the remote first, a later outbound to the same remote is silently
// skipped by the final dedup (Python's connect_to_slave returns "" when the
// slave is already in slave_ids).
func TestXshardPool_InboundFirstOutboundSkipped(t *testing.T) {
	pool := NewXshardPool([]byte("local"), []uint32{0x00030004}, 0, log.New())
	defer pool.Close()

	// Inbound first.
	establishInbound(t, pool, []byte("remote-slave"), []uint32{0x00010001})
	if !pool.hasSlaveID([]byte("remote-slave")) {
		t.Fatal("slaveID not registered after inbound")
	}

	// Outbound should be silently skipped.
	client1, server1, cleanup1 := newTestConnPairWithIdentity(
		t, []byte("local"), []uint32{0x00030004}, []byte("remote-slave"), []uint32{0x00010001},
	)
	defer cleanup1()
	client1.Start()
	server1.Start()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pool.verifyAndAddToShards(ctx, client1, []byte("remote-slave"), []uint32{0x00010001}); err != nil {
		t.Fatalf("outbound should be silently skipped, got error: %v", err)
	}

	// Only the original inbound connection remains indexed.
	if conns := pool.get(0x00010001); len(conns) != 1 {
		t.Fatalf("expected 1 connection (inbound only), got %d", len(conns))
	}
	if !pool.hasSlaveID([]byte("remote-slave")) {
		t.Fatal("slaveID should still be tracked")
	}
}

// TestXshardPool_HandleInboundPendingClose verifies the Go safety enhancement:
// a pending inbound connection (PING not yet received) is closed by pool Close,
// whereas Python leaks it.
func TestXshardPool_HandleInboundPendingClose(t *testing.T) {
	pool := NewXshardPool([]byte("local-slave"), []uint32{0x00030004}, 0, log.New())

	clientConn, serverConn := newRawConnPair(t)
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		pool.HandleInbound(serverConn)
		close(done)
	}()

	// Wait until HandleInbound registers the connection as pending inbound.
	deadline := time.Now().Add(2 * time.Second)
	for pool.inboundSize() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("HandleInbound did not register pending inbound")
		}
		time.Sleep(time.Millisecond)
	}

	pool.Close()

	// HandleInbound must unblock (waitUntilPingReceived returns false on close).
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleInbound did not return after pool Close")
	}

	// The remote side observes EOF: the pending inbound was closed by Close.
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := clientConn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the pending inbound connection to be closed")
	}
}

// TestXshardPool_SelfConnectionSkipped verifies verifyAndAddToShards skips a
// connection whose expected ID equals the pool's own ID (Python's
// connect_to_slave returns "" without dialing when slave_info.id ==
// slave_server.id).
func TestXshardPool_SelfConnectionSkipped(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()
	server.Start()
	client.Start()

	pool := NewXshardPool([]byte("client-slave"), nil, 0, log.New())
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := pool.verifyAndAddToShards(ctx, client, []byte("client-slave"), []uint32{0x00030004}); err != nil {
		t.Fatalf("self connection should be silently skipped, got error: %v", err)
	}
	if !client.IsClosed() {
		t.Fatal("self connection should be closed")
	}
	if pool.hasSlaveID([]byte("client-slave")) {
		t.Fatal("self ID should not be registered")
	}
	if pool.outboundSize() != 0 {
		t.Fatalf("expected 0 outbound connections, got %d", pool.outboundSize())
	}
}

func TestXshardPool_ClosedPoolRejectsAdd(t *testing.T) {
	pool := NewXshardPool(nil, nil, 0, log.New())
	pool.Close()

	_, xc, cleanup := newTestConnPair(t)
	defer cleanup()

	xc.Start()
	pool.add(0x00010001, xc)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := xc.SendRPC(ctx, byte(wire.ClusterOpPing), []byte("ping"))
	if err != conn.ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
}

// ── parse tests ───────────────────────────────────────────────────────────────

func TestParseAddXshardTxListResponse_NonZeroErrorCode(t *testing.T) {
	const errCode uint32 = 2
	payload, err := serialize.SerializeToBytes(&wire.AddXshardTxListResponse{ErrorCode: errCode})
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	frame := &wire.Frame{
		Opcode:  byte(wire.ClusterOpAddXshardTxListResponse),
		Payload: payload,
	}
	resp, err := parseAddXshardTxListResponse(frame)
	if err == nil {
		t.Fatal("expected error for non-zero error_code, got nil")
	}
	if resp == nil || resp.ErrorCode != errCode {
		t.Fatalf("expected decoded response with error_code %d, got resp=%v err=%v", errCode, resp, err)
	}
}

func TestParseAddXshardTxListResponse_ZeroErrorCode(t *testing.T) {
	payload, err := serialize.SerializeToBytes(&wire.AddXshardTxListResponse{ErrorCode: 0})
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	frame := &wire.Frame{
		Opcode:  byte(wire.ClusterOpAddXshardTxListResponse),
		Payload: payload,
	}
	resp, err := parseAddXshardTxListResponse(frame)
	if err != nil {
		t.Fatalf("expected success for error_code 0, got: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("expected error_code 0, got %d", resp.ErrorCode)
	}
}

func TestParseAddXshardTxListResponse_WrongOpcode(t *testing.T) {
	payload, _ := serialize.SerializeToBytes(&wire.AddXshardTxListResponse{ErrorCode: 0})
	frame := &wire.Frame{
		Opcode:  byte(wire.ClusterOpPong),
		Payload: payload,
	}
	if _, err := parseAddXshardTxListResponse(frame); err == nil {
		t.Fatal("expected error for wrong opcode, got nil")
	}
}

func TestNewXshardPool_NilLogger(t *testing.T) {
	pool := NewXshardPool(nil, nil, 0, nil)
	if pool == nil {
		t.Fatal("NewXshardPool(nil) returned nil")
	}
	pool.Close()
}

// ── remote slave helper ───────────────────────────────────────────────────────

// remoteSlave simulates a remote slave that answers PING with PONG. It counts
// accepted connections so tests can assert whether a dial happened.
type remoteSlave struct {
	ln       net.Listener
	host     string
	port     uint16
	accepted int32 // atomic

	mu    sync.Mutex
	conns []*xshardConn
	wg    sync.WaitGroup
}

func startRemoteSlave(t *testing.T, remoteID []byte, remoteShards []uint32) *remoteSlave {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	rs := &remoteSlave{ln: ln, host: addr.IP.String(), port: uint16(addr.Port)}
	rs.wg.Add(1)
	go rs.acceptLoop(remoteID, remoteShards)
	return rs
}

// slaveInfo builds a wire.SlaveInfo describing the simulated remote.
func (rs *remoteSlave) slaveInfo(id []byte, shards []uint32) wire.SlaveInfo {
	return wire.SlaveInfo{
		ID:              id,
		Host:            []byte(rs.host),
		Port:            rs.port,
		FullShardIDList: shards,
	}
}

func (rs *remoteSlave) acceptLoop(remoteID []byte, remoteShards []uint32) {
	defer rs.wg.Done()
	for {
		c, err := rs.ln.Accept()
		if err != nil {
			return
		}
		atomic.AddInt32(&rs.accepted, 1)
		conn := newXshardConn(c, 0, remoteID, remoteShards, log.New())
		conn.Start()
		rs.mu.Lock()
		rs.conns = append(rs.conns, conn)
		rs.mu.Unlock()
	}
}

func (rs *remoteSlave) acceptedCount() int {
	return int(atomic.LoadInt32(&rs.accepted))
}

func (rs *remoteSlave) close() {
	rs.ln.Close()
	rs.wg.Wait()
	rs.mu.Lock()
	for _, c := range rs.conns {
		c.Close()
	}
	rs.mu.Unlock()
}

// ── DialToSlave tests ─────────────────────────────────────────────────────────

// TestXshardPool_DialToSlaveSkipsExistingRemote verifies pre-dial dedup: dialing
// an already-tracked remote does not open a new TCP connection.
func TestXshardPool_DialToSlaveSkipsExistingRemote(t *testing.T) {
	rs := startRemoteSlave(t, []byte("remote-slave"), []uint32{0x00010001})
	defer rs.close()

	pool := NewXshardPool([]byte("local-slave"), []uint32{0x00030004}, 0, log.New())
	defer pool.Close()

	ctx := context.Background()

	if err := pool.DialToSlave(ctx, rs.slaveInfo([]byte("remote-slave"), []uint32{0x00010001})); err != nil {
		t.Fatalf("first dial: %v", err)
	}
	if rs.acceptedCount() != 1 {
		t.Fatalf("expected 1 accepted connection, got %d", rs.acceptedCount())
	}

	if err := pool.DialToSlave(ctx, rs.slaveInfo([]byte("remote-slave"), []uint32{0x00010001})); err != nil {
		t.Fatalf("second dial should be skipped: %v", err)
	}
	if rs.acceptedCount() != 1 {
		t.Fatalf("expected no new connection, got %d accepted", rs.acceptedCount())
	}
}

// TestXshardPool_DialToSlaveSkipsSelf verifies the pre-dial self guard.
func TestXshardPool_DialToSlaveSkipsSelf(t *testing.T) {
	rs := startRemoteSlave(t, []byte("local-slave"), []uint32{0x00030004})
	defer rs.close()

	pool := NewXshardPool([]byte("local-slave"), []uint32{0x00030004}, 0, log.New())
	defer pool.Close()

	ctx := context.Background()
	if err := pool.DialToSlave(ctx, rs.slaveInfo([]byte("local-slave"), []uint32{0x00030004})); err != nil {
		t.Fatalf("self dial should be skipped: %v", err)
	}
	if rs.acceptedCount() != 0 {
		t.Fatalf("expected no connection for self, got %d", rs.acceptedCount())
	}
}

// TestXshardPool_DialToSlaveConcurrentDedup verifies the final dedup safety net:
// concurrent dials to the same remote result in a single registered outbound.
func TestXshardPool_DialToSlaveConcurrentDedup(t *testing.T) {
	rs := startRemoteSlave(t, []byte("remote-slave"), []uint32{0x00010001})
	defer rs.close()

	pool := NewXshardPool([]byte("local-slave"), []uint32{0x00030004}, 0, log.New())
	defer pool.Close()

	ctx := context.Background()

	const n = 2
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = pool.DialToSlave(ctx, rs.slaveInfo([]byte("remote-slave"), []uint32{0x00010001}))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
	}

	if got := pool.outboundSize(); got != 1 {
		t.Fatalf("expected 1 outbound connection, got %d", got)
	}
	if !pool.hasSlaveID([]byte("remote-slave")) {
		t.Fatal("remote-slave should be tracked")
	}
}

// TestXshardPool_DialToSlaveRetryAfterFailure verifies a failed dial does not
// register the remote, so a later retry can still connect.
func TestXshardPool_DialToSlaveRetryAfterFailure(t *testing.T) {
	pool := NewXshardPool([]byte("local-slave"), []uint32{0x00030004}, 0, log.New())
	defer pool.Close()

	ctx := context.Background()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadAddr := ln.Addr().(*net.TCPAddr)
	ln.Close()
	deadInfo := wire.SlaveInfo{
		ID:              []byte("remote-slave"),
		Host:            []byte(deadAddr.IP.String()),
		Port:            uint16(deadAddr.Port),
		FullShardIDList: []uint32{0x00010001},
	}

	if err := pool.DialToSlave(ctx, deadInfo); err == nil {
		t.Fatal("expected dial failure to dead address")
	}
	if pool.hasSlaveID([]byte("remote-slave")) {
		t.Fatal("failed dial should not register the remote")
	}

	rs := startRemoteSlave(t, []byte("remote-slave"), []uint32{0x00010001})
	defer rs.close()
	if err := pool.DialToSlave(ctx, rs.slaveInfo([]byte("remote-slave"), []uint32{0x00010001})); err != nil {
		t.Fatalf("retry dial: %v", err)
	}
	if !pool.hasSlaveID([]byte("remote-slave")) {
		t.Fatal("retry should register the remote")
	}
}

// TestXshardPool_DialToSlaveCompletesHandshake verifies the normal outbound flow:
// dial, PING/PONG verification, and indexing all complete.
func TestXshardPool_DialToSlaveCompletesHandshake(t *testing.T) {
	rs := startRemoteSlave(t, []byte("remote-slave"), []uint32{0x00010001})
	defer rs.close()

	pool := NewXshardPool([]byte("local-slave"), []uint32{0x00030004}, 0, log.New())
	defer pool.Close()

	ctx := context.Background()
	if err := pool.DialToSlave(ctx, rs.slaveInfo([]byte("remote-slave"), []uint32{0x00010001})); err != nil {
		t.Fatalf("dial: %v", err)
	}
	if pool.outboundSize() != 1 {
		t.Fatalf("expected 1 outbound connection, got %d", pool.outboundSize())
	}
	if !pool.hasSlaveID([]byte("remote-slave")) {
		t.Fatal("remote-slave should be tracked")
	}
	if conns := pool.get(0x00010001); len(conns) != 1 {
		t.Fatalf("expected 1 connection for shard 0x00010001, got %d", len(conns))
	}
}
