// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/conn"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// ── TCP test pair helper ──────────────────────────────────────────────────────

// newTestConnPair creates a pair of XshardConns connected over a local TCP
// socket. The caller is responsible for calling cleanup.
func newTestConnPair(t *testing.T) (client, server *XshardConn, cleanup func()) {
	t.Helper()
	return newTestConnPairWithIdentity(t, []byte("client-slave"), []uint32{0x00010001}, []byte("server-slave"), []uint32{0x00030004})
}

func newTestConnPairWithIdentity(t *testing.T, clientID []byte, clientShards []uint32, serverID []byte, serverShards []uint32) (client, server *XshardConn, cleanup func()) {
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
	client = NewXshardConnFromConn(clientConn, 0, clientID, clientShards, logger) // 0 = no limit (matches Python)
	server = NewXshardConnFromConn(serverConn, 0, serverID, serverShards, logger)
	cleanup = func() {
		client.Close()
		server.Close()
	}
	return
}

// TestXshardConn_BuiltinPingHandler verifies that the PING handler
// auto-registered by newXshardConn correctly records peer identity and
// returns a PONG with the server's own identity.
func TestXshardConn_BuiltinPingHandler(t *testing.T) {
	clientID := []byte("client-slave")
	clientShards := []uint32{0x00010001}
	serverID := []byte("server-slave")
	serverShards := []uint32{0x00030004}

	client, server, cleanup := newTestConnPairWithIdentity(t, clientID, clientShards, serverID, serverShards)
	defer cleanup()

	// PING is auto-registered by newXshardConn; no explicit handler needed.
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

	if !server.WaitUntilPingReceived() {
		t.Fatal("server did not receive ping")
	}
	if string(server.RemoteID()) != string(clientID) {
		t.Fatalf("server remote id mismatch: got %s", server.RemoteID())
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
		RootTip:         nil, // OK for SlaveConnection (master doesn't use it)
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

	if !server.WaitUntilPingReceived() {
		t.Fatal("server did not receive ping")
	}
	if string(server.RemoteID()) != string(clientID) {
		t.Fatalf("server remote id mismatch: got %s", server.RemoteID())
	}
	if len(server.RemoteFullShardIDList()) != len(clientShards) {
		t.Fatalf("server remote shard list mismatch: got %v", server.RemoteFullShardIDList())
	}
}

// TestXshardConn_XshardRPCStubClosesConnection verifies that the
// ADD_XSHARD_TX_LIST_REQUEST stub returns ErrHandlerNotImplemented, which
// BaseConn treats as a connection-fatal error (matches Python's
// close_with_error). The RPC fails with ErrConnectionClosed on the caller
// side and both endpoints end up closed.
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
	_, err = client.SendXshardTxList(ctx, payload)
	if err != conn.ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
	<-client.WaitUntilClosed()
	<-server.WaitUntilClosed()
}

// TestXshardConn_SendPingRejectsWrongResponseOpcode verifies that a PING
// answered with a well-formed but wrong-opcode response is rejected by the
// application-level opcode check in SendPing, but the connection is not closed.
// Python matches responses by rpc_id only and does not close the connection on
// opcode mismatch (see AbstractConnection.handle_metadata_and_raw_data).
func TestXshardConn_SendPingRejectsWrongResponseOpcode(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := NewXshardConnFromConn(clientConn, 0, []byte("client"), []uint32{1}, log.New())
	client.Start()
	peerDone := make(chan error, 1)
	go func() {
		request, err := wire.ReadFrameNoMeta(serverConn, 0)
		if err != nil {
			peerDone <- err
			return
		}
		// Reply with a valid AddXshardTxListResponse payload carrying the same
		// RPCID but the wrong opcode. BaseConn delivers the response to the
		// caller by rpc_id; SendPing's application-level opcode check rejects it.
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
	_, _, err := client.SendPing(ctx)
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
		result <- server.WaitUntilPingReceived()
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
		t.Fatal("WaitUntilPingReceived did not return after close")
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
	if string(server.RemoteID()) != "bad-slave" {
		t.Fatalf("expected remote ID 'bad-slave', got %v", server.RemoteID())
	}
}

// TestXshardConn_RecordPingOnlyOnce verifies that recordPing only updates
// on first PING (matches Python's handle_ping behavior).
func TestXshardConn_RecordPingOnlyOnce(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	server.Start()
	client.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First PING with one shard list.
	ping1, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client1"),
		FullShardIDList: []uint32{0x00010001, 0x00010002},
	})
	_, err := client.SendRPC(ctx, byte(wire.ClusterOpPing), ping1)
	if err != nil {
		t.Fatalf("first ping failed: %v", err)
	}

	firstID := server.RemoteID()
	firstShards := server.RemoteFullShardIDList()

	// Second PING with different shard list (should not overwrite).
	ping2, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("client2"),
		FullShardIDList: []uint32{0x00030004},
	})
	_, err = client.SendRPC(ctx, byte(wire.ClusterOpPing), ping2)
	if err != nil {
		t.Fatalf("second ping failed: %v", err)
	}

	if string(server.RemoteID()) != string(firstID) {
		t.Fatalf("remote ID changed: got %s, expected %s", server.RemoteID(), firstID)
	}
	if len(server.RemoteFullShardIDList()) != len(firstShards) {
		t.Fatalf("remote shard list changed: got %v, expected %v", server.RemoteFullShardIDList(), firstShards)
	}
}

func TestXshardPool_AddGet(t *testing.T) {
	pool := NewXshardPool(log.New())
	defer pool.Close()

	// Use stub connections that are never started.
	_, conn1, cleanup1 := newTestConnPair(t)
	defer cleanup1()
	_, conn2, cleanup2 := newTestConnPair(t)
	defer cleanup2()

	pool.add(0x00010001, conn1)
	pool.add(0x00010001, conn2)
	pool.add(0x00020001, conn1)

	if got := pool.OutboundSize(); got != 2 {
		t.Fatalf("expected pool outbound size 2 (unique conns), got %d", got)
	}

	conns := pool.Get(0x00010001)
	if len(conns) != 2 {
		t.Fatalf("expected 2 conns for shard 0x00010001, got %d", len(conns))
	}

	targets := pool.Targets()
	if len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(targets))
	}
}

// TestXshardPool_ClosedConnectionStaysIndexed verifies the Python parity
// behavior: SlaveConnectionManager never evicts a connection when it closes,
// so a CLOSED connection remains in the routing index and the slave ID
// registry.
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
	pool := NewXshardPool(log.New())
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.VerifyAndAddToShards(ctx, client, []byte("server-slave"), []uint32{0x00030004, 0x00030005}); err != nil {
		t.Fatalf("verify and add: %v", err)
	}

	client.Close()

	// The closed connection must NOT be evicted from the routing index.
	for _, shardID := range []uint32{0x00030004, 0x00030005} {
		if conns := pool.Get(shardID); len(conns) != 1 || conns[0] != client {
			t.Fatalf("route 0x%x no longer contains the closed connection: %v", shardID, conns)
		}
	}
	// The slave ID registry is never pruned.
	if !pool.HasSlaveID([]byte("server-slave")) {
		t.Fatal("slave ID was removed after connection close")
	}
	if len(pool.Targets()) != 2 {
		t.Fatalf("expected both shard targets to remain, got %v", pool.Targets())
	}
}

// TestXshardPool_SendXshardTxToClosedConnectionFails verifies that broadcast
// attempts a CLOSED connection (Python never filters it out) and fails, instead
// of silently skipping it.
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
	pool := NewXshardPool(log.New())
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.VerifyAndAddToShards(ctx, client, []byte("server-slave"), []uint32{0x00030004, 0x00030005}); err != nil {
		t.Fatalf("verify and add: %v", err)
	}

	client.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if _, err := pool.SendXshardTx(ctx2, 0x00030004, []byte("tx")); err == nil {
		t.Fatal("expected SendXshardTx to fail on a CLOSED connection (Python parity)")
	}
}

func TestXshardPool_WatchAndIndexAllowsMultipleInboundConnections(t *testing.T) {
	// Two inbound connections from the same remote slave should both be accepted
	// (matches Python's handle_new_connection which does not check slave_ids).
	client1, server1, cleanup1 := newTestConnPairWithIdentity(
		t, []byte("same-slave"), []uint32{0x00010001}, []byte("server-1"), []uint32{0x00030004},
	)
	defer cleanup1()
	client2, server2, cleanup2 := newTestConnPairWithIdentity(
		t, []byte("same-slave"), []uint32{0x00010001}, []byte("server-2"), []uint32{0x00030004},
	)
	defer cleanup2()
	client1.Start()
	server1.Start()
	client2.Start()
	server2.Start()

	pool := NewXshardPool(log.New())
	defer pool.Close()
	pool.TrackInbound(server1)
	pool.TrackInbound(server2)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := client1.SendPing(ctx); err != nil {
		t.Fatalf("first ping: %v", err)
	}
	if _, _, err := client2.SendPing(ctx); err != nil {
		t.Fatalf("second ping: %v", err)
	}
	if !pool.WatchAndIndex(server1) {
		t.Fatal("first inbound connection was not indexed")
	}
	if !pool.WatchAndIndex(server2) {
		t.Fatal("second inbound connection was rejected (should be allowed)")
	}

	// Both connections should be indexed for the shard.
	conns := pool.Get(0x00010001)
	if len(conns) != 2 {
		t.Fatalf("expected 2 connections for shard, got %d", len(conns))
	}
	if !pool.HasSlaveID([]byte("same-slave")) {
		t.Fatal("slaveID not tracked")
	}
}

func TestXshardPool_OutboundAndInboundCoexist(t *testing.T) {
	// Simulates S1 (local) ↔ S2 (remote-slave) with bidirectional connections.
	// S1 → S2 (outbound): client1 connects to server1
	// S2 → S1 (inbound):  client2 connects to server2
	// Both connections share the same remote slave identity and should coexist.
	client1, server1, cleanup1 := newTestConnPairWithIdentity(
		t, []byte("local"), []uint32{0x00030004}, []byte("remote-slave"), []uint32{0x00010001},
	)
	defer cleanup1()
	client2, server2, cleanup2 := newTestConnPairWithIdentity(
		t, []byte("remote-slave"), []uint32{0x00010001}, []byte("local"), []uint32{0x00030004},
	)
	defer cleanup2()
	client1.Start()
	server1.Start()
	client2.Start()
	server2.Start()

	pool := NewXshardPool(log.New())
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Add outbound connection (S1 → S2).
	if err := pool.VerifyAndAddToShards(ctx, client1, []byte("remote-slave"), []uint32{0x00010001}); err != nil {
		t.Fatalf("outbound verify and add: %v", err)
	}

	// Add inbound connection (S2 → S1).
	pool.TrackInbound(server2)
	if _, _, err := client2.SendPing(ctx); err != nil {
		t.Fatalf("inbound ping: %v", err)
	}
	if !pool.WatchAndIndex(server2) {
		t.Fatal("inbound connection was rejected")
	}

	// Both connections should be indexed for the remote shard.
	conns := pool.Get(0x00010001)
	if len(conns) != 2 {
		t.Fatalf("expected 2 connections, got %d", len(conns))
	}
	if !pool.HasSlaveID([]byte("remote-slave")) {
		t.Fatal("slaveID not tracked")
	}
}

func TestXshardPool_InboundFirstOutboundSkipped(t *testing.T) {
	// Simulates S1 ↔ S2 where inbound (S2→S1) completes first, then
	// outbound (S1→S2) should be silently skipped (Python's connect_to_slave
	// returns "" when slave is already in slave_ids).
	// S1 is "local", S2 is "remote-slave".
	client1, server1, cleanup1 := newTestConnPairWithIdentity(
		t, []byte("local"), []uint32{0x00030004}, []byte("remote-slave"), []uint32{0x00010001},
	)
	defer cleanup1()
	client2, server2, cleanup2 := newTestConnPairWithIdentity(
		t, []byte("remote-slave"), []uint32{0x00010001}, []byte("local"), []uint32{0x00030004},
	)
	defer cleanup2()
	client1.Start()
	server1.Start()
	client2.Start()
	server2.Start()

	pool := NewXshardPool(log.New())
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Step 1: inbound connection (S2 → S1) arrives first.
	// client2 (S2) connects to server2 (S1); pool tracks server2 as the inbound side.
	pool.TrackInbound(server2)
	if _, _, err := client2.SendPing(ctx); err != nil {
		t.Fatalf("inbound ping: %v", err)
	}
	if !pool.WatchAndIndex(server2) {
		t.Fatal("inbound connection was not indexed")
	}
	if !pool.HasSlaveID([]byte("remote-slave")) {
		t.Fatal("slaveID not registered after inbound")
	}

	// Step 2: outbound connection (S1 → S2) should be silently skipped.
	// Python's connect_to_slave returns "" (success) when slave is already in slave_ids.
	if err := pool.VerifyAndAddToShards(ctx, client1, []byte("remote-slave"), []uint32{0x00010001}); err != nil {
		t.Fatalf("outbound should be silently skipped, got error: %v", err)
	}

	// The original inbound connection should still be indexed.
	conns := pool.Get(0x00010001)
	if len(conns) != 1 {
		t.Fatalf("expected 1 connection (inbound only), got %d", len(conns))
	}
	if !pool.HasSlaveID([]byte("remote-slave")) {
		t.Fatal("slaveID should still be tracked")
	}
}

func TestXshardPool_TrackInboundClose(t *testing.T) {
	pool := NewXshardPool(log.New())

	_, xc, cleanup := newTestConnPair(t)
	defer cleanup()

	xc.Start()
	pool.TrackInbound(xc)
	pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := xc.SendRPC(ctx, byte(wire.ClusterOpPing), []byte("ping"))
	if err != conn.ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed after pool close, got %v", err)
	}
}

func TestXshardPool_SendXshardTxNoConnection(t *testing.T) {
	pool := NewXshardPool(log.New())
	defer pool.Close()

	ctx := context.Background()
	_, err := pool.SendXshardTx(ctx, 0x00010001, []byte("tx"))
	if err == nil {
		t.Fatal("expected error when no connection exists")
	}
}

func TestXshardPool_ClosedPoolRejectsAdd(t *testing.T) {
	pool := NewXshardPool(log.New())
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

// TestParseAddXshardTxListResponse_NonZeroErrorCode verifies that a non-zero
// error_code in an AddXshardTxListResponse is treated as an operation failure.
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
	resp, err := ParseAddXshardTxListResponse(frame)
	if err == nil {
		t.Fatal("expected error for non-zero error_code, got nil")
	}
	if resp == nil || resp.ErrorCode != errCode {
		t.Fatalf("expected decoded response with error_code %d, got resp=%v err=%v", errCode, resp, err)
	}
}

// TestParseAddXshardTxListResponse_ZeroErrorCode verifies that a zero
// error_code is accepted as success.
func TestParseAddXshardTxListResponse_ZeroErrorCode(t *testing.T) {
	payload, err := serialize.SerializeToBytes(&wire.AddXshardTxListResponse{ErrorCode: 0})
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	frame := &wire.Frame{
		Opcode:  byte(wire.ClusterOpAddXshardTxListResponse),
		Payload: payload,
	}
	resp, err := ParseAddXshardTxListResponse(frame)
	if err != nil {
		t.Fatalf("expected success for error_code 0, got: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("expected error_code 0, got %d", resp.ErrorCode)
	}
}

// TestParseAddXshardTxListResponse_WrongOpcode verifies that a response frame
// with an unexpected opcode is rejected.
func TestParseAddXshardTxListResponse_WrongOpcode(t *testing.T) {
	payload, _ := serialize.SerializeToBytes(&wire.AddXshardTxListResponse{ErrorCode: 0})
	frame := &wire.Frame{
		Opcode:  byte(wire.ClusterOpPong),
		Payload: payload,
	}
	if _, err := ParseAddXshardTxListResponse(frame); err == nil {
		t.Fatal("expected error for wrong opcode, got nil")
	}
}

// TestNewXshardPool_NilLogger verifies that NewXshardPool(nil) does not panic
// and subsequent log calls are safe.
func TestNewXshardPool_NilLogger(t *testing.T) {
	pool := NewXshardPool(nil)
	if pool == nil {
		t.Fatal("NewXshardPool(nil) returned nil")
	}
	// Close should not panic on nil logger.
	pool.Close()
}

// TestXshardConn_RejectEmptyPingID verifies that a PING with an empty slave ID
// is rejected and the connection is closed.
func TestXshardConn_RejectEmptyPingID(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()

	client.Start()
	server.Start()

	pingPayload, err := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte{}, // empty ID
		FullShardIDList: []uint32{0x00010001},
		RootTip:         nil,
	})
	if err != nil {
		t.Fatalf("serialize ping: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Send PING from client to server; server's handlePing rejects empty ID.
	_, err = client.SendRPC(ctx, byte(wire.ClusterOpPing), pingPayload)
	if err != conn.ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
	// Verify server recorded no identity.
	if len(server.RemoteID()) != 0 {
		t.Fatalf("expected empty remote ID, got %s", server.RemoteID())
	}
}

// TestXshardPool_WatchAndIndexIdempotent verifies that calling WatchAndIndex
// twice on the same connection does not create duplicate route entries.
func TestXshardPool_WatchAndIndexIdempotent(t *testing.T) {
	client, server, cleanup := newTestConnPairWithIdentity(
		t, []byte("client-slave"), []uint32{0x00010001}, []byte("server-slave"), []uint32{0x00030004},
	)
	defer cleanup()
	client.Start()
	server.Start()

	pool := NewXshardPool(log.New())
	defer pool.Close()
	pool.TrackInbound(server)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := client.SendPing(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// First call.
	if !pool.WatchAndIndex(server) {
		t.Fatal("first WatchAndIndex failed")
	}

	// Second call on the same connection — must be idempotent.
	if !pool.WatchAndIndex(server) {
		t.Fatal("second WatchAndIndex failed")
	}

	conns := pool.Get(0x00010001)
	if len(conns) != 1 {
		t.Fatalf("expected 1 connection, got %d (duplicate route entry)", len(conns))
	}
}
