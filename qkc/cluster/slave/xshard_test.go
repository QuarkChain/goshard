// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"net"
	"syscall"
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

// TestXshardConn_DefaultPingHandler verifies that PING is handled internally
// even when the server does not register a PING handler. The server still
// records peer identity and returns a PONG with its own identity.
func TestXshardConn_DefaultPingHandler(t *testing.T) {
	clientID := []byte("client-slave")
	clientShards := []uint32{0x00010001}
	serverID := []byte("server-slave")
	serverShards := []uint32{0x00030004}

	client, server, cleanup := newTestConnPairWithIdentity(t, clientID, clientShards, serverID, serverShards)
	defer cleanup()

	// Server does NOT register any handler; PING should be handled internally.
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

// TestXshardConn_RejectEmptyShardList verifies that empty shard list causes
// connection close (Python's close_with_error behavior). The peer ID is still
// recorded before closing, matching Python's handle_ping.
func TestXshardConn_XshardRPCStubReturnsProtocolError(t *testing.T) {
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
	frame, err := client.SendXshardTxList(ctx, payload)
	if err != nil {
		t.Fatalf("send xshard RPC: %v", err)
	}
	if frame.Opcode != byte(wire.ClusterOpAddXshardTxListResponse) {
		t.Fatalf("unexpected response opcode: 0x%x", frame.Opcode)
	}
	resp, err := ParseAddXshardTxListResponse(frame)
	if err == nil {
		t.Fatal("expected unavailable-shard response")
	}
	if resp == nil || resp.ErrorCode != uint32(syscall.ENOENT) {
		t.Fatalf("unexpected response: %#v, err=%v", resp, err)
	}
	if client.IsClosed() || server.IsClosed() {
		t.Fatal("xshard RPC stub closed a live connection")
	}
}

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
		payload, err := serialize.SerializeToBytes(&wire.PongResponse{
			ID:              []byte("server"),
			FullShardIDList: []uint32{2},
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
		t.Fatal("wrong PING response opcode unexpectedly closed connection")
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

func TestXshardPool_AddGetRemove(t *testing.T) {
	pool := NewXshardPool(log.New())
	defer pool.Close()

	// Use stub connections that are never started.
	_, conn1, cleanup1 := newTestConnPair(t)
	defer cleanup1()
	_, conn2, cleanup2 := newTestConnPair(t)
	defer cleanup2()

	pool.Add(0x00010001, conn1)
	pool.Add(0x00010001, conn2)
	pool.Add(0x00020001, conn1)

	if got := pool.OutboundSize(); got != 3 {
		t.Fatalf("expected pool outbound size 3, got %d", got)
	}

	conns := pool.Get(0x00010001)
	if len(conns) != 2 {
		t.Fatalf("expected 2 conns for shard 0x00010001, got %d", len(conns))
	}

	pool.Remove(0x00010001, conn1)
	if got := pool.OutboundSize(); got != 1 {
		t.Fatalf("expected pool outbound size 1 after remove, got %d", got)
	}
	conns = pool.Get(0x00010001)
	if len(conns) != 1 || conns[0] != conn2 {
		t.Fatalf("expected only conn2 for shard 0x00010001")
	}

	targets := pool.Targets()
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
}

func TestXshardPool_RemoveRemovesAllRoutes(t *testing.T) {
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

	pool.Remove(0x00030004, client)
	for _, shardID := range []uint32{0x00030004, 0x00030005} {
		if conns := pool.Get(shardID); len(conns) != 0 {
			t.Fatalf("route 0x%x still contains %d connections", shardID, len(conns))
		}
	}
	if pool.HasSlaveID([]byte("server-slave")) {
		t.Fatal("slave ID remains after removing all routes")
	}
}

func TestXshardPool_RemoveTargetRemovesAllRoutes(t *testing.T) {
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

	pool.RemoveTarget(0x00030004)
	for _, shardID := range []uint32{0x00030004, 0x00030005} {
		if conns := pool.Get(shardID); len(conns) != 0 {
			t.Fatalf("route 0x%x still contains %d connections", shardID, len(conns))
		}
	}
	if pool.HasSlaveID([]byte("server-slave")) {
		t.Fatal("slave ID remains after removing target")
	}
}

func TestXshardPool_ClosedConnectionEvictedFromAllRoutes(t *testing.T) {
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
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(pool.Targets()) == 0 && !pool.HasSlaveID([]byte("server-slave")) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("closed connection was not evicted: targets=%v has_slave_id=%v",
		pool.Targets(), pool.HasSlaveID([]byte("server-slave")))
}

func TestXshardPool_WatchAndIndexRejectsDuplicateInboundSlave(t *testing.T) {
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
	if pool.WatchAndIndex(server2) {
		t.Fatal("duplicate inbound slave was indexed")
	}
	<-server2.WaitUntilClosed()

	conns := pool.Get(0x00010001)
	if len(conns) != 1 || conns[0] != server1 {
		t.Fatalf("expected only first connection to be indexed, got %v", conns)
	}
	if !pool.HasSlaveID([]byte("same-slave")) {
		t.Fatal("duplicate eviction removed the active slave ID")
	}
}

func TestXshardPool_DelayedWatcherDoesNotDeleteReusedSlaveID(t *testing.T) {
	connA, connB, cleanup := newTestConnPair(t)
	defer cleanup()

	const remoteID = "same-slave"
	connA.SetRemoteIdentity([]byte(remoteID), []uint32{0x00030004})
	connB.SetRemoteIdentity([]byte(remoteID), []uint32{0x00030005})
	pool := NewXshardPool(log.New())
	defer pool.Close()

	pool.mu.Lock()
	pool.conns[0x00030004] = []*XshardConn{connA}
	pool.slaveIDs[remoteID] = true
	pool.watchConnectionLocked(connA)

	// Simulate RemoveTarget completing before the old connection's watcher runs.
	pool.removeConnectionLocked(connA)
	pool.conns[0x00030005] = []*XshardConn{connB}
	pool.slaveIDs[remoteID] = true
	pool.watchConnectionLocked(connB)
	connA.Close()
	pool.mu.Unlock()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		pool.mu.RLock()
		_, watcherPending := pool.watched[connA]
		slaveIDTracked := pool.slaveIDs[remoteID]
		connBIndexed := len(pool.conns[0x00030005]) == 1 && pool.conns[0x00030005][0] == connB
		pool.mu.RUnlock()
		if !watcherPending {
			if !slaveIDTracked {
				t.Fatal("delayed connA watcher deleted connB's slave ID")
			}
			if !connBIndexed {
				t.Fatal("connB route was removed unexpectedly")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("connA watcher did not finish")
}

func TestXshardPool_RemoveTargetClosesConnections(t *testing.T) {
	pool := NewXshardPool(log.New())
	defer pool.Close()

	_, xc, cleanup := newTestConnPair(t)
	defer cleanup()

	xc.Start()
	pool.Add(0x00010001, xc)
	pool.RemoveTarget(0x00010001)

	if pool.OutboundSize() != 0 {
		t.Fatalf("expected pool outbound size 0, got %d", pool.OutboundSize())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := xc.SendRPC(ctx, byte(wire.ClusterOpPing), []byte("ping"))
	if err != conn.ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
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
	pool.Add(0x00010001, xc)

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
