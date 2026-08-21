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
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// ── pool test helpers (white-box, same package) ──────────────────────────────

// hasSlaveID reports whether the pool tracks the given peer identity.
func (p *XshardPool) hasSlaveID(id []byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.slaveIDs[string(id)]
	return ok
}

// outboundSize returns the number of distinct connections in the shard index.
func (p *XshardPool) outboundSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	seen := make(map[*xshardConn]struct{})
	for _, conns := range p.conns {
		for _, conn := range conns {
			seen[conn] = struct{}{}
		}
	}
	return len(seen)
}

// connectionsSize returns the number of tracked connections (including conns
// still in the PING handshake).
func (p *XshardPool) connectionsSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.connections)
}

// targets returns all full shard IDs that have connections.
func (p *XshardPool) targets() []uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	targets := make([]uint32, 0, len(p.conns))
	for id := range p.conns {
		targets = append(targets, id)
	}
	return targets
}

// ── business handler test hook ────────────────────────────────────────────────

// testXshardHandler is a test hook standing in for the business handler: it
// acknowledges every request so tests without real business logic do not fail.
type testXshardHandler struct{}

func (testXshardHandler) AddXshardTxList(*wire.AddXshardTxListRequest) (*wire.AddXshardTxListResponse, error) {
	return &wire.AddXshardTxListResponse{}, nil
}

func (testXshardHandler) BatchAddXshardTxList(*wire.BatchAddXshardTxListRequest) (*wire.BatchAddXshardTxListResponse, error) {
	return &wire.BatchAddXshardTxListResponse{}, nil
}

// mustNewXshardPool creates a pool with the test hook (maxPayloadSize 0).
func mustNewXshardPool(t *testing.T, selfID []byte, shards []uint32) *XshardPool {
	t.Helper()
	pool, err := NewXshardPool(selfID, shards, 0, testXshardHandler{}, log.New())
	if err != nil {
		t.Fatalf("new xshard pool: %v", err)
	}
	return pool
}

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
	client, err = newXshardConn(clientConn, 0, clientID, clientShards, nil, nil, testXshardHandler{}, logger) // 0 = no limit (matches Python)
	if err != nil {
		t.Fatalf("new client conn: %v", err)
	}
	server, err = newXshardConn(serverConn, 0, serverID, serverShards, nil, nil, testXshardHandler{}, logger)
	if err != nil {
		t.Fatalf("new server conn: %v", err)
	}
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

	client, err := newXshardConn(clientConn, 0, remoteID, remoteShards, nil, nil, testXshardHandler{}, log.New())
	if err != nil {
		clientConn.Close()
		t.Fatalf("new inbound client conn: %v", err)
	}
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

	pong, ok := resp.(*wire.PongResponse)
	if !ok {
		t.Fatalf("expected *wire.PongResponse, got %T", resp)
	}
	if string(pong.ID) != string(serverID) {
		t.Fatalf("pong id mismatch: got %s, expected %s", pong.ID, serverID)
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

// TestXshardConn_XshardTxListServedByHandler verifies AddXshardTxList is
// served by the injected business handler and keeps the connection open.
func TestXshardConn_XshardTxListServedByHandler(t *testing.T) {
	client, server, cleanup := newTestConnPair(t)
	defer cleanup()
	server.Start()
	client.Start()

	txList := wire.RawBytes{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.sendAddXshardTxList(ctx, &wire.AddXshardTxListRequest{
		Branch: 1,
		TxList: &txList,
	}); err != nil {
		t.Fatalf("sendAddXshardTxList: %v", err)
	}
	if client.IsClosed() || server.IsClosed() {
		t.Fatal("connection should stay open after AddXshardTxList")
	}
}

// TestXshardConn_SendPingRejectsWrongResponseOpcode verifies a wrong-opcode
// PONG is rejected by sendPing's opcode check but does not close the connection.
func TestXshardConn_SendPingRejectsWrongResponseOpcode(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client, err := newXshardConn(clientConn, 0, []byte("client"), []uint32{1}, nil, nil, testXshardHandler{}, log.New())
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	client.Start()
	// Reply with an AddXshardTxListResponse (wrong opcode for PING).
	peerDone := rawXshardPeer(t, serverConn, 0)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err = client.sendPing(ctx)
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
	server.Close()
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

// ── XshardPool indexing tests ─────────────────────────────────────────────────

// TestXshardPool_ClosedConnectionStaysIndexed verifies Python parity: a CLOSED
// connection is never evicted from the routing index or slave ID registry.
func TestXshardPool_ClosedConnectionStaysIndexed(t *testing.T) {
	rs := startRemoteSlave(t, []byte("server-slave"), []uint32{0x00030004, 0x00030005})
	defer rs.close()

	pool := mustNewXshardPool(t, []byte("local-slave"), []uint32{0x00030004})
	defer pool.Close()

	if err := pool.DialToSlave(context.Background(), rs.slaveInfo([]byte("server-slave"), []uint32{0x00030004, 0x00030005})); err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Grab the indexed outbound connection and close it directly.
	var target *xshardConn
	for _, shardID := range []uint32{0x00030004, 0x00030005} {
		conns := pool.get(shardID)
		if len(conns) != 1 {
			t.Fatalf("expected 1 conn for shard 0x%x, got %d", shardID, len(conns))
		}
		target = conns[0]
	}
	target.Close()

	for _, shardID := range []uint32{0x00030004, 0x00030005} {
		if conns := pool.get(shardID); len(conns) != 1 || conns[0] != target {
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

// ── inbound tests ─────────────────────────────────────────────────────────────

// TestXshardPool_HandleInboundAllowsMultipleInboundConnections verifies two
// inbound connections from the same remote are both accepted (Python's
// handle_new_connection does not check slave_ids).
func TestXshardPool_HandleInboundAllowsMultipleInboundConnections(t *testing.T) {
	pool := mustNewXshardPool(t, []byte("local-slave"), []uint32{0x00030004})
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
	pool := mustNewXshardPool(t, []byte("local"), []uint32{0x00030004})
	defer pool.Close()

	// Outbound (local → remote-slave) via a simulated remote slave.
	rs := startRemoteSlave(t, []byte("remote-slave"), []uint32{0x00010001})
	defer rs.close()
	if err := pool.DialToSlave(context.Background(), rs.slaveInfo([]byte("remote-slave"), []uint32{0x00010001})); err != nil {
		t.Fatalf("outbound dial: %v", err)
	}

	// Inbound (remote-slave → local).
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
// skipped by DialToSlave's pre-check (Python's connect_to_slave returns "" when
// the slave is already in slave_ids).
func TestXshardPool_InboundFirstOutboundSkipped(t *testing.T) {
	pool := mustNewXshardPool(t, []byte("local"), []uint32{0x00030004})
	defer pool.Close()

	// Inbound first.
	establishInbound(t, pool, []byte("remote-slave"), []uint32{0x00010001})
	if !pool.hasSlaveID([]byte("remote-slave")) {
		t.Fatal("slaveID not registered after inbound")
	}

	// Outbound should be silently skipped (already known from inbound).
	rs := startRemoteSlave(t, []byte("remote-slave"), []uint32{0x00010001})
	defer rs.close()
	if err := pool.DialToSlave(context.Background(), rs.slaveInfo([]byte("remote-slave"), []uint32{0x00010001})); err != nil {
		t.Fatalf("outbound should be silently skipped, got error: %v", err)
	}
	if rs.acceptedCount() != 0 {
		t.Fatalf("expected no accepted connection on the remote, got %d", rs.acceptedCount())
	}

	// Only the original inbound connection remains indexed.
	if conns := pool.get(0x00010001); len(conns) != 1 {
		t.Fatalf("expected 1 connection (inbound only), got %d", len(conns))
	}
	if !pool.hasSlaveID([]byte("remote-slave")) {
		t.Fatal("slaveID should still be tracked")
	}
}

// TestXshardPool_HandleInboundDeadConnEvicted verifies that an inbound conn
// which closes before sending PING is evicted from the tracking set: dead
// connections must not accumulate (F3).
func TestXshardPool_HandleInboundDeadConnEvicted(t *testing.T) {
	pool := mustNewXshardPool(t, []byte("local-slave"), []uint32{0x00030004})
	defer pool.Close()

	clientConn, serverConn := newRawConnPair(t)
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		pool.HandleInbound(serverConn)
		close(done)
	}()

	// Wait until HandleInbound registers the connection as pending inbound.
	deadline := time.Now().Add(2 * time.Second)
	for pool.connectionsSize() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("HandleInbound did not register pending inbound")
		}
		time.Sleep(time.Millisecond)
	}

	// Remote disconnects without ever sending PING.
	clientConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleInbound did not return after remote close")
	}

	if pool.connectionsSize() != 0 {
		t.Fatalf("expected dead inbound conn to be evicted, connectionsSize=%d", pool.connectionsSize())
	}
}

// TestXshardPool_HandleInboundPendingClose verifies the Go safety enhancement:
// a pending inbound connection (PING not yet received) is closed by pool Close,
// whereas Python leaks it.
func TestXshardPool_HandleInboundPendingClose(t *testing.T) {
	pool := mustNewXshardPool(t, []byte("local-slave"), []uint32{0x00030004})

	clientConn, serverConn := newRawConnPair(t)
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		pool.HandleInbound(serverConn)
		close(done)
	}()

	// Wait until HandleInbound registers the connection as pending inbound.
	deadline := time.Now().Add(2 * time.Second)
	for pool.connectionsSize() == 0 {
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

// TestXshardPool_InboundDoesNotSkipSelf verifies the deliberate Python-parity
// divergence: HandleInbound does NOT skip a self connection, whereas DialToSlave
// does. Only the outbound pre-check guards against self; inbound is allowed to
// index a remote that claims our own identity (Python's handle_new_connection
// performs no self check).
func TestXshardPool_InboundDoesNotSkipSelf(t *testing.T) {
	pool := mustNewXshardPool(t, []byte("local-slave"), []uint32{0x00030004})
	defer pool.Close()

	// Inbound connection claiming to be self must still be indexed.
	establishInbound(t, pool, []byte("local-slave"), []uint32{0x00030004})

	if conns := pool.get(0x00030004); len(conns) != 1 {
		t.Fatalf("expected self inbound connection to be indexed, got %d", len(conns))
	}
	if !pool.hasSlaveID([]byte("local-slave")) {
		t.Fatal("self ID should be tracked for inbound")
	}
}

// ── send-side response verification tests ────────────────────────────────────

// rawXshardPeer answers a single inbound request frame with an
// AddXshardTxListResponse carrying the given error code, over a net.Pipe.
func rawXshardPeer(t *testing.T, serverConn net.Conn, errCode uint32) <-chan error {
	t.Helper()
	peerDone := make(chan error, 1)
	go func() {
		request, err := wire.ReadFrameNoMeta(serverConn, 0)
		if err != nil {
			peerDone <- err
			return
		}
		payload, err := serialize.SerializeToBytes(&wire.AddXshardTxListResponse{ErrorCode: errCode})
		if err == nil {
			err = wire.WriteFrameNoMeta(serverConn, &wire.Frame{
				Opcode:  byte(wire.ClusterOpAddXshardTxListResponse),
				RPCID:   request.RPCID,
				Payload: payload,
			})
		}
		peerDone <- err
	}()
	return peerDone
}

func TestXshardConn_SendXshardTxListErrorCode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		errCode uint32
		wantErr bool
	}{
		{"zero error code succeeds", 0, false},
		{"non-zero error code fails", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			client, err := newXshardConn(clientConn, 0, []byte("client"), []uint32{1}, nil, nil, testXshardHandler{}, log.New())
			if err != nil {
				t.Fatalf("new conn: %v", err)
			}
			client.Start()
			peerDone := rawXshardPeer(t, serverConn, tc.errCode)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = client.sendAddXshardTxList(ctx, &wire.AddXshardTxListRequest{Branch: 1, TxList: &wire.RawBytes{}})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error for non-zero error_code, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("expected success for error_code 0, got: %v", err)
				}
			}
			if err := <-peerDone; err != nil {
				t.Fatalf("raw peer failed: %v", err)
			}
			if client.IsClosed() {
				t.Fatal("error_code should not close the connection")
			}
		})
	}
}

func TestNewXshardPool_NilLogger(t *testing.T) {
	pool, err := NewXshardPool(nil, nil, 0, testXshardHandler{}, nil)
	if err != nil {
		t.Fatalf("nil logger should be accepted: %v", err)
	}
	if pool == nil {
		t.Fatal("NewXshardPool returned nil")
	}
	pool.Close()
}

func TestNewXshardPool_NilHandler(t *testing.T) {
	if _, err := NewXshardPool(nil, nil, 0, nil, log.New()); err == nil {
		t.Fatal("expected error for nil handler")
	}
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
		conn, err := newXshardConn(c, 0, remoteID, remoteShards, nil, nil, testXshardHandler{}, log.New())
		if err != nil {
			c.Close()
			continue
		}
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

	pool := mustNewXshardPool(t, []byte("local-slave"), []uint32{0x00030004})
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

	pool := mustNewXshardPool(t, []byte("local-slave"), []uint32{0x00030004})
	defer pool.Close()

	ctx := context.Background()
	if err := pool.DialToSlave(ctx, rs.slaveInfo([]byte("local-slave"), []uint32{0x00030004})); err != nil {
		t.Fatalf("self dial should be skipped: %v", err)
	}
	if rs.acceptedCount() != 0 {
		t.Fatalf("expected no connection for self, got %d", rs.acceptedCount())
	}
	if pool.hasSlaveID([]byte("local-slave")) {
		t.Fatal("self ID should not be registered")
	}
}

// TestXshardPool_DialToSlaveConcurrentDedup verifies the final dedup safety net:
// concurrent dials to the same remote result in a single registered outbound.
func TestXshardPool_DialToSlaveConcurrentDedup(t *testing.T) {
	rs := startRemoteSlave(t, []byte("remote-slave"), []uint32{0x00010001})
	defer rs.close()

	pool := mustNewXshardPool(t, []byte("local-slave"), []uint32{0x00030004})
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
	pool := mustNewXshardPool(t, []byte("local-slave"), []uint32{0x00030004})
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
