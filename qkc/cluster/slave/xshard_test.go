// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

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
	if got := pool.OutboundSize(); got != 2 {
		t.Fatalf("expected pool outbound size 2 after remove, got %d", got)
	}
	conns = pool.Get(0x00010001)
	if len(conns) != 1 || conns[0] != conn2 {
		t.Fatalf("expected only conn2 for shard 0x00010001")
	}

	targets := pool.Targets()
	if len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(targets))
	}
}

func TestXshardPool_RemoveTargetClosesConnections(t *testing.T) {
	pool := NewXshardPool(log.New())
	defer pool.Close()

	_, conn, cleanup := newTestConnPair(t)
	defer cleanup()

	conn.Start()
	pool.Add(0x00010001, conn)
	pool.RemoveTarget(0x00010001)

	if pool.OutboundSize() != 0 {
		t.Fatalf("expected pool outbound size 0, got %d", pool.OutboundSize())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), []byte("ping"))
	if err != ErrConnectionClosed {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
}

func TestXshardPool_TrackInboundClose(t *testing.T) {
	pool := NewXshardPool(log.New())

	_, conn, cleanup := newTestConnPair(t)
	defer cleanup()

	conn.Start()
	pool.TrackInbound(conn)
	pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), []byte("ping"))
	if err != ErrConnectionClosed {
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

	_, conn, cleanup := newTestConnPair(t)
	defer cleanup()

	conn.Start()
	pool.Add(0x00010001, conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := conn.SendRPC(ctx, byte(wire.ClusterOpPing), []byte("ping"))
	if err != ErrConnectionClosed {
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
