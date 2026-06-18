// Copyright 2026-2027, QuarkChain.
package cluster

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// =============================================================================
// SlaveRPC integration tests — demonstrate all three communication modes
// =============================================================================
//
// Three modes:
//
//	Mode 1: Master ↔ Slave  (cluster_peer_id == 0)
//	Mode 2: Slave ↔ Slave   (direct xshard TCP)
//	Mode 3: Peer → Master → Slave  (cluster_peer_id != 0, virtual P2P)

// framedServer is a single-connection TCP server that speaks the cluster
// frame protocol.
type framedServer struct {
	listener net.Listener
	conn     net.Conn
	wg       sync.WaitGroup
	mu       sync.Mutex
}

func newFramedServer(t *testing.T) *framedServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &framedServer{listener: l}
	s.wg.Add(1)
	go s.accept(t)
	return s
}

func (s *framedServer) accept(t *testing.T) {
	defer s.wg.Done()
	conn, err := s.listener.Accept()
	if err != nil {
		return
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
}

func (s *framedServer) addr() string { return s.listener.Addr().String() }
func (s *framedServer) close()       { s.listener.Close(); s.wg.Wait() }

func (s *framedServer) sendFrame(t *testing.T, f *Frame) {
	t.Helper()
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		t.Fatal("no connection")
	}
	if err := WriteFrameToWriter(conn, f); err != nil {
		t.Fatalf("sendFrame: %v", err)
	}
}

func (s *framedServer) readFrame(t *testing.T) *Frame {
	t.Helper()
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		t.Fatal("no connection")
	}
	f, err := ReadFrameFromReader(conn)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	return f
}

// =============================================================================
// Mode 1: Master ↔ Slave
// =============================================================================

func TestSlaveRPC_PingPong(t *testing.T) {
	master := newFramedServer(t)
	defer master.close()

	rpc, err := NewSlaveRPC(&Config{MasterAddr: master.addr(), ListenAddr: ""})
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	rpc.RegisterHandlers()
	time.Sleep(100 * time.Millisecond)

	pingReq := &PingRequest{ID: []byte("slave-1"), FullShardIDList: []uint32{4, 5, 6}}
	pingPayload, _ := serialize.SerializeToBytes(pingReq)
	master.sendFrame(t, &Frame{Opcode: OP_PING, RPCID: 100, Payload: pingPayload})

	resp := master.readFrame(t)
	if resp.Opcode != OP_PONG {
		t.Fatalf("expected OP_PONG, got 0x%x", resp.Opcode)
	}

	var pong PongResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &pong); err != nil {
		t.Fatal(err)
	}
	if string(pong.ID) != "slave-1" {
		t.Errorf("expected slave-1, got %s", pong.ID)
	}
	t.Log("OK")
}

func TestSlaveRPC_StubHandler(t *testing.T) {
	master := newFramedServer(t)
	defer master.close()

	rpc, err := NewSlaveRPC(&Config{MasterAddr: master.addr(), ListenAddr: ""})
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	rpc.RegisterHandlers()
	time.Sleep(100 * time.Millisecond)

	master.sendFrame(t, &Frame{Opcode: OP_ADD_ROOT_BLOCK_REQUEST, RPCID: 1, Payload: []byte("dummy")})
	time.Sleep(200 * time.Millisecond) // stub returns ErrNotImplemented → no response
	t.Log("OK")
}

func TestSlaveRPC_SendRPCToMaster(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn == nil {
			return
		}
		defer conn.Close()
		frame, _ := ReadFrameFromReader(conn)
		WriteFrameToWriter(conn, &Frame{
			Opcode:  frame.Opcode + 1,
			RPCID:   frame.RPCID,
			Payload: []byte("master-ack"),
		})
	}()

	rpc, err := NewSlaveRPC(&Config{MasterAddr: listener.Addr().String(), ListenAddr: ""})
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := rpc.slave.SendRPCToMaster(ctx, OP_ADD_MINOR_BLOCK_HEADER_REQUEST, []byte("header-data"))
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Payload) != "master-ack" {
		t.Errorf("expected master-ack, got %s", resp.Payload)
	}
}

// =============================================================================
// Mode 2: Slave ↔ Slave
// =============================================================================

func TestSlaveRPC_XshardPingHandshake(t *testing.T) {
	slaveBListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer slaveBListener.Close()

	pingSent := make(chan struct{})
	pongReceived := make(chan []byte, 1)
	go func() {
		conn, _ := slaveBListener.Accept()
		if conn == nil {
			return
		}
		defer conn.Close()
		pingReq := &PingRequest{ID: []byte("slave-B"), FullShardIDList: []uint32{0, 1}}
		payload, _ := serialize.SerializeToBytes(pingReq)
		WriteFrameToWriter(conn, &Frame{Opcode: OP_PING, RPCID: 7, Payload: payload})
		close(pingSent)
		resp, _ := ReadFrameFromReader(conn)
		if resp != nil {
			pongReceived <- resp.Payload
		}
	}()

	// Use a real Slave + SlaveRPC to hold the xshard handler map
	master := newFramedServer(t)
	defer master.close()

	rpc, err := NewSlaveRPC(&Config{MasterAddr: master.addr(), ListenAddr: ""})
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	rpc.RegisterHandlers() // includes xshard handlers

	xc, err := NewXshardConn(slaveBListener.Addr().String(), log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer xc.Close()
	rpc.slave.applyXshardHandlers(xc)

	select {
	case <-pingSent:
	case <-time.After(5 * time.Second):
		t.Fatal("slave B did not send PING")
	}

	select {
	case payload := <-pongReceived:
		var pong PongResponse
		if err := serialize.Deserialize(serialize.NewByteBuffer(payload), &pong); err != nil {
			t.Fatal(err)
		}
		if string(pong.ID) != "slave-B" {
			t.Errorf("expected slave-B, got %s", pong.ID)
		}
		t.Log("OK")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for PONG")
	}
}

func TestSlaveRPC_XshardSendTxList(t *testing.T) {
	slaveBListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer slaveBListener.Close()

	received := make(chan *Frame, 1)
	go func() {
		conn, _ := slaveBListener.Accept()
		if conn == nil {
			return
		}
		defer conn.Close()
		frame, _ := ReadFrameFromReader(conn)
		received <- frame
	}()

	xc, err := NewXshardConn(slaveBListener.Addr().String(), log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer xc.Close()

	if err := xc.SendXshardTxList(2, []byte("cross-shard-tx-data")); err != nil {
		t.Fatal(err)
	}

	select {
	case frame := <-received:
		if frame == nil {
			t.Fatal("nil frame")
		}
		if frame.Opcode != OP_ADD_XSHARD_TX_LIST_REQUEST {
			t.Errorf("expected ADD_XSHARD_TX_LIST_REQUEST, got 0x%x", frame.Opcode)
		}
		t.Log("OK")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestSlaveRPC_ConnectToSlaves(t *testing.T) {
	remoteListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer remoteListener.Close()

	connected := make(chan struct{})
	go func() {
		conn, _ := remoteListener.Accept()
		if conn != nil {
			conn.Close()
		}
		close(connected)
	}()

	master := newMockMaster(t)
	defer master.close()

	rpc, err := NewSlaveRPC(&Config{MasterAddr: master.addr(), ListenAddr: ""})
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	rpc.RegisterHandlers()

	host, port := "127.0.0.1", remoteListener.Addr().(*net.TCPAddr).Port
	req := ConnectToSlavesRequest{
		SlaveInfoList: []SlaveInfo{
			{ID: []byte("remote"), Host: []byte(host), Port: uint16(port), FullShardIDList: []uint32{2, 3}},
		},
	}
	payload, _ := serialize.SerializeToBytes(&req)
	respPayload, err := rpc.slave.ConnectToSlaves(payload)
	if err != nil {
		t.Fatal(err)
	}

	var resp ConnectToSlavesResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(respPayload), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.ResultList) != 1 || len(resp.ResultList[0]) != 0 {
		t.Errorf("expected success (empty result), got %v", resp.ResultList)
	}

	select {
	case <-connected:
		t.Log("OK")
	case <-time.After(5 * time.Second):
		t.Fatal("remote not connected")
	}
}

// =============================================================================
// Mode 3: Peer → Master → Slave
// =============================================================================

func TestSlaveRPC_PeerCommandRouting(t *testing.T) {
	master := newMockMaster(t)
	defer master.close()

	rpc, err := NewSlaveRPC(&Config{MasterAddr: master.addr(), OwnBranches: []uint32{1}, ListenAddr: ""})
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()

	// Create peer connection
	createDone := make(chan struct{})
	rpc.slave.RegisterMasterHandler(OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, func(frame *Frame) ([]byte, error) {
		rpc.slave.HandleCreateClusterPeerConnection(frame)
		close(createDone)
		return nil, nil
	})
	rpc.slave.masterConn.Handle(&Frame{Meta: Metadata{ClusterPeerID: 42}, Opcode: OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, RPCID: 1})

	select {
	case <-createDone:
	case <-time.After(5 * time.Second):
		t.Fatal("create timeout")
	}

	// Register handler and dispatch peer frame via Dispatcher
	peerBlockReceived := make(chan []byte, 1)
	rpc.RegisterPeerHandler(1, OP_NEW_MINOR_BLOCK_HEADER_LIST, func(frame *Frame) ([]byte, error) {
		peerBlockReceived <- frame.Payload
		return nil, nil
	})
	rpc.slave.masterConn.OnFrame(&Frame{
		Meta:    Metadata{Branch: 1, ClusterPeerID: 42},
		Opcode:  OP_NEW_MINOR_BLOCK_HEADER_LIST,
		Payload: []byte("peer-block-data"),
	})

	select {
	case payload := <-peerBlockReceived:
		if string(payload) != "peer-block-data" {
			t.Errorf("expected peer-block-data, got %s", payload)
		}
		t.Log("OK")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

// =============================================================================
// Full integration: all three modes
// =============================================================================

func TestSlaveRPC_FullIntegration(t *testing.T) {
	remoteSlaveListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer remoteSlaveListener.Close()

	remoteConnected := make(chan struct{})
	go func() {
		conn, _ := remoteSlaveListener.Accept()
		if conn != nil {
			conn.Close()
		}
		close(remoteConnected)
	}()

	master := newFramedServer(t)
	defer master.close()

	rpc, err := NewSlaveRPC(&Config{MasterAddr: master.addr(), OwnBranches: []uint32{0}, ListenAddr: ""})
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	rpc.RegisterHandlers()
	time.Sleep(100 * time.Millisecond)

	// 1. PING/PONG
	pingPayload, _ := serialize.SerializeToBytes(&PingRequest{ID: []byte("test-slave"), FullShardIDList: []uint32{0}})
	master.sendFrame(t, &Frame{Opcode: OP_PING, RPCID: 1, Payload: pingPayload})
	if resp := master.readFrame(t); resp.Opcode != OP_PONG {
		t.Fatal("step 1: PING/PONG failed")
	}
	t.Log("step 1: PING/PONG OK")

	// 2. CONNECT_TO_SLAVES
	host, remotePort := "127.0.0.1", remoteSlaveListener.Addr().(*net.TCPAddr).Port
	connectReq := ConnectToSlavesRequest{
		SlaveInfoList: []SlaveInfo{
			{ID: []byte("remote"), Host: []byte(host), Port: uint16(remotePort), FullShardIDList: []uint32{0x00030004}},
		},
	}
	connectPayload, _ := serialize.SerializeToBytes(&connectReq)
	master.sendFrame(t, &Frame{Opcode: OP_CONNECT_TO_SLAVES_REQUEST, RPCID: 2, Payload: connectPayload})
	select {
	case <-remoteConnected:
		t.Log("step 2: CONNECT_TO_SLAVES OK")
	case <-time.After(5 * time.Second):
		t.Fatal("step 2: timeout")
	}

	// 3. Peer command routing
	peerCreated := make(chan struct{})
	rpc.slave.RegisterMasterHandler(OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, func(frame *Frame) ([]byte, error) {
		rpc.slave.HandleCreateClusterPeerConnection(frame)
		close(peerCreated)
		return nil, nil
	})
	rpc.slave.masterConn.Handle(&Frame{Meta: Metadata{ClusterPeerID: 99}, Opcode: OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, RPCID: 3})

	select {
	case <-peerCreated:
	case <-time.After(5 * time.Second):
		t.Fatal("step 3: create timeout")
	}

	peerDone := make(chan []byte, 1)
	rpc.RegisterPeerHandler(0, OP_NEW_TRANSACTION_LIST, func(frame *Frame) ([]byte, error) {
		peerDone <- frame.Payload
		return nil, nil
	})
	rpc.slave.masterConn.OnFrame(&Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 99},
		Opcode:  OP_NEW_TRANSACTION_LIST,
		Payload: []byte("tx-from-peer"),
	})

	select {
	case payload := <-peerDone:
		if string(payload) != "tx-from-peer" {
			t.Errorf("step 3: expected tx-from-peer, got %s", payload)
		}
		t.Log("step 3: peer routing OK")
	case <-time.After(5 * time.Second):
		t.Fatal("step 3: timeout")
	}

	// 4. Slave → Master send
	if err := rpc.slave.SendToMaster(OP_GET_WORK_REQUEST, []byte("work-request")); err != nil {
		t.Fatal("step 4:", err)
	}
	t.Log("step 4: slave → master send OK")
	t.Log("full integration PASSED")
}
