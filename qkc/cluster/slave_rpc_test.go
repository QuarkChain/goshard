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

// framedServer is a mock TCP client that dials a Go slave and speaks the
// cluster frame protocol.  It simulates a Python Master for integration testing.
type framedServer struct {
	conn net.Conn
	mu   sync.Mutex
}

// newFramedServer dials the slave at slaveAddr and returns a connected mock master.
func newFramedServer(t *testing.T, slaveAddr string) *framedServer {
	t.Helper()
	conn, err := net.DialTimeout("tcp", slaveAddr, 5*time.Second)
	if err != nil {
		t.Fatal("dial slave:", err)
	}
	return &framedServer{conn: conn}
}

func (s *framedServer) close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *framedServer) sendFrame(t *testing.T, f *Frame) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		t.Fatal("no connection")
	}
	if err := WriteFrameToWriter(s.conn, f); err != nil {
		t.Fatalf("sendFrame: %v", err)
	}
}

func (s *framedServer) readFrame(t *testing.T) *Frame {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		t.Fatal("no connection")
	}
	f, err := ReadFrameFromReader(s.conn)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	return f
}

// newSlaveWithFramedServer creates a Slave listening on an ephemeral port and
// a framedServer (mock master) that dials it.  Waits for master connection.
func newSlaveWithFramedServer(t *testing.T, cfg *Config) (*SlaveRPC, *framedServer) {
	t.Helper()
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:0"
	}
	rpc, err := NewSlaveRPC(cfg)
	if err != nil {
		t.Fatal("NewSlaveRPC:", err)
	}
	// RegisterHandlers must be called BEFORE Start so handlers are in place
	// before the master connects.  Callers using this helper register after
	// it returns — so we Start() here only after the caller registers.
	// To preserve the existing test pattern (register after this helper
	// returns), we Start() here and rely on RegisterMasterHandlers applying
	// to the already-connected masterConn (it checks masterConn != nil).
	rpc.Start()
	master := newFramedServer(t, rpc.slave.listener.Addr().String())
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rpc.slave.mu.RLock()
		mc := rpc.slave.masterConn
		rpc.slave.mu.RUnlock()
		if mc != nil {
			return rpc, master
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("slave did not accept master connection in time")
	return nil, nil
}

// =============================================================================
// Mode 1: Master ↔ Slave
// =============================================================================

func TestSlaveRPC_PingPong(t *testing.T) {
	rpc, master := newSlaveWithFramedServer(t, &Config{ID: "slave-1"})
	defer master.close()
	defer rpc.Close()
	rpc.RegisterHandlers()

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
	rpc, master := newSlaveWithFramedServer(t, &Config{})
	defer master.close()
	defer rpc.Close()
	rpc.RegisterHandlers()

	master.sendFrame(t, &Frame{Opcode: OP_ADD_ROOT_BLOCK_REQUEST, RPCID: 1, Payload: []byte("dummy")})
	time.Sleep(200 * time.Millisecond) // stub returns ErrNotImplemented → no response
	t.Log("OK")
}

func TestSlaveRPC_SendRPCToMaster(t *testing.T) {
	// Create slave listening on ephemeral port
	rpc, err := NewSlaveRPC(&Config{ID: "slave-1", ListenAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	rpc.Start() // begin accepting connections before master dials in

	// Mock master dials in and responds to the RPC
	conn, err := net.DialTimeout("tcp", rpc.slave.listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal("dial slave:", err)
	}
	defer conn.Close()

	// Wait for slave to accept the connection
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rpc.slave.mu.RLock()
		mc := rpc.slave.masterConn
		rpc.slave.mu.RUnlock()
		if mc != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rpc.slave.masterConn == nil {
		t.Fatal("slave did not accept master connection in time")
	}

	// Mock master reads the incoming RPC request and sends response
	go func() {
		frame, err := ReadFrameFromReader(conn)
		if err != nil {
			return
		}
		WriteFrameToWriter(conn, &Frame{
			Opcode:  frame.Opcode + 1,
			RPCID:   frame.RPCID,
			Payload: []byte("master-ack"),
		})
	}()

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
		// SlaveConnection uses 0-byte Metadata (not ClusterMetadata).
		WriteFrameNoMetaToWriter(conn, &Frame{Opcode: OP_PING, RPCID: 7, Payload: payload})
		close(pingSent)
		resp, _ := ReadFrameNoMetaFromReader(conn)
		if resp != nil {
			pongReceived <- resp.Payload
		}
	}()

	// Create slave (listen-only, no master needed for xshard test)
	rpc, err := NewSlaveRPC(&Config{ID: "slave-A", ListenAddr: "127.0.0.1:0"})
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
	xc.Start()

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
		if string(pong.ID) != "slave-A" {
			t.Errorf("expected slave-A, got %s", pong.ID)
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
		// SlaveConnection uses 0-byte Metadata (not ClusterMetadata).
		frame, _ := ReadFrameNoMetaFromReader(conn)
		received <- frame
	}()

	xc, err := NewXshardConn(slaveBListener.Addr().String(), log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer xc.Close()
	xc.Start()

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
		if conn == nil {
			return
		}
		defer conn.Close()
		// Read PING frame, reply with PONG matching remote's advertised info.
		// SlaveConnection uses 0-byte Metadata (not ClusterMetadata).
		frame, _ := ReadFrameNoMetaFromReader(conn)
		pong := &PongResponse{ID: []byte("remote"), FullShardIDList: []uint32{2, 3}}
		pongPayload, _ := serialize.SerializeToBytes(pong)
		WriteFrameNoMetaToWriter(conn, &Frame{
			Opcode:  frame.Opcode + 1,
			RPCID:   frame.RPCID,
			Payload: pongPayload,
		})
		close(connected)
	}()

	// Create slave (listen-only, no master needed for connect-to-slaves test)
	rpc, err := NewSlaveRPC(&Config{ID: "slave-A", ListenAddr: "127.0.0.1:0"})
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
	rpc, master := newSlaveWithFramedServer(t, &Config{OwnBranches: []uint32{1}})
	defer master.close()
	defer rpc.Close()

	// Create peer connection. Wire: cluster_peer_id is in the PAYLOAD.
	createPayload, err := serialize.SerializeToBytes(&CreateClusterPeerConnectionRequest{ClusterPeerID: 42})
	if err != nil {
		t.Fatal("serialize create request:", err)
	}
	createDone := make(chan struct{})
	rpc.slave.RegisterMasterHandler(OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, func(frame *Frame) ([]byte, error) {
		rpc.slave.HandleCreateClusterPeerConnection(frame)
		close(createDone)
		return nil, nil
	})
	rpc.slave.masterConn.Handle(&Frame{
		Meta:    Metadata{ClusterPeerID: 0},
		Opcode:  OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
		RPCID:   1,
		Payload: createPayload,
	})

	select {
	case <-createDone:
	case <-time.After(5 * time.Second):
		t.Fatal("create timeout")
	}

	// Register handler and dispatch peer frame via Dispatcher
	peerBlockReceived := make(chan []byte, 1)
	if err := rpc.RegisterPeerHandler(1, OP_NEW_MINOR_BLOCK_HEADER_LIST, func(frame *Frame) ([]byte, error) {
		peerBlockReceived <- frame.Payload
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
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

	rpc, master := newSlaveWithFramedServer(t, &Config{ID: "test-slave", OwnBranches: []uint32{0}})
	defer master.close()
	defer rpc.Close()
	rpc.RegisterHandlers()

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

	// 3. Peer command routing. Wire: cluster_peer_id is in the PAYLOAD.
	createPeerPayload, err := serialize.SerializeToBytes(&CreateClusterPeerConnectionRequest{ClusterPeerID: 99})
	if err != nil {
		t.Fatal("serialize create request:", err)
	}
	peerCreated := make(chan struct{})
	rpc.slave.RegisterMasterHandler(OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, func(frame *Frame) ([]byte, error) {
		rpc.slave.HandleCreateClusterPeerConnection(frame)
		close(peerCreated)
		return nil, nil
	})
	rpc.slave.masterConn.Handle(&Frame{
		Meta:    Metadata{ClusterPeerID: 0},
		Opcode:  OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
		RPCID:   3,
		Payload: createPeerPayload,
	})

	select {
	case <-peerCreated:
	case <-time.After(5 * time.Second):
		t.Fatal("step 3: create timeout")
	}

	peerDone := make(chan []byte, 1)
	if err := rpc.RegisterPeerHandler(0, OP_NEW_TRANSACTION_LIST, func(frame *Frame) ([]byte, error) {
		peerDone <- frame.Payload
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
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

// =============================================================================
// Real e2e: two real Go Slaves talking to each other over TCP.
//
// This test replaces the previous "mock slave B = bare net.Listener" approach
// which could not exercise recordPing / WaitUntilPingReceived / xshardPool
// indexing on the inbound side.  Here both peers are real Slave instances, so
// every code path on both sides is actually executed.
// =============================================================================

// TestE2E_TwoSlavesHandshake verifies the full Slave ↔ Slave connection
// lifecycle: master triggers ConnectToSlaves on slaveA, which dials slaveB's
// listening port.  Both sides must:
//   - complete the PING/PONG handshake
//   - record each other's id and full_shard_id_list (recordPing)
//   - index the conn by peer shards in their own xshardPool
//   - register the peer id in connectedSlaveIDs
//
// After the handshake we send an ADD_XSHARD_TX_LIST from A to B and verify
// B's xshard handler is invoked — i.e. the conn is actually usable.
func TestE2E_TwoSlavesHandshake(t *testing.T) {
	// --- Two real slaves, each with its own listen port ---
	rpcA, err := NewSlaveRPC(&Config{
		ID:          "slave-A",
		ListenAddr:  "127.0.0.1:0",
		OwnBranches: []uint32{0x00030000, 0x00030001}, // chain 3, shards 0 and 1
	})
	if err != nil {
		t.Fatal("NewSlaveRPC A:", err)
	}
	defer rpcA.Close()
	rpcB, err := NewSlaveRPC(&Config{
		ID:          "slave-B",
		ListenAddr:  "127.0.0.1:0",
		OwnBranches: []uint32{0x00030002, 0x00030003}, // chain 3, shards 2 and 3
	})
	if err != nil {
		t.Fatal("NewSlaveRPC B:", err)
	}
	defer rpcB.Close()

	// Register handlers on both.  We replace the default OP_ADD_XSHARD_TX_LIST
	// stub on B with a real handler so we can observe inbound xshard traffic.
	rpcA.RegisterHandlers()
	rpcB.RegisterHandlers()

	xshardReceivedB := make(chan []byte, 1)
	rpcB.slave.SetXshardHandlers(map[byte]MasterHandler{
		OP_PING: rpcB.handleXshardPing,
		OP_ADD_XSHARD_TX_LIST_REQUEST: func(frame *Frame) ([]byte, error) {
			xshardReceivedB <- frame.Payload
			return serialize.SerializeToBytes(&AddXshardTxListResponse{ErrorCode: 0})
		},
		OP_BATCH_ADD_XSHARD_TX_LIST_REQUEST: rpcB.stub(),
	})

	rpcA.Start()
	rpcB.Start()

	// --- Mock master connects to both A and B and sends PING ---
	masterA := newFramedServer(t, rpcA.slave.listener.Addr().String())
	defer masterA.close()
	masterB := newFramedServer(t, rpcB.slave.listener.Addr().String())
	defer masterB.close()
	waitForMasterConn(t, rpcA.slave)
	waitForMasterConn(t, rpcB.slave)

	// PING/PONG with A
	pingA, _ := serialize.SerializeToBytes(&PingRequest{ID: []byte("slave-A"), FullShardIDList: rpcA.slave.FullShardIDList()})
	masterA.sendFrame(t, &Frame{Opcode: OP_PING, RPCID: 1, Payload: pingA})
	if r := masterA.readFrame(t); r.Opcode != OP_PONG {
		t.Fatal("A PONG missing")
	}
	// PING/PONG with B
	pingB, _ := serialize.SerializeToBytes(&PingRequest{ID: []byte("slave-B"), FullShardIDList: rpcB.slave.FullShardIDList()})
	masterB.sendFrame(t, &Frame{Opcode: OP_PING, RPCID: 1, Payload: pingB})
	if r := masterB.readFrame(t); r.Opcode != OP_PONG {
		t.Fatal("B PONG missing")
	}

	// --- Master tells A to connect to B (ConnectToSlaves) ---
	host, port := "127.0.0.1", rpcB.slave.listener.Addr().(*net.TCPAddr).Port
	req := ConnectToSlavesRequest{
		SlaveInfoList: []SlaveInfo{
			{
				ID:              []byte("slave-B"),
				Host:            []byte(host),
				Port:            uint16(port),
				FullShardIDList: rpcB.slave.FullShardIDList(),
			},
		},
	}
	reqPayload, _ := serialize.SerializeToBytes(&req)
	masterA.sendFrame(t, &Frame{
		Opcode:  OP_CONNECT_TO_SLAVES_REQUEST,
		RPCID:   2,
		Payload: reqPayload,
	})
	resp := masterA.readFrame(t)
	var cResp ConnectToSlavesResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &cResp); err != nil {
		t.Fatal("deserialize connect resp:", err)
	}
	if len(cResp.ResultList) != 1 || len(cResp.ResultList[0]) != 0 {
		t.Fatalf("ConnectToSlaves failed: %v", cResp.ResultList)
	}

	// --- Wait for B's inbound handshake goroutine to finish indexing ---
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rpcB.slave.mu.RLock()
		_, ok := rpcB.slave.connectedSlaveIDs["slave-A"]
		rpcB.slave.mu.RUnlock()
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	rpcB.slave.mu.RLock()
	_, bHasA := rpcB.slave.connectedSlaveIDs["slave-A"]
	rpcB.slave.mu.RUnlock()
	if !bHasA {
		t.Fatal("B never indexed inbound conn from A (recordPing / xshardPool.Add not reached)")
	}

	// --- Verify A also recorded B ---
	rpcA.slave.mu.RLock()
	_, aHasB := rpcA.slave.connectedSlaveIDs["slave-B"]
	rpcA.slave.mu.RUnlock()
	if !aHasB {
		t.Fatal("A never recorded B's id (recordPing in ConnectToSlaves not reached)")
	}

	// --- Verify xshardPool indexing on both sides ---
	// A's pool must have entries for B's shards.
	aTargets := rpcA.slave.xshardPool.Targets()
	if len(aTargets) == 0 {
		t.Fatal("A.xshardPool has no targets — outbound conn not indexed")
	}
	// B's pool must have entries for A's shards (added by the inbound goroutine
	// in handleConn after WaitUntilPingReceived).
	bTargets := rpcB.slave.xshardPool.Targets()
	if len(bTargets) == 0 {
		t.Fatal("B.xshardPool has no targets — inbound conn not indexed")
	}

	// --- Send a real xshard tx list from A to B ---
	// A's pool indexed B by B's full_shard_id_list; pick one of B's shards
	// as the routing target and call SendXshardTx.
	target := FullShardID{
		ChainID: 0x0003,
		ShardID: 0x0002,
	}
	if err := rpcA.slave.xshardPool.SendXshardTx(target, 0x00030002, []byte("tx-list-from-A")); err != nil {
		t.Fatal("SendXshardTx A→B:", err)
	}
	select {
	case payload := <-xshardReceivedB:
		if string(payload) != "tx-list-from-A" {
			t.Errorf("B received wrong payload: %s", payload)
		}
		t.Log("A→B xshard tx delivered OK")
	case <-time.After(3 * time.Second):
		t.Fatal("B did not receive ADD_XSHARD_TX_LIST from A")
	}
}

// waitForMasterConn blocks until the slave's masterConn is set or times out.
func waitForMasterConn(t *testing.T, s *Slave) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		mc := s.masterConn
		s.mu.RUnlock()
		if mc != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("slave did not accept master connection in time")
}

// TestE2E_PythonMasterPeerRouting simulates the exact wire behavior of the
// Python master for Mode 3 (peer → master → slave) over a real TCP socket.
//
// Python master's broadcast_rpc sends CreateClusterPeerConnectionRequest with
// metadata ClusterMetadata(ROOT_BRANCH=0, cluster_peer_id=0); the real
// cluster_peer_id lives in the PAYLOAD.  Subsequent peer-shard P2P frames
// (e.g. OP_NEW_TRANSACTION_LIST) carry metadata with the real cluster_peer_id
// in the frame header and the branch in the same 12-byte header.
//
// This test verifies the slave correctly:
//  1. reads cluster_peer_id from the payload (not from metadata) on create
//  2. creates a PeerConn for that cluster_peer_id on every owned branch
//  3. routes subsequent peer frames through the Dispatcher to the PeerConn
//  4. invokes the registered per-branch peer handler
//
// If any of the wire-format assumptions drift from the Python master, this
// test fails — which the previous tests using masterConn.Handle(frame)
// directly could not catch.
func TestE2E_PythonMasterPeerRouting(t *testing.T) {
	rpc, err := NewSlaveRPC(&Config{
		ID:          "slave-X",
		ListenAddr:  "127.0.0.1:0",
		OwnBranches: []uint32{0x00030000, 0x00030001}, // chain 3, shard 0 and 1
	})
	if err != nil {
		t.Fatal("NewSlaveRPC:", err)
	}
	defer rpc.Close()
	rpc.RegisterHandlers()

	// Register a real peer handler on shard 0x00030000 BEFORE the master
	// sends CreateClusterPeerConnection — applyPeerHandlers copies from
	// s.peerHandlers when the PeerConn is created.
	peerTxReceived := make(chan []byte, 1)
	if err := rpc.RegisterPeerHandler(0x00030000, OP_NEW_TRANSACTION_LIST, func(frame *Frame) ([]byte, error) {
		peerTxReceived <- frame.Payload
		return nil, nil
	}); err != nil {
		t.Fatal("RegisterPeerHandler:", err)
	}

	rpc.Start()
	master := newFramedServer(t, rpc.slave.listener.Addr().String())
	defer master.close()
	waitForMasterConn(t, rpc.slave)

	// --- 1. Master PING/PONG to bring the slave online ---
	pingPayload, _ := serialize.SerializeToBytes(&PingRequest{
		ID:              []byte("slave-X"),
		FullShardIDList: rpc.slave.FullShardIDList(),
	})
	master.sendFrame(t, &Frame{Opcode: OP_PING, RPCID: 1, Payload: pingPayload})
	if r := master.readFrame(t); r.Opcode != OP_PONG {
		t.Fatal("PONG missing")
	}

	// --- 2. Master broadcasts CreateClusterPeerConnection ---
	// Wire: metadata = (Branch=0, ClusterPeerID=0), payload carries real id.
	createReq := &CreateClusterPeerConnectionRequest{ClusterPeerID: 12345}
	createPayload, _ := serialize.SerializeToBytes(createReq)
	master.sendFrame(t, &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
		RPCID:   2,
		Payload: createPayload,
	})
	createResp := master.readFrame(t)
	if createResp.Opcode != OP_CREATE_CLUSTER_PEER_CONNECTION_RESPONSE {
		t.Fatalf("expected CREATE_CLUSTER_PEER_CONNECTION_RESPONSE, got 0x%x", createResp.Opcode)
	}
	var cResp CreateClusterPeerConnectionResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(createResp.Payload), &cResp); err != nil {
		t.Fatal("deserialize create resp:", err)
	}
	if cResp.ErrorCode != 0 {
		t.Fatalf("create failed, error_code=%d", cResp.ErrorCode)
	}

	// Verify the PeerConn was actually created and indexed in the dispatcher.
	if rpc.slave.dispatcher.PeerConnCount() == 0 {
		t.Fatal("no PeerConn registered in dispatcher after create")
	}

	// --- 3. Master forwards a peer P2P command (OP_NEW_TRANSACTION_LIST) ---
	// Wire: metadata = (Branch=0x00030000, ClusterPeerID=12345).
	// The slave's readLoop → Dispatcher.Dispatch → cluster_peer_id != 0 →
	// peerConns[12345][0x00030000] → PeerConn.HandleFrame → handler.
	master.sendFrame(t, &Frame{
		Meta:    Metadata{Branch: 0x00030000, ClusterPeerID: 12345},
		Opcode:  OP_NEW_TRANSACTION_LIST,
		RPCID:   0, // NON-RPC, no response expected
		Payload: []byte("peer-tx-list"),
	})

	select {
	case payload := <-peerTxReceived:
		if string(payload) != "peer-tx-list" {
			t.Errorf("peer handler got wrong payload: %s", payload)
		}
		t.Log("Mode 3 peer routing OK — handler invoked via real TCP")
	case <-time.After(3 * time.Second):
		t.Fatal("peer handler not invoked — Dispatcher routing broken over TCP")
	}

	// --- 4. Master sends DestroyClusterPeerConnection ---
	destroyReq := &DestroyClusterPeerConnectionCommand{ClusterPeerID: 12345}
	destroyPayload, _ := serialize.SerializeToBytes(destroyReq)
	master.sendFrame(t, &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND,
		RPCID:   0, // NON-RPC
		Payload: destroyPayload,
	})
	// Give the slave a moment to process the destroy command.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rpc.slave.dispatcher.PeerConnCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rpc.slave.dispatcher.PeerConnCount() != 0 {
		t.Fatalf("PeerConn not removed after destroy, count=%d", rpc.slave.dispatcher.PeerConnCount())
	}
	t.Log("Mode 3 destroy OK — PeerConn removed")
}
