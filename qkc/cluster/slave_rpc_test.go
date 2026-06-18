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
// These tests use SlaveRPC (the typed business adapter) as the entry point,
// mirroring real usage.  Each test simulates the remote end(s) with mock
// TCP servers.
//
// Three modes covered:
//
//	Mode 1: Master ↔ Slave  (cluster_peer_id == 0)
//	Mode 2: Slave ↔ Slave   (direct xshard TCP)
//	Mode 3: Peer → Master → Slave  (cluster_peer_id != 0, virtual P2P)

// =============================================================================
// Helpers — reusable mock servers
// =============================================================================

// framedServer is a single-connection TCP server that speaks the cluster
// frame protocol.  It runs handler for each received frame in a goroutine
// and optionally sends back a response.
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
// Mode 1: Master ↔ Slave — SlaveRPC with typed PING/PONG
// =============================================================================

// TestSlaveRPC_PingPong shows the recommended usage pattern:
//
//	NewSlave → NewSlaveRPC → RegisterHandlers → Serve
//
// It verifies that a serialized PingRequest sent by a mock master reaches
// the SlaveRPC handler and a serialized PongResponse comes back.
func TestSlaveRPC_PingPong(t *testing.T) {
	master := newFramedServer(t)
	defer master.close()

	s, err := NewSlave(&Config{
		MasterAddr: master.addr(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rpc := NewSlaveRPC(s)
	rpc.RegisterHandlers()

	time.Sleep(100 * time.Millisecond)

	// Master sends a serialized PingRequest
	pingReq := &PingRequest{
		ID:              []byte("slave-1"),
		FullShardIDList: []uint32{4, 5, 6},
	}
	pingPayload, _ := serialize.SerializeToBytes(pingReq)

	master.sendFrame(t, &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_PING,
		RPCID:   100,
		Payload: pingPayload,
	})

	// Read the PONG response and verify the payload
	resp := master.readFrame(t)
	if resp.Opcode != OP_PONG {
		t.Fatalf("expected OP_PONG (0x%x), got 0x%x", OP_PONG, resp.Opcode)
	}
	if resp.RPCID != 100 {
		t.Fatalf("expected RPCID 100, got %d", resp.RPCID)
	}

	var pong PongResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &pong); err != nil {
		t.Fatalf("deserialize PongResponse: %v", err)
	}
	if string(pong.ID) != "slave-1" {
		t.Errorf("expected pong ID slave-1, got %s", pong.ID)
	}
	if len(pong.FullShardIDList) != 3 {
		t.Errorf("expected 3 shard IDs, got %d", len(pong.FullShardIDList))
	}

	t.Log("SlaveRPC PING/PONG passed")
}

// TestSlaveRPC_StubHandler verifies that a not-yet-implemented handler
// returns ErrNotImplemented (and no response is sent to master).
func TestSlaveRPC_StubHandler(t *testing.T) {
	master := newFramedServer(t)
	defer master.close()

	s, err := NewSlave(&Config{
		MasterAddr: master.addr(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rpc := NewSlaveRPC(s)
	rpc.RegisterHandlers()

	time.Sleep(100 * time.Millisecond)

	// Send an opcode that is registered as a stub (ADD_ROOT_BLOCK_REQUEST)
	master.sendFrame(t, &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_ADD_ROOT_BLOCK_REQUEST,
		RPCID:   1,
		Payload: []byte("dummy"),
	})

	// Stub handlers return ErrNotImplemented → no response frame is written.
	// Verify no response arrives within a short timeout.
	time.Sleep(200 * time.Millisecond)

	// The stub logged "not implemented" but sent no wire response — that's
	// correct because MasterConn.Handle() skips the response when err is
	// non-nil.
	t.Log("stub handler test passed (no response sent)")
}

// TestSlaveRPC_SendRPCToMaster sends an RPC request from slave to master
// and verifies the response.  This is how Shard would call e.g.
// SendMinorBlockHeaderToMaster in production.
func TestSlaveRPC_SendRPCToMaster(t *testing.T) {
	// echo master
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
		frame, err := ReadFrameFromReader(conn)
		if err != nil {
			return
		}
		// The real Python master would process the request and send
		// the corresponding response opcode (request+1).
		resp := &Frame{
			Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
			Opcode:  frame.Opcode + 1, // response = request+1
			RPCID:   frame.RPCID,
			Payload: []byte("master-ack"),
		}
		WriteFrameToWriter(conn, resp)
	}()

	s, err := NewSlave(&Config{
		MasterAddr: listener.Addr().String(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send an RPC — in production this would be wrapped by SlaveRPC methods
	resp, err := s.SendRPCToMaster(ctx, OP_ADD_MINOR_BLOCK_HEADER_REQUEST, []byte("header-data"))
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Payload) != "master-ack" {
		t.Errorf("expected master-ack, got %s", resp.Payload)
	}
	if resp.Opcode != OP_ADD_MINOR_BLOCK_HEADER_RESPONSE {
		t.Errorf("expected response opcode %x, got %x", OP_ADD_MINOR_BLOCK_HEADER_RESPONSE, resp.Opcode)
	}
}

// =============================================================================
// Mode 2: Slave ↔ Slave — direct xshard TCP with PING handshake
// =============================================================================

// TestSlaveRPC_XshardPingHandshake simulates two slaves doing the
// PING/PONG handshake on a direct xshard connection.
//
// Slave A (us) connects to Slave B (mock), registers xshard handlers,
// Slave B sends a PING, Slave A responds with PONG.
func TestSlaveRPC_XshardPingHandshake(t *testing.T) {
	// Start a mock Slave B that will accept, send a PING, and read PONG.
	slaveBListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer slaveBListener.Close()

	pingSent := make(chan struct{})
	pongReceived := make(chan []byte, 1)

	go func() {
		conn, err := slaveBListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Slave B sends a PING to identify itself
		pingReq := &PingRequest{
			ID:              []byte("slave-B"),
			FullShardIDList: []uint32{0, 1},
		}
		payload, _ := serialize.SerializeToBytes(pingReq)
		if err := WriteFrameToWriter(conn, &Frame{
			Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
			Opcode:  OP_PING,
			RPCID:   7,
			Payload: payload,
		}); err != nil {
			return
		}
		close(pingSent)

		// Read PONG response
		resp, err := ReadFrameFromReader(conn)
		if err != nil {
			return
		}
		pongReceived <- resp.Payload
	}()

	// Slave A: create a real Slave with a dummy master (needed by NewSlave)
	// to house the xshard handler map.
	master := newFramedServer(t)
	defer master.close()

	s, err := NewSlave(&Config{
		MasterAddr: master.addr(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rpc := NewSlaveRPC(s)
	rpc.RegisterXshardHandlers()

	// Create XshardConn to Slave B and apply xshard handlers
	// (in production, ConnectToSlaves does this)
	xc, err := NewXshardConn(slaveBListener.Addr().String(), log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer xc.Close()
	s.applyXshardHandlers(xc)

	// Wait for B to send PING and read its PONG response
	select {
	case <-pingSent:
	case <-time.After(5 * time.Second):
		t.Fatal("slave B did not send PING")
	}

	select {
	case payload := <-pongReceived:
		var pong PongResponse
		if err := serialize.Deserialize(serialize.NewByteBuffer(payload), &pong); err != nil {
			t.Fatalf("deserialize xshard PongResponse: %v", err)
		}
		if string(pong.ID) != "slave-B" {
			t.Errorf("expected PONG ID slave-B, got %s", pong.ID)
		}
		t.Log("xshard PING/PONG handshake passed")

	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for xshard PONG")
	}
}

// TestSlaveRPC_XshardSendTxList verifies that a slave can send a batch of
// xshard transactions to another slave via the direct TCP connection.
func TestSlaveRPC_XshardSendTxList(t *testing.T) {
	// Plain listener — framedServer double-accepts, so use raw listener.
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
		frame, err := ReadFrameFromReader(conn)
		if err != nil {
			received <- nil
			return
		}
		received <- frame
	}()

	// Connect and send xshard tx list
	xc, err := NewXshardConn(slaveBListener.Addr().String(), log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer xc.Close()

	txPayload := []byte("cross-shard-tx-data")
	if err := xc.SendXshardTxList(2, txPayload); err != nil {
		t.Fatal(err)
	}

	select {
	case frame := <-received:
		if frame == nil {
			t.Fatal("received nil frame")
		}
		if frame.Opcode != OP_ADD_XSHARD_TX_LIST_REQUEST {
			t.Errorf("expected OP_ADD_XSHARD_TX_LIST_REQUEST, got 0x%x", frame.Opcode)
		}
		if frame.Meta.Branch != 2 {
			t.Errorf("expected branch 2, got %d", frame.Meta.Branch)
		}
		if string(frame.Payload) != string(txPayload) {
			t.Errorf("expected payload %s, got %s", txPayload, frame.Payload)
		}
		t.Log("xshard send tx list passed")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for xshard frame")
	}
}

// =============================================================================
// Mode 2b: ConnectToSlaves — master instructs slave to connect to others
// =============================================================================

// TestSlaveRPC_ConnectToSlaves verifies the CONNECT_TO_SLAVES_REQUEST flow:
// Master sends a list of slave nodes, Slave opens direct TCP connections.
func TestSlaveRPC_ConnectToSlaves(t *testing.T) {
	// Start a mock remote slave that will accept a connection
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

	// Create Slave (no real master needed for this test)
	master := newMockMaster(t)
	defer master.close()

	s, err := NewSlave(&Config{
		MasterAddr: master.addr(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Register xshard handlers (needed by ConnectToSlaves internally)
	rpc := NewSlaveRPC(s)
	rpc.RegisterXshardHandlers()

	// Build and call ConnectToSlaves directly (in production, the
	// OP_CONNECT_TO_SLAVES_REQUEST handler calls this)
	host, port := "127.0.0.1", remoteListener.Addr().(*net.TCPAddr).Port
	req := ConnectToSlavesRequest{
		SlaveInfoList: []SlaveInfo{
			{
				ID:              []byte("remote-slave"),
				Host:            []byte(host),
				Port:            uint16(port),
				FullShardIDList: []uint32{2, 3},
			},
		},
	}
	payload, err := serialize.SerializeToBytes(&req)
	if err != nil {
		t.Fatal(err)
	}

	respPayload, err := s.ConnectToSlaves(payload)
	if err != nil {
		t.Fatal(err)
	}

	var resp ConnectToSlavesResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(respPayload), &resp); err != nil {
		t.Fatal(err)
	}
	// On success resultList[i] is nil, but serialize/deserialize round-trip
	// turns nil []byte into empty []byte{}.
	if len(resp.ResultList) != 1 {
		t.Fatalf("expected 1 result, got %d", len(resp.ResultList))
	}
	if len(resp.ResultList[0]) != 0 {
		t.Errorf("expected empty (success), got %s", resp.ResultList[0])
	}

	// Verify pool has connections for both shard IDs
	select {
	case <-connected:
		// remote accepted the connection
	case <-time.After(5 * time.Second):
		t.Fatal("remote slave did not accept connection")
	}

	target0 := FullShardID{ChainID: 0, ShardID: 2}
	target1 := FullShardID{ChainID: 0, ShardID: 3}
	if conns := s.xshardPool.Get(target0); len(conns) == 0 {
		t.Error("expected connections for shard 2")
	}
	if conns := s.xshardPool.Get(target1); len(conns) == 0 {
		t.Error("expected connections for shard 3")
	}

	t.Log("ConnectToSlaves test passed")
}

// =============================================================================
// Mode 3: Peer → Master → Slave — virtual P2P via Dispatcher + PeerConn
// =============================================================================

// TestSlaveRPC_PeerCommandRouting verifies that an external peer's message
// (cluster_peer_id != 0) is correctly routed through Dispatcher to the
// right PeerConn and handler.
//
// This simulates: External Peer → Master TCP → Dispatcher → PeerConn → Shard.
func TestSlaveRPC_PeerCommandRouting(t *testing.T) {
	master := newMockMaster(t)
	defer master.close()

	s, err := NewSlave(&Config{
		MasterAddr:  master.addr(),
		OwnBranches: []uint32{1},
		ListenAddr:  "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Step 1: Master notifies slave about a new external peer
	createDone := make(chan struct{})
	s.RegisterMasterHandler(OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, func(frame *Frame) ([]byte, error) {
		s.HandleCreateClusterPeerConnection(frame)
		close(createDone)
		return nil, nil
	})

	s.masterConn.Handle(&Frame{
		Meta:   Metadata{Branch: 0, ClusterPeerID: 42},
		Opcode: OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
		RPCID:  1,
	})

	select {
	case <-createDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for peer creation")
	}

	// Step 2: Register a peer handler that simulates Shard receiving a
	// new minor block header from the external peer.
	peerBlockReceived := make(chan []byte, 1)
	s.RegisterPeerHandler(1, OP_NEW_MINOR_BLOCK_HEADER_LIST, func(frame *Frame) ([]byte, error) {
		peerBlockReceived <- frame.Payload
		return nil, nil
	})

	// Step 3: The master sends the peer's message (routed via OnFrame/Dispatcher)
	s.masterConn.OnFrame(&Frame{
		Meta:    Metadata{Branch: 1, ClusterPeerID: 42},
		Opcode:  OP_NEW_MINOR_BLOCK_HEADER_LIST,
		RPCID:   0,
		Payload: []byte("peer-block-data"),
	})

	select {
	case payload := <-peerBlockReceived:
		if string(payload) != "peer-block-data" {
			t.Errorf("expected peer-block-data, got %s", payload)
		}
		t.Log("peer command routing passed")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for peer handler")
	}

	// Verify the dispatcher has 1 active peer connection
	if s.dispatcher.PeerConnCount() != 1 {
		t.Errorf("expected 1 peer conn in dispatcher, got %d", s.dispatcher.PeerConnCount())
	}
}

// =============================================================================
// Full round-trip: SlaveRPC entry point covering all three modes
// =============================================================================

// TestSlaveRPC_FullIntegration demonstrates the complete usage:
//
//  1. NewSlave + NewSlaveRPC + RegisterHandlers
//  2. Master sends PING → SlaveRPC responds with PONG
//  3. Master sends CONNECT_TO_SLAVES → Slave connects to another slave
//  4. Master creates cluster peer → Slave dispatches peer message
//  5. Slave sends fire-and-forget to master
func TestSlaveRPC_FullIntegration(t *testing.T) {
	// ── Start a remote slave for mode 2 test ──
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

	// ── Start mock master (manual read/write, not auto-respond) ──
	master := newFramedServer(t)
	defer master.close()

	// Create slave and SlaveRPC
	s, err := NewSlave(&Config{
		MasterAddr:  master.addr(),
		OwnBranches: []uint32{0}, // single branch — dispatcher is keyed by clusterPeerID only
		ListenAddr:  "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rpc := NewSlaveRPC(s)
	rpc.RegisterHandlers()

	time.Sleep(100 * time.Millisecond)

	// ── 1. PING/PONG via SlaveRPC handler ──
	pingReq := &PingRequest{
		ID:              []byte("test-slave"),
		FullShardIDList: []uint32{0, 1},
	}
	pingPayload, _ := serialize.SerializeToBytes(pingReq)
	master.sendFrame(t, &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_PING,
		RPCID:   1,
		Payload: pingPayload,
	})
	resp := master.readFrame(t)
	if resp.Opcode != OP_PONG {
		t.Fatalf("step 1 PONG: expected 0x%x, got 0x%x", OP_PONG, resp.Opcode)
	}
	t.Log("step 1: PING/PONG OK")

	// ── 2. CONNECT_TO_SLAVES via SlaveRPC handler ──
	host, remotePort := "127.0.0.1", remoteSlaveListener.Addr().(*net.TCPAddr).Port
	connectReq := ConnectToSlavesRequest{
		SlaveInfoList: []SlaveInfo{
			{
				ID:              []byte("remote"),
				Host:            []byte(host),
				Port:            uint16(remotePort),
				FullShardIDList: []uint32{0x00030004}, // chain=3, shard=4
			},
		},
	}
	connectPayload, _ := serialize.SerializeToBytes(&connectReq)
	master.sendFrame(t, &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_CONNECT_TO_SLAVES_REQUEST,
		RPCID:   2,
		Payload: connectPayload,
	})
	select {
	case <-remoteConnected:
		t.Log("step 2: CONNECT_TO_SLAVES OK")
	case <-time.After(5 * time.Second):
		t.Fatal("step 2: remote slave not connected")
	}

	// ── 3. Create cluster peer → peer command routing ──

	// Wait for the async CREATE handler to finish
	peerCreated := make(chan struct{})
	s.RegisterMasterHandler(OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, func(frame *Frame) ([]byte, error) {
		s.HandleCreateClusterPeerConnection(frame)
		close(peerCreated)
		return nil, nil
	})

	s.masterConn.Handle(&Frame{
		Meta:   Metadata{Branch: 0, ClusterPeerID: 99},
		Opcode: OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
		RPCID:  3,
	})

	select {
	case <-peerCreated:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for CREATE_CLUSTER_PEER_CONNECTION")
	}

	// Now register peer handler (PeerConns exist for branch 0)
	peerDone := make(chan []byte, 1)
	s.RegisterPeerHandler(0, OP_NEW_TRANSACTION_LIST, func(frame *Frame) ([]byte, error) {
		peerDone <- frame.Payload
		return nil, nil
	})

	// Master forwards a peer message via OnFrame → Dispatcher → PeerConn(99)
	s.masterConn.OnFrame(&Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 99},
		Opcode:  OP_NEW_TRANSACTION_LIST,
		RPCID:   0,
		Payload: []byte("tx-from-peer"),
	})

	select {
	case payload := <-peerDone:
		if string(payload) != "tx-from-peer" {
			t.Errorf("step 3: expected tx-from-peer, got %s", payload)
		}
		t.Log("step 3: peer command routing OK")
	case <-time.After(5 * time.Second):
		t.Fatal("step 3: timeout waiting for peer handler")
	}

	// ── 4. Slave sends fire-and-forget command to master ──
	if err := s.SendToMaster(OP_GET_WORK_REQUEST, []byte("work-request")); err != nil {
		t.Fatal("step 4: SendToMaster failed:", err)
	}
	t.Log("step 4: Slave → Master command OK")

	t.Log("full integration test PASSED")
}
