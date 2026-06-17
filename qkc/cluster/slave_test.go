package cluster

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

// TestSlaveFullLifecycle tests the complete Slave lifecycle:
// create → connect to mock master → verify state → close.
func TestSlaveFullLifecycle(t *testing.T) {
	master := newMockMaster(t)
	defer master.close()

	slave, err := NewSlave(&Config{
		MasterAddr: master.addr(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	// Verify initial state
	if len(slave.ClusterPeerIDs()) != 0 {
		t.Errorf("expected 0 cluster peer IDs, got %d", len(slave.ClusterPeerIDs()))
	}
}

// TestSlavePingPong tests the PING/PONG handshake with a mock master.
func TestSlavePingPong(t *testing.T) {
	master := newMockMaster(t)
	defer master.close()

	slave, err := NewSlave(&Config{
		MasterAddr: master.addr(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	// Send PING via RPC, mock master responds with PONG
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := slave.SendRPCToMaster(ctx, OP_PING, []byte("PING"))
	if err != nil {
		t.Fatal("SendRPCToMaster failed:", err)
	}
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if string(resp.Payload) != "PONG" {
		t.Errorf("expected PONG, got %s", resp.Payload)
	}
}

// TestSlaveMasterHandler tests that a custom handler registered via
// RegisterMasterHandler is correctly invoked when a frame with that opcode
// arrives on the master connection (cluster_peer_id=0).
func TestSlaveMasterHandler(t *testing.T) {
	master := newMockMaster(t)
	defer master.close()

	slave, err := NewSlave(&Config{
		MasterAddr: master.addr(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	handlerCalled := make(chan struct{})
	slave.RegisterMasterHandler(OP_ADD_ROOT_BLOCK_REQUEST, func(frame *Frame) ([]byte, error) {
		close(handlerCalled)
		return []byte("block received"), nil
	})

	// Simulate the master sending an ADD_ROOT_BLOCK_REQUEST to the slave
	// by injecting the frame directly into the master connection.
	frame := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_ADD_ROOT_BLOCK_REQUEST,
		RPCID:   42,
		Payload: []byte("test block"),
	}
	slave.masterConn.Handle(frame)

	select {
	case <-handlerCalled:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}
}

// TestSlaveSendToMaster tests the fire-and-forget SendToMaster method.
func TestSlaveSendToMaster(t *testing.T) {
	master := newMockMaster(t)
	defer master.close()

	slave, err := NewSlave(&Config{
		MasterAddr: master.addr(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	// Send a fire-and-forget command (no response expected)
	err = slave.SendToMaster(OP_ADD_MINOR_BLOCK_HEADER_REQUEST, []byte("header"))
	if err != nil {
		t.Fatal("SendToMaster failed:", err)
	}
}

// TestSlaveCreateDestroyPeerConnection tests the CREATE/DESTROY_CLUSTER_PEER_CONNECTION
// flow via the Slave's default handlers.
func TestSlaveCreateDestroyPeerConnection(t *testing.T) {
	master := newMockMaster(t)
	defer master.close()

	slave, err := NewSlave(&Config{
		MasterAddr:  master.addr(),
		OwnBranches: []uint32{0, 1},
		ListenAddr:  "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	// Simulate master sending CREATE_CLUSTER_PEER_CONNECTION_REQUEST.
	// Handle() runs the handler in a goroutine, so we use a channel to synchronize.
	createDone := make(chan struct{})
	slave.RegisterMasterHandler(OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, func(frame *Frame) ([]byte, error) {
		slave.handleCreateClusterPeerConnection(frame)
		close(createDone)
		return nil, nil
	})

	createFrame := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 12345},
		Opcode:  OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
		RPCID:   1,
		Payload: nil,
	}
	slave.masterConn.Handle(createFrame)

	select {
	case <-createDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for create handler")
	}

	// Verify peer connection was created
	ids := slave.ClusterPeerIDs()
	if len(ids) != 1 || ids[0] != 12345 {
		t.Errorf("expected cluster_peer_id=12345, got %v", ids)
	}

	// Destroy peer connection
	destroyDone := make(chan struct{})
	slave.RegisterMasterHandler(OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND, func(frame *Frame) ([]byte, error) {
		slave.handleDestroyClusterPeerConnection(frame)
		close(destroyDone)
		return nil, nil
	})

	destroyFrame := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 12345},
		Opcode:  OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND,
		RPCID:   0,
		Payload: nil,
	}
	slave.masterConn.Handle(destroyFrame)

	select {
	case <-destroyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for destroy handler")
	}

	ids = slave.ClusterPeerIDs()
	if len(ids) != 0 {
		t.Errorf("expected 0 cluster peer IDs after destroy, got %d", len(ids))
	}
}

// TestSlaveDispatcherRoutingEndToEnd tests the full dispatcher routing:
// master command (cluster_peer_id=0) vs peer command (cluster_peer_id!=0).
func TestSlaveDispatcherRoutingEndToEnd(t *testing.T) {
	master := newMockMaster(t)
	defer master.close()

	slave, err := NewSlave(&Config{
		MasterAddr:  master.addr(),
		OwnBranches: []uint32{0},
		ListenAddr:  "",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	// Create a peer connection first (synchronous, via callback)
	createDone := make(chan struct{})
	slave.RegisterMasterHandler(OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST, func(frame *Frame) ([]byte, error) {
		slave.handleCreateClusterPeerConnection(frame)
		close(createDone)
		return nil, nil
	})

	createFrame := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 9999},
		Opcode:  OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
		RPCID:   1,
		Payload: nil,
	}
	slave.masterConn.Handle(createFrame)

	select {
	case <-createDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for create handler")
	}

	// Now test routing: master command vs peer command
	masterDone := make(chan struct{})
	peerDone := make(chan struct{})

	slave.RegisterMasterHandler(OP_GET_WORK_REQUEST, func(frame *Frame) ([]byte, error) {
		close(masterDone)
		return nil, nil
	})

	slave.RegisterPeerHandler(0, OP_NEW_MINOR_BLOCK_HEADER_LIST, func(frame *Frame) ([]byte, error) {
		close(peerDone)
		return nil, nil
	})

	// Send master command (cluster_peer_id=0) — must go through OnFrame to be dispatched
	masterFrame := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_GET_WORK_REQUEST,
		RPCID:   2,
		Payload: []byte("get work"),
	}
	slave.masterConn.OnFrame(masterFrame)

	// Send peer command (cluster_peer_id=9999) — must go through OnFrame to be dispatched
	peerFrame := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 9999},
		Opcode:  OP_NEW_MINOR_BLOCK_HEADER_LIST,
		RPCID:   3,
		Payload: []byte("peer block header"),
	}
	slave.masterConn.OnFrame(peerFrame)

	select {
	case <-masterDone:
	case <-time.After(5 * time.Second):
		t.Fatal("master handler not called")
	}

	select {
	case <-peerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("peer handler not called")
	}
}

// TestSlaveCloseIdempotent tests that Close() can be called multiple times safely.
func TestSlaveCloseIdempotent(t *testing.T) {
	master := newMockMaster(t)
	defer master.close()

	slave, err := NewSlave(&Config{
		MasterAddr: master.addr(),
		ListenAddr: "",
	})
	if err != nil {
		t.Fatal(err)
	}

	slave.Close()
	slave.Close() // second close should be safe
	slave.Close() // third close should be safe
}

// TestSlaveXshardServer tests xshard communication via XshardConn (direct TCP).
func TestSlaveXshardServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	received := make(chan *Frame, 1)
	go func() {
		conn, _ := listener.Accept()
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

	conn, err := NewXshardConn(listener.Addr().String(), log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.SendXshardTxList(1, []byte("xshard tx data")); err != nil {
		t.Fatal("SendXshardTxList failed:", err)
	}

	select {
	case frame := <-received:
		if frame == nil {
			t.Fatal("received nil frame")
		}
		if frame.Opcode != OP_ADD_XSHARD_TX_LIST_REQUEST {
			t.Errorf("expected OP_ADD_XSHARD_TX_LIST_REQUEST, got %x", frame.Opcode)
		}
		if string(frame.Payload) != "xshard tx data" {
			t.Errorf("unexpected payload: %s", frame.Payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for frame")
	}
}

// TestSlaveXshardPool tests the XshardPool connection management.
func TestSlaveXshardPool(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			frame, _ := ReadFrameFromReader(conn)
			if frame != nil {
				resp := &Frame{
					Meta:    frame.Meta,
					Opcode:  frame.Opcode,
					RPCID:   frame.RPCID,
					Payload: []byte("response"),
				}
				WriteFrameToWriter(conn, resp)
			}
			conn.Close()
		}
	}()

	pool := NewXshardPool(log.New())
	defer pool.Close()

	target := FullShardID{ChainID: 0, ShardID: 1}

	conn, err := NewXshardConn(listener.Addr().String(), log.New())
	if err != nil {
		t.Fatal(err)
	}
	pool.Add(target, conn)

	if pool.Size() != 1 {
		t.Errorf("expected pool size 1, got %d", pool.Size())
	}

	err = pool.SendXshardTx(target, 1, []byte("test"))
	if err != nil {
		t.Fatal("SendXshardTx failed:", err)
	}

	pool.Remove(target, conn)
	if pool.Size() != 0 {
		t.Errorf("expected pool size 0 after remove, got %d", pool.Size())
	}
}

// ── mockMaster ────────────────────────────────────────────────────────────

// mockMaster is a minimal TCP server that acts like a Python master for testing.
type mockMaster struct {
	listener net.Listener
	addrStr  string
	wg       sync.WaitGroup
}

func newMockMaster(t *testing.T) *mockMaster {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	m := &mockMaster{
		listener: listener,
		addrStr:  listener.Addr().String(),
	}

	m.wg.Add(1)
	go m.serve(t)

	return m
}

func (m *mockMaster) serve(t *testing.T) {
	defer m.wg.Done()

	conn, err := m.listener.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	for {
		frame, err := ReadFrameFromReader(conn)
		if err != nil {
			return
		}

		switch frame.Opcode {
		case OP_PING:
			resp := &Frame{
				Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
				Opcode:  OP_PING,
				RPCID:   frame.RPCID,
				Payload: []byte("PONG"),
			}
			WriteFrameToWriter(conn, resp)

		case OP_ADD_ROOT_BLOCK_REQUEST:
			resp := &Frame{
				Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
				Opcode:  OP_ADD_ROOT_BLOCK_REQUEST,
				RPCID:   frame.RPCID,
				Payload: []byte("block received"),
			}
			WriteFrameToWriter(conn, resp)

		case OP_ADD_MINOR_BLOCK_HEADER_REQUEST:
			// Fire-and-forget, no response needed

		default:
			// Echo back for unknown opcodes
			resp := &Frame{
				Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
				Opcode:  frame.Opcode,
				RPCID:   frame.RPCID,
				Payload: []byte("OK"),
			}
			WriteFrameToWriter(conn, resp)
		}
	}
}

func (m *mockMaster) addr() string { return m.addrStr }

func (m *mockMaster) close() {
	m.listener.Close()
	m.wg.Wait()
}
