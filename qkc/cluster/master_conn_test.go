package cluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

// TestMasterConnBasic tests basic handshake with a mock master.
func TestMasterConnBasic(t *testing.T) {
	// Start a mock master server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// Accept in background
	serverReady := make(chan struct{})
	go func() {
		close(serverReady)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read PING frame
		frame, err := ReadFrameFromReader(conn)
		if err != nil {
			t.Logf("server read error: %v", err)
			return
		}
		if frame.Opcode != OP_PING {
			t.Errorf("expected PING opcode, got %x", frame.Opcode)
		}

		// Send PONG response
		resp := &Frame{
			Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
			Opcode:  OP_PING,
			RPCID:   frame.RPCID,
			Payload: []byte("PONG"),
		}
		if err := WriteFrameToWriter(conn, resp); err != nil {
			t.Logf("server write error: %v", err)
		}
	}()
	<-serverReady

	// Create MasterConn
	addr := listener.Addr().String()
	mc, err := NewMasterConn(addr, log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer mc.Close()

	// Send PING via RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := mc.SendRPC(ctx, OP_PING, []byte("PING"))
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Payload) != "PONG" {
		t.Errorf("expected PONG, got %s", resp.Payload)
	}
}

// TestMasterConnRegisterHandler tests handler registration and dispatch.
func TestMasterConnRegisterHandler(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverReady := make(chan struct{})
	received := make(chan *Frame, 1)

	go func() {
		close(serverReady)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Send a custom command to the slave
		cmd := &Frame{
			Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
			Opcode:  OP_ADD_ROOT_BLOCK_REQUEST,
			RPCID:   42,
			Payload: []byte("test block"),
		}
		WriteFrameToWriter(conn, cmd)

		// Read response
		resp, err := ReadFrameFromReader(conn)
		if err != nil {
			t.Logf("server read error: %v", err)
			return
		}
		received <- resp
	}()
	<-serverReady

	addr := listener.Addr().String()
	mc, err := NewMasterConn(addr, log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer mc.Close()

	handlerCalled := make(chan struct{})
	mc.RegisterHandler(OP_ADD_ROOT_BLOCK_REQUEST, func(frame *Frame) ([]byte, error) {
		close(handlerCalled)
		if string(frame.Payload) != "test block" {
			t.Errorf("unexpected payload: %s", frame.Payload)
		}
		return []byte("block received"), nil
	})

	select {
	case <-handlerCalled:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}

	select {
	case resp := <-received:
		if string(resp.Payload) != "block received" {
			t.Errorf("unexpected response: %s", resp.Payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no response received")
	}
}

// TestMasterConnSendCommand tests fire-and-forget command sending.
func TestMasterConnSendCommand(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverReady := make(chan struct{})
	received := make(chan *Frame, 1)

	go func() {
		close(serverReady)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		frame, err := ReadFrameFromReader(conn)
		if err != nil {
			return
		}
		received <- frame
	}()
	<-serverReady

	addr := listener.Addr().String()
	mc, err := NewMasterConn(addr, log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer mc.Close()

	err = mc.SendCommand(OP_GET_WORK_REQUEST, []byte("beat"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case frame := <-received:
		if frame.Opcode != OP_GET_WORK_REQUEST {
			t.Errorf("expected HEART_BEAT, got %x", frame.Opcode)
		}
		if frame.RPCID != 0 {
			t.Errorf("expected RPCID 0 for command, got %d", frame.RPCID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no frame received")
	}
}

// TestDispatcherRouting tests that frames are routed to the correct handlers.
func TestDispatcherRouting(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverReady := make(chan struct{})

	go func() {
		close(serverReady)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Send a master command (cluster_peer_id == 0)
		cmd := &Frame{
			Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
			Opcode:  OP_PING,
			RPCID:   1,
			Payload: []byte("master ping"),
		}
		WriteFrameToWriter(conn, cmd)

		// Send a peer command (cluster_peer_id != 0)
		peerCmd := &Frame{
			Meta:    Metadata{Branch: 1, ClusterPeerID: 12345},
			Opcode:  OP_NEW_MINOR_BLOCK_HEADER_LIST,
			RPCID:   2,
			Payload: []byte("peer block"),
		}
		WriteFrameToWriter(conn, peerCmd)

		// Read responses
		ReadFrameFromReader(conn) // master response
		ReadFrameFromReader(conn) // peer response
	}()
	<-serverReady

	addr := listener.Addr().String()
	mc, err := NewMasterConn(addr, log.New())
	if err != nil {
		t.Fatal(err)
	}
	defer mc.Close()

	dispatcher := NewDispatcher(mc, log.New())
	mc.OnFrame = dispatcher.Dispatch

	// Register handler for master command
	masterDone := make(chan struct{})
	mc.RegisterHandler(OP_PING, func(frame *Frame) ([]byte, error) {
		close(masterDone)
		if string(frame.Payload) != "master ping" {
			t.Errorf("unexpected master payload: %s", frame.Payload)
		}
		return []byte("master pong"), nil
	})

	// Create peer connection and register handler
	peer := NewPeerConn(12345, 1, mc, log.New())
	dispatcher.AddPeerConn(peer)

	peerDone := make(chan struct{})
	peer.RegisterHandler(OP_NEW_MINOR_BLOCK_HEADER_LIST, func(frame *Frame) ([]byte, error) {
		close(peerDone)
		if string(frame.Payload) != "peer block" {
			t.Errorf("unexpected peer payload: %s", frame.Payload)
		}
		return []byte("peer ack"), nil
	})

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

	if dispatcher.PeerConnCount() != 1 {
		t.Errorf("expected 1 peer conn, got %d", dispatcher.PeerConnCount())
	}
}
