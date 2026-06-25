// Copyright 2026-2027, QuarkChain.
package cluster

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// =============================================================================
// Integration Test: Full PING/PONG via Cluster Protocol
// =============================================================================
// This test verifies the complete message flow:
//   1. Mock Master sends a PING frame with serialized PingRequest
//   2. Go Slave receives it, deserializes, processes, serializes PongResponse
//   3. Mock Master reads the PONG response and verifies the payload

// TestIntegrationPingPong verifies the full PING/PONG handshake between
// a mock master (speaking the cluster protocol) and a Go Slave.
func TestIntegrationPingPong(t *testing.T) {
	// Step 1: Create slave listening on an ephemeral port
	slave, err := NewSlave(&Config{
		ID:          "test-slave-1",
		ListenAddr:  "127.0.0.1:0",
		OwnBranches: []uint32{0, 1, 2},
	})
	if err != nil {
		t.Fatal("NewSlave:", err)
	}
	defer slave.Close()
	// Start accepting connections.  For this test we start before registering
	// the PING handler (step 4) — RegisterMasterHandler applies to the
	// already-connected masterConn, and the mock master only sends PING after
	// the handler is registered (step 5).
	slave.Start()

	// Step 2: Mock master dials the slave
	master := newProtocolMaster(t, slave.listener.Addr().String())
	defer master.close()

	// Step 3: Wait for slave to accept the master connection
	// (acceptLoop runs in background; masterConn is set asynchronously)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		slave.mu.RLock()
		mc := slave.masterConn
		slave.mu.RUnlock()
		if mc != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if slave.masterConn == nil {
		t.Fatal("slave did not accept master connection in time")
	}

	// Step 4: Register a PING handler on the slave that returns a proper PongResponse
	pongReceived := make(chan struct{})
	slave.RegisterMasterHandler(OP_PING, func(frame *Frame) ([]byte, error) {
		// Deserialize the incoming PingRequest
		var req PingRequest
		if err := serialize.Deserialize(serialize.NewByteBuffer(frame.Payload), &req); err != nil {
			t.Errorf("deserialize PingRequest: %v", err)
			return nil, err
		}

		// Verify the request content
		if string(req.ID) != "test-slave-1" {
			t.Errorf("expected ID test-slave-1, got %s", req.ID)
		}
		if len(req.FullShardIDList) != 3 {
			t.Errorf("expected 3 shard IDs, got %d", len(req.FullShardIDList))
		}

		// Build and serialize the PongResponse
		pong := &PongResponse{
			ID:              []byte("test-slave-1"),
			FullShardIDList: []uint32{0, 1, 2},
		}
		pongPayload, err := serialize.SerializeToBytes(pong)
		if err != nil {
			t.Errorf("serialize PongResponse: %v", err)
			return nil, err
		}

		close(pongReceived)
		return pongPayload, nil
	})

	// Step 5: Mock master sends a PING with serialized PingRequest
	pingReq := &PingRequest{
		ID:              []byte("test-slave-1"),
		FullShardIDList: []uint32{0, 1, 2},
	}
	pingPayload, err := serialize.SerializeToBytes(pingReq)
	if err != nil {
		t.Fatal("serialize PingRequest:", err)
	}

	// Step 6: Send PING frame to slave
	master.sendFrame(t, &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_PING,
		RPCID:   42, // non-zero RPC ID for RPC request
		Payload: pingPayload,
	})

	// Step 7: Wait for slave to process and respond
	select {
	case <-pongReceived:
		// Handler was called, now read the response
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for PING handler to be called")
	}

	// Step 8: Read the PONG response frame from slave
	respFrame := master.readFrame(t)
	if respFrame == nil {
		t.Fatal("expected PONG response frame, got nil")
	}
	if respFrame.Opcode != OP_PONG {
		t.Errorf("expected opcode OP_PONG (0x%x), got 0x%x", OP_PONG, respFrame.Opcode)
	}
	if respFrame.RPCID != 42 {
		t.Errorf("expected RPCID 42, got %d", respFrame.RPCID)
	}

	// Step 9: Deserialize and verify the PongResponse
	var pong PongResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(respFrame.Payload), &pong); err != nil {
		t.Fatal("deserialize PongResponse:", err)
	}
	if string(pong.ID) != "test-slave-1" {
		t.Errorf("expected PongResponse.ID test-slave-1, got %s", pong.ID)
	}
	if len(pong.FullShardIDList) != 3 {
		t.Errorf("expected 3 shard IDs in PongResponse, got %d", len(pong.FullShardIDList))
	}

	t.Log("PING/PONG integration test passed!")
}

// =============================================================================
// Test: Serialization round-trip for PingRequest/PongResponse
// =============================================================================

// TestPingPongSerializationRoundTrip verifies that PingRequest and PongResponse
// serialize/deserialize correctly (byte-compatible with pyquarkchain).
func TestPingPongSerializationRoundTrip(t *testing.T) {
	// Test PingRequest
	ping := &PingRequest{
		ID:              []byte("slave-001"),
		FullShardIDList: []uint32{1, 2, 3},
	}

	pingBytes, err := serialize.SerializeToBytes(ping)
	if err != nil {
		t.Fatal("serialize PingRequest:", err)
	}

	var ping2 PingRequest
	if err := serialize.Deserialize(serialize.NewByteBuffer(pingBytes), &ping2); err != nil {
		t.Fatal("deserialize PingRequest:", err)
	}

	if string(ping2.ID) != "slave-001" {
		t.Errorf("ID mismatch: %s", ping2.ID)
	}
	if len(ping2.FullShardIDList) != 3 {
		t.Errorf("FullShardIDList length mismatch: %d", len(ping2.FullShardIDList))
	}
	for i, v := range ping2.FullShardIDList {
		if v != ping.FullShardIDList[i] {
			t.Errorf("FullShardIDList[%d] mismatch: %d != %d", i, v, ping.FullShardIDList[i])
		}
	}

	// Test PongResponse
	pong := &PongResponse{
		ID:              []byte("slave-001"),
		FullShardIDList: []uint32{1, 2, 3},
	}

	pongBytes, err := serialize.SerializeToBytes(pong)
	if err != nil {
		t.Fatal("serialize PongResponse:", err)
	}

	var pong2 PongResponse
	if err := serialize.Deserialize(serialize.NewByteBuffer(pongBytes), &pong2); err != nil {
		t.Fatal("deserialize PongResponse:", err)
	}

	if string(pong2.ID) != "slave-001" {
		t.Errorf("ID mismatch: %s", pong2.ID)
	}
	if len(pong2.FullShardIDList) != 3 {
		t.Errorf("FullShardIDList length mismatch: %d", len(pong2.FullShardIDList))
	}
}

// =============================================================================
// Test: SlaveInfo serialization round-trip
// =============================================================================

// TestSlaveInfoSerializationRoundTrip verifies SlaveInfo serialization.
func TestSlaveInfoSerializationRoundTrip(t *testing.T) {
	info := &SlaveInfo{
		ID:              []byte("slave-1"),
		Host:            []byte("127.0.0.1"),
		Port:            38291,
		FullShardIDList: []uint32{0, 1},
	}

	bytes, err := serialize.SerializeToBytes(info)
	if err != nil {
		t.Fatal("serialize SlaveInfo:", err)
	}

	var info2 SlaveInfo
	if err := serialize.Deserialize(serialize.NewByteBuffer(bytes), &info2); err != nil {
		t.Fatal("deserialize SlaveInfo:", err)
	}

	if string(info2.ID) != "slave-1" {
		t.Errorf("ID mismatch: %s", info2.ID)
	}
	if string(info2.Host) != "127.0.0.1" {
		t.Errorf("Host mismatch: %s", info2.Host)
	}
	if info2.Port != 38291 {
		t.Errorf("Port mismatch: %d", info2.Port)
	}
}

// =============================================================================
// protocolMaster: mock TCP client that speaks the cluster protocol
// =============================================================================

// protocolMaster is a mock TCP client that dials a Go slave and sends/receives
// cluster protocol frames.  It simulates a Python Master for integration testing.
type protocolMaster struct {
	conn    net.Conn
	addrStr string
	mu      sync.Mutex
}

// newProtocolMaster dials the slave at slaveAddr and returns a connected mock master.
func newProtocolMaster(t *testing.T, slaveAddr string) *protocolMaster {
	t.Helper()
	conn, err := net.DialTimeout("tcp", slaveAddr, 5*time.Second)
	if err != nil {
		t.Fatal("dial slave:", err)
	}
	return &protocolMaster{conn: conn, addrStr: slaveAddr}
}

func (m *protocolMaster) addr() string { return m.addrStr }

func (m *protocolMaster) sendFrame(t *testing.T, frame *Frame) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn == nil {
		t.Fatal("no connection")
	}
	if err := WriteFrameToWriter(m.conn, frame); err != nil {
		t.Fatal("WriteFrameToWriter:", err)
	}
}

func (m *protocolMaster) readFrame(t *testing.T) *Frame {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conn == nil {
		t.Fatal("no connection")
	}
	frame, err := ReadFrameFromReader(m.conn)
	if err != nil {
		t.Fatal("ReadFrameFromReader:", err)
	}
	return frame
}

func (m *protocolMaster) close() {
	if m.conn != nil {
		m.conn.Close()
	}
}
