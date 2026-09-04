// Copyright 2026-2027, QuarkChain.

//go:build interop

package slave

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// TestRealMasterPeerLifecycle — real Python Master peer lifecycle
// =============================================================================
//
// Verifies that the real Python Master correctly triggers
// CREATE_CLUSTER_PEER_CONNECTION_REQUEST and
// DESTROY_CLUSTER_PEER_CONNECTION_COMMAND when an external peer connects
// and disconnects via SimpleNetwork.
//
// The test uses net.Dial to connect to the master's SimpleNetwork port,
// sends a minimal HELLO frame, and verifies that the Go slave's
// dispatcher.peers map is correctly updated.

// TestRealMasterPeerLifecycle verifies the full peer lifecycle through the
// real Python Master: connect → dispatcher.peers created → disconnect →
// dispatcher.peers removed.
func TestRealMasterPeerLifecycle(t *testing.T) {
	// ── 1. Start cluster with 1 slave ──────────────────────────────────
	cluster := startInteropCluster(t, [][]uint32{{0x00000001}})
	defer cluster.Stop()

	if !cluster.WaitBootstrap(30 * time.Second) {
		t.Fatalf("bootstrap timed out\nmaster output:\n%s", cluster.MasterOutput())
	}

	// The master uses SimpleNetwork (via bootstrap_master_wrapper.py
	// monkey-patch). SimpleNetwork assigns cluster_peer_id=1 to the
	// first external peer (0 is reserved for the master itself).
	clusterPeerID := uint64(1)

	// ── 2. Compute genesis hash and header via Python helper ───────────
	genesisHash, headerBytes := runGenesisHelper(t, cluster.ConfigPath())

	// ── 3. Connect to master's SimpleNetwork port and send HELLO ───────
	addr := fmt.Sprintf("127.0.0.1:%d", cluster.P2PPort())
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial master SimpleNetwork: %v", err)
	}

	helloFrame := buildHelloFrame(genesisHash, headerBytes)
	if _, err := conn.Write(helloFrame); err != nil {
		conn.Close()
		t.Fatalf("write HELLO: %v", err)
	}

	// ── 4. Verify peer appears in dispatcher.peers ─────────────────────
	s0 := cluster.Slave(0)
	if !waitForPeer(t, s0, clusterPeerID, true, 10*time.Second) {
		conn.Close()
		t.Fatalf("peer %d not created in dispatcher.peers\nmaster output:\n%s",
			clusterPeerID, cluster.MasterOutput())
	}
	t.Logf("peer %d created in dispatcher.peers", clusterPeerID)

	// ── 5. Close connection → triggers DESTROY_CLUSTER_PEER_CONNECTION ─
	conn.Close()

	// ── 6. Verify peer disappears from dispatcher.peers ─────────────────
	if !waitForPeer(t, s0, clusterPeerID, false, 10*time.Second) {
		t.Fatalf("peer %d not removed from dispatcher.peers\nmaster output:\n%s",
			clusterPeerID, cluster.MasterOutput())
	}
	t.Logf("peer %d removed from dispatcher.peers", clusterPeerID)
}

// =============================================================================
// Helpers
// =============================================================================

// runGenesisHelper runs the testdata/genesis_helper.py script and returns
// the genesis block hash and serialized RootBlockHeader bytes.
func runGenesisHelper(t *testing.T, configPath string) (hash []byte, header []byte) {
	t.Helper()

	out := runPythonScript(t, "genesis_helper.py", configPath)
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 2 {
		t.Fatalf("genesis_helper.py output too short:\n%s", string(out))
	}

	hash, err := hex.DecodeString(strings.TrimSpace(lines[0]))
	if err != nil {
		t.Fatalf("decode genesis hash: %v\noutput:\n%s", err, string(out))
	}
	if len(hash) != 32 {
		t.Fatalf("genesis hash wrong length: got %d, want 32", len(hash))
	}

	header, err = hex.DecodeString(strings.TrimSpace(lines[1]))
	if err != nil {
		t.Fatalf("decode genesis header: %v\noutput:\n%s", err, string(out))
	}

	return hash, header
}

// buildHelloFrame constructs a minimal HELLO frame for SimpleNetwork.
//
// Wire format:
//
//	[4B: cmd_data_size] [4B: metadata(Branch=0)] [1B: op=0] [8B: rpc_id=0] [cmd_data_size B: HelloCommand]
//
// HelloCommand fields:
//
//	version(uint32) | network_id(uint32) | peer_id(32B) | peer_ip(uint128) |
//	peer_port(uint16) | chain_mask_list(4B prefix + 0) | root_block_header |
//	genesis_root_block_hash(32B)
func buildHelloFrame(genesisHash, headerBytes []byte) []byte {
	var cmdBuf []byte

	// version (uint32) = 0
	cmdBuf = append(cmdBuf, 0, 0, 0, 0)
	// network_id (uint32) = 255
	cmdBuf = append(cmdBuf, 0, 0, 0, 255)
	// peer_id (32 bytes, random)
	peerID := make([]byte, 32)
	if _, err := rand.Read(peerID); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	cmdBuf = append(cmdBuf, peerID...)
	// peer_ip (uint128, 127.0.0.1 = 0x7F000001)
	cmdBuf = append(cmdBuf,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x7F, 0, 0, 1,
	)
	// peer_port (uint16) = 0
	cmdBuf = append(cmdBuf, 0, 0)
	// chain_mask_list (4B prefix = 0, empty list)
	cmdBuf = append(cmdBuf, 0, 0, 0, 0)
	// root_block_header (from Python helper)
	cmdBuf = append(cmdBuf, headerBytes...)
	// genesis_root_block_hash (32 bytes, from Python helper)
	cmdBuf = append(cmdBuf, genesisHash...)

	cmdSize := len(cmdBuf)
	frame := make([]byte, 0, 4+4+1+8+cmdSize)

	// 4 bytes: command data size (big-endian)
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(cmdSize))
	frame = append(frame, sizeBuf...)

	// 4 bytes: P2PMetadata (Branch = 0 = ROOT_SHARD_ID)
	frame = append(frame, 0, 0, 0, 0)

	// 1 byte: op = HELLO (0)
	frame = append(frame, 0)

	// 8 bytes: rpc_id = 0 (non-RPC)
	frame = append(frame, 0, 0, 0, 0, 0, 0, 0, 0)

	// cmdSize bytes: serialized HelloCommand
	frame = append(frame, cmdBuf...)

	return frame
}

// waitForPeer polls dispatcher.peers until the given clusterPeerID exists
// (or does not exist, if expectExists=false). Returns false on timeout.
func waitForPeer(t *testing.T, srv *SlaveComm, clusterPeerID uint64, expectExists bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		srv.dispatcher.mu.RLock()
		_, exists := srv.dispatcher.peers[clusterPeerID]
		srv.dispatcher.mu.RUnlock()

		if exists == expectExists {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
