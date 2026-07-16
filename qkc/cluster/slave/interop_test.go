// Copyright 2026-2027, QuarkChain.

//go:build interop

package slave

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

// =============================================================================
// §1 Master connection and PING/PONG
// =============================================================================

func TestSlaveServer_PythonMasterPing(t *testing.T) {
	masterShards := []uint32{0x00010001}
	slaveShards := []uint32{0x00010001, 0x00020001}
	srv, port := startTestSlave(t, "S0", slaveShards)
	_ = srv

	out := runPythonHelper(t,
		"ping",
		"127.0.0.1", strconv.Itoa(port),
		shardStr(masterShards),
	)

	want := fmt.Sprintf("PONG id=%x shards=%s", []byte("S0"), shardStr(slaveShards))
	if !bytes.Contains(out, []byte(want)) {
		t.Fatalf("expected output to contain %q, got:\n%s", want, string(out))
	}
}

// =============================================================================
// §2 PeerConn lifecycle and routing
// =============================================================================

func TestSlaveServer_PythonMasterPeerLifecycle(t *testing.T) {
	masterShards := []uint32{0x00010001}
	slaveShards := []uint32{0x00010001, 0x00020001}
	srv, port := startTestSlave(t, "S0", slaveShards)

	clusterPeerID := uint64(42)
	out := runPythonHelper(t,
		"peer",
		"127.0.0.1", strconv.Itoa(port),
		shardStr(masterShards),
		strconv.FormatUint(clusterPeerID, 10),
	)

	if !bytes.Contains(out, []byte("CREATE_PEER error_code=0")) {
		t.Fatalf("peer creation failed, output:\n%s", string(out))
	}
	if !bytes.Contains(out, []byte("PEER_ROUTE sent")) {
		t.Fatalf("peer route command not sent, output:\n%s", string(out))
	}
	if !bytes.Contains(out, []byte("DESTROY_PEER sent")) {
		t.Fatalf("peer destroy command not sent, output:\n%s", string(out))
	}

	// Verify that the Go runtime created and then removed the PeerConn.
	srv.dispatcher.mu.RLock()
	_, exists := srv.dispatcher.peers[clusterPeerID]
	srv.dispatcher.mu.RUnlock()
	if exists {
		t.Fatalf("peer connection %d still registered after destroy", clusterPeerID)
	}
}

// =============================================================================
// §3 Slave-to-slave (xshard) connectivity
// =============================================================================

func TestSlaveServer_PythonMasterXshard(t *testing.T) {
	masterShards := []uint32{0x00010001, 0x00020001}
	slave1Shards := []uint32{0x00010001}
	slave2Shards := []uint32{0x00010001}

	srv1, port1 := startTestSlave(t, "S1", slave1Shards)
	srv2, port2 := startTestSlave(t, "S2", slave2Shards)

	cmd, out, cancel := startPythonHelper(t,
		"xshard",
		"127.0.0.1", strconv.Itoa(port1),
		"127.0.0.1", strconv.Itoa(port2),
		shardStr(masterShards),
		"S1", shardStr(slave1Shards),
		"S2", shardStr(slave2Shards),
	)
	defer cmd.Wait()
	defer cancel()

	// Wait until the Python helper reports that both slaves have successfully
	// connected to each other. We must inspect the pool while the master
	// connections are still open: once the helper closes them, the Go slaves
	// will stop and tear down the xshard pool.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(out.String(), "XSHARD_OK") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !strings.Contains(out.String(), "XSHARD_OK") {
		t.Fatalf("xshard handshake did not complete, output:\n%s", out.String())
	}

	// The helper keeps master connections open for args.linger seconds after
	// printing XSHARD_OK. Poll the pool briefly so the indexing has time to
	// settle without racing against teardown.
	checkDeadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(checkDeadline) {
		if srv1.xshardPool.OutboundSize() > 0 && srv2.xshardPool.OutboundSize() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Logf("python helper output:\n%s", out.String())
	t.Logf("slave1 targets=%v outbound=%d inbound=%d", srv1.xshardPool.Targets(), srv1.xshardPool.OutboundSize(), srv1.xshardPool.InboundSize())
	t.Logf("slave2 targets=%v outbound=%d inbound=%d", srv2.xshardPool.Targets(), srv2.xshardPool.OutboundSize(), srv2.xshardPool.InboundSize())

	if srv1.xshardPool.OutboundSize() == 0 {
		t.Fatalf("slave 1 has no outbound xshard connections")
	}
	if srv2.xshardPool.OutboundSize() == 0 {
		t.Fatalf("slave 2 has no outbound xshard connections")
	}
}
