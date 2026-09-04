// Copyright 2026-2027, QuarkChain.

//go:build interop

package slave

import (
	"testing"
	"time"
)

// =============================================================================
// §4 Real Python Master Bootstrap Smoke Test
// =============================================================================
//
// This test starts a real Python Master (pyquarkchain) and connects it to
// Go Slaves. It verifies the full bootstrap handshake:
//   - Master → Slave PING/PONG
//   - CONNECT_TO_SLAVES_REQUEST → Slave-to-Slave xshard connections
//   - PING with root_tip (Go slave stub handles it gracefully)
//
// No Python slaves are started. No cluster.py is used.

func TestRealMasterBootstrap(t *testing.T) {
	cluster := startInteropCluster(t, [][]uint32{
		{0x00000001},
		{0x00010001},
	})
	defer cluster.Stop()

	if !cluster.WaitBootstrap(30 * time.Second) {
		t.Fatal("bootstrap timed out — master did not connect to all slaves")
	}

	s0 := cluster.Slave(0)
	s1 := cluster.Slave(1)

	// ── Verify master connections ────────────────────────────────────────
	mc0 := s0.MasterConn()
	mc1 := s1.MasterConn()
	if mc0 == nil {
		t.Fatal("slave 0 has no master connection — master PING was not received")
	}
	if mc1 == nil {
		t.Fatal("slave 1 has no master connection — master PING was not received")
	}
	t.Logf("master connections established: S0=%v S1=%v", mc0.RemoteAddr(), mc1.RemoteAddr())

	// ── Verify xshard connections ────────────────────────────────────────
	x0Out := s0.xshardPool.OutboundSize()
	x0In := s0.xshardPool.InboundSize()
	x1Out := s1.xshardPool.OutboundSize()
	x1In := s1.xshardPool.InboundSize()

	t.Logf("slave0 xshard: outbound=%d inbound=%d", x0Out, x0In)
	t.Logf("slave1 xshard: outbound=%d inbound=%d", x1Out, x1In)

	if x0Out+x0In == 0 {
		t.Fatal("slave 0 has no xshard connections — ConnectToSlavesRequest did not establish slave-to-slave link")
	}
	if x1Out+x1In == 0 {
		t.Fatal("slave 1 has no xshard connections — ConnectToSlavesRequest did not establish slave-to-slave link")
	}

	// ── Verify dispatcher has no unexpected peer connections ─────────────
	// The bootstrap flow does not create any cluster peer connections.
	s0.dispatcher.mu.RLock()
	peerCount0 := len(s0.dispatcher.peers)
	s0.dispatcher.mu.RUnlock()
	s1.dispatcher.mu.RLock()
	peerCount1 := len(s1.dispatcher.peers)
	s1.dispatcher.mu.RUnlock()

	if peerCount0 != 0 {
		t.Errorf("slave 0 dispatcher has %d unexpected peer connections", peerCount0)
	}
	if peerCount1 != 0 {
		t.Errorf("slave 1 dispatcher has %d unexpected peer connections", peerCount1)
	}
	t.Logf("peer connections: S0=%d S1=%d (expected 0)", peerCount0, peerCount1)

	t.Logf("master output:\n%s", cluster.MasterOutput())
}
