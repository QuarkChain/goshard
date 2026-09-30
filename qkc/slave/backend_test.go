// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/qkc"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/conn"
	"github.com/ethereum/go-ethereum/qkc/shard"
	"github.com/ethereum/go-ethereum/qkc/slaveconn"
	"github.com/ethereum/go-ethereum/qkc/types"
	"github.com/ethereum/go-ethereum/qkc/wire"
)

const (
	fixtureMainnet = "../config/singularity/mainnet.json"
	fixtureDevnet  = "../config/singularity/devnet.json"
)

// bootEnv resolves S0 from a fixture with its db path root redirected into
// t.TempDir(), plus the derived root genesis header.
func bootEnv(t *testing.T, path string) (*config.SlaveContext, *types.RootBlockHeader) {
	t.Helper()
	cfg, err := config.LoadClusterConfig(path)
	if err != nil {
		t.Fatalf("LoadClusterConfig(%s): %v", path, err)
	}
	ctx, err := cfg.ResolveSlave("S0")
	if err != nil {
		t.Fatalf("ResolveSlave: %v", err)
	}
	ctx.DBPathRoot = t.TempDir()
	root, err := qkc.CreateRootBlock(cfg.Quarkchain)
	if err != nil {
		t.Fatalf("CreateRootBlock: %v", err)
	}
	return ctx, root
}

// TestSlaveBootAndReopen boots S0 from each real network config, checks its shard
// registry, stops it, and verifies that the same databases reopen cleanly.
//
// TODO: inject the real chain service here once it exists and assert its
// canonical genesis/head plus blocking shutdown of its background work.
func TestSlaveBootAndReopen(t *testing.T) {
	for _, path := range []string{fixtureMainnet, fixtureDevnet} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			ctx, root := bootEnv(t, path)

			b, err := New(ctx, root, Options{}, shard.Options{})
			if err != nil {
				t.Fatalf("slave.New: %v", err)
			}
			if b.ID != "S0" {
				t.Errorf("ID = %q, want S0", b.ID)
			}

			shards := b.Shards()
			if len(shards) != len(ctx.FullShardIDs()) {
				t.Fatalf("booted %d shards, config assigns %d", len(shards), len(ctx.FullShardIDs()))
			}
			for i, id := range ctx.FullShardIDs() {
				branch := account.NewBranch(id)
				s := b.Shard(branch)
				if s == nil || s != shards[i] {
					t.Fatalf("shard 0x%08x: registry lookup and boot order disagree", id)
				}
				if height, _ := s.Chain().Head(); height != 0 {
					t.Errorf("shard 0x%08x head height = %d, want 0", id, height)
				}
			}
			if b.Shard(account.NewBranch(0x00990099)) != nil {
				t.Error("lookup of an unowned branch returned a shard")
			}

			if err := b.Stop(); err != nil {
				t.Fatalf("Stop: %v", err)
			}

			b, err = New(ctx, root, Options{}, shard.Options{})
			if err != nil {
				t.Fatalf("slave.New(reopen): %v", err)
			}
			if err := b.Stop(); err != nil {
				t.Fatalf("Stop(reopen): %v", err)
			}
		})
	}
}

// failingChainService fails chain construction after failAfter successful builds,
// injected through the Options.Chain seam to exercise the boot rollback.
type failingChainService struct {
	failAfter int
	built     int
}

func (s *failingChainService) New(db ethdb.Database, genesis *types.MinorBlock, chainConfig *params.ChainConfig) (shard.ShardChain, error) {
	if s.built >= s.failAfter {
		return nil, errors.New("injected chain failure")
	}
	s.built++
	return shard.StubChainService{}.New(db, genesis, chainConfig)
}

// TestSlaveBootRollback: when a later shard fails to boot, the shards already
// started are stopped and their databases closed (a still-open pebble instance
// would hold its directory lock and make the reboot below fail), and the datadir
// remains reopenable.
func TestSlaveBootRollback(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	if len(ctx.FullShardIDs()) < 2 {
		t.Fatal("fixture must assign S0 at least two shards to exercise rollback")
	}

	_, err := New(ctx, root, Options{}, shard.Options{Chain: &failingChainService{failAfter: 1}})
	if err == nil || !strings.Contains(err.Error(), "injected chain failure") ||
		!strings.Contains(err.Error(), "slave S0") {
		t.Fatalf("slave.New err = %v, want injected chain failure attributed to slave S0", err)
	}

	b, err := New(ctx, root, Options{}, shard.Options{})
	if err != nil {
		t.Fatalf("slave.New after rollback: %v", err)
	}
	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSlaveStopIdempotent(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	b, err := New(ctx, root, Options{}, shard.Options{})
	if err != nil {
		t.Fatalf("slave.New: %v", err)
	}
	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := b.Stop(); err != nil {
		t.Fatalf("Stop(again): %v", err)
	}
}

// TestSlaveNewRejectsZeroPort: a zero port is never a valid listen address, and
// SlaveContext can be hand-built past ClusterConfig.Validate, so New refuses it
// before opening any shard or socket.
func TestSlaveNewRejectsZeroPort(t *testing.T) {
	ctx, root := bootEnv(t, fixtureMainnet)
	ctx.Slave.Port = 0
	if _, err := New(ctx, root, Options{}, shard.Options{}); err == nil {
		t.Fatal("New with port 0 err = nil, want error")
	}
}

// ── comm-layer tests (the former SlaveComm surface, now on SlaveBackend) ─────

// freeTestPort reserves an ephemeral TCP port and releases it. The tiny reuse
// race is acceptable for tests, matching cmd/slave/run_test.go.
func freeTestPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// newTestBackend boots S0 with a free listener port. The backend is not
// started; Stop is registered as cleanup either way.
func newTestBackend(t *testing.T) *SlaveBackend {
	t.Helper()
	ctx, root := bootEnv(t, fixtureMainnet)
	ctx.Slave.Port = uint16(freeTestPort(t))
	b, err := New(ctx, root, Options{}, shard.Options{})
	if err != nil {
		t.Fatalf("slave.New: %v", err)
	}
	t.Cleanup(func() { b.Stop() })
	return b
}

// pipeMasterConn builds an unstarted MasterConn over a net.Pipe backed by the
// backend's own handler set (SlaveBackend implements MasterHandler and
// PeerResolver), and publishes it in b.master exactly as runMasterConn would,
// so topology handlers and Send*ToMaster see an established master without a
// TCP handshake.
func pipeMasterConn(t *testing.T, b *SlaveBackend) *slaveconn.MasterConn {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { clientEnd.Close(); serverEnd.Close() })
	mc, err := slaveconn.NewMasterConn(slaveconn.MasterConnConfig{
		Conn:                 serverEnd,
		LocalID:              []byte(b.ID),
		LocalFullShardIDList: b.localFullShardIDList,
		ClusterShardIDs:      b.clusterShardIDs,
		Handler:              b,
		PeerResolver:         b,
		Logger:               b.logger,
	})
	if err != nil {
		t.Fatalf("NewMasterConn: %v", err)
	}
	t.Cleanup(func() { mc.Close() })
	b.master.Store(mc)
	return mc
}

// TestSlaveSendToMasterNotActive: before the master connection is established
// both header sends report ErrNotActive (py: master is None → send is skipped;
// Go surfaces the state as an error instead of a silent drop).
func TestSlaveSendToMasterNotActive(t *testing.T) {
	b := newTestBackend(t)

	if _, err := b.SendMinorBlockHeaderToMaster(context.Background(), &wire.AddMinorBlockHeaderRequest{}); !errors.Is(err, conn.ErrNotActive) {
		t.Errorf("SendMinorBlockHeaderToMaster err = %v, want ErrNotActive", err)
	}
	if _, err := b.SendMinorBlockHeaderListToMaster(context.Background(), &wire.AddMinorBlockHeaderListRequest{}); !errors.Is(err, conn.ErrNotActive) {
		t.Errorf("SendMinorBlockHeaderListToMaster err = %v, want ErrNotActive", err)
	}
}

// TestSlaveBroadcastEmptyPoolIsNoop: with no xshard connections dialed, both
// broadcasts are no-ops matching py's gather([]).
func TestSlaveBroadcastEmptyPoolIsNoop(t *testing.T) {
	b := newTestBackend(t)
	branch := b.order[0].GetFullShardID()

	if err := b.SendXshardTxList(context.Background(), branch, &wire.AddXshardTxListRequest{Branch: branch}); err != nil {
		t.Errorf("SendXshardTxList err = %v, want nil (empty conn set)", err)
	}
	if err := b.SendBatchXshardTxList(context.Background(), branch, &wire.BatchAddXshardTxListRequest{}); err != nil {
		t.Errorf("SendBatchXshardTxList err = %v, want nil (empty conn set)", err)
	}
}

// TestSlaveConnectToSlavesEmptyList: an empty advertised list yields an empty
// result list and no error.
func TestSlaveConnectToSlavesEmptyList(t *testing.T) {
	b := newTestBackend(t)

	resp, err := b.ConnectToSlaves(&wire.ConnectToSlavesRequest{})
	if err != nil {
		t.Fatalf("ConnectToSlaves: %v", err)
	}
	if len(resp.ResultList) != 0 {
		t.Errorf("ResultList = %v, want empty", resp.ResultList)
	}
}

// TestSlaveClusterPeerLifecycle: CreateClusterPeerConnection wires a PeerConn
// on every shard, a duplicate create is tolerated, DestroyClusterPeerConnection
// closes them all, and destroying an unknown id is a no-op.
func TestSlaveClusterPeerLifecycle(t *testing.T) {
	b := newTestBackend(t)
	pipeMasterConn(t, b)

	const peerID = uint64(7)
	if _, err := b.CreateClusterPeerConnection(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: peerID}); err != nil {
		t.Fatalf("CreateClusterPeerConnection: %v", err)
	}
	for _, s := range b.Shards() {
		if s.Peer(peerID) == nil {
			t.Errorf("shard 0x%08x: Peer(%d) = nil, want connected", s.Branch.GetFullShardID(), peerID)
		}
	}

	// Duplicate create must not panic, must not double-register.
	if _, err := b.CreateClusterPeerConnection(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: peerID}); err != nil {
		t.Fatalf("CreateClusterPeerConnection(duplicate): %v", err)
	}

	if err := b.DestroyClusterPeerConnection(&wire.DestroyClusterPeerConnectionCommand{ClusterPeerID: peerID}); err != nil {
		t.Fatalf("DestroyClusterPeerConnection: %v", err)
	}
	for _, s := range b.Shards() {
		if s.Peer(peerID) != nil {
			t.Errorf("shard 0x%08x: Peer(%d) = connected, want removed", s.Branch.GetFullShardID(), peerID)
		}
	}

	// Destroying an unknown id is a no-op, not an error.
	if err := b.DestroyClusterPeerConnection(&wire.DestroyClusterPeerConnectionCommand{ClusterPeerID: peerID}); err != nil {
		t.Errorf("DestroyClusterPeerConnection(unknown): %v", err)
	}
}

// TestSlaveStopClosesMasterPeersAndPool: Stop closes the master connection and
// drains every shard's PeerConns; sends to the master afterwards fail with
// ErrConnectionClosed (not ErrNotActive — the master was established).
func TestSlaveStopClosesMasterPeersAndPool(t *testing.T) {
	b := newTestBackend(t)
	mc := pipeMasterConn(t, b)

	const peerID = uint64(7)
	if _, err := b.CreateClusterPeerConnection(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: peerID}); err != nil {
		t.Fatalf("CreateClusterPeerConnection: %v", err)
	}

	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if !mc.IsClosed() {
		t.Error("master connection not closed by Stop")
	}
	for _, s := range b.Shards() {
		if s.Peer(peerID) != nil {
			t.Errorf("shard 0x%08x: Peer(%d) survived Stop", s.Branch.GetFullShardID(), peerID)
		}
	}
	select {
	case <-b.Done():
	default:
		t.Error("Done() not closed after Stop")
	}

	_, err := b.SendMinorBlockHeaderToMaster(context.Background(), &wire.AddMinorBlockHeaderRequest{})
	if !errors.Is(err, conn.ErrConnectionClosed) {
		t.Errorf("SendMinorBlockHeaderToMaster after Stop err = %v, want ErrConnectionClosed", err)
	}
	// Pool closed: lookups drain to empty and broadcasts degrade to no-ops.
	if conns := b.xshardPool.Lookup(b.order[0].GetFullShardID()); len(conns) != 0 {
		t.Errorf("Lookup after Stop = %d conns, want 0", len(conns))
	}
}

// TestSlaveAddPeerRefusedAfterStop: a shard refuses new PeerConns once
// stopped, and the backend's create path degrades to a successful no-op
// response (shutdown is not a business failure).
func TestSlaveAddPeerRefusedAfterStop(t *testing.T) {
	b := newTestBackend(t)
	pipeMasterConn(t, b)

	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	s := b.Shards()[0]
	if _, err := s.AddPeer(7, b.master.Load()); !errors.Is(err, shard.ErrStopped) {
		t.Errorf("AddPeer after Stop err = %v, want shard.ErrStopped", err)
	}

	resp, err := b.CreateClusterPeerConnection(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: 7})
	if err != nil || resp == nil {
		t.Fatalf("CreateClusterPeerConnection after Stop = %v, %v, want a successful refusal", resp, err)
	}
}

// TestSlaveStartBindFailureStopsBackend: when the listener cannot bind, Start
// reports the error and the backend is fully stopped, leaving the datadir
// reopenable. The collision is staged deterministically: a first backend holds
// the port, a second boots against the same address.
func TestSlaveStartBindFailureStopsBackend(t *testing.T) {
	holder := newTestBackend(t)
	if err := holder.Start(); err != nil {
		t.Fatalf("holder Start: %v", err)
	}

	ctx, root := bootEnv(t, fixtureMainnet)
	ctx.Slave.Port = uint16(holder.port)
	b, err := New(ctx, root, Options{}, shard.Options{})
	if err != nil {
		t.Fatalf("slave.New: %v", err)
	}
	t.Cleanup(func() { b.Stop() })

	if err := b.Start(); err == nil {
		t.Fatal("Start on an occupied port err = nil, want bind failure")
	}
	select {
	case <-b.Done():
	default:
		t.Error("Done() not closed after failed Start")
	}

	// The failed boot left the datadir reopenable.
	b2, err := New(ctx, root, Options{}, shard.Options{})
	if err != nil {
		t.Fatalf("slave.New(reopen after failed Start): %v", err)
	}
	if err := b2.Stop(); err != nil {
		t.Fatalf("Stop(reopen): %v", err)
	}
}

// TestSlaveStartStopLifecycle: a normal Start binds the listener and Stop
// resolves Done exactly once.
func TestSlaveStartStopLifecycle(t *testing.T) {
	b := newTestBackend(t)

	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if b.listener == nil {
		t.Fatal("Start left no listener")
	}
	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-b.Done():
	default:
		t.Error("Done() not closed after Stop")
	}
}

// TestSlaveMasterLossTriggersStop: the first inbound connection becomes the
// master; when the master's TCP connection drops, the backend stops itself and
// Done resolves (py: MasterConnection.close → SlaveServer.shutdown,
// slave.py:155-162).
func TestSlaveMasterLossTriggersStop(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	client, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port)))
	if err != nil {
		t.Fatalf("dial slave: %v", err)
	}
	defer client.Close()

	// Wait until the master pointer is published before dropping the socket,
	// so the test exercises the established-master path, not the compensation
	// path.
	deadline := time.Now().Add(2 * time.Second)
	for b.master.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("master connection never established")
		}
		time.Sleep(2 * time.Millisecond)
	}

	client.Close()

	select {
	case <-b.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("master loss did not stop the backend")
	}
	if b.master.Load() == nil {
		t.Error("master pointer unpublished after loss")
	}
}

// TestSlaveGarbageSecondConnectionDoesNotKillSlave: only the first inbound
// connection is the master; a subsequent connection that speaks no protocol is
// rejected by the xshard pool without taking the backend down (accept-loop
// classification stays loop-local).
func TestSlaveGarbageSecondConnectionDoesNotKillSlave(t *testing.T) {
	b := newTestBackend(t)
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	master, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port)))
	if err != nil {
		t.Fatalf("dial master conn: %v", err)
	}
	defer master.Close()

	stranger, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(b.port)))
	if err != nil {
		t.Fatalf("dial stranger conn: %v", err)
	}
	stranger.Close() // never handshakes; the pool must reject it silently

	deadline := time.Now().Add(2 * time.Second)
	for b.master.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("master connection never established")
		}
		time.Sleep(2 * time.Millisecond)
	}

	select {
	case <-b.Done():
		t.Fatal("a rejected xshard connection stopped the backend")
	case <-time.After(200 * time.Millisecond):
	}

	master.Close()
	select {
	case <-b.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("master loss did not stop the backend")
	}
}

// TestSlaveStopConcurrentTriggers: Stop, topology churn, and outbound sends
// racing each other must be data-race free (run under -race) and must leave
// the backend stopped exactly once.
func TestSlaveStopConcurrentTriggers(t *testing.T) {
	b := newTestBackend(t)
	pipeMasterConn(t, b)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Stop()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.CreateClusterPeerConnection(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: 7})
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.DestroyClusterPeerConnection(&wire.DestroyClusterPeerConnectionCommand{ClusterPeerID: 7})
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.SendXshardTxList(context.Background(), b.order[0].GetFullShardID(), &wire.AddXshardTxListRequest{})
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.LookupPeer(7, b.order[0].GetFullShardID())
	}()
	wg.Wait()

	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-b.Done():
	default:
		t.Error("Done() not closed after concurrent Stops")
	}
}
