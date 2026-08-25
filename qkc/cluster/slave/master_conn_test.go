// Copyright 2026-2027, QuarkChain.

package slave

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
	"github.com/ethereum/go-ethereum/qkc/serialize"
)

// ── test handler ─────────────────────────────────────────────────────────────

// fakeMasterHandler stands in for the service-layer MasterHandler: it
// acknowledges every business request with a zero-value response.
type fakeMasterHandler struct {
	// errGenTx, if set, is returned by GenTx to simulate a handler failure.
	errGenTx error
}

func (h *fakeMasterHandler) ConnectToSlaves(req *wire.ConnectToSlavesRequest) (*wire.ConnectToSlavesResponse, error) {
	resp := &wire.ConnectToSlavesResponse{ResultList: make([]wire.PrependedSizeBytes4, len(req.SlaveInfoList))}
	return resp, nil
}

func (h *fakeMasterHandler) Mine(*wire.MineRequest) (*wire.MineResponse, error) {
	return &wire.MineResponse{}, nil
}

func (h *fakeMasterHandler) GenTx(*wire.GenTxRequest) (*wire.GenTxResponse, error) {
	if h.errGenTx != nil {
		return nil, h.errGenTx
	}
	return &wire.GenTxResponse{}, nil
}

func (h *fakeMasterHandler) AddRootBlock(*wire.AddRootBlockRequest) (*wire.AddRootBlockResponse, error) {
	return &wire.AddRootBlockResponse{}, nil
}

func (h *fakeMasterHandler) GetEcoInfoList(*wire.GetEcoInfoListRequest) (*wire.GetEcoInfoListResponse, error) {
	return &wire.GetEcoInfoListResponse{}, nil
}

func (h *fakeMasterHandler) GetNextBlockToMine(*wire.GetNextBlockToMineRequest) (*wire.GetNextBlockToMineResponse, error) {
	return &wire.GetNextBlockToMineResponse{}, nil
}

func (h *fakeMasterHandler) AddMinorBlock(*wire.AddMinorBlockRequest) (*wire.AddMinorBlockResponse, error) {
	return &wire.AddMinorBlockResponse{}, nil
}

func (h *fakeMasterHandler) GetUnconfirmedHeaders(*wire.GetUnconfirmedHeadersRequest) (*wire.GetUnconfirmedHeadersResponse, error) {
	return &wire.GetUnconfirmedHeadersResponse{}, nil
}

func (h *fakeMasterHandler) GetAccountData(*wire.GetAccountDataRequest) (*wire.GetAccountDataResponse, error) {
	return &wire.GetAccountDataResponse{}, nil
}

func (h *fakeMasterHandler) AddTransaction(*wire.AddTransactionRequest) (*wire.AddTransactionResponse, error) {
	return &wire.AddTransactionResponse{}, nil
}

func (h *fakeMasterHandler) GetMinorBlock(*wire.GetMinorBlockRequest) (*wire.GetMinorBlockResponse, error) {
	return &wire.GetMinorBlockResponse{}, nil
}

func (h *fakeMasterHandler) GetTransaction(*wire.GetTransactionRequest) (*wire.GetTransactionResponse, error) {
	return &wire.GetTransactionResponse{}, nil
}

func (h *fakeMasterHandler) SyncMinorBlockList(*wire.SyncMinorBlockListRequest) (*wire.SyncMinorBlockListResponse, error) {
	return &wire.SyncMinorBlockListResponse{}, nil
}

func (h *fakeMasterHandler) ExecuteTransaction(*wire.ExecuteTransactionRequest) (*wire.ExecuteTransactionResponse, error) {
	return &wire.ExecuteTransactionResponse{}, nil
}

func (h *fakeMasterHandler) GetTransactionReceipt(*wire.GetTransactionReceiptRequest) (*wire.GetTransactionReceiptResponse, error) {
	return &wire.GetTransactionReceiptResponse{}, nil
}

func (h *fakeMasterHandler) GetTransactionListByAddress(*wire.GetTransactionListByAddressRequest) (*wire.GetTransactionListByAddressResponse, error) {
	return &wire.GetTransactionListByAddressResponse{}, nil
}

func (h *fakeMasterHandler) GetLogs(*wire.GetLogRequest) (*wire.GetLogResponse, error) {
	return &wire.GetLogResponse{}, nil
}

func (h *fakeMasterHandler) EstimateGas(*wire.EstimateGasRequest) (*wire.EstimateGasResponse, error) {
	return &wire.EstimateGasResponse{}, nil
}

func (h *fakeMasterHandler) GetStorageAt(*wire.GetStorageRequest) (*wire.GetStorageResponse, error) {
	return &wire.GetStorageResponse{}, nil
}

func (h *fakeMasterHandler) GetCode(*wire.GetCodeRequest) (*wire.GetCodeResponse, error) {
	return &wire.GetCodeResponse{}, nil
}

func (h *fakeMasterHandler) GasPrice(*wire.GasPriceRequest) (*wire.GasPriceResponse, error) {
	return &wire.GasPriceResponse{}, nil
}

func (h *fakeMasterHandler) GetWork(*wire.GetWorkRequest) (*wire.GetWorkResponse, error) {
	return &wire.GetWorkResponse{}, nil
}

func (h *fakeMasterHandler) SubmitWork(*wire.SubmitWorkRequest) (*wire.SubmitWorkResponse, error) {
	return &wire.SubmitWorkResponse{}, nil
}

func (h *fakeMasterHandler) CheckMinorBlock(*wire.CheckMinorBlockRequest) (*wire.CheckMinorBlockResponse, error) {
	return &wire.CheckMinorBlockResponse{}, nil
}

func (h *fakeMasterHandler) GetAllTransactions(*wire.GetAllTransactionsRequest) (*wire.GetAllTransactionsResponse, error) {
	return &wire.GetAllTransactionsResponse{}, nil
}

func (h *fakeMasterHandler) GetRootChainStakes(*wire.GetRootChainStakesRequest) (*wire.GetRootChainStakesResponse, error) {
	return &wire.GetRootChainStakesResponse{}, nil
}

func (h *fakeMasterHandler) GetTotalBalance(*wire.GetTotalBalanceRequest) (*wire.GetTotalBalanceResponse, error) {
	return &wire.GetTotalBalanceResponse{}, nil
}

// ── TCP pair helper ──────────────────────────────────────────────────────────

// newMasterTestConnPairWithIdentity creates a pair of MasterConns connected
// over a local TCP socket with the given identities and the default fake
// handler. The caller is responsible for calling cleanup.
func newMasterTestConnPairWithIdentity(
	t *testing.T,
	clientID []byte, clientShards []uint32,
	serverID []byte, serverShards []uint32,
) (client, server *MasterConn, cleanup func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var serverConn net.Conn
	var acceptErr error
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		serverConn, acceptErr = ln.Accept()
		ln.Close()
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	<-accepted
	if acceptErr != nil {
		t.Fatalf("accept: %v", acceptErr)
	}

	logger := log.New()
	client, err = NewMasterConn(MasterConnConfig{
		Conn:                 clientConn,
		LocalID:              clientID,
		LocalFullShardIDList: clientShards,
		Handler:              &fakeMasterHandler{},
		Logger:               logger,
	})
	if err != nil {
		t.Fatalf("new client master conn: %v", err)
	}
	server, err = NewMasterConn(MasterConnConfig{
		Conn:                 serverConn,
		LocalID:              serverID,
		LocalFullShardIDList: serverShards,
		Handler:              &fakeMasterHandler{},
		Logger:               logger,
	})
	if err != nil {
		t.Fatalf("new server master conn: %v", err)
	}
	cleanup = func() {
		client.Close()
		server.Close()
	}
	return
}

// ── raw master peer helper ───────────────────────────────────────────────────

// masterTestPeer drives the master side of the protocol over a net.Pipe: it
// writes raw frames to the slave and collects frames written by the slave.
type masterTestPeer struct {
	conn   net.Conn
	frames chan *wire.Frame
}

func newMasterTestPeer(conn net.Conn) *masterTestPeer {
	p := &masterTestPeer{
		conn:   conn,
		frames: make(chan *wire.Frame, 16),
	}
	go func() {
		r := bufio.NewReader(conn)
		for {
			f, err := wire.ReadFrame(r, 0)
			if err != nil {
				return
			}
			select {
			case p.frames <- f:
			default: // drop if the test is not consuming; avoid blocking
			}
		}
	}()
	return p
}

func (p *masterTestPeer) send(f *wire.Frame) error {
	return wire.WriteFrame(p.conn, f)
}

func (p *masterTestPeer) nextFrame(t *testing.T, timeout time.Duration) *wire.Frame {
	t.Helper()
	select {
	case f := <-p.frames:
		return f
	case <-time.After(timeout):
		t.Fatal("timed out waiting for frame from slave")
		return nil
	}
}

// newMasterConnWithPeer creates a started MasterConn over a net.Pipe with a
// raw master peer on the other end. All frames go through the real wire
// encode/decode path.
func newMasterConnWithPeer(t *testing.T, handler MasterHandler) (*MasterConn, *masterTestPeer, func()) {
	t.Helper()
	peerConn, slaveConn := net.Pipe()
	mc, err := NewMasterConn(MasterConnConfig{
		Conn:                 slaveConn,
		LocalID:              []byte("go-slave"),
		LocalFullShardIDList: []uint32{0x00010001},
		Handler:              handler,
		Logger:               log.New(),
	})
	if err != nil {
		peerConn.Close()
		slaveConn.Close()
		t.Fatalf("new master conn: %v", err)
	}
	mc.Start()
	peer := newMasterTestPeer(peerConn)
	cleanup := func() {
		mc.Close()
		peerConn.Close()
		slaveConn.Close()
	}
	return mc, peer, cleanup
}

// ── construction ─────────────────────────────────────────────────────────────

func TestMasterConn_ConfigValidation(t *testing.T) {
	// Nil conn / nil handler must be rejected.
	if _, err := NewMasterConn(MasterConnConfig{}); err == nil {
		t.Fatal("expected error for nil conn")
	}
	if _, err := NewMasterConn(MasterConnConfig{Conn: &net.TCPConn{}}); err == nil {
		t.Fatal("expected error for nil handler")
	}

	// Identity getters return copies: source slices are stored by value and
	// later mutation must not leak into the conn.
	id := []byte("slave-a")
	shards := []uint32{0x00010001, 0x00020001}
	client, _, cleanup := newMasterTestConnPairWithIdentity(t, id, shards, []byte("b"), []uint32{0x00010001})
	defer cleanup()

	if !bytes.Equal(client.LocalID(), id) {
		t.Fatalf("LocalID: got %s, want %s", client.LocalID(), id)
	}
	got := client.LocalFullShardIDList()
	if len(got) != len(shards) || got[0] != shards[0] || got[1] != shards[1] {
		t.Fatalf("LocalFullShardIDList: got %v, want %v", got, shards)
	}

	id[0] = 'X'
	shards[0] = 0
	if c := client.LocalID(); !bytes.Equal(c, []byte("slave-a")) {
		t.Fatalf("LocalID changed after source mutation: got %s", c)
	}
	if c := client.LocalFullShardIDList(); c[0] != 0x00010001 {
		t.Fatalf("LocalFullShardIDList changed after source mutation: %v", c)
	}
}

// ── communication handlers ───────────────────────────────────────────────────

// TestMasterConn_Ping verifies PING→PONG across the real wire path: it echoes
// the slave's configured identity (never the PING payload's), behaves the same
// with a root tip set (shard creation is a PR7 TODO and must not corrupt the
// reply), and keeps the connection open.
func TestMasterConn_Ping(t *testing.T) {
	server, peer, cleanup := newMasterConnWithPeer(t, &fakeMasterHandler{})
	defer cleanup()

	for i, rootTip := range []*wire.RawBytes{nil, {0x01, 0x02}} {
		payload, err := serialize.SerializeToBytes(&wire.PingRequest{
			ID:              []byte("master"),
			FullShardIDList: []uint32{0x00010001},
			RootTip:         rootTip,
		})
		if err != nil {
			t.Fatalf("serialize ping: %v", err)
		}
		// RPC IDs strictly increase across both pings.
		if err := peer.send(&wire.Frame{
			Meta:    wire.ClusterMetadata{},
			Opcode:  byte(wire.ClusterOpPing),
			RPCID:   uint64(i + 1),
			Payload: payload,
		}); err != nil {
			t.Fatalf("send ping: %v", err)
		}

		resp := peer.nextFrame(t, 2*time.Second)
		if resp.Opcode != byte(wire.ClusterOpPong) {
			t.Fatalf("expected pong, got opcode 0x%x", resp.Opcode)
		}
		var pong wire.PongResponse
		if err := serialize.Deserialize(serialize.NewByteBuffer(resp.Payload), &pong); err != nil {
			t.Fatalf("deserialize pong: %v", err)
		}
		if string(pong.ID) != "go-slave" {
			t.Fatalf("pong id mismatch: got %s, want go-slave", pong.ID)
		}
		if len(pong.FullShardIDList) != 1 || pong.FullShardIDList[0] != 0x00010001 {
			t.Fatalf("pong shard list mismatch: %v", pong.FullShardIDList)
		}
	}

	select {
	case <-server.WaitUntilClosed():
		t.Fatal("connection closed by PING")
	default:
	}
}

// TestMasterConn_CreateClusterPeerConnectionNotImplemented verifies that
// CreateClusterPeerConnection fails honestly before PR6: the handler returns
// ErrHandlerNotImplemented (closes the connection, no response) instead of a
// false error_code=0 success — the master ignores the error code and would
// immediately send peer frames this conn cannot route.
func TestMasterConn_CreateClusterPeerConnectionNotImplemented(t *testing.T) {
	server, peer, cleanup := newMasterConnWithPeer(t, &fakeMasterHandler{})
	defer cleanup()

	payload, _ := serialize.SerializeToBytes(&wire.CreateClusterPeerConnectionRequest{ClusterPeerID: 7})
	if err := peer.send(&wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpCreateClusterPeerConnectionRequest),
		RPCID:   1,
		Payload: payload,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close after CreateClusterPeerConnection")
	}

	// No response frame may be written.
	select {
	case f := <-peer.frames:
		t.Fatalf("unexpected response frame: opcode 0x%x", f.Opcode)
	default:
	}
}

// TestMasterConn_NonRPCDispatch verifies that the fire-and-forget
// DESTROY_CLUSTER_PEER_CONNECTION_COMMAND is accepted with rpc_id == 0 and does
// not produce a response or close the connection.
func TestMasterConn_NonRPCDispatch(t *testing.T) {
	server, peer, cleanup := newMasterConnWithPeer(t, &fakeMasterHandler{})
	defer cleanup()

	// Fire-and-forget: rpc_id == 0, no response expected.
	payload, _ := serialize.SerializeToBytes(&wire.DestroyClusterPeerConnectionCommand{ClusterPeerID: 42})
	if err := peer.send(&wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x00010001},
		Opcode:  byte(wire.ClusterOpDestroyClusterPeerConnectionCommand),
		RPCID:   0,
		Payload: payload,
	}); err != nil {
		t.Fatalf("send destroy: %v", err)
	}

	// A subsequent RPC must still work.
	pingPayload, _ := serialize.SerializeToBytes(&wire.PingRequest{
		ID:              []byte("master"),
		FullShardIDList: []uint32{0x00010001},
	})
	if err := peer.send(&wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   1,
		Payload: pingPayload,
	}); err != nil {
		t.Fatalf("send ping: %v", err)
	}

	resp := peer.nextFrame(t, 2*time.Second)
	if resp.Opcode != byte(wire.ClusterOpPong) {
		t.Fatalf("expected pong, got opcode 0x%x", resp.Opcode)
	}

	select {
	case <-server.WaitUntilClosed():
		t.Fatal("connection closed by non-rpc command")
	default:
	}
}

// ── business handler delegation ──────────────────────────────────────────────

// TestMasterConn_BusinessHandlerErrorClosesConnection verifies that a handler
// error is treated as a connection-level failure.
func TestMasterConn_BusinessHandlerErrorClosesConnection(t *testing.T) {
	server, peer, cleanup := newMasterConnWithPeer(t, &fakeMasterHandler{
		errGenTx: errors.New("boom"),
	})
	defer cleanup()

	payload, _ := serialize.SerializeToBytes(&wire.GenTxRequest{})
	if err := peer.send(&wire.Frame{
		Meta:    wire.ClusterMetadata{},
		Opcode:  byte(wire.ClusterOpGenTxRequest),
		RPCID:   1,
		Payload: payload,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case <-server.WaitUntilClosed():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not close after handler error")
	}
}

// TestMasterConn_MasterToSlaveOpcodeMatrix walks every delegated master→slave
// RPC on a single connection: each request must be dispatched to the handler
// and answered with the matching response opcode. This is the opcode coverage
// matrix mirroring Python slave.py's MasterConnection handler registrations.
func TestMasterConn_MasterToSlaveOpcodeMatrix(t *testing.T) {
	server, peer, cleanup := newMasterConnWithPeer(t, &fakeMasterHandler{})
	defer cleanup()

	cases := []struct {
		name    string
		op      wire.ClusterOp
		respOp  wire.ClusterOp
		request any
	}{
		{"ConnectToSlaves", wire.ClusterOpConnectToSlavesRequest, wire.ClusterOpConnectToSlavesResponse, &wire.ConnectToSlavesRequest{}},
		{"Mine", wire.ClusterOpMineRequest, wire.ClusterOpMineResponse, &wire.MineRequest{}},
		{"GenTx", wire.ClusterOpGenTxRequest, wire.ClusterOpGenTxResponse, &wire.GenTxRequest{}},
		{"AddRootBlock", wire.ClusterOpAddRootBlockRequest, wire.ClusterOpAddRootBlockResponse, &wire.AddRootBlockRequest{}},
		{"GetEcoInfoList", wire.ClusterOpGetEcoInfoListRequest, wire.ClusterOpGetEcoInfoListResponse, &wire.GetEcoInfoListRequest{}},
		{"GetNextBlockToMine", wire.ClusterOpGetNextBlockToMineRequest, wire.ClusterOpGetNextBlockToMineResponse, &wire.GetNextBlockToMineRequest{}},
		{"AddMinorBlock", wire.ClusterOpAddMinorBlockRequest, wire.ClusterOpAddMinorBlockResponse, &wire.AddMinorBlockRequest{}},
		{"GetUnconfirmedHeaders", wire.ClusterOpGetUnconfirmedHeadersRequest, wire.ClusterOpGetUnconfirmedHeadersResponse, &wire.GetUnconfirmedHeadersRequest{}},
		{"GetAccountData", wire.ClusterOpGetAccountDataRequest, wire.ClusterOpGetAccountDataResponse, &wire.GetAccountDataRequest{}},
		{"AddTransaction", wire.ClusterOpAddTransactionRequest, wire.ClusterOpAddTransactionResponse, &wire.AddTransactionRequest{}},
		{"GetMinorBlock", wire.ClusterOpGetMinorBlockRequest, wire.ClusterOpGetMinorBlockResponse, &wire.GetMinorBlockRequest{}},
		{"GetTransaction", wire.ClusterOpGetTransactionRequest, wire.ClusterOpGetTransactionResponse, &wire.GetTransactionRequest{}},
		{"SyncMinorBlockList", wire.ClusterOpSyncMinorBlockListRequest, wire.ClusterOpSyncMinorBlockListResponse, &wire.SyncMinorBlockListRequest{}},
		{"ExecuteTransaction", wire.ClusterOpExecuteTransactionRequest, wire.ClusterOpExecuteTransactionResponse, &wire.ExecuteTransactionRequest{}},
		{"GetTransactionReceipt", wire.ClusterOpGetTransactionReceiptRequest, wire.ClusterOpGetTransactionReceiptResponse, &wire.GetTransactionReceiptRequest{}},
		{"GetTransactionListByAddress", wire.ClusterOpGetTransactionListByAddressRequest, wire.ClusterOpGetTransactionListByAddressResponse, &wire.GetTransactionListByAddressRequest{}},
		{"GetLogs", wire.ClusterOpGetLogRequest, wire.ClusterOpGetLogResponse, &wire.GetLogRequest{}},
		{"EstimateGas", wire.ClusterOpEstimateGasRequest, wire.ClusterOpEstimateGasResponse, &wire.EstimateGasRequest{}},
		{"GetStorageAt", wire.ClusterOpGetStorageRequest, wire.ClusterOpGetStorageResponse, &wire.GetStorageRequest{}},
		{"GetCode", wire.ClusterOpGetCodeRequest, wire.ClusterOpGetCodeResponse, &wire.GetCodeRequest{}},
		{"GasPrice", wire.ClusterOpGasPriceRequest, wire.ClusterOpGasPriceResponse, &wire.GasPriceRequest{}},
		{"GetWork", wire.ClusterOpGetWorkRequest, wire.ClusterOpGetWorkResponse, &wire.GetWorkRequest{}},
		{"SubmitWork", wire.ClusterOpSubmitWorkRequest, wire.ClusterOpSubmitWorkResponse, &wire.SubmitWorkRequest{}},
		{"CheckMinorBlock", wire.ClusterOpCheckMinorBlockRequest, wire.ClusterOpCheckMinorBlockResponse, &wire.CheckMinorBlockRequest{}},
		{"GetAllTransactions", wire.ClusterOpGetAllTransactionsRequest, wire.ClusterOpGetAllTransactionsResponse, &wire.GetAllTransactionsRequest{}},
		{"GetRootChainStakes", wire.ClusterOpGetRootChainStakesRequest, wire.ClusterOpGetRootChainStakesResponse, &wire.GetRootChainStakesRequest{}},
		{"GetTotalBalance", wire.ClusterOpGetTotalBalanceRequest, wire.ClusterOpGetTotalBalanceResponse, &wire.GetTotalBalanceRequest{}},
	}

	for i, c := range cases {
		payload, err := serialize.SerializeToBytes(c.request)
		if err != nil {
			t.Fatalf("%s: serialize request: %v", c.name, err)
		}
		if err := peer.send(&wire.Frame{
			Meta:    wire.ClusterMetadata{},
			Opcode:  byte(c.op),
			RPCID:   uint64(i + 1), // strictly increasing
			Payload: payload,
		}); err != nil {
			t.Fatalf("%s: send: %v", c.name, err)
		}

		resp := peer.nextFrame(t, 2*time.Second)
		if resp.Opcode != byte(c.respOp) {
			t.Fatalf("%s: response opcode: got 0x%x, want 0x%x", c.name, resp.Opcode, c.respOp)
		}
		if resp.RPCID != uint64(i+1) {
			t.Fatalf("%s: rpc_id echo: got %d, want %d", c.name, resp.RPCID, i+1)
		}
	}

	select {
	case <-server.WaitUntilClosed():
		t.Fatal("connection closed during opcode matrix")
	default:
	}
}

// ── outbound RPCs ────────────────────────────────────────────────────────────

// TestMasterConn_OutboundRPCMeta verifies that outbound RPCs from the slave
// encode ClusterMetadata correctly on the wire and that responses match by
// rpc_id.
func TestMasterConn_OutboundRPCMeta(t *testing.T) {
	client, peer, cleanup := newMasterConnWithPeer(t, &fakeMasterHandler{})
	defer cleanup()

	meta := wire.ClusterMetadata{Branch: 0x00010001}
	payload, _ := serialize.SerializeToBytes(&wire.GetEcoInfoListRequest{})

	type rpcResult struct {
		resp any
		err  error
	}
	result := make(chan rpcResult, 1)
	go func() {
		resp, err := client.SendRPCMeta(context.Background(), byte(wire.ClusterOpGetEcoInfoListRequest), payload, meta)
		result <- rpcResult{resp, err}
	}()

	// Capture the outbound request frame and verify its metadata.
	reqFrame := peer.nextFrame(t, 2*time.Second)
	if reqFrame.Meta != meta {
		t.Fatalf("request metadata mismatch: got %+v, want %+v", reqFrame.Meta, meta)
	}

	// Reply with the matching rpc_id.
	respPayload, _ := serialize.SerializeToBytes(&wire.GetEcoInfoListResponse{ErrorCode: 0})
	if err := peer.send(&wire.Frame{
		Meta:    meta,
		Opcode:  byte(wire.ClusterOpGetEcoInfoListResponse),
		RPCID:   reqFrame.RPCID,
		Payload: respPayload,
	}); err != nil {
		t.Fatalf("send response: %v", err)
	}

	select {
	case r := <-result:
		if r.err != nil {
			t.Fatalf("rpc failed: %v", r.err)
		}
		if _, ok := r.resp.(*wire.GetEcoInfoListResponse); !ok {
			t.Fatalf("unexpected response type %T", r.resp)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rpc result not received")
	}
}

// TestMasterConn_SendAddMinorBlockHeader verifies the typed outbound helper
// against a raw master peer.
func TestMasterConn_SendAddMinorBlockHeader(t *testing.T) {
	clientConn, masterConn := net.Pipe()
	defer clientConn.Close()
	defer masterConn.Close()

	client, err := NewMasterConn(MasterConnConfig{
		Conn:                 clientConn,
		LocalID:              []byte("slave"),
		LocalFullShardIDList: []uint32{0x00010001},
		Handler:              &fakeMasterHandler{},
		Logger:               log.New(),
	})
	if err != nil {
		t.Fatalf("new master conn: %v", err)
	}
	client.Start()

	// Raw master peer: read the request, reply with a success response.
	type readResult struct {
		frame *wire.Frame
		err   error
	}
	readCh := make(chan readResult, 1)
	go func() {
		frame, err := wire.ReadFrame(bufio.NewReader(masterConn), 0)
		readCh <- readResult{frame, err}
	}()

	req := &wire.AddMinorBlockHeaderRequest{
		MinorBlockHeader:  &wire.RawBytes{},
		TxCount:           5,
		XShardTxCount:     0,
		CoinbaseAmountMap: &wire.RawBytes{},
		ShardStats:        wire.ShardStats{Branch: 0x00010001},
	}
	type sendResult struct {
		resp *wire.AddMinorBlockHeaderResponse
		err  error
	}
	sendCh := make(chan sendResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := client.SendAddMinorBlockHeader(ctx, req)
		sendCh <- sendResult{resp, err}
	}()

	select {
	case r := <-readCh:
		if r.err != nil {
			t.Fatalf("master peer read: %v", r.err)
		}
		if r.frame.Opcode != byte(wire.ClusterOpAddMinorBlockHeaderRequest) {
			t.Fatalf("unexpected request opcode 0x%x", r.frame.Opcode)
		}
		respPayload, _ := serialize.SerializeToBytes(&wire.AddMinorBlockHeaderResponse{ErrorCode: 0})
		if err := wire.WriteFrame(masterConn, &wire.Frame{
			Meta:    r.frame.Meta,
			Opcode:  byte(wire.ClusterOpAddMinorBlockHeaderResponse),
			RPCID:   r.frame.RPCID,
			Payload: respPayload,
		}); err != nil {
			t.Fatalf("master peer write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request frame not received by master peer")
	}

	select {
	case r := <-sendCh:
		if r.err != nil {
			t.Fatalf("SendAddMinorBlockHeader: %v", r.err)
		}
		if r.resp.ErrorCode != 0 {
			t.Fatalf("unexpected error_code: %d", r.resp.ErrorCode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendAddMinorBlockHeader did not return")
	}
}

// TestMasterConn_SendAddMinorBlockHeaderList verifies the typed batch outbound
// helper against a raw master peer.
func TestMasterConn_SendAddMinorBlockHeaderList(t *testing.T) {
	clientConn, masterConn := net.Pipe()
	defer clientConn.Close()
	defer masterConn.Close()

	client, err := NewMasterConn(MasterConnConfig{
		Conn:                 clientConn,
		LocalID:              []byte("slave"),
		LocalFullShardIDList: []uint32{0x00010001},
		Handler:              &fakeMasterHandler{},
		Logger:               log.New(),
	})
	if err != nil {
		t.Fatalf("new master conn: %v", err)
	}
	client.Start()

	type readResult struct {
		frame *wire.Frame
		err   error
	}
	readCh := make(chan readResult, 1)
	go func() {
		frame, err := wire.ReadFrame(bufio.NewReader(masterConn), 0)
		readCh <- readResult{frame, err}
	}()

	req := &wire.AddMinorBlockHeaderListRequest{
		MinorBlockHeaderList:  []*wire.RawBytes{{0x01}},
		CoinbaseAmountMapList: []*wire.RawBytes{{0x02}},
	}
	type sendResult struct {
		resp *wire.AddMinorBlockHeaderListResponse
		err  error
	}
	sendCh := make(chan sendResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := client.SendAddMinorBlockHeaderList(ctx, req)
		sendCh <- sendResult{resp, err}
	}()

	select {
	case r := <-readCh:
		if r.err != nil {
			t.Fatalf("master peer read: %v", r.err)
		}
		if r.frame.Opcode != byte(wire.ClusterOpAddMinorBlockHeaderListRequest) {
			t.Fatalf("unexpected request opcode 0x%x", r.frame.Opcode)
		}
		respPayload, _ := serialize.SerializeToBytes(&wire.AddMinorBlockHeaderListResponse{ErrorCode: 0})
		if err := wire.WriteFrame(masterConn, &wire.Frame{
			Meta:    r.frame.Meta,
			Opcode:  byte(wire.ClusterOpAddMinorBlockHeaderListResponse),
			RPCID:   r.frame.RPCID,
			Payload: respPayload,
		}); err != nil {
			t.Fatalf("master peer write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request frame not received by master peer")
	}

	select {
	case r := <-sendCh:
		if r.err != nil {
			t.Fatalf("SendAddMinorBlockHeaderList: %v", r.err)
		}
		if r.resp.ErrorCode != 0 {
			t.Fatalf("unexpected error_code: %d", r.resp.ErrorCode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendAddMinorBlockHeaderList did not return")
	}
}

// ── wire format ──────────────────────────────────────────────────────────────

// TestMasterConn_FrameWireLayout verifies the full ClusterMetadata frame layout
// written by MasterConn matches the Python protocol.
func TestMasterConn_FrameWireLayout(t *testing.T) {
	var buf bytes.Buffer
	frame := &wire.Frame{
		Meta:    wire.ClusterMetadata{Branch: 0x01020304, ClusterPeerID: 0x1122334455667788},
		Opcode:  byte(wire.ClusterOpPing),
		RPCID:   0xAABBCCDDEEFF0011,
		Payload: []byte{0xAA, 0xBB},
	}
	if err := wire.WriteFrame(&buf, frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	wireBytes := buf.Bytes()
	if len(wireBytes) != 4+12+1+8+2 {
		t.Fatalf("frame length: got %d, want %d", len(wireBytes), 4+12+1+8+2)
	}

	if got := binary.BigEndian.Uint32(wireBytes[0:4]); got != 2 {
		t.Fatalf("payload_len: got %d, want 2", got)
	}
	if got := binary.BigEndian.Uint32(wireBytes[4:8]); got != frame.Meta.Branch {
		t.Fatalf("branch mismatch: got 0x%x", got)
	}
	if got := binary.BigEndian.Uint64(wireBytes[8:16]); got != frame.Meta.ClusterPeerID {
		t.Fatalf("cluster_peer_id mismatch: got 0x%x", got)
	}
	if wireBytes[16] != frame.Opcode {
		t.Fatalf("opcode mismatch: got 0x%x", wireBytes[16])
	}
	if got := binary.BigEndian.Uint64(wireBytes[17:25]); got != frame.RPCID {
		t.Fatalf("rpc_id mismatch: got 0x%x", got)
	}
	if !bytes.Equal(wireBytes[25:], frame.Payload) {
		t.Fatalf("payload mismatch: got %x", wireBytes[25:])
	}
}
