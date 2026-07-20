// Copyright 2026-2027, QuarkChain.

package slave

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// SlaveConfig holds the runtime configuration for a SlaveServer.
type SlaveConfig struct {
	// ID is this slave's unique identifier (e.g., []byte("S0")).
	ID []byte
	// FullShardIDList contains the shards managed by this slave.
	FullShardIDList []uint32
	// Port is the TCP port on which the slave listens for cluster connections.
	Port int
	// MaxPayloadSize limits incoming frame payload size; 0 disables the limit.
	MaxPayloadSize uint32
}

// Validate returns an error if the configuration is unusable.
func (cfg *SlaveConfig) Validate() error {
	if len(cfg.ID) == 0 {
		return errors.New("slave id is required")
	}
	if len(cfg.FullShardIDList) == 0 {
		return errors.New("full shard id list is required")
	}
	if cfg.Port <= 0 {
		return errors.New("slave port must be positive")
	}
	return nil
}

// SlaveServer is the runtime orchestration layer for the Go slave. It wires
// together the listener, MasterConn, XshardPool, Dispatcher and PeerConns,
// matching Python's quarkchain.cluster.slave.SlaveServer.
type SlaveServer struct {
	cfg SlaveConfig

	master        *MasterConn
	masterClaimed bool // true once acceptLoop has dispatched a master conn
	masterMu      sync.RWMutex

	dispatcher *Dispatcher
	xshardPool *XshardPool

	listener   net.Listener
	listenerMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	stopOnce sync.Once
	stopped  chan struct{}

	logger log.Logger
}

// NewSlaveServer creates an unstarted SlaveServer.
func NewSlaveServer(cfg SlaveConfig, logger log.Logger) (*SlaveServer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid slave config: %w", err)
	}
	if logger == nil {
		logger = log.Root()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &SlaveServer{
		cfg:     cfg,
		stopped: make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
	}, nil
}

// Start begins listening for cluster connections. The first inbound connection
// becomes the MasterConn; all subsequent inbound connections are treated as
// xshard slave connections.
func (s *SlaveServer) Start() error {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()

	if s.listener != nil {
		return errors.New("slave server already started")
	}

	s.dispatcher = NewDispatcher(s.logger)
	s.xshardPool = NewXshardPool(s.logger)

	addr := net.JoinHostPort("0.0.0.0", strconv.Itoa(s.cfg.Port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	s.listener = ln

	s.wg.Add(1)
	go s.acceptLoop()

	s.logger.Info("slave server started", "addr", ln.Addr().String(), "id", string(s.cfg.ID))
	return nil
}

// Stop closes the listener and all managed connections, then waits for
// background goroutines to exit.
func (s *SlaveServer) Stop() {
	s.stopOnce.Do(func() {
		s.logger.Info("slave server stopping")
		s.cancel()

		s.listenerMu.Lock()
		if s.listener != nil {
			s.listener.Close()
			s.listener = nil
		}
		s.listenerMu.Unlock()

		s.masterMu.RLock()
		master := s.master
		s.masterMu.RUnlock()
		if master != nil {
			master.Close()
		}

		if s.xshardPool != nil {
			s.xshardPool.Close()
		}
		if s.dispatcher != nil {
			s.dispatcher.Close()
		}

		s.wg.Wait()
		close(s.stopped)
		s.logger.Info("slave server stopped")
	})
}

// WaitStopped blocks until Stop has completed.
func (s *SlaveServer) WaitStopped() <-chan struct{} {
	return s.stopped
}

// MasterConn returns the current master connection, if any.
func (s *SlaveServer) MasterConn() *MasterConn {
	s.masterMu.RLock()
	defer s.masterMu.RUnlock()
	return s.master
}

// acceptLoop accepts incoming TCP connections and dispatches them.
func (s *SlaveServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			s.logger.Warn("accept failed", "err", err)
			continue
		}

		// Atomically check and claim the master slot to prevent race condition
		// where two concurrent connections could both see hasMaster=false.
		// wg.Add(1) is inside the lock to prevent a race with Stop()'s wg.Wait().
		s.masterMu.Lock()
		if s.masterClaimed {
			s.wg.Add(1)
			s.masterMu.Unlock()
			go s.runXshardConn(conn)
		} else {
			s.masterClaimed = true
			s.wg.Add(1)
			s.masterMu.Unlock()
			go s.runMasterConn(conn)
		}
	}
}

// runMasterConn sets up the first inbound connection as the MasterConn and
// shuts down the server when the master connection closes.
func (s *SlaveServer) runMasterConn(conn net.Conn) {
	defer s.wg.Done()

	mc := NewMasterConnFromConn(conn, s.cfg.MaxPayloadSize, s.cfg.ID, s.cfg.FullShardIDList, s.logger)
	mc.SetDispatcher(s.dispatcher)

	// Register handlers owned by SlaveServer runtime.
	// Connection-level handlers remain registered by MasterConn.
	mc.RegisterTypedHandlers(map[byte]TypedHandler{
		byte(wire.ClusterOpConnectToSlavesRequest): s.handleConnectToSlaves,
	})

	s.masterMu.Lock()
	s.master = mc
	s.masterMu.Unlock()

	mc.Start()
	s.logger.Info("master connection established", "remote", conn.RemoteAddr())

	<-mc.WaitUntilClosed()
	s.logger.Info("master connection closed")
	// Trigger shutdown asynchronously: Stop() waits on this goroutine via wg,
	// so calling it synchronously would deadlock.
	go s.Stop()
}

// runXshardConn accepts a subsequent inbound connection as a slave-to-slave
// xshard connection, tracks it in the pool and cleans it up on close.
func (s *SlaveServer) runXshardConn(conn net.Conn) {
	defer s.wg.Done()

	s.logger.Info("accepted xshard connection", "remote", conn.RemoteAddr())
	xc := NewXshardConnFromConn(conn, s.cfg.MaxPayloadSize, s.cfg.ID, s.cfg.FullShardIDList, s.logger)
	s.xshardPool.TrackInbound(xc)
	xc.Start()

	if !s.xshardPool.WatchAndIndex(xc) {
		s.logger.Warn("xshard connection closed before identity exchange", "remote", conn.RemoteAddr())
		xc.Close()
		s.xshardPool.RemoveInbound(xc)
		return
	}

	s.logger.Info("xshard connection indexed", "remote", conn.RemoteAddr(), "remote_id", string(xc.RemoteID()))
	<-xc.WaitUntilClosed()

	for _, shardID := range xc.RemoteFullShardIDList() {
		s.xshardPool.Remove(shardID, xc)
	}
}

// handleConnectToSlaves is the Master handler for CONNECT_TO_SLAVES_REQUEST.
// It dials every slave in the request (except itself and already-connected
// slaves), verifies identity via PING/PONG and indexes the connection.
func (s *SlaveServer) handleConnectToSlaves(req any) (any, error) {
	r := req.(*wire.ConnectToSlavesRequest)
	resultList := make([]wire.PrependedSizeBytes4, len(r.SlaveInfoList))

	for i, info := range r.SlaveInfoList {
		if string(info.ID) == string(s.cfg.ID) {
			continue // success: no need to connect to ourselves
		}
		if s.xshardPool.HasSlaveID(info.ID) {
			continue // success: already connected
		}

		addr := net.JoinHostPort(string(info.Host), strconv.Itoa(int(info.Port)))
		xc, err := NewXshardConn(addr, s.cfg.MaxPayloadSize, s.cfg.ID, s.cfg.FullShardIDList, s.logger)
		if err != nil {
			resultList[i] = wire.PrependedSizeBytes4([]byte(err.Error()))
			continue
		}
		xc.Start()

		if err := s.xshardPool.VerifyAndAddToShards(s.ctx, xc, info.ID, info.FullShardIDList); err != nil {
			resultList[i] = wire.PrependedSizeBytes4([]byte(err.Error()))
			continue
		}
	}

	return &wire.ConnectToSlavesResponse{ResultList: resultList}, nil
}
