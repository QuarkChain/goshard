package cluster

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

// MasterConn is the single TCP connection to the Python master.
// It handles frame encoding/decoding, RPC request/response matching, and
// dispatches inbound frames to registered handlers.
//
// For peer-shard P2P traffic (cluster_peer_id != 0), the OnFrame callback
// routes frames through the Dispatcher to the appropriate PeerConn.
type MasterConn struct {
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	closed  bool
	closeMu sync.Mutex

	// RPC response matching
	pendingMu sync.Mutex
	pending   map[uint64]chan *Frame // rpcID -> response channel

	// Handlers for inbound cluster RPCs (cluster_peer_id == 0)
	handlersMu sync.RWMutex
	handlers   map[byte]func(*Frame) ([]byte, error)

	// Error channel for fatal connection errors
	errChan chan error

	// OnFrame is set by the caller to intercept inbound frames before they
	// reach handlers. This is used to route frames through the Dispatcher:
	//   cluster_peer_id == 0 → MasterConn.handlers
	//   cluster_peer_id != 0 → PeerConn
	OnFrame func(*Frame)

	log log.Logger
}

// NewMasterConn creates a new connection to the master.
func NewMasterConn(addr string, logger log.Logger) (*MasterConn, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial master: %w", err)
	}

	m := &MasterConn{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
		pending:  make(map[uint64]chan *Frame),
		handlers: make(map[byte]func(*Frame) ([]byte, error)),
		errChan:  make(chan error, 1),
		log:      logger,
	}

	go m.readLoop()
	return m, nil
}

// readLoop continuously reads frames from the master connection.
func (m *MasterConn) readLoop() {
	defer m.Close()

	for {
		frame, err := ReadFrame(m.reader)
		if err != nil {
			select {
			case m.errChan <- err:
			default:
			}
			return
		}

		if m.OnFrame != nil {
			m.OnFrame(frame)
		} else {
			m.Handle(frame)
		}
	}
}

// Handle processes an inbound frame as a cluster RPC (cluster_peer_id == 0).
// It is called by the Dispatcher for cluster RPC frames.
func (m *MasterConn) Handle(frame *Frame) {
	// Check if this is a response to a pending RPC
	if frame.RPCID != 0 {
		m.pendingMu.Lock()
		if ch, ok := m.pending[frame.RPCID]; ok {
			delete(m.pending, frame.RPCID)
			m.pendingMu.Unlock()
			select {
			case ch <- frame:
			default:
				m.log.Warn("response channel full, dropping frame", "rpcid", frame.RPCID)
			}
			return
		}
		m.pendingMu.Unlock()
	}

	// It's a request - dispatch to handler
	m.handlersMu.RLock()
	handler, ok := m.handlers[frame.Opcode]
	m.handlersMu.RUnlock()

	if !ok {
		m.log.Warn("no handler for opcode", "opcode", frame.Opcode)
		return
	}

	go func() {
		respPayload, err := handler(frame)
		if err != nil {
			m.log.Error("handler failed", "opcode", frame.Opcode, "err", err)
			return
		}
		if frame.RPCID != 0 && respPayload != nil {
			resp := &Frame{
				Meta:    frame.Meta,
				Opcode:  frame.Opcode,
				RPCID:   frame.RPCID,
				Payload: respPayload,
			}
			if err := m.WriteFrame(resp); err != nil {
				m.log.Error("failed to send response", "opcode", frame.Opcode, "err", err)
			}
		}
	}()
}

// RegisterHandler registers a handler for a specific opcode.
func (m *MasterConn) RegisterHandler(opcode byte, handler func(*Frame) ([]byte, error)) {
	m.handlersMu.Lock()
	m.handlers[opcode] = handler
	m.handlersMu.Unlock()
}

// SendRPC sends an RPC request to the master and waits for a response.
func (m *MasterConn) SendRPC(ctx context.Context, opcode byte, payload []byte) (*Frame, error) {
	m.closeMu.Lock()
	if m.closed {
		m.closeMu.Unlock()
		return nil, fmt.Errorf("connection closed")
	}

	rpcID := uint64(time.Now().UnixNano())
	respChan := make(chan *Frame, 1)
	m.pendingMu.Lock()
	m.pending[rpcID] = respChan
	m.pendingMu.Unlock()
	m.closeMu.Unlock()

	defer func() {
		m.pendingMu.Lock()
		delete(m.pending, rpcID)
		m.pendingMu.Unlock()
	}()

	frame := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  opcode,
		RPCID:   rpcID,
		Payload: payload,
	}

	if err := m.WriteFrame(frame); err != nil {
		return nil, fmt.Errorf("write frame: %w", err)
	}

	select {
	case resp := <-respChan:
		return resp, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("rpc timeout: %w", ctx.Err())
	}
}

// SendCommand sends a fire-and-forget command (no response expected).
func (m *MasterConn) SendCommand(opcode byte, payload []byte) error {
	frame := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  opcode,
		RPCID:   0,
		Payload: payload,
	}
	return m.WriteFrame(frame)
}

// WriteFrame writes a frame to the connection.
func (m *MasterConn) WriteFrame(frame *Frame) error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()

	if m.closed {
		return ErrConnectionClosed
	}

	if err := WriteFrame(m.writer, frame); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	if err := m.writer.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// Close closes the connection.
func (m *MasterConn) Close() error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	err := m.conn.Close()

	m.pendingMu.Lock()
	for _, ch := range m.pending {
		close(ch)
	}
	m.pending = nil
	m.pendingMu.Unlock()

	return err
}

// Error returns a channel that receives fatal connection errors.
func (m *MasterConn) Error() <-chan error {
	return m.errChan
}

// RemoteAddr returns the remote address of the connection.
func (m *MasterConn) RemoteAddr() net.Addr {
	return m.conn.RemoteAddr()
}
