package cluster

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

// XshardConn is a direct TCP connection to another slave node for xshard
// communication. Unlike MasterConn which carries multiplexed traffic
// (master commands + peer P2P), this is a dedicated physical connection
// solely for cross-shard transaction delivery.
//
// This is a separate physical connection from the master connection —
// xshard traffic does NOT go through master.
type XshardConn struct {
	conn       net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	remoteAddr string
	closed     bool
	closeMu    sync.Mutex

	handlersMu sync.RWMutex
	handlers   map[byte]func(*Frame) ([]byte, error)

	// RPC response matching (matches Python Connection.rpc_future_map)
	pendingMu sync.Mutex
	pending   map[uint64]chan *Frame // rpcID -> response channel
	nextRPCID uint64                 // atomic counter for RPC IDs

	errChan   chan error
	startOnce sync.Once // ensures readLoop is started at most once
	log       log.Logger
}

// NewXshardConn creates a new connection to another slave.
// Call Start() after registering handlers to begin the read loop.
func NewXshardConn(addr string, logger log.Logger) (*XshardConn, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial xshard slave: %w", err)
	}

	s := &XshardConn{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		remoteAddr: addr,
		handlers:   make(map[byte]func(*Frame) ([]byte, error)),
		pending:    make(map[uint64]chan *Frame),
		errChan:    make(chan error, 1),
		log:        logger,
	}

	return s, nil
}

// NewXshardConnFromConn creates an XshardConn from an existing connection (for accepting).
// Call Start() after registering handlers to begin the read loop.
func NewXshardConnFromConn(conn net.Conn, logger log.Logger) *XshardConn {
	s := &XshardConn{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		remoteAddr: conn.RemoteAddr().String(),
		handlers:   make(map[byte]func(*Frame) ([]byte, error)),
		pending:    make(map[uint64]chan *Frame),
		errChan:    make(chan error, 1),
		log:        logger,
	}

	return s
}

// Start begins the read loop. Call this after registering handlers.
// Safe to call multiple times — only the first call launches readLoop.
func (s *XshardConn) Start() {
	s.startOnce.Do(func() { go s.readLoop() })
}

func (s *XshardConn) readLoop() {
	defer s.Close()

	for {
		// SlaveConnection uses 0-byte Metadata (not ClusterMetadata).
		// Matches Python's SlaveConnection which inherits Connection with
		// metadata_class=Metadata (get_byte_size() == 0).
		frame, err := ReadFrameNoMeta(s.reader)
		if err != nil {
			select {
			case s.errChan <- err:
			default:
			}
			return
		}

		// Match Python handle_metadata_and_raw_data:
		// If RPCID != 0, check if it's a response to a pending RPC first.
		if frame.RPCID != 0 {
			s.pendingMu.Lock()
			if ch, ok := s.pending[frame.RPCID]; ok {
				delete(s.pending, frame.RPCID)
				s.pendingMu.Unlock()
				select {
				case ch <- frame:
				default:
					s.log.Warn("xshard response channel full, dropping frame", "rpcid", frame.RPCID)
				}
				continue
			}
			s.pendingMu.Unlock()
		}

		// Not a response — dispatch as request
		s.handlersMu.RLock()
		handler, ok := s.handlers[frame.Opcode]
		s.handlersMu.RUnlock()

		if !ok {
			s.log.Warn("no handler for xshard opcode", "opcode", frame.Opcode)
			s.sendEmptyResponse(frame, "no handler")
			continue
		}

		go func() {
			defer func() {
				if r := recover(); r != nil {
					s.log.Error("xshard handler panic recovered", "opcode", frame.Opcode, "panic", r)
					s.sendEmptyResponse(frame, "panic")
				}
			}()

			respPayload, err := handler(frame)
			if err != nil {
				s.log.Error("xshard handler failed", "opcode", frame.Opcode, "err", err)
				s.sendEmptyResponse(frame, "error")
				return
			}
			if frame.RPCID != 0 {
				resp := &Frame{
					Meta:    frame.Meta,
					Opcode:  frame.Opcode + 1, // response opcode = request opcode + 1
					RPCID:   frame.RPCID,
					Payload: respPayload,
				}
				if err := s.WriteFrame(resp); err != nil {
					s.log.Error("failed to send xshard response", "err", err)
				}
			}
		}()
	}
}

// RegisterHandler registers a handler for a specific xshard opcode.
func (s *XshardConn) RegisterHandler(opcode byte, handler func(*Frame) ([]byte, error)) {
	s.handlersMu.Lock()
	s.handlers[opcode] = handler
	s.handlersMu.Unlock()
}

// sendEmptyResponse sends an empty (nil-payload) response for error/no-handler/panic cases.
func (s *XshardConn) sendEmptyResponse(frame *Frame, reason string) {
	if frame.RPCID == 0 {
		return
	}
	resp := &Frame{
		Meta:    frame.Meta,
		Opcode:  frame.Opcode + 1,
		RPCID:   frame.RPCID,
		Payload: nil,
	}
	if err := s.WriteFrame(resp); err != nil {
		s.log.Error("failed to send response", "reason", reason, "err", err)
	}
}

// SendXshardTxList sends a list of xshard deposits to the target slave.
func (s *XshardConn) SendXshardTxList(branch uint32, payload []byte) error {
	return s.WriteFrame(&Frame{
		Meta:    Metadata{Branch: branch, ClusterPeerID: 0},
		Opcode:  OP_ADD_XSHARD_TX_LIST_REQUEST,
		RPCID:   0,
		Payload: payload,
	})
}

// SendBatchXshardTxList sends multiple xshard deposit lists in batch.
func (s *XshardConn) SendBatchXshardTxList(payload []byte) error {
	return s.WriteFrame(&Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  OP_BATCH_ADD_XSHARD_TX_LIST_REQUEST,
		RPCID:   0,
		Payload: payload,
	})
}

// SendRPC sends an RPC request and waits for the response.
// Matches Python's write_rpc_request.
func (s *XshardConn) SendRPC(ctx context.Context, opcode byte, payload []byte) (*Frame, error) {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil, ErrConnectionClosed
	}

	rpcID := atomic.AddUint64(&s.nextRPCID, 1)
	respChan := make(chan *Frame, 1)
	s.pendingMu.Lock()
	s.pending[rpcID] = respChan
	s.pendingMu.Unlock()
	s.closeMu.Unlock()

	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, rpcID)
		s.pendingMu.Unlock()
	}()

	frame := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  opcode,
		RPCID:   rpcID,
		Payload: payload,
	}

	if err := s.WriteFrame(frame); err != nil {
		return nil, fmt.Errorf("write frame: %w", err)
	}

	select {
	case resp := <-respChan:
		if resp == nil {
			return nil, ErrConnectionClosed
		}
		return resp, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("rpc timeout: %w", ctx.Err())
	}
}

// WriteFrame writes a frame to the connection using 0-byte Metadata
// (matches Python SlaveConnection which uses Metadata with get_byte_size()==0).
func (s *XshardConn) WriteFrame(frame *Frame) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	if s.closed {
		return ErrConnectionClosed
	}

	if err := WriteFrameNoMeta(s.writer, frame); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// Close closes the connection.
func (s *XshardConn) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	// Wake up any pending RPC callers
	s.pendingMu.Lock()
	for rpcID, ch := range s.pending {
		select {
		case ch <- nil:
		default:
		}
		delete(s.pending, rpcID)
	}
	s.pendingMu.Unlock()

	return s.conn.Close()
}

// RemoteAddr returns the remote address.
func (s *XshardConn) RemoteAddr() string { return s.remoteAddr }

// Error returns a channel that receives fatal connection errors.
func (s *XshardConn) Error() <-chan error { return s.errChan }
