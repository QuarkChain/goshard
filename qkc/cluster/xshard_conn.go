package cluster

import (
	"bufio"
	"fmt"
	"net"
	"sync"
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
		frame, err := ReadFrame(s.reader)
		if err != nil {
			select {
			case s.errChan <- err:
			default:
			}
			return
		}

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

// WriteFrame writes a frame to the connection.
func (s *XshardConn) WriteFrame(frame *Frame) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	if s.closed {
		return ErrConnectionClosed
	}

	if err := WriteFrame(s.writer, frame); err != nil {
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
	return s.conn.Close()
}

// RemoteAddr returns the remote address.
func (s *XshardConn) RemoteAddr() string { return s.remoteAddr }

// Error returns a channel that receives fatal connection errors.
func (s *XshardConn) Error() <-chan error { return s.errChan }
