// Copyright 2026-2027, QuarkChain.

package conn

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

// FrameTransport is the frame I/O contract required by BaseConn.
type FrameTransport interface {
	ReadFrame() (*wire.Frame, error)
	WriteFrame(*wire.Frame) error
	Close() error
	RemoteAddr() string
}

// interruptibleTransport can interrupt a blocked read or write. TCP transport
// implements this separately from close so BaseConn can wait for the writer
// before completing the transport shutdown.
type interruptibleTransport interface {
	interrupt() error
}

type transport struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	readFrameFn  func(io.Reader) (*wire.Frame, error)
	writeFrameFn func(io.Writer, *wire.Frame) error

	remoteAddr string
}

// NewTCPTransport creates a FrameTransport backed by a real TCP connection.
// The readFrame and writeFrame functions define the wire codec (e.g.
// wire.ReadFrame/wire.WriteFrame for master connections with 12-byte
// metadata, or wire.ReadFrameNoMeta/wire.WriteFrameNoMeta for slave-slave
// connections with 0-byte metadata).
//
// The returned transport implements interruptibleTransport, so BaseConn can
// interrupt blocked reads during shutdown without closing the underlying
// socket (the socket is closed separately in the shutdown barrier).
func NewTCPTransport(
	conn net.Conn,
	readFrame func(io.Reader) (*wire.Frame, error),
	writeFrame func(io.Writer, *wire.Frame) error,
) FrameTransport {
	return newTransport(conn, readFrame, writeFrame)
}

func newTransport(
	conn net.Conn,
	readFrame func(io.Reader) (*wire.Frame, error),
	writeFrame func(io.Writer, *wire.Frame) error,
) *transport {
	return &transport{
		conn:         conn,
		r:            bufio.NewReader(conn),
		w:            bufio.NewWriter(conn),
		readFrameFn:  readFrame,
		writeFrameFn: writeFrame,
		remoteAddr:   conn.RemoteAddr().String(),
	}
}

func (t *transport) ReadFrame() (*wire.Frame, error) {
	return t.readFrameFn(t.r)
}

func (t *transport) WriteFrame(f *wire.Frame) error {
	if err := t.writeFrameFn(t.w, f); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	if err := t.w.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

func (t *transport) interrupt() error {
	return t.conn.Close()
}

func (t *transport) Close() error {
	return t.conn.Close()
}

func (t *transport) RemoteAddr() string {
	return t.remoteAddr
}
