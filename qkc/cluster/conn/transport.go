// Copyright 2026-2027, QuarkChain.

package conn

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

type frameTransport interface {
	readFrame() (*wire.Frame, error)
	writeFrame(*wire.Frame) error
	close() error
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

func (t *transport) readFrame() (*wire.Frame, error) {
	return t.readFrameFn(t.r)
}

func (t *transport) writeFrame(f *wire.Frame) error {
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

func (t *transport) close() error {
	return t.conn.Close()
}

func (t *transport) RemoteAddr() string {
	return t.remoteAddr
}
