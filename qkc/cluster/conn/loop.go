// Copyright 2026-2027, QuarkChain.

package conn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/qkc/cluster/wire"
)

type rpcResult struct {
	frame *wire.Frame
	err   error
}

type pendingRPC struct {
	result chan rpcResult
	stop   func() bool
}

type connEvent interface{ isConnEvent() }

type startEvent struct{}

func (startEvent) isConnEvent() {}

type outboundRPCEvent struct {
	ctx     context.Context
	opcode  byte
	payload []byte
	meta    wire.ClusterMetadata
	call    *pendingRPC
}

func (outboundRPCEvent) isConnEvent() {}

type cancelRPCEvent struct {
	rpcID uint64
	err   error
}

func (cancelRPCEvent) isConnEvent() {}

type expireTimedOutEvent struct {
	rpcID uint64
}

func (expireTimedOutEvent) isConnEvent() {}

type frameReceivedEvent struct {
	frame *wire.Frame
}

func (frameReceivedEvent) isConnEvent() {}

type readFailedEvent struct {
	err error
}

func (readFailedEvent) isConnEvent() {}

type writeFailedEvent struct {
	err error
}

func (writeFailedEvent) isConnEvent() {}

type handlerCompletedEvent struct {
	frame    *wire.Frame
	response *wire.Frame
	err      error
}

func (handlerCompletedEvent) isConnEvent() {}

type readerStoppedEvent struct{}

func (readerStoppedEvent) isConnEvent() {}

type writerStoppedEvent struct{}

func (writerStoppedEvent) isConnEvent() {}

type closeRequestedEvent struct {
	err error
}

func (closeRequestedEvent) isConnEvent() {}

// frameMailbox is an unbounded owner-to-writer queue. Its mutex protects only
// the queue; BaseConn protocol state remains owned by the owner goroutine.
type frameMailbox struct {
	mu     sync.Mutex
	frames []queuedFrame
	wake   chan struct{}
	closed bool
}

type queuedFrame struct {
	frame *wire.Frame
	rpcID uint64
}

func newFrameMailbox() *frameMailbox {
	return &frameMailbox{wake: make(chan struct{}, 1)}
}

func (m *frameMailbox) enqueue(frame *wire.Frame, rpcID uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	m.frames = append(m.frames, queuedFrame{frame: frame, rpcID: rpcID})
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return true
}

func (m *frameMailbox) next() (*queuedFrame, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.frames) > 0 {
		frame := &m.frames[0]
		m.frames = m.frames[1:]
		return frame, true
	}
	return nil, !m.closed
}

func (m *frameMailbox) removeRPC(rpcID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.frames[:0]
	for _, queued := range m.frames {
		if queued.rpcID == rpcID {
			continue
		}
		kept = append(kept, queued)
	}
	clear(m.frames[len(kept):])
	m.frames = kept
}

func (m *frameMailbox) close() {
	m.mu.Lock()
	m.closed = true
	m.frames = nil
	m.mu.Unlock()
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (c *BaseConn) ensureOwner() {
	c.ownerOnce.Do(func() {
		go c.ownerLoop()
	})
}

func (c *BaseConn) submitEvent(event connEvent) bool {
	c.submitMu.Lock()
	defer c.submitMu.Unlock()
	if c.finished {
		return false
	}
	select {
	case c.events <- event:
		return true
	case <-c.done:
		return false
	}
}

func (c *BaseConn) defaultValidateRPCID(clusterPeerID uint64, rpcID uint64) bool {
	if int64(rpcID) <= c.peerRPCID {
		return false
	}
	c.peerRPCID = int64(rpcID)
	return true
}

func (c *BaseConn) ownerLoop() {
	for {
		event := <-c.events
		switch event := event.(type) {
		case startEvent:
			c.handleStart()
		case outboundRPCEvent:
			c.handleOutboundRPC(event)
		case cancelRPCEvent:
			c.handleCancelRPC(event)
		case expireTimedOutEvent:
			c.expireTimedOut(event.rpcID)
		case frameReceivedEvent:
			c.handleFrame(event.frame)
		case readFailedEvent:
			c.handleReadFailed(event.err)
		case writeFailedEvent:
			c.handleWriteFailed(event.err)
		case handlerCompletedEvent:
			c.handleHandlerCompleted(event)
		case readerStoppedEvent:
			c.readerStopped = true
			c.finishShutdownIfReady()
		case writerStoppedEvent:
			c.writerStopped = true
			c.closeTransport()
			c.finishShutdownIfReady()
		case closeRequestedEvent:
			c.beginShutdown(event.err)
		}
		if c.shuttingDown && c.readerStopped && c.writerStopped {
			c.finishOwner()
			return
		}
	}
}

func (c *BaseConn) finishOwner() {
	c.submitMu.Lock()
	defer c.submitMu.Unlock()
	if c.finished {
		return
	}
	c.finished = true
	for {
		select {
		case event := <-c.events:
			if outbound, ok := event.(outboundRPCEvent); ok {
				outbound.call.result <- rpcResult{err: ErrConnectionClosed}
			}
		default:
			close(c.done)
			close(c.shutdownDone)
			return
		}
	}
}

func (c *BaseConn) handleStart() {
	if c.state == ConnectionStateClosed {
		return
	}
	c.started = true
	c.state = ConnectionStateActive
	c.stateSnapshot.Store(int32(ConnectionStateActive))
	close(c.activeChan)
	go c.readerLoop()
	go c.writerLoop()
}

func (c *BaseConn) handleOutboundRPC(event outboundRPCEvent) {
	if c.state == ConnectionStateClosed {
		event.call.result <- rpcResult{err: ErrConnectionClosed}
		return
	}
	if c.state != ConnectionStateActive {
		event.call.result <- rpcResult{err: ErrNotActive}
		return
	}
	if err := event.ctx.Err(); err != nil {
		event.call.result <- rpcResult{err: rpcTimeoutError(err)}
		return
	}

	c.nextRPCID++
	rpcID := c.nextRPCID
	event.call.stop = context.AfterFunc(event.ctx, func() {
		c.submitEvent(cancelRPCEvent{rpcID: rpcID, err: event.ctx.Err()})
	})
	c.pending[rpcID] = event.call
	c.pendingCount.Add(1)

	frame := &wire.Frame{
		Meta:    event.meta,
		Opcode:  event.opcode,
		RPCID:   rpcID,
		Payload: event.payload,
	}
	if !c.writer.enqueue(frame, rpcID) {
		delete(c.pending, rpcID)
		c.pendingCount.Add(-1)
		event.call.stop()
		event.call.result <- rpcResult{err: ErrConnectionClosed}
	}
}

func (c *BaseConn) handleCancelRPC(event cancelRPCEvent) {
	call, ok := c.pending[event.rpcID]
	if !ok {
		return
	}
	delete(c.pending, event.rpcID)
	c.pendingCount.Add(-1)
	c.writer.removeRPC(event.rpcID)
	c.timedOut[event.rpcID] = time.AfterFunc(lateResponseGracePeriod, func() {
		c.submitEvent(expireTimedOutEvent{rpcID: event.rpcID})
	})
	if call.stop != nil {
		call.stop()
	}
	call.result <- rpcResult{err: rpcTimeoutError(event.err)}
}

var lateResponseGracePeriod = time.Minute

func (c *BaseConn) expireTimedOut(rpcID uint64) {
	delete(c.timedOut, rpcID)
}

func (c *BaseConn) handleFrame(frame *wire.Frame) {
	c.configMu.RLock()
	forwarder := c.forwarder
	handler, isRequest := c.typedHandlers[frame.Opcode]
	_, isNonRPC := c.nonRPCOps[frame.Opcode]
	ser := c.serializers[frame.Opcode]
	c.configMu.RUnlock()
	if forwarder != nil && forwarder(frame) {
		return
	}

	if !isRequest {
		// Response path: opcode lookup -> deserialize -> rpc_id matching -> deliver.
		// An unknown opcode or malformed payload closes the connection regardless
		// of rpc_id. The serializers map covers both request and response opcodes,
		// so a single lookup by frame.Opcode handles both directions.
		if ser == nil {
			c.log.Warn("unknown response opcode", "opcode", frame.Opcode)
			c.beginShutdown(fmt.Errorf("unknown response opcode 0x%x", frame.Opcode))
			return
		}
		resp := ser.NewResponse()
		if err := ser.Deserialize(frame.Payload, resp); err != nil {
			c.log.Warn("malformed response payload", "opcode", frame.Opcode, "err", err)
			c.beginShutdown(fmt.Errorf("malformed response payload for opcode 0x%x: %w", frame.Opcode, err))
			return
		}
		call, ok := c.pending[frame.RPCID]
		if !ok {
			if timer, timedOut := c.timedOut[frame.RPCID]; timedOut {
				delete(c.timedOut, frame.RPCID)
				timer.Stop()
				c.log.Debug("ignoring late rpc response", "rpcid", frame.RPCID)
				return
			}
			c.log.Error("unexpected rpc response", "rpcid", frame.RPCID, "opcode", frame.Opcode)
			c.beginShutdown(fmt.Errorf("unexpected rpc response %d", frame.RPCID))
			return
		}
		delete(c.pending, frame.RPCID)
		c.pendingCount.Add(-1)
		if call.stop != nil {
			call.stop()
		}
		call.result <- rpcResult{frame: frame}
		return
	}

	if ser == nil {
		c.log.Warn("handler without serializer", "opcode", frame.Opcode)
		c.beginShutdown(fmt.Errorf("handler without serializer for opcode 0x%x", frame.Opcode))
		return
	}
	if isNonRPC && frame.RPCID != 0 {
		c.log.Warn("non-rpc command with non-zero rpc_id", "opcode", frame.Opcode, "rpcid", frame.RPCID)
		c.beginShutdown(fmt.Errorf("non-rpc command with rpc id %d", frame.RPCID))
		return
	}
	if !isNonRPC && !c.validateRPCID(frame.Meta.ClusterPeerID, frame.RPCID) {
		c.log.Warn("incorrect rpc request id sequence", "rpcid", frame.RPCID)
		c.beginShutdown(fmt.Errorf("incorrect rpc request id sequence"))
		return
	}
	go c.dispatch(frame, handler, ser)
}

func (c *BaseConn) dispatch(frame *wire.Frame, handler TypedHandler, ser *OpSerializer) {
	var result handlerCompletedEvent
	result.frame = frame
	defer func() {
		if recovered := recover(); recovered != nil {
			result.err = fmt.Errorf("handler panic: %v", recovered)
		}
		c.submitEvent(result)
	}()

	req := ser.NewRequest()
	if err := ser.Deserialize(frame.Payload, req); err != nil {
		result.err = fmt.Errorf("deserialize failed: %w", err)
		return
	}
	resp, err := handler(req)
	if err != nil {
		result.err = err
		return
	}
	if frame.RPCID == 0 {
		return
	}
	respPayload, err := ser.Serialize(resp)
	if err != nil {
		result.err = fmt.Errorf("serialize response failed: %w", err)
		return
	}
	result.response = &wire.Frame{
		Meta:    frame.Meta,
		Opcode:  ser.ResponseOpCode,
		RPCID:   frame.RPCID,
		Payload: respPayload,
	}
}

func (c *BaseConn) handleHandlerCompleted(event handlerCompletedEvent) {
	if event.err != nil {
		c.log.Error("request handler failed", "opcode", event.frame.Opcode, "err", event.err)
		c.beginShutdown(event.err)
		return
	}
	if event.response != nil && c.state == ConnectionStateActive {
		if !c.writer.enqueue(event.response, 0) {
			c.beginShutdown(ErrConnectionClosed)
		}
	}
}

func (c *BaseConn) handleReadFailed(err error) {
	c.beginShutdown(err)
}

func (c *BaseConn) handleWriteFailed(err error) {
	c.beginShutdown(err)
}

func (c *BaseConn) beginShutdown(err error) {
	if c.shuttingDown {
		return
	}
	c.shuttingDown = true
	c.state = ConnectionStateClosed
	c.stateSnapshot.Store(int32(ConnectionStateClosed))
	close(c.closedChan)
	select {
	case <-c.activeChan:
	default:
		close(c.activeChan)
	}
	for rpcID, call := range c.pending {
		delete(c.pending, rpcID)
		c.pendingCount.Add(-1)
		if call.stop != nil {
			call.stop()
		}
		call.result <- rpcResult{err: ErrConnectionClosed}
	}
	for rpcID, timer := range c.timedOut {
		delete(c.timedOut, rpcID)
		timer.Stop()
	}
	if !c.started {
		c.readerStopped = true
		c.writerStopped = true
		c.closeTransport()
		return
	}
	c.writer.close()
	if interrupter, ok := c.FrameTransport.(interruptibleTransport); ok {
		_ = interrupter.interrupt()
	}
	if err != nil {
		select {
		case c.errChan <- err:
		default:
		}
	}
	if c.state == ConnectionStateClosed && c.readerStopped && c.writerStopped {
		c.closeTransport()
	}
}

func (c *BaseConn) closeTransport() {
	if c.transportClosed {
		return
	}
	c.transportClosed = true
	if err := c.FrameTransport.Close(); err != nil && !errors.Is(err, net.ErrClosed) && c.closeErr == nil {
		c.closeErr = err
	}
}

func (c *BaseConn) finishShutdownIfReady() {
	if c.shuttingDown && c.readerStopped && c.writerStopped {
		c.closeTransport()
	}
}

func (c *BaseConn) readerLoop() {
	defer c.submitEvent(readerStoppedEvent{})
	for {
		frame, err := c.FrameTransport.ReadFrame()
		if err != nil {
			c.submitEvent(readFailedEvent{err: err})
			return
		}
		select {
		case c.events <- frameReceivedEvent{frame: frame}:
		case <-c.done:
			return
		}
	}
}

func (c *BaseConn) writerLoop() {
	defer c.submitEvent(writerStoppedEvent{})
	for {
		queued, available := c.writer.next()
		if queued != nil {
			if err := c.FrameTransport.WriteFrame(queued.frame); err != nil {
				c.submitEvent(writeFailedEvent{err: err})
				return
			}
			continue
		}
		if !available {
			return
		}
		select {
		case <-c.writer.wake:
		case <-c.done:
			return
		}
	}
}

func rpcTimeoutError(err error) error {
	return fmt.Errorf("rpc timeout: %w", err)
}

func (c *BaseConn) pendingLen() int {
	return int(c.pendingCount.Load())
}
