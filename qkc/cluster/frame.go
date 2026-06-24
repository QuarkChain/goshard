// Package cluster implements the pyquarkchain-compatible cluster protocol wire
// library: frame codec, TCP connection management, and VirtualConnection multiplexing.
//
// Wire format (per-frame, matching pyquarkchain protocol.py):
//
//	[4B payload_len] [12B Metadata] [1B opcode] [8B rpc_id] [payload_len bytes]
//
// Metadata layout:
//
//	[4B branch (uint32 BE)] [8B cluster_peer_id (uint64 BE)]
//
// cluster_peer_id == 0  →  cluster RPC (master commands)
// cluster_peer_id != 0  →  peer-shard P2P traffic
package cluster

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Metadata is the 12-byte frame header that carries routing information.
// It matches pyquarkchain's ClusterMetadata wire format.
type Metadata struct {
	Branch        uint32 // shard identifier
	ClusterPeerID uint64 // 0 = master/cluster RPC, ≠0 = specific external peer
}

// Frame is a complete protocol frame received from or sent to the wire.
type Frame struct {
	Meta    Metadata
	Opcode  byte
	RPCID   uint64
	Payload []byte
}

const (
	metaSize       = 12 // branch(4) + cluster_peer_id(8)
	opcodeSize     = 1
	rpcIDSize      = 8
	frameHeader    = 4                                               // payload_len prefix
	totalOverhead  = frameHeader + metaSize + opcodeSize + rpcIDSize // 4+12+1+8=25
	maxPayloadSize = 16 << 20                                        // 16 MiB - maximum allowed payload size
)

// ReadFrame reads a single complete frame from r.
// It returns the frame or an error if the stream is malformed.
func ReadFrame(r io.Reader) (*Frame, error) {
	// 1. Read 4-byte big-endian payload length
	var payloadLen uint32
	if err := binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
		return nil, fmt.Errorf("reading frame length: %w", err)
	}

	// Validate payload size to prevent memory exhaustion
	if payloadLen > maxPayloadSize {
		return nil, fmt.Errorf("frame too large: %d bytes (max %d)", payloadLen, maxPayloadSize)
	}

	// 2. Read 12-byte Metadata
	var metaBuf [metaSize]byte
	if _, err := io.ReadFull(r, metaBuf[:]); err != nil {
		return nil, fmt.Errorf("reading metadata: %w", err)
	}
	meta := Metadata{
		Branch:        binary.BigEndian.Uint32(metaBuf[0:4]),
		ClusterPeerID: binary.BigEndian.Uint64(metaBuf[4:12]),
	}

	// 3. Read opcode + rpc_id + payload
	bodySize := opcodeSize + rpcIDSize + int(payloadLen)
	body := make([]byte, bodySize)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("reading frame body (payload_len=%d): %w", payloadLen, err)
	}

	return &Frame{
		Meta:    meta,
		Opcode:  body[0],
		RPCID:   binary.BigEndian.Uint64(body[1:9]),
		Payload: body[9:],
	}, nil
}

// WriteFrame serializes f and writes it to w.
func WriteFrame(w io.Writer, f *Frame) error {
	payloadLen := uint32(len(f.Payload))
	if int(payloadLen) != len(f.Payload) {
		return errors.New("payload too large")
	}

	// Build the buffer to write in one shot.
	// Layout: [4B payloadLen] [12B Metadata] [1B opcode] [8B rpcID] [payload]
	total := frameHeader + metaSize + opcodeSize + rpcIDSize + int(payloadLen)
	buf := make([]byte, total)

	// Frame length (payload only, matches Python: len(raw_data) - 8 - 1)
	binary.BigEndian.PutUint32(buf[0:4], payloadLen)

	// Metadata
	binary.BigEndian.PutUint32(buf[4:8], f.Meta.Branch)
	binary.BigEndian.PutUint64(buf[8:16], f.Meta.ClusterPeerID)

	// Opcode
	buf[16] = f.Opcode

	// RPC ID
	binary.BigEndian.PutUint64(buf[17:25], f.RPCID)

	// Payload
	copy(buf[25:], f.Payload)

	_, err := w.Write(buf)
	return err
}

// MarshalMetadata serializes Metadata into its 12-byte wire representation.
func MarshalMetadata(m Metadata) []byte {
	buf := make([]byte, metaSize)
	binary.BigEndian.PutUint32(buf[0:4], m.Branch)
	binary.BigEndian.PutUint64(buf[4:12], m.ClusterPeerID)
	return buf
}

// UnmarshalMetadata deserializes a 12-byte wire representation into Metadata.
func UnmarshalMetadata(b []byte) (Metadata, error) {
	if len(b) != metaSize {
		return Metadata{}, fmt.Errorf("metadata must be %d bytes, got %d", metaSize, len(b))
	}
	return Metadata{
		Branch:        binary.BigEndian.Uint32(b[0:4]),
		ClusterPeerID: binary.BigEndian.Uint64(b[4:12]),
	}, nil
}

// ReadFrameFromReader is a convenience wrapper that wraps r in a bufio.Reader.
func ReadFrameFromReader(r io.Reader) (*Frame, error) {
	return ReadFrame(bufio.NewReader(r))
}

// WriteFrameToWriter is a convenience wrapper that wraps w with a bufio.Writer and flushes.
func WriteFrameToWriter(w io.Writer, frame *Frame) error {
	bw := bufio.NewWriter(w)
	if err := WriteFrame(bw, frame); err != nil {
		return err
	}
	return bw.Flush()
}
