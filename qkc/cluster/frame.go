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
	metaSize      = 12 // branch(4) + cluster_peer_id(8)
	opcodeSize    = 1
	rpcIDSize     = 8
	frameHeader   = 4                                               // payload_len prefix
	totalOverhead = frameHeader + metaSize + opcodeSize + rpcIDSize // 4+12+1+8=25
)

// ReadFrame reads a single complete frame with 12-byte ClusterMetadata from r.
// It returns the frame or an error if the stream is malformed.
//
// Used for master ↔ slave traffic (ClusterMetadata: branch + cluster_peer_id).
// For slave ↔ slave traffic that uses 0-byte Metadata, use ReadFrameNoMeta.
func ReadFrame(r io.Reader) (*Frame, error) {
	return ReadFrameWithMetaSize(r, metaSize)
}

// ReadFrameNoMeta reads a frame with 0-byte Metadata (slave ↔ slave traffic).
// Matches Python's SlaveConnection which uses Metadata (get_byte_size() == 0).
func ReadFrameNoMeta(r io.Reader) (*Frame, error) {
	return ReadFrameWithMetaSize(r, 0)
}

// ReadFrameWithMetaSize reads a frame with the given metadata size in bytes.
// The wire layout is:
//
//	[4B payload_len] [metaSize B metadata] [1B opcode] [8B rpc_id] [payload]
func ReadFrameWithMetaSize(r io.Reader, metaSize int) (*Frame, error) {
	// 1. Read 4-byte big-endian payload length
	var payloadLen uint32
	if err := binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
		return nil, fmt.Errorf("reading frame length: %w", err)
	}

	// 2. Read metadata (may be 0 bytes for slave-to-slave connections)
	var meta Metadata
	if metaSize > 0 {
		if metaSize != 12 {
			return nil, fmt.Errorf("unsupported metaSize %d (only 0 or 12 supported)", metaSize)
		}
		metaBuf := make([]byte, metaSize)
		if _, err := io.ReadFull(r, metaBuf); err != nil {
			return nil, fmt.Errorf("reading metadata: %w", err)
		}
		meta = Metadata{
			Branch:        binary.BigEndian.Uint32(metaBuf[0:4]),
			ClusterPeerID: binary.BigEndian.Uint64(metaBuf[4:12]),
		}
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

// WriteFrame serializes f with 12-byte ClusterMetadata and writes it to w.
//
// Used for master ↔ slave traffic.  For slave ↔ slave traffic that uses
// 0-byte Metadata, use WriteFrameNoMeta.
func WriteFrame(w io.Writer, f *Frame) error {
	return WriteFrameWithMetaSize(w, f, metaSize)
}

// WriteFrameNoMeta writes a frame with 0-byte Metadata (slave ↔ slave traffic).
// Matches Python's SlaveConnection which uses Metadata (get_byte_size() == 0).
func WriteFrameNoMeta(w io.Writer, f *Frame) error {
	return WriteFrameWithMetaSize(w, f, 0)
}

// WriteFrameWithMetaSize serializes f with the given metadata size and writes
// it to w.  metaSize must match what the peer expects (12 for cluster RPC, 0
// for direct slave-to-slave).
func WriteFrameWithMetaSize(w io.Writer, f *Frame, metaSize int) error {
	payloadLen := uint32(len(f.Payload))
	if int(payloadLen) != len(f.Payload) {
		return errors.New("payload too large")
	}

	// Build the buffer to write in one shot.
	// Layout: [4B payloadLen] [metaSize B Metadata] [1B opcode] [8B rpcID] [payload]
	total := frameHeader + metaSize + opcodeSize + rpcIDSize + int(payloadLen)
	buf := make([]byte, total)

	// Frame length (payload only, matches Python: len(raw_data) - 8 - 1)
	binary.BigEndian.PutUint32(buf[0:frameHeader], payloadLen)

	// Metadata (only if metaSize > 0)
	if metaSize > 0 {
		binary.BigEndian.PutUint32(buf[frameHeader:frameHeader+4], f.Meta.Branch)
		binary.BigEndian.PutUint64(buf[frameHeader+4:frameHeader+metaSize], f.Meta.ClusterPeerID)
	}

	// Opcode
	buf[frameHeader+metaSize] = f.Opcode

	// RPC ID
	binary.BigEndian.PutUint64(buf[frameHeader+metaSize+opcodeSize:frameHeader+metaSize+opcodeSize+rpcIDSize], f.RPCID)

	// Payload
	copy(buf[frameHeader+metaSize+opcodeSize+rpcIDSize:], f.Payload)

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

// ReadFrameNoMetaFromReader is a convenience wrapper for ReadFrameNoMeta.
// Used in tests that simulate a Python SlaveConnection peer.
func ReadFrameNoMetaFromReader(r io.Reader) (*Frame, error) {
	return ReadFrameNoMeta(bufio.NewReader(r))
}

// WriteFrameNoMetaToWriter is a convenience wrapper for WriteFrameNoMeta.
// Used in tests that simulate a Python SlaveConnection peer.
func WriteFrameNoMetaToWriter(w io.Writer, frame *Frame) error {
	bw := bufio.NewWriter(w)
	if err := WriteFrameNoMeta(bw, frame); err != nil {
		return err
	}
	return bw.Flush()
}
