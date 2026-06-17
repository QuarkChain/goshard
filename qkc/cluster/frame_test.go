package cluster

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

// TestRoundTrip verifies that a frame survives WriteFrame → ReadFrame unchanged.
func TestRoundTrip(t *testing.T) {
	original := &Frame{
		Meta:    Metadata{Branch: 3, ClusterPeerID: 0},
		Opcode:  0x01, // PING
		RPCID:   42,
		Payload: []byte("hello"),
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, original); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if got.Meta.Branch != original.Meta.Branch {
		t.Errorf("Branch: got %d, want %d", got.Meta.Branch, original.Meta.Branch)
	}
	if got.Meta.ClusterPeerID != original.Meta.ClusterPeerID {
		t.Errorf("ClusterPeerID: got %d, want %d", got.Meta.ClusterPeerID, original.Meta.ClusterPeerID)
	}
	if got.Opcode != original.Opcode {
		t.Errorf("Opcode: got %d, want %d", got.Opcode, original.Opcode)
	}
	if got.RPCID != original.RPCID {
		t.Errorf("RPCID: got %d, want %d", got.RPCID, original.RPCID)
	}
	if !bytes.Equal(got.Payload, original.Payload) {
		t.Errorf("Payload: got %v, want %v", got.Payload, original.Payload)
	}
}

// TestWireFormat verifies the exact byte layout matches the Python protocol.
// Python (protocol.py):
//
//	cmd_length_bytes = (len(raw_data) - 8 - 1).to_bytes(4, "big")  → 4B payload_len
//	writer.write(cmd_length_bytes)
//	writer.write(metadata.serialize())   → 12B (branch 4B + cluster_peer_id 8B)
//	writer.write(raw_data)               → 1B opcode + 8B rpc_id + payload
func TestWireFormat(t *testing.T) {
	frame := &Frame{
		Meta:    Metadata{Branch: 1, ClusterPeerID: 0x1122334455667788},
		Opcode:  0x42,
		RPCID:   0xDEADBEEFCAFEBABE,
		Payload: []byte{0xAA, 0xBB, 0xCC},
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, frame); err != nil {
		t.Fatal(err)
	}
	wire := buf.Bytes()

	// Expected layout:
	// [0:4]   payload_len = 3 (len(Payload))
	// [4:8]   branch = 1
	// [8:16]  cluster_peer_id = 0x1122334455667788
	// [16]    opcode = 0x42
	// [17:25] rpc_id = 0xDEADBEEFCAFEBABE
	// [25:28] payload = 0xAA, 0xBB, 0xCC
	expectedLen := 4 + 12 + 1 + 8 + 3 // = 28
	if len(wire) != expectedLen {
		t.Fatalf("wire length: got %d, want %d", len(wire), expectedLen)
	}

	// Verify payload_len
	if pl := binary.BigEndian.Uint32(wire[0:4]); pl != 3 {
		t.Errorf("payload_len: got %d, want 3", pl)
	}

	// Verify branch
	if br := binary.BigEndian.Uint32(wire[4:8]); br != 1 {
		t.Errorf("branch: got %d, want 1", br)
	}

	// Verify cluster_peer_id
	if cpid := binary.BigEndian.Uint64(wire[8:16]); cpid != 0x1122334455667788 {
		t.Errorf("cluster_peer_id: got 0x%x, want 0x1122334455667788", cpid)
	}

	// Verify opcode
	if wire[16] != 0x42 {
		t.Errorf("opcode: got 0x%x, want 0x42", wire[16])
	}

	// Verify rpc_id
	if rid := binary.BigEndian.Uint64(wire[17:25]); rid != 0xDEADBEEFCAFEBABE {
		t.Errorf("rpc_id: got 0x%x, want 0xDEADBEEFCAFEBABE", rid)
	}

	// Verify payload
	if !bytes.Equal(wire[25:28], []byte{0xAA, 0xBB, 0xCC}) {
		t.Errorf("payload: got %x, want aabbcc", wire[25:28])
	}

	// Now read it back and verify
	got, err := ReadFrame(bytes.NewReader(wire))
	if err != nil {
		t.Fatalf("ReadFrame from known wire: %v", err)
	}
	if got.Opcode != 0x42 || got.RPCID != 0xDEADBEEFCAFEBABE {
		t.Errorf("round-trip mismatch: opcode=%x rpc_id=%x", got.Opcode, got.RPCID)
	}
	if !bytes.Equal(got.Payload, []byte{0xAA, 0xBB, 0xCC}) {
		t.Errorf("round-trip payload mismatch: %x", got.Payload)
	}
}

// TestReadFrameAfterWriteFrame verifies multiple frames in a stream.
// In production, frames arrive back-to-back on the same TCP connection.
func TestReadFrameAfterWriteFrame(t *testing.T) {
	frames := []*Frame{
		{Meta: Metadata{Branch: 0, ClusterPeerID: 0}, Opcode: 1, RPCID: 0, Payload: []byte("ping")},
		{Meta: Metadata{Branch: 2, ClusterPeerID: 999}, Opcode: 5, RPCID: 100, Payload: []byte("block_data")},
		{Meta: Metadata{Branch: 1, ClusterPeerID: 0}, Opcode: 3, RPCID: 200, Payload: []byte{}},
	}

	var buf bytes.Buffer
	for _, f := range frames {
		if err := WriteFrame(&buf, f); err != nil {
			t.Fatal(err)
		}
	}

	reader := bytes.NewReader(buf.Bytes())
	for i, want := range frames {
		got, err := ReadFrame(reader)
		if err != nil {
			t.Fatalf("frame %d: ReadFrame failed: %v", i, err)
		}
		if got.Opcode != want.Opcode || got.RPCID != want.RPCID {
			t.Errorf("frame %d: opcode=%x rpcid=%d, want opcode=%x rpcid=%d",
				i, got.Opcode, got.RPCID, want.Opcode, want.RPCID)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Errorf("frame %d: payload mismatch", i)
		}
	}
}

// TestReadFrameEmptyStream verifies EOF handling.
func TestReadFrameEmptyStream(t *testing.T) {
	_, err := ReadFrame(bytes.NewReader(nil))
	if err == nil {
		t.Error("expected error on empty stream")
	}
	if !errorsIsOrContains(err, io.EOF, "EOF") {
		t.Errorf("expected EOF error, got: %v", err)
	}
}

// TestReadFrameTruncated verifies error on truncated frames.
func TestReadFrameTruncated(t *testing.T) {
	// Write a valid 4-byte length header, but no data follows
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, 100) // claim 100 bytes of payload
	_, err := ReadFrame(bytes.NewReader(buf))
	if err == nil {
		t.Error("expected error on truncated frame")
	}
}

// TestMetadataRoundTrip tests MarshalMetadata / UnmarshalMetadata.
func TestMetadataRoundTrip(t *testing.T) {
	original := Metadata{Branch: 7, ClusterPeerID: 0xABCDEF0123456789}
	b := MarshalMetadata(original)

	got, err := UnmarshalMetadata(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != original {
		t.Errorf("got %+v, want %+v", got, original)
	}
}

// TestUnmarshalMetadataWrongSize verifies size checking.
func TestUnmarshalMetadataWrongSize(t *testing.T) {
	_, err := UnmarshalMetadata(make([]byte, 4)) // too short
	if err == nil {
		t.Error("expected error for short metadata")
	}
}

// TestRPCIDZeroRoundTrip verifies fire-and-forget (RPCID=0) frames work.
func TestRPCIDZeroRoundTrip(t *testing.T) {
	original := &Frame{
		Meta:    Metadata{Branch: 0, ClusterPeerID: 0},
		Opcode:  0x02, // PONG
		RPCID:   0,    // fire-and-forget, no response expected
		Payload: []byte("pong_data"),
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, original); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.RPCID != 0 {
		t.Errorf("RPCID: got %d, want 0", got.RPCID)
	}
}

// TestLargePayload verifies frames with substantial payloads work.
func TestLargePayload(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 10000)
	original := &Frame{
		Meta:    Metadata{Branch: 5, ClusterPeerID: 0},
		Opcode:  0x10,
		RPCID:   1,
		Payload: payload,
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, original); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Errorf("large payload mismatch: len=%d vs len=%d", len(got.Payload), len(payload))
	}
}

func errorsIsOrContains(err error, target error, substr string) bool {
	if err == nil {
		return false
	}
	// Check if it wraps the target
	// (errors.Is would work if we used %w, but io.ReadFull wraps with fmt.Errorf)
	return strings.Contains(err.Error(), substr)
}
