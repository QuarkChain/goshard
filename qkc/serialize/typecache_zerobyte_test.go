package serialize

import "testing"

// allIgnoredFields encodes to zero bytes: A is ignored, b is unexported.
type allIgnoredFields struct {
	A int `ser:"-"`
	b int
}

// TestZeroByteListElementRejected pins the contract that a slice whose element
// encodes to zero bytes is unsupported and is rejected SYMMETRICALLY — both
// serialize and deserialize must fail. (Previously serialize emitted just the
// length prefix while deserialize refused it, an encode/decode asymmetry.)
func TestZeroByteListElementRejected(t *testing.T) {
	cases := []struct {
		name string
		ser  func() error
		des  func() error
	}{
		{
			"struct{}",
			func() error { _, e := SerializeToBytes([]struct{}{{}, {}}); return e },
			func() error { var v []struct{}; return DeserializeFromBytes([]byte{0x02}, &v) },
		},
		{
			"all ignored/unexported fields",
			func() error { _, e := SerializeToBytes([]allIgnoredFields{{}, {}}); return e },
			func() error { var v []allIgnoredFields; return DeserializeFromBytes([]byte{0x02}, &v) },
		},
		{
			"zero-length array element",
			func() error { _, e := SerializeToBytes([][0]uint32{{}, {}}); return e },
			func() error { var v [][0]uint32; return DeserializeFromBytes([]byte{0x02}, &v) },
		},
	}
	for _, c := range cases {
		if err := c.ser(); err == nil {
			t.Errorf("%s: SerializeToBytes should be rejected, got nil", c.name)
		}
		if err := c.des(); err == nil {
			t.Errorf("%s: DeserializeFromBytes should be rejected, got nil", c.name)
		}
	}
	_ = allIgnoredFields{}.b // silence unused-field linters
}

// TestNonZeroByteListStillRoundTrips guards against over-rejection: a slice
// whose element consumes >= 1 byte must still encode and decode normally.
func TestNonZeroByteListStillRoundTrips(t *testing.T) {
	in := []uint32{7, 9}
	b, err := SerializeToBytes(in)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	var out []uint32
	if err := DeserializeFromBytes(b, &out); err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if len(out) != 2 || out[0] != 7 || out[1] != 9 {
		t.Fatalf("round-trip mismatch: %v", out)
	}
}
