// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestWriteFrameAndReadFrameHeaderRoundTrip verifies that a frame written
// by WriteFrame can be fully read back by ReadFrameHeader plus payload read.
func TestWriteFrameAndReadFrameHeaderRoundTrip(t *testing.T) {
	payload := []byte("hello quicraft")

	var buf bytes.Buffer
	if err := WriteFrame(&buf, payload, FlagNone); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	length, flags, err := ReadFrameHeader(r)
	if err != nil {
		t.Fatalf("ReadFrameHeader failed: %v", err)
	}
	if flags != FlagNone {
		t.Fatalf("expected FlagNone, got %d", flags)
	}
	if length != uint32(len(payload)) {
		t.Fatalf("expected length %d, got %d", len(payload), length)
	}

	result := make([]byte, length)
	if _, err := io.ReadFull(r, result); err != nil {
		t.Fatalf("payload read failed: %v", err)
	}
	if !bytes.Equal(result, payload) {
		t.Fatalf("payload mismatch: got %q, want %q", result, payload)
	}
}

// TestWriteFrameEmptyPayload verifies that writing an empty payload works.
func TestWriteFrameEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, nil, FlagNone); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	length, _, err := ReadFrameHeader(r)
	if err != nil {
		t.Fatalf("ReadFrameHeader failed: %v", err)
	}
	if length != 0 {
		t.Fatalf("expected length 0, got %d", length)
	}
}

// TestWriteFramePayloadTooLarge verifies that WriteFrame rejects payloads
// exceeding MaxFrameSize.
func TestWriteFramePayloadTooLarge(t *testing.T) {
	// Create a fake large payload by using a large length value.
	// We don't actually allocate 16MB+1 here; WriteFrame checks len(payload).
	payload := make([]byte, MaxFrameSize+1)
	var buf bytes.Buffer
	err := WriteFrame(&buf, payload, FlagNone)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

// TestWriteFrameCompressedFlag verifies the compression flag round-trip.
func TestWriteFrameCompressedFlag(t *testing.T) {
	payload := []byte("compressed-data")
	var buf bytes.Buffer
	if err := WriteFrame(&buf, payload, FlagCompressed); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	length, flags, err := ReadFrameHeader(r)
	if err != nil {
		t.Fatalf("ReadFrameHeader failed: %v", err)
	}
	if flags != FlagCompressed {
		t.Fatalf("expected FlagCompressed, got %d", flags)
	}
	if length != uint32(len(payload)) {
		t.Fatalf("expected length %d, got %d", len(payload), length)
	}
}

// TestWriteFrameWriterError verifies WriteFrame propagates writer errors.
func TestWriteFrameWriterError(t *testing.T) {
	w := &failWriter{failAfter: 0}
	err := WriteFrame(w, []byte("data"), FlagNone)
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

// TestWriteFrameWriterErrorOnPayload verifies WriteFrame propagates
// writer errors that occur during payload write.
func TestWriteFrameWriterErrorOnPayload(t *testing.T) {
	w := &failWriter{failAfter: FrameHeaderSize}
	err := WriteFrame(w, []byte("data"), FlagNone)
	if err == nil {
		t.Fatal("expected error from failing writer on payload")
	}
}

// TestReadFrameHeaderTruncated verifies ReadFrameHeader returns an error
// when the reader provides fewer than FrameHeaderSize bytes.
func TestReadFrameHeaderTruncated(t *testing.T) {
	r := bytes.NewReader([]byte{0x51, 0x43, 0x01})
	_, _, err := ReadFrameHeader(r)
	if err == nil {
		t.Fatal("expected error from truncated header")
	}
}

// TestReadFrameHeaderInvalidMagic verifies ReadFrameHeader detects bad magic.
func TestReadFrameHeaderInvalidMagic(t *testing.T) {
	header := [FrameHeaderSize]byte{0xFF, 0xFF, WireVersion, 0x00, 0, 0, 0, 10}
	r := bytes.NewReader(header[:])
	_, _, err := ReadFrameHeader(r)
	if !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("expected ErrInvalidMagic, got %v", err)
	}
}

// TestReadFrameHeaderUnsupportedVersion verifies version check.
func TestReadFrameHeaderUnsupportedVersion(t *testing.T) {
	header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, WireVersion + 1, 0x00, 0, 0, 0, 10}
	r := bytes.NewReader(header[:])
	_, _, err := ReadFrameHeader(r)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
	}
}

// TestReadFrameHeaderOversizedLength verifies the length check.
func TestReadFrameHeaderOversizedLength(t *testing.T) {
	header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, WireVersion, 0x00}
	binary.BigEndian.PutUint32(header[4:8], MaxFrameSize+1)
	r := bytes.NewReader(header[:])
	_, _, err := ReadFrameHeader(r)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}

// TestValidateFrameHeaderValid verifies a valid header passes validation.
func TestValidateFrameHeaderValid(t *testing.T) {
	header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, WireVersion, 0x00}
	binary.BigEndian.PutUint32(header[4:8], 1024)
	if err := ValidateFrameHeader(header[:]); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidateFrameHeaderZeroLength verifies zero-length frames pass.
func TestValidateFrameHeaderZeroLength(t *testing.T) {
	header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, WireVersion, 0x00, 0, 0, 0, 0}
	if err := ValidateFrameHeader(header[:]); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidateFrameHeaderMaxLength verifies max length passes.
func TestValidateFrameHeaderMaxLength(t *testing.T) {
	header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, WireVersion, 0x00}
	binary.BigEndian.PutUint32(header[4:8], MaxFrameSize)
	if err := ValidateFrameHeader(header[:]); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidateFrameHeaderWrongMagic verifies detection of bad magic bytes.
func TestValidateFrameHeaderWrongMagic(t *testing.T) {
	tests := []struct {
		name   string
		header [FrameHeaderSize]byte
	}{
		{
			name:   "both wrong",
			header: [FrameHeaderSize]byte{0x00, 0x00, WireVersion, 0x00, 0, 0, 0, 10},
		},
		{
			name:   "first wrong",
			header: [FrameHeaderSize]byte{0x00, MagicByte1, WireVersion, 0x00, 0, 0, 0, 10},
		},
		{
			name:   "second wrong",
			header: [FrameHeaderSize]byte{MagicByte0, 0x00, WireVersion, 0x00, 0, 0, 0, 10},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateFrameHeader(tc.header[:])
			if !errors.Is(err, ErrInvalidMagic) {
				t.Fatalf("expected ErrInvalidMagic, got %v", err)
			}
		})
	}
}

// TestValidateFrameHeaderOversized verifies oversized frame detection.
func TestValidateFrameHeaderOversized(t *testing.T) {
	header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, WireVersion, 0x00}
	binary.BigEndian.PutUint32(header[4:8], MaxFrameSize+1)
	err := ValidateFrameHeader(header[:])
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}

// TestValidateFrameHeaderBufferTooSmall verifies short buffer detection.
func TestValidateFrameHeaderBufferTooSmall(t *testing.T) {
	err := ValidateFrameHeader([]byte{0x51, 0x43})
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("expected ErrBufferTooSmall, got %v", err)
	}
}

// TestValidateFrameHeaderEmptyBuffer verifies empty buffer detection.
func TestValidateFrameHeaderEmptyBuffer(t *testing.T) {
	err := ValidateFrameHeader(nil)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("expected ErrBufferTooSmall, got %v", err)
	}
}

// TestMarshalFrameRoundTrip verifies MarshalFrame produces valid output
// that ValidateFrameHeader accepts and the payload is correctly stored.
func TestMarshalFrameRoundTrip(t *testing.T) {
	payload := []byte("round trip payload")
	buf := make([]byte, FrameHeaderSize+len(payload))

	n, err := MarshalFrame(buf, payload, FlagNone)
	if err != nil {
		t.Fatalf("MarshalFrame failed: %v", err)
	}
	if n != FrameHeaderSize+len(payload) {
		t.Fatalf("expected %d bytes, got %d", FrameHeaderSize+len(payload), n)
	}

	// Validate header portion.
	if err := ValidateFrameHeader(buf[:FrameHeaderSize]); err != nil {
		t.Fatalf("ValidateFrameHeader failed: %v", err)
	}

	// Verify payload.
	length := binary.BigEndian.Uint32(buf[4:8])
	if length != uint32(len(payload)) {
		t.Fatalf("expected length %d, got %d", len(payload), length)
	}
	if !bytes.Equal(buf[FrameHeaderSize:n], payload) {
		t.Fatalf("payload mismatch")
	}
}

// TestMarshalFrameEmptyPayload verifies marshaling with empty payload.
func TestMarshalFrameEmptyPayload(t *testing.T) {
	buf := make([]byte, FrameHeaderSize)
	n, err := MarshalFrame(buf, nil, FlagNone)
	if err != nil {
		t.Fatalf("MarshalFrame failed: %v", err)
	}
	if n != FrameHeaderSize {
		t.Fatalf("expected %d bytes, got %d", FrameHeaderSize, n)
	}
	length := binary.BigEndian.Uint32(buf[4:8])
	if length != 0 {
		t.Fatalf("expected length 0, got %d", length)
	}
}

// TestMarshalFrameBufferTooSmall verifies buffer size check.
func TestMarshalFrameBufferTooSmall(t *testing.T) {
	payload := []byte("data")
	buf := make([]byte, FrameHeaderSize+len(payload)-1) // 1 byte short
	_, err := MarshalFrame(buf, payload, FlagNone)
	if !errors.Is(err, ErrBufferTooSmall) {
		t.Fatalf("expected ErrBufferTooSmall, got %v", err)
	}
}

// TestMarshalFramePayloadTooLarge verifies payload size check.
func TestMarshalFramePayloadTooLarge(t *testing.T) {
	payload := make([]byte, MaxFrameSize+1)
	buf := make([]byte, FrameHeaderSize+len(payload))
	_, err := MarshalFrame(buf, payload, FlagNone)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

// TestMarshalFrameWithCompressedFlag verifies flag preservation.
func TestMarshalFrameWithCompressedFlag(t *testing.T) {
	payload := []byte("compressed-content")
	buf := make([]byte, FrameHeaderSize+len(payload))
	n, err := MarshalFrame(buf, payload, FlagCompressed)
	if err != nil {
		t.Fatalf("MarshalFrame failed: %v", err)
	}

	// Read back via ReadFrameHeader.
	r := bytes.NewReader(buf[:n])
	length, flags, err := ReadFrameHeader(r)
	if err != nil {
		t.Fatalf("ReadFrameHeader failed: %v", err)
	}
	if flags != FlagCompressed {
		t.Fatalf("expected FlagCompressed, got %d", flags)
	}
	if length != uint32(len(payload)) {
		t.Fatalf("expected length %d, got %d", len(payload), length)
	}
}

// TestMarshalFrameAndWriteFrameConsistency verifies that MarshalFrame and
// WriteFrame produce identical output for the same input.
func TestMarshalFrameAndWriteFrameConsistency(t *testing.T) {
	payload := []byte("consistency check payload")
	flags := FlagCompressed

	// WriteFrame.
	var wbuf bytes.Buffer
	if err := WriteFrame(&wbuf, payload, flags); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	// MarshalFrame.
	mbuf := make([]byte, FrameHeaderSize+len(payload))
	n, err := MarshalFrame(mbuf, payload, flags)
	if err != nil {
		t.Fatalf("MarshalFrame failed: %v", err)
	}

	if !bytes.Equal(wbuf.Bytes(), mbuf[:n]) {
		t.Fatal("WriteFrame and MarshalFrame produced different output")
	}
}

// TestFrameConstantValues verifies the documented constant values are correct.
func TestFrameConstantValues(t *testing.T) {
	if MagicByte0 != 0x51 {
		t.Fatalf("MagicByte0: expected 0x51, got 0x%02x", MagicByte0)
	}
	if MagicByte1 != 0x43 {
		t.Fatalf("MagicByte1: expected 0x43, got 0x%02x", MagicByte1)
	}
	if WireVersion != 0x01 {
		t.Fatalf("WireVersion: expected 0x01, got 0x%02x", WireVersion)
	}
	if MaxFrameSize != 16*1024*1024 {
		t.Fatalf("MaxFrameSize: expected %d, got %d", 16*1024*1024, MaxFrameSize)
	}
	if FrameHeaderSize != 8 {
		t.Fatalf("FrameHeaderSize: expected 8, got %d", FrameHeaderSize)
	}
	if proto.MaxMessagesPerBatch != 10000 {
		t.Fatalf("proto.MaxMessagesPerBatch: expected 10000, got %d", proto.MaxMessagesPerBatch)
	}
	if maxPoolBufSize != 256*1024 {
		t.Fatalf("maxPoolBufSize: expected %d, got %d", 256*1024, maxPoolBufSize)
	}
}

// TestFrameFlagsValues verifies the flag constants.
func TestFrameFlagsValues(t *testing.T) {
	if FlagNone != 0x00 {
		t.Fatalf("FlagNone: expected 0x00, got 0x%02x", FlagNone)
	}
	if FlagCompressed != 0x01 {
		t.Fatalf("FlagCompressed: expected 0x01, got 0x%02x", FlagCompressed)
	}
}

// ---------------------------------------------------------------------------
// Frame version validation is EXACT
// ---------------------------------------------------------------------------

// TestValidateFrameHeaderVersionIsExact sweeps every one of the 256 values the
// version byte can hold and asserts that exactly one of them — WireVersion — is
// accepted.
//
// The exhaustive form is the point. A gate reading
// `buf[2] < 1 || buf[2] > WireVersion` is the same test as equality
// while WireVersion is 1 and becomes a compatibility WINDOW the moment the
// constant moves. A table of hand-picked values passes against both shapes; a
// sweep that asserts a single accepted value cannot.
func TestValidateFrameHeaderVersionIsExact(t *testing.T) {
	accepted := 0
	for v := 0; v < 256; v++ {
		header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, byte(v), 0x00, 0, 0, 0, 10}
		err := ValidateFrameHeader(header[:])
		if byte(v) == WireVersion {
			if err != nil {
				t.Fatalf("version 0x%02x is WireVersion and must be accepted, got %v", v, err)
			}
			accepted++
			continue
		}
		if !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("version 0x%02x: got %v, want ErrUnsupportedVersion; the gate "+
				"must be exact equality with WireVersion, not a range that widens "+
				"the moment the constant moves", v, err)
		}
	}
	if accepted != 1 {
		t.Fatalf("%d version bytes were accepted, want exactly 1 (WireVersion)", accepted)
	}
}

// TestValidateFrameHeaderVersionErrorIsUnchanged pins the sentinel the version
// gate returns, separately from the values it applies to. Tightening the gate
// must not change what a peer on the wrong dialect sees: connection handling
// keys off ErrUnsupportedVersion, and swapping it for a generic parse error
// would turn a named refusal into an opaque one.
func TestValidateFrameHeaderVersionErrorIsUnchanged(t *testing.T) {
	header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, WireVersion + 1, 0x00, 0, 0, 0, 10}
	err := ValidateFrameHeader(header[:])
	if err != ErrUnsupportedVersion {
		t.Fatalf("ValidateFrameHeader returned %v (%T), want the ErrUnsupportedVersion "+
			"sentinel itself", err, err)
	}
	// The magic is checked BEFORE the version, so a header that is wrong in
	// both ways still reports the magic. That ordering is what makes a probe
	// from an unrelated protocol distinguishable from a version skew.
	bad := [FrameHeaderSize]byte{0x00, 0x00, WireVersion + 1, 0x00, 0, 0, 0, 10}
	if err := ValidateFrameHeader(bad[:]); !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("ValidateFrameHeader on a bad magic AND a bad version = %v, want ErrInvalidMagic", err)
	}
}

// TestValidateFrameHeaderVersionGateIsNotARange reads the gate's SOURCE and
// fails if the version byte is compared with a relational operator.
//
// A behavioural test cannot separate the two shapes while WireVersion is 1:
// `buf[2] < 1 || buf[2] > WireVersion` and `buf[2] != WireVersion` accept
// exactly the same single value today, so every input-driven assertion passes
// against both. The difference is entirely in what happens at the FIRST bump —
// the range form silently becomes a compatibility window admitting every
// version below the new one, which is the drift the pinning policy exists to
// prevent, and it does so with no test failing to announce it.
//
// So the invariant is pinned where it actually lives, in the form of the
// expression. This mirrors TestMembershipVersionNotDerivedFromWireVersion in
// pkg/proto, which guards a rule with the same shape for the same reason.
func TestValidateFrameHeaderVersionGateIsNotARange(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "config.go", nil, 0)
	if err != nil {
		t.Fatalf("parse config.go: %v", err)
	}

	var gate *ast.FuncDecl
	for _, decl := range f.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "AcceptsFrame" && fn.Recv != nil {
			gate = fn
			break
		}
	}
	if gate == nil {
		t.Fatal("WireWindow.AcceptsFrame not found in config.go; update this guard")
	}

	// The frame version is gated by set membership: exactly one equality
	// test per member (FrameCurrent, FramePrevious) and no relational
	// comparison, because a relational form widens into a compatibility
	// window the moment WireVersion moves.
	exact := 0
	ast.Inspect(gate.Body, func(n ast.Node) bool {
		bin, ok := n.(*ast.BinaryExpr)
		if !ok {
			return true
		}
		switch bin.Op {
		case token.EQL:
			exact++
		case token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ:
			t.Errorf("AcceptsFrame compares the frame version with %q at %s; "+
				"the gate must be set membership by equality",
				bin.Op, fset.Position(bin.Pos()))
		}
		return true
	})
	if exact != 2 {
		t.Fatalf("AcceptsFrame makes %d equality test(s), want exactly 2 "+
			"(FrameCurrent, FramePrevious)", exact)
	}
	if got := DefaultWireWindow(); got.FrameCurrent != WireVersion || got.FramePrevious != PreviousWireVersion {
		t.Fatalf("DefaultWireWindow() = %+v, want {WireVersion, PreviousWireVersion}", got)
	}
}

// TestWriteFrameVersion1ProducesValidHeader verifies that WriteFrame
// produces a header with WireVersion=1 that passes validation.
func TestWriteFrameVersion1ProducesValidHeader(t *testing.T) {
	payload := []byte("test payload")
	var buf bytes.Buffer
	if err := WriteFrame(&buf, payload, FlagNone); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	raw := buf.Bytes()
	if raw[2] != WireVersion {
		t.Fatalf("expected version %d in frame, got %d", WireVersion, raw[2])
	}
	if err := ValidateFrameHeader(raw[:FrameHeaderSize]); err != nil {
		t.Fatalf("WriteFrame produced invalid header: %v", err)
	}
}

// BenchmarkMarshalFrame measures zero-copy frame serialization with a 1KB payload
// into a pre-allocated buffer. This is the hot path for outbound message framing.
func BenchmarkMarshalFrame(b *testing.B) {
	b.ReportAllocs()
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	buf := make([]byte, FrameHeaderSize+len(payload))
	for b.Loop() {
		_, _ = MarshalFrame(buf, payload, FlagNone)
	}
}

// BenchmarkReadFrameHeader measures header parsing from an io.Reader.
// This is the first operation on every inbound frame.
func BenchmarkReadFrameHeader(b *testing.B) {
	b.ReportAllocs()
	var header [FrameHeaderSize]byte
	header[0] = MagicByte0
	header[1] = MagicByte1
	header[2] = WireVersion
	header[3] = byte(FlagNone)
	binary.BigEndian.PutUint32(header[4:8], 1024)
	for b.Loop() {
		r := bytes.NewReader(header[:])
		_, _, _ = ReadFrameHeader(r)
	}
}

// BenchmarkValidateFrameHeader measures magic/version/size validation on a
// raw header buffer. This is a pure computation check with no I/O.
func BenchmarkValidateFrameHeader(b *testing.B) {
	b.ReportAllocs()
	var header [FrameHeaderSize]byte
	header[0] = MagicByte0
	header[1] = MagicByte1
	header[2] = WireVersion
	header[3] = byte(FlagNone)
	binary.BigEndian.PutUint32(header[4:8], 1024)
	for b.Loop() {
		_ = ValidateFrameHeader(header[:])
	}
}

// failWriter is a test helper that fails after writing failAfter bytes.
type failWriter struct {
	written   int
	failAfter int
}

func (w *failWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.failAfter {
		remaining := w.failAfter - w.written
		if remaining <= 0 {
			return 0, io.ErrClosedPipe
		}
		w.written += remaining
		return remaining, io.ErrClosedPipe
	}
	w.written += len(p)
	return len(p), nil
}
