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
	"io"
	"testing"
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

// TestValidateFrameHeaderUnsupportedVersion verifies version validation.
func TestValidateFrameHeaderUnsupportedVersion(t *testing.T) {
	header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, WireVersion + 5, 0x00, 0, 0, 0, 10}
	err := ValidateFrameHeader(header[:])
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
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
	if maxMessagesPerBatch != 10000 {
		t.Fatalf("maxMessagesPerBatch: expected 10000, got %d", maxMessagesPerBatch)
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

// TestValidateFrameHeaderVersionZeroRejected verifies wire version 0
// is rejected as unsupported.
func TestValidateFrameHeaderVersionZeroRejected(t *testing.T) {
	header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, 0x00, 0x00, 0, 0, 0, 10}
	err := ValidateFrameHeader(header[:])
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("version 0 should be rejected as ErrUnsupportedVersion, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// L5: Frame version validation rejects version 0
// ---------------------------------------------------------------------------

// TestValidateFrameHeaderVersionRangeTable exercises the full version range
// to verify that only valid versions (1 through WireVersion) are accepted.
func TestValidateFrameHeaderVersionRangeTable(t *testing.T) {
	tests := []struct {
		name    string
		version byte
		wantErr error
	}{
		{"version_0_rejected", 0x00, ErrUnsupportedVersion},
		{"version_1_accepted", 0x01, nil},
		{"version_above_wire_rejected", WireVersion + 1, ErrUnsupportedVersion},
		{"version_255_rejected", 0xFF, ErrUnsupportedVersion},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			header := [FrameHeaderSize]byte{MagicByte0, MagicByte1, tc.version, 0x00, 0, 0, 0, 10}
			err := ValidateFrameHeader(header[:])
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("version 0x%02x: expected no error, got %v", tc.version, err)
				}
			} else {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("version 0x%02x: expected %v, got %v", tc.version, tc.wantErr, err)
				}
			}
		})
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
