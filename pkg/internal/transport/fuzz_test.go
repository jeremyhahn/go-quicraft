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

//go:build quicraft_fuzz

package transport

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// FuzzFrameHeaderRoundTrip constructs a FrameHeader from fuzzed fields,
// marshals it via MarshalFrame, then validates it via ValidateFrameHeader
// and verifies all fields survive the round trip unchanged.
func FuzzFrameHeaderRoundTrip(f *testing.F) {
	// Seed: minimal valid frame with empty payload and no flags.
	f.Add(byte(FlagNone), uint32(0))

	// Seed: frame with compressed flag and small payload length.
	f.Add(byte(FlagCompressed), uint32(64))

	// Seed: maximum allowed payload length with no flags.
	f.Add(byte(FlagNone), MaxFrameSize)

	// Seed: all flag bits set with a moderate payload length.
	f.Add(byte(0xFF), uint32(1024))

	// Seed: zero flags with a 1-byte payload.
	f.Add(byte(0x00), uint32(1))

	f.Fuzz(func(t *testing.T, flagsByte byte, payloadLen uint32) {
		// Clamp payload length to MaxFrameSize so we test valid frames.
		if payloadLen > MaxFrameSize {
			payloadLen = payloadLen % (MaxFrameSize + 1)
		}

		// Cap allocation to prevent OOM in the fuzzer. We only need
		// the header bytes to verify the round trip; use a synthetic
		// payload of zeros up to a reasonable limit.
		const maxAllocPayload = 4096
		allocLen := payloadLen
		if allocLen > maxAllocPayload {
			allocLen = maxAllocPayload
		}

		payload := make([]byte, allocLen)
		flags := FrameFlags(flagsByte)

		buf := make([]byte, FrameHeaderSize+len(payload))
		n, err := MarshalFrame(buf, payload, flags)
		if err != nil {
			t.Fatalf("MarshalFrame failed: %v", err)
		}
		if n != FrameHeaderSize+len(payload) {
			t.Fatalf("MarshalFrame wrote %d bytes, expected %d", n, FrameHeaderSize+len(payload))
		}

		// Validate the header.
		if err := ValidateFrameHeader(buf[:FrameHeaderSize]); err != nil {
			t.Fatalf("ValidateFrameHeader rejected a frame we just marshaled: %v", err)
		}

		// Verify magic bytes survived.
		if buf[0] != MagicByte0 || buf[1] != MagicByte1 {
			t.Fatalf("magic bytes mismatch: got [0x%02X, 0x%02X], want [0x%02X, 0x%02X]",
				buf[0], buf[1], MagicByte0, MagicByte1)
		}

		// Verify version survived.
		if buf[2] != WireVersion {
			t.Fatalf("version mismatch: got 0x%02X, want 0x%02X", buf[2], WireVersion)
		}

		// Verify flags survived.
		if FrameFlags(buf[3]) != flags {
			t.Fatalf("flags mismatch: got 0x%02X, want 0x%02X", buf[3], flagsByte)
		}

		// Verify encoded payload length matches what we supplied.
		encodedLen := binary.BigEndian.Uint32(buf[4:8])
		if encodedLen != uint32(len(payload)) {
			t.Fatalf("encoded length mismatch: got %d, want %d", encodedLen, len(payload))
		}

		// Verify payload data survived.
		if !bytes.Equal(buf[FrameHeaderSize:n], payload) {
			t.Fatalf("payload mismatch after round trip")
		}
	})
}

// FuzzFrameHeaderUnmarshalCrash feeds random bytes to ValidateFrameHeader
// and ReadFrameHeader. Neither function may panic regardless of input.
func FuzzFrameHeaderUnmarshalCrash(f *testing.F) {
	// Seed: empty input.
	f.Add([]byte{})

	// Seed: single byte.
	f.Add([]byte{0x00})

	// Seed: exactly 7 bytes (one short of FrameHeaderSize).
	f.Add([]byte{0x51, 0x43, 0x01, 0x00, 0x00, 0x00, 0x00})

	// Seed: valid minimal header with zero-length payload.
	validHeader := make([]byte, FrameHeaderSize)
	validHeader[0] = MagicByte0
	validHeader[1] = MagicByte1
	validHeader[2] = WireVersion
	validHeader[3] = byte(FlagNone)
	binary.BigEndian.PutUint32(validHeader[4:8], 0)
	f.Add(validHeader)

	// Seed: header with maximum payload length.
	maxHeader := make([]byte, FrameHeaderSize)
	maxHeader[0] = MagicByte0
	maxHeader[1] = MagicByte1
	maxHeader[2] = WireVersion
	maxHeader[3] = byte(FlagNone)
	binary.BigEndian.PutUint32(maxHeader[4:8], MaxFrameSize)
	f.Add(maxHeader)

	// Seed: header with payload length exceeding max (should be rejected).
	oversizeHeader := make([]byte, FrameHeaderSize)
	oversizeHeader[0] = MagicByte0
	oversizeHeader[1] = MagicByte1
	oversizeHeader[2] = WireVersion
	oversizeHeader[3] = byte(FlagNone)
	binary.BigEndian.PutUint32(oversizeHeader[4:8], MaxFrameSize+1)
	f.Add(oversizeHeader)

	// Seed: wrong magic bytes.
	badMagic := make([]byte, FrameHeaderSize)
	badMagic[0] = 0xFF
	badMagic[1] = 0xFF
	badMagic[2] = WireVersion
	f.Add(badMagic)

	// Seed: unsupported version (0x00).
	badVersion := make([]byte, FrameHeaderSize)
	badVersion[0] = MagicByte0
	badVersion[1] = MagicByte1
	badVersion[2] = 0x00
	f.Add(badVersion)

	// Seed: unsupported version (future version 0x02).
	futureVersion := make([]byte, FrameHeaderSize)
	futureVersion[0] = MagicByte0
	futureVersion[1] = MagicByte1
	futureVersion[2] = 0x02
	f.Add(futureVersion)

	// Seed: all 0xFF bytes.
	f.Add(bytes.Repeat([]byte{0xFF}, FrameHeaderSize))

	// Seed: all zero bytes.
	f.Add(make([]byte, FrameHeaderSize))

	// Seed: oversized random-looking input.
	f.Add(bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		// ValidateFrameHeader must never panic.
		_ = ValidateFrameHeader(data)

		// ReadFrameHeader must never panic.
		r := bytes.NewReader(data)
		_, _, _ = ReadFrameHeader(r)
	})
}

// FuzzHeartbeatDetection feeds random bytes to isHeartbeatBatch and
// isHeartbeatMessage. The functions must never panic and must return
// consistent results: a message type that is detected as a heartbeat
// on first call must also be detected on a second call with the same
// input.
func FuzzHeartbeatDetection(f *testing.F) {
	// Seed: zero (not a heartbeat).
	f.Add(uint64(0))

	// Seed: Heartbeat type (17).
	f.Add(uint64(17))

	// Seed: HeartbeatResp type (18).
	f.Add(uint64(18))

	// Seed: LeaderHeartbeat type (2) - not a transport heartbeat.
	f.Add(uint64(2))

	// Seed: max uint64.
	f.Add(uint64(0xFFFFFFFFFFFFFFFF))

	// Seed: boundary values around heartbeat types.
	f.Add(uint64(16))
	f.Add(uint64(19))

	// Seed: 1.
	f.Add(uint64(1))

	f.Fuzz(func(t *testing.T, msgType uint64) {
		// isHeartbeatMessage must never panic.
		result1 := isHeartbeatMessage(msgType)

		// Verify consistency: calling again with the same input must
		// produce the same result (deterministic, no side effects).
		result2 := isHeartbeatMessage(msgType)
		if result1 != result2 {
			t.Fatalf("isHeartbeatMessage(%d) returned %v then %v: non-deterministic",
				msgType, result1, result2)
		}

		// Verify semantic correctness: only types 17 (Heartbeat) and
		// 18 (HeartbeatResp) should be detected.
		if msgType == 17 || msgType == 18 {
			if !result1 {
				t.Fatalf("isHeartbeatMessage(%d) returned false, expected true", msgType)
			}
		} else {
			if result1 {
				t.Fatalf("isHeartbeatMessage(%d) returned true, expected false", msgType)
			}
		}
	})
}

// FuzzFrameWithPayload fuzzes the full frame format (header + payload)
// through the WriteFrame/ReadFrameHeader round trip, exercising the
// combination of fuzzed flags and arbitrary payload content.
func FuzzFrameWithPayload(f *testing.F) {
	// Seed: empty payload, no flags.
	f.Add(byte(FlagNone), []byte{})

	// Seed: minimal payload with no flags.
	f.Add(byte(FlagNone), []byte{0x00})

	// Seed: small payload with compressed flag.
	f.Add(byte(FlagCompressed), []byte("hello quicraft"))

	// Seed: all flag bits set with multi-byte payload.
	f.Add(byte(0xFF), []byte{0x01, 0x02, 0x03, 0x04, 0x05})

	// Seed: payload filled with 0xFF bytes.
	f.Add(byte(FlagNone), bytes.Repeat([]byte{0xFF}, 256))

	// Seed: payload at a power-of-two boundary.
	f.Add(byte(FlagNone), make([]byte, 4096))

	// Seed: single null byte with compressed flag.
	f.Add(byte(FlagCompressed), []byte{0x00})

	f.Fuzz(func(t *testing.T, flagsByte byte, payload []byte) {
		// Cap payload size to prevent OOM in the fuzzer while still
		// exercising a meaningful range of sizes.
		const maxFuzzPayload = 16 * 1024
		if len(payload) > maxFuzzPayload {
			payload = payload[:maxFuzzPayload]
		}

		flags := FrameFlags(flagsByte)

		// Write the frame to a buffer via WriteFrame.
		var buf bytes.Buffer
		err := WriteFrame(&buf, payload, flags)
		if err != nil {
			t.Fatalf("WriteFrame failed: %v", err)
		}

		// Read back via ReadFrameHeader.
		r := bytes.NewReader(buf.Bytes())
		length, gotFlags, err := ReadFrameHeader(r)
		if err != nil {
			t.Fatalf("ReadFrameHeader failed: %v", err)
		}

		// Verify flags round trip.
		if gotFlags != flags {
			t.Fatalf("flags mismatch: got 0x%02X, want 0x%02X", gotFlags, flags)
		}

		// Verify payload length.
		if length != uint32(len(payload)) {
			t.Fatalf("length mismatch: got %d, want %d", length, len(payload))
		}

		// Read and verify payload content.
		gotPayload := make([]byte, length)
		if length > 0 {
			n, err := r.Read(gotPayload)
			if err != nil {
				t.Fatalf("payload read failed: %v", err)
			}
			if uint32(n) != length {
				t.Fatalf("payload read %d bytes, want %d", n, length)
			}
		}
		if !bytes.Equal(gotPayload, payload) {
			t.Fatalf("payload content mismatch after round trip")
		}

		// Also verify via MarshalFrame for completeness.
		marshalBuf := make([]byte, FrameHeaderSize+len(payload))
		n, err := MarshalFrame(marshalBuf, payload, flags)
		if err != nil {
			t.Fatalf("MarshalFrame failed: %v", err)
		}
		if n != FrameHeaderSize+len(payload) {
			t.Fatalf("MarshalFrame wrote %d bytes, expected %d", n, FrameHeaderSize+len(payload))
		}

		// The header bytes from WriteFrame and MarshalFrame must match.
		if !bytes.Equal(buf.Bytes()[:FrameHeaderSize], marshalBuf[:FrameHeaderSize]) {
			t.Fatalf("WriteFrame and MarshalFrame produced different headers")
		}

		// The full frame bytes must match.
		if !bytes.Equal(buf.Bytes(), marshalBuf[:n]) {
			t.Fatalf("WriteFrame and MarshalFrame produced different frames")
		}
	})
}
