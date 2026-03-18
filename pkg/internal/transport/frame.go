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
	"encoding/binary"
	"io"
)

// Wire format constants for the transport frame protocol.
//
// Frame layout: [Magic:2 | Version:1 | Flags:1 | Length:4 | Payload:Length]
//
// Magic:   0x51 0x43 ("QC" for QuicRaft)
// Version: 0x01
// Flags:   bitfield for compression, batching hints, etc.
// Length:  uint32 big-endian, max 16MB
// Payload: serialized proto.MessageBatch or other data
const (
	// MagicByte0 is the first byte of the frame magic ("Q").
	MagicByte0 byte = 0x51

	// MagicByte1 is the second byte of the frame magic ("C").
	MagicByte1 byte = 0x43

	// WireVersion is the current wire format version.
	WireVersion byte = 0x01

	// MaxFrameSize is the maximum allowed frame payload size (16MB).
	// Frames exceeding this limit are rejected to prevent memory exhaustion.
	MaxFrameSize uint32 = 16 * 1024 * 1024

	// FrameHeaderSize is the size in bytes of the frame header.
	// Layout: Magic(2) + Version(1) + Flags(1) + Length(4) = 8 bytes.
	FrameHeaderSize = 8

	// maxMessagesPerBatch is the maximum number of messages allowed in
	// a single batch, as a sanity limit against malformed payloads.
	maxMessagesPerBatch = 10000

	// maxPoolBufSize is the maximum buffer size returned to sync.Pool.
	// Buffers larger than this are left for GC to prevent pool pollution.
	maxPoolBufSize = 256 * 1024
)

// FrameFlags is a bitfield encoding frame-level options in a single byte.
type FrameFlags byte

const (
	// FlagNone indicates no flags are set.
	FlagNone FrameFlags = 0x00

	// FlagCompressed indicates the payload is Snappy-compressed.
	FlagCompressed FrameFlags = 0x01
)

// WriteFrame writes a complete frame (header + payload) to w. The payload
// must not exceed MaxFrameSize. Returns ErrPayloadTooLarge if the payload
// is oversized.
func WriteFrame(w io.Writer, payload []byte, flags FrameFlags) error {
	if uint32(len(payload)) > MaxFrameSize {
		return ErrPayloadTooLarge
	}
	var header [FrameHeaderSize]byte
	header[0] = MagicByte0
	header[1] = MagicByte1
	header[2] = WireVersion
	header[3] = byte(flags)
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadFrameHeader reads and validates a frame header from r. It returns
// the payload length, flags, and any error. The caller should then read
// exactly length bytes of payload data.
func ReadFrameHeader(r io.Reader) (length uint32, flags FrameFlags, err error) {
	var header [FrameHeaderSize]byte
	if _, err = io.ReadFull(r, header[:]); err != nil {
		return 0, FlagNone, err
	}
	if err = ValidateFrameHeader(header[:]); err != nil {
		return 0, FlagNone, err
	}
	flags = FrameFlags(header[3])
	length = binary.BigEndian.Uint32(header[4:8])
	return length, flags, nil
}

// ValidateFrameHeader validates the 8-byte frame header in buf. Checks
// magic bytes, wire version, and payload length. Returns the appropriate
// typed error on failure.
func ValidateFrameHeader(buf []byte) error {
	if len(buf) < FrameHeaderSize {
		return ErrBufferTooSmall
	}
	if buf[0] != MagicByte0 || buf[1] != MagicByte1 {
		return ErrInvalidMagic
	}
	if buf[2] < 1 || buf[2] > WireVersion {
		return ErrUnsupportedVersion
	}
	length := binary.BigEndian.Uint32(buf[4:8])
	if length > MaxFrameSize {
		return ErrFrameTooLarge
	}
	return nil
}

// MarshalFrame marshals a complete frame (header + payload) into buf.
// Returns the total number of bytes written (FrameHeaderSize + len(payload)).
// Returns ErrBufferTooSmall if buf is too short, or ErrPayloadTooLarge if
// the payload exceeds MaxFrameSize.
func MarshalFrame(buf []byte, payload []byte, flags FrameFlags) (int, error) {
	if uint32(len(payload)) > MaxFrameSize {
		return 0, ErrPayloadTooLarge
	}
	total := FrameHeaderSize + len(payload)
	if len(buf) < total {
		return 0, ErrBufferTooSmall
	}
	buf[0] = MagicByte0
	buf[1] = MagicByte1
	buf[2] = WireVersion
	buf[3] = byte(flags)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)))
	copy(buf[FrameHeaderSize:], payload)
	return total, nil
}
