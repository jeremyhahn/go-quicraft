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

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// Frame layout: [Magic:2 "QC" | Version:1 | Flags:1 | Length:4 big-endian, max 16MB | Payload].
const (
	// MagicByte0 is the first byte of the frame magic ("Q").
	MagicByte0 byte = 0x51

	// MagicByte1 is the second byte of the frame magic ("C").
	MagicByte1 byte = 0x43

	// WireVersion is the frame header version, pinned at 1 before release; receivers require an exact match.
	WireVersion byte = 0x01

	// PreviousWireVersion is the previous minor's frame version, equal to WireVersion until a release moves it.
	PreviousWireVersion byte = WireVersion

	// MaxFrameSize is the maximum allowed frame payload size (16MB).
	// Frames exceeding this limit are rejected to prevent memory exhaustion.
	MaxFrameSize uint32 = 16 * 1024 * 1024

	// FrameHeaderSize is the size in bytes of the frame header.
	// Layout: Magic(2) + Version(1) + Flags(1) + Length(4) = 8 bytes.
	FrameHeaderSize = 8

	// maxPoolBufSize is the maximum buffer size returned to sync.Pool.
	// Buffers larger than this are left for GC to prevent pool pollution.
	maxPoolBufSize = 256 * 1024

	// batchFramingReserve is frame headroom for the MessageBatch header and per-message framing.
	batchFramingReserve = 64 * 1024
)

// MaxReplicationPayloadBytes is the largest raw entry payload one replication batch may carry in one frame; Config.MaxApplyEntrySize must be clamped to it.
const MaxReplicationPayloadBytes = int(MaxFrameSize) - FrameHeaderSize - batchFramingReserve

// Fail the build if config.MaxReplicationPayloadBytes drifts from this value.
const _ = uint64(MaxReplicationPayloadBytes) - config.MaxReplicationPayloadBytes

// Send-side admission: decide before writing whether a batch fits, sized on the uncompressed payload, splitting or dropping what cannot be framed.

// batchHeaderSize returns the marshaled size of an empty MessageBatch, the per-frame overhead.
func batchHeaderSize(sourceAddress string) int {
	header := proto.MessageBatch{SourceAddress: sourceAddress}
	return header.Size()
}

// messageFrameError reports why msg of size n can never be framed, or nil.
func messageFrameError(msg *proto.Message, headerSize, size int) error {
	if len(msg.Entries) > proto.MaxEntriesPerMessage {
		return proto.ErrTooManyMessages
	}
	if headerSize+size > int(MaxFrameSize) {
		return ErrPayloadTooLarge
	}
	return nil
}

// messageFrameable reports whether msg can be framed at all.
func messageFrameable(msg *proto.Message, headerSize, size int) bool {
	return messageFrameError(msg, headerSize, size) == nil
}

// sendableGroup returns how many leading msgs fit one frame and its payload size; it errors only when msgs[0] can never be framed.
func sendableGroup(headerSize int, msgs []proto.Message) (count, payload int, err error) {
	size := msgs[0].Size()
	if err := messageFrameError(&msgs[0], headerSize, size); err != nil {
		return 0, 0, err
	}
	total := headerSize + size
	n := 1
	for n < len(msgs) && n < proto.MaxMessagesPerBatch {
		size = msgs[n].Size()
		if !messageFrameable(&msgs[n], headerSize, size) {
			break
		}
		if total+size > int(MaxFrameSize) {
			break
		}
		total += size
		n++
	}
	return n, total, nil
}

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
	return writeFrameAt(w, payload, flags, WireVersion)
}

// writeFrameAt is WriteFrame stamping the frame version ver.
func writeFrameAt(w io.Writer, payload []byte, flags FrameFlags, ver byte) error {
	if uint32(len(payload)) > MaxFrameSize {
		return ErrPayloadTooLarge
	}
	var header [FrameHeaderSize]byte
	header[0] = MagicByte0
	header[1] = MagicByte1
	header[2] = ver
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
	return readFrameHeaderWindow(r, DefaultWireWindow())
}

// readFrameHeaderWindow is ReadFrameHeader against the accept set w.
func readFrameHeaderWindow(r io.Reader, w WireWindow) (length uint32, flags FrameFlags, err error) {
	var header [FrameHeaderSize]byte
	if _, err = io.ReadFull(r, header[:]); err != nil {
		return 0, FlagNone, err
	}
	if err = ValidateFrameHeaderWindow(header[:], w); err != nil {
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
	return ValidateFrameHeaderWindow(buf, DefaultWireWindow())
}

// ValidateFrameHeaderWindow is ValidateFrameHeader against an explicit
// accept set; the transport passes its own.
func ValidateFrameHeaderWindow(buf []byte, w WireWindow) error {
	if len(buf) < FrameHeaderSize {
		return ErrBufferTooSmall
	}
	if buf[0] != MagicByte0 || buf[1] != MagicByte1 {
		return ErrInvalidMagic
	}
	// Accept exactly the set {FrameCurrent, FramePrevious}, not a range.
	if !w.AcceptsFrame(buf[2]) {
		return ErrUnsupportedVersion
	}
	length := binary.BigEndian.Uint32(buf[4:8])
	if length > MaxFrameSize {
		return ErrFrameTooLarge
	}
	return nil
}

// MarshalFrame writes a frame into buf and returns its length, or ErrBufferTooSmall or ErrPayloadTooLarge.
func MarshalFrame(buf []byte, payload []byte, flags FrameFlags) (int, error) {
	return marshalFrameAt(buf, payload, flags, WireVersion)
}

// marshalFrameAt is MarshalFrame stamping the frame version ver.
func marshalFrameAt(buf []byte, payload []byte, flags FrameFlags, ver byte) (int, error) {
	if uint32(len(payload)) > MaxFrameSize {
		return 0, ErrPayloadTooLarge
	}
	total := FrameHeaderSize + len(payload)
	if len(buf) < total {
		return 0, ErrBufferTooSmall
	}
	buf[0] = MagicByte0
	buf[1] = MagicByte1
	buf[2] = ver
	buf[3] = byte(flags)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)))
	copy(buf[FrameHeaderSize:], payload)
	return total, nil
}
