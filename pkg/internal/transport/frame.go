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

	// maxPoolBufSize is the maximum buffer size returned to sync.Pool.
	// Buffers larger than this are left for GC to prevent pool pollution.
	maxPoolBufSize = 256 * 1024

	// batchFramingReserve is headroom reserved inside a frame for the
	// MessageBatch header (BinVer, DeploymentID, SourceEpoch, SourceAddress,
	// request count) and the per-message framing that sits on top of the raw
	// entry payload bytes a replication batch is sized against.
	batchFramingReserve = 64 * 1024
)

// MaxReplicationPayloadBytes is the largest raw entry payload a single
// replication batch may carry and still fit in one transport frame.
//
// A message above this bound cannot be framed and is DROPPED by the send path:
// raft does not advance the follower's next index, so it rebuilds the identical
// oversized message on every replicate cycle and the follower can never catch
// up (no backoff, no batch-size reduction, and no snapshot fallback because the
// entries are still present). Configuration that sizes replication batches —
// raft's maxReplicationPayload, i.e. Config.MaxApplyEntrySize — MUST be clamped
// to this bound.
//
// This is only the BYTE half of the contract. proto.MaxEntriesPerMessage is the
// other half: a byte-bounded batch of small entries still exceeds the decoder's
// entry-count limit, with the same starvation outcome. Both must be enforced
// where a replication message is built (see raft's limitEntryCount).
const MaxReplicationPayloadBytes = int(MaxFrameSize) - FrameHeaderSize - batchFramingReserve

// config.MaxReplicationPayloadBytes duplicates this value because this package
// imports pkg/config (so config cannot import back). This assertion fails the
// build if the two ever drift: config uses it to reject a MaxApplyEntrySize
// that would make raft build replication batches this transport cannot frame.
const _ = uint64(MaxReplicationPayloadBytes) - config.MaxReplicationPayloadBytes

// Send-side framing admission control.
//
// A frame the peer will not accept is worse than no frame at all: the peer's
// decoder rejects it and resets the stream, the sender reads that reset as a
// generic write failure, and retrying the identical batch reproduces it
// forever. The sender must therefore decide BEFORE writing whether a batch can
// be carried, splitting what can be split and dropping (not retrying, and
// never at the cost of the connection) what cannot.
//
// Sizing is done on the UNCOMPRESSED marshaled payload even though the wire
// may carry a Snappy-compressed one. The receiver's MaxDecompressedSize is
// itself capped at MaxFrameSize, so a payload that only fits after compression
// is rejected on arrival with ErrDecompressedSizeLimitExceeded — checking the
// compressed length alone (what MarshalFrame does) is not sufficient.

// batchHeaderSize returns the marshaled byte size of a MessageBatch carrying
// no requests, i.e. the per-frame overhead every group of messages pays.
// MessageBatch.Size() is the sum of this and each request's Size(), so a group
// fits a frame exactly when headerSize + sum(Size()) <= MaxFrameSize.
func batchHeaderSize(sourceAddress string) int {
	header := proto.MessageBatch{SourceAddress: sourceAddress}
	return header.Size()
}

// messageFrameError reports why msg can never be delivered in a frame, given
// its already-computed marshaled size, or nil if it can. The two conditions
// are the wire limits the peer's decoder enforces: a message that violates
// either is rejected by every peer, so emitting it can only destroy the stream
// it is written to.
func messageFrameError(msg *proto.Message, headerSize, size int) error {
	if len(msg.Entries) > proto.MaxEntriesPerMessage {
		return proto.ErrTooManyMessages
	}
	if headerSize+size > int(MaxFrameSize) {
		return ErrPayloadTooLarge
	}
	return nil
}

// messageFrameable reports whether msg can be carried by a frame at all. It is
// the predicate form of messageFrameError, for the group scan, which ends a
// group at an unframeable message rather than reporting the reason: the caller
// takes that message as the next head and drops it there, once.
func messageFrameable(msg *proto.Message, headerSize, size int) bool {
	return messageFrameError(msg, headerSize, size) == nil
}

// sendableGroup returns how many of the leading messages in msgs marshal into
// a single frame together, and the payload size that frame will have. The
// group ends at the first message that would overflow the frame, at the first
// message that can never be framed (the caller drops it as the next head), or
// at proto.MaxMessagesPerBatch messages.
//
// It returns a non-nil error only when msgs[0] itself cannot ever be framed;
// otherwise the count is >= 1, so a caller looping over the batch always makes
// progress. Each message's size is computed once here and handed to the
// marshal path, keeping the send hot path to a single sizing pass.
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
