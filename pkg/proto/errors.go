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

package proto

import (
	"errors"
	"fmt"
)

// Sentinel errors for wire format validation. These are returned by
// UnmarshalFrom methods when input data is malformed, truncated, or
// exceeds configured limits.
var (
	// ErrBufferTooSmall is returned when the input buffer is shorter
	// than the minimum required size for the type being unmarshaled.
	ErrBufferTooSmall = errors.New("proto: buffer too small")

	// ErrInvalidEntryType is returned when an Entry.Type field (masked
	// with EntryTypeMask) does not match any known entry type constant.
	ErrInvalidEntryType = errors.New("proto: invalid entry type")

	// ErrInvalidMessageType is returned when a Message.Type field does
	// not match any known message type constant.
	ErrInvalidMessageType = errors.New("proto: invalid message type")

	// ErrAddressTooLong is returned when a node address string exceeds
	// MaxAddressLength bytes during unmarshal.
	ErrAddressTooLong = errors.New("proto: address exceeds MaxAddressLength")

	// ErrNodeIdentityTooLong is returned when a committed node identity
	// string exceeds MaxNodeIdentityLength bytes during unmarshal.
	ErrNodeIdentityTooLong = errors.New("proto: node identity exceeds MaxNodeIdentityLength")

	// ErrEntrySizeLimitExceeded is returned when an Entry.Cmd length
	// exceeds MaxEntrySize during unmarshal.
	ErrEntrySizeLimitExceeded = errors.New("proto: entry size exceeds MaxEntrySize")

	// ErrBatchSizeLimitExceeded is returned when the cumulative size
	// of a MessageBatch exceeds MaxBatchSize during unmarshal.
	ErrBatchSizeLimitExceeded = errors.New("proto: batch size exceeds MaxBatchSize")

	// ErrTooManyMessages is returned when the message count in a
	// MessageBatch (MaxMessagesPerBatch) or the entry count in a Message
	// (MaxEntriesPerMessage) is exceeded during unmarshal.
	ErrTooManyMessages = errors.New("proto: message count exceeds limit")

	// ErrTooManyMembershipEntries is returned when a membership map
	// count exceeds MaxMembershipEntries during unmarshal.
	ErrTooManyMembershipEntries = errors.New("proto: membership entry count exceeds limit")

	// ErrDuplicateMapKey is returned when a membership map on the wire
	// repeats a replica ID. The encoder writes each key exactly once in
	// ascending order, so a repeat is a non-canonical encoding.
	ErrDuplicateMapKey = errors.New("proto: duplicate map key")

	// ErrInvalidConfigChangeType is returned when a ConfigChange.Type
	// field does not match any known config change type constant
	// (AddNode, RemoveNode, AddNonVoting, AddWitness).
	ErrInvalidConfigChangeType = errors.New("proto: invalid config change type")

	// ErrUnsupportedVersion is returned when a version stamp in the buffer
	// does not name the layout this build decodes. Every format is pinned at
	// version 1 pre-release and every gate is an EXACT match (see the
	// compatibility contract in the package comment), so this sentinel covers
	// both version namespaces identically:
	//
	//   - transport-scoped: MessageBatch.BinVer and SnapshotChunk.BinVer must
	//     equal WireVersion.
	//   - persisted-scoped: the Membership section's leading version byte must
	//     equal membershipFormatVersion. That case returns the richer
	//     *UnsupportedMembershipVersionError, which matches this sentinel
	//     under errors.Is.
	ErrUnsupportedVersion = errors.New("proto: unsupported wire version")

	// ErrFilepathTooLong is returned when a filepath string exceeds
	// MaxFilepathLen during unmarshal.
	ErrFilepathTooLong = errors.New("proto: filepath exceeds MaxFilepathLen")

	// ErrMetadataTooLong is returned when a metadata field exceeds
	// MaxMetadataLen during unmarshal.
	ErrMetadataTooLong = errors.New("proto: metadata exceeds MaxMetadataLen")

	// ErrChunkDataTooLong is returned when a snapshot chunk data payload
	// exceeds MaxSnapshotChunkDataLen during unmarshal.
	ErrChunkDataTooLong = errors.New("proto: chunk data exceeds MaxSnapshotChunkDataLen")

	// ErrChunkSizeMismatch is returned when a snapshot chunk's ChunkSize
	// header does not equal the actual length of its Data payload. The two
	// are written consistently by the sender, so a mismatch is a malformed
	// or tampered chunk and is rejected rather than trusted for downstream
	// allocation or offset math.
	ErrChunkSizeMismatch = errors.New("proto: chunk size does not match data length")

	// ErrInvalidSnapshotType is the sentinel for InvalidSnapshotTypeError.
	// Use errors.Is to match.
	ErrInvalidSnapshotType = errors.New("proto: invalid snapshot state machine type")

	// ErrSnapshotWithoutPosition is returned by Message.MarshalTo when the
	// embedded Snapshot carries content but has zero Index and zero Term.
	// The message layout signals snapshot presence with Snapshot.IsEmpty,
	// which asks only about the log position, so such a snapshot would encode
	// as absent and be discarded whole. It is refused instead: a raft snapshot
	// always covers a real log index, so the value is a construction bug at the
	// sender rather than anything a peer should be asked to interpret.
	ErrSnapshotWithoutPosition = errors.New("proto: snapshot has content but no log position")
)

// UnsupportedMembershipVersionError is returned when a persisted Membership
// section's leading version byte is not the one version this build decodes. It
// names the version found alongside the supported one so an operator reading a
// log line can tell the two causes apart without consulting the source: a byte
// that is a plausible version number means the data was written by a binary
// speaking a different format generation, and a byte that is not means the
// section is corrupt or the offset is wrong.
type UnsupportedMembershipVersionError struct {
	// Found is the version byte read from the buffer.
	Found uint8
}

// Error returns a human-readable description naming the version found and
// the one this build supports.
func (e *UnsupportedMembershipVersionError) Error() string {
	return fmt.Sprintf(
		"proto: unsupported membership format version %d (supported: %d)",
		e.Found, membershipFormatVersion)
}

// Is reports whether target matches the sentinel ErrUnsupportedVersion error,
// so existing errors.Is checks on the wire-version sentinel keep working.
func (e *UnsupportedMembershipVersionError) Is(target error) bool {
	return target == ErrUnsupportedVersion
}

// InvalidSnapshotTypeError is returned when a Snapshot.Type field does
// not match any known state machine type constant (RegularStateMachine,
// ConcurrentStateMachine, OnDiskStateMachine).
type InvalidSnapshotTypeError struct {
	Type uint64
}

// Error returns a human-readable description of the invalid snapshot type.
func (e *InvalidSnapshotTypeError) Error() string {
	return fmt.Sprintf("proto: invalid snapshot state machine type: %d", e.Type)
}

// Is reports whether target matches the sentinel ErrInvalidSnapshotType error.
func (e *InvalidSnapshotTypeError) Is(target error) bool {
	return target == ErrInvalidSnapshotType
}
