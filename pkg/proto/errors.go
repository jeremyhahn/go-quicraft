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

	// ErrUnsupportedVersion is returned when a version stamp is not the one this build decodes: BinVer against WireVersion, or the Membership version byte (as *UnsupportedMembershipVersionError).
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

	// ErrChunkSizeMismatch is returned when a snapshot chunk's ChunkSize does not equal len(Data).
	ErrChunkSizeMismatch = errors.New("proto: chunk size does not match data length")

	// ErrSnapshotWithoutPosition is returned by Message.MarshalTo for a snapshot with content but zero Index and Term.
	ErrSnapshotWithoutPosition = errors.New("proto: snapshot has content but no log position")

	// ErrLogIndexOutOfRange is returned by Snapshot.CheckBounds when the
	// snapshot's Index is at or above MaxLogIndex, and by raft when persisted
	// state names such an index. See MaxLogIndex.
	ErrLogIndexOutOfRange = errors.New("proto: log index at or above MaxLogIndex")

	// ErrTermOutOfRange is returned for a term at or above MaxTerm, or a replicated entry term of 0 or above the leader's. See MaxTerm.
	ErrTermOutOfRange = errors.New("proto: term out of range")
)

// UnsupportedMembershipVersionError reports a Membership version byte other than the supported one, naming both.
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
