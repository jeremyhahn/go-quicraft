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

	// ErrEntrySizeLimitExceeded is returned when an Entry.Cmd length
	// exceeds MaxEntrySize during unmarshal.
	ErrEntrySizeLimitExceeded = errors.New("proto: entry size exceeds MaxEntrySize")

	// ErrBatchSizeLimitExceeded is returned when the cumulative size
	// of a MessageBatch exceeds MaxBatchSize during unmarshal.
	ErrBatchSizeLimitExceeded = errors.New("proto: batch size exceeds MaxBatchSize")

	// ErrTooManyMessages is returned when the message count in a
	// MessageBatch exceeds maxMessagesPerBatch during unmarshal.
	ErrTooManyMessages = errors.New("proto: message count exceeds limit")

	// ErrTooManyMembershipEntries is returned when a membership map
	// count exceeds maxMembershipEntries during unmarshal.
	ErrTooManyMembershipEntries = errors.New("proto: membership entry count exceeds limit")

	// ErrInvalidConfigChangeType is returned when a ConfigChange.Type
	// field does not match any known config change type constant
	// (AddNode, RemoveNode, AddNonVoting, AddWitness).
	ErrInvalidConfigChangeType = errors.New("proto: invalid config change type")

	// ErrUnsupportedVersion is returned when the wire version in the
	// buffer exceeds the current WireVersion.
	ErrUnsupportedVersion = errors.New("proto: unsupported wire version")

	// ErrFilepathTooLong is returned when a filepath string exceeds
	// MaxFilepathLen during unmarshal.
	ErrFilepathTooLong = errors.New("proto: filepath exceeds MaxFilepathLen")

	// ErrChecksumTooLong is returned when a checksum field exceeds
	// MaxChecksumLen during unmarshal.
	ErrChecksumTooLong = errors.New("proto: checksum exceeds MaxChecksumLen")

	// ErrMetadataTooLong is returned when a metadata field exceeds
	// MaxMetadataLen during unmarshal.
	ErrMetadataTooLong = errors.New("proto: metadata exceeds MaxMetadataLen")

	// ErrChunkDataTooLong is returned when a snapshot chunk data payload
	// exceeds MaxSnapshotChunkDataLen during unmarshal.
	ErrChunkDataTooLong = errors.New("proto: chunk data exceeds MaxSnapshotChunkDataLen")

	// ErrInvalidSnapshotType is the sentinel for InvalidSnapshotTypeError.
	// Use errors.Is to match.
	ErrInvalidSnapshotType = errors.New("proto: invalid snapshot state machine type")
)

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
