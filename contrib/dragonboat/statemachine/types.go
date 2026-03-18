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

// Package statemachine provides a dragonboat-compatible state machine API
// layer that adapts dragonboat's single-entry Update convention to
// QuicRaft's batch-oriented state machine interfaces. Users migrating from
// dragonboat implement the familiar IStateMachine, IConcurrentStateMachine,
// or IOnDiskStateMachine interfaces and use the Wrap*CreateFunc helpers to
// produce a quicraft sm.CreateFunc.
package statemachine

import "log/slog"

// Entry represents a Raft log entry applied to the state machine.
// In dragonboat's convention, the Result field is populated in-place
// by the Update method.
type Entry struct {
	// Index is the Raft log index of this entry.
	Index uint64

	// Cmd is the proposal payload.
	Cmd []byte

	// Result is populated by Update to communicate the outcome of
	// applying this entry.
	Result Result
}

// Result is the outcome of a state machine Update operation.
type Result struct {
	// Value is an application-defined result code.
	Value uint64

	// Data carries optional result payload.
	Data []byte
}

// SnapshotFile represents an external file included in a snapshot.
type SnapshotFile struct {
	// FileID is the application-defined identifier for this file.
	FileID uint64

	// Filepath is the absolute path to the file on disk.
	Filepath string

	// Metadata is application-defined metadata about this file.
	Metadata []byte
}

// ISnapshotFileCollection manages external snapshot files during
// snapshot creation.
type ISnapshotFileCollection interface {
	// AddFile registers an external file to be included in the snapshot.
	AddFile(fileID uint64, path string, metadata []byte)
}

// snapshotFileCollection is a no-op implementation of ISnapshotFileCollection.
// QuicRaft does not support external snapshot files, so this adapter
// silently discards any files added through the dragonboat API.
type snapshotFileCollection struct{}

// AddFile logs a warning and discards the file. QuicRaft does not support
// external snapshot files; callers are warned so silent data loss is avoided.
func (s *snapshotFileCollection) AddFile(fileID uint64, path string, metadata []byte) {
	slog.Warn("dragonboat: external snapshot files are not supported by quicraft, file discarded",
		"fileID", fileID,
		"path", path,
		"metadataLen", len(metadata),
	)
}

// Type identifies the state machine variant.
type Type uint64

const (
	// RegularStateMachine identifies an in-memory state machine where all
	// operations are serialized by the framework.
	RegularStateMachine Type = 1

	// ConcurrentStateMachine identifies a state machine that supports
	// concurrent reads during writes.
	ConcurrentStateMachine Type = 2

	// OnDiskStateMachine identifies a state machine backed by its own
	// persistent storage.
	OnDiskStateMachine Type = 3
)
