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
//
// MIGRATION NOTE: quicraft passes an initial snapshot directory to an
// on-disk state machine's Open. Dragonboat's IOnDiskStateMachine.Open
// takes only a stop channel, so the adapter discards that directory: an
// on-disk state machine keeps managing its own storage paths exactly as
// it did on dragonboat, and does not receive quicraft's managed
// directory.
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

// snapshotFileCollection records external files added through the
// dragonboat ISnapshotFileCollection API. QuicRaft does not support
// external snapshot files: the SaveSnapshot adapters inspect the
// collection after the user state machine returns and FAIL the snapshot
// with ErrSnapshotFilesNotSupported when any file was added. Discarding
// the files instead would produce a snapshot that recovers without its
// external data — permanent, silent data loss.
type snapshotFileCollection struct {
	files []SnapshotFile
}

// AddFile records the external file so the surrounding SaveSnapshot
// adapter can fail the snapshot. QuicRaft does not support external
// snapshot files; the warning makes the subsequent snapshot failure
// attributable.
func (s *snapshotFileCollection) AddFile(fileID uint64, path string, metadata []byte) {
	s.files = append(s.files, SnapshotFile{
		FileID:   fileID,
		Filepath: path,
		Metadata: metadata,
	})
	slog.Warn("dragonboat: external snapshot files are not supported by quicraft, snapshot will fail",
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
