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

package statemachine

import "io"

// IStateMachine is the dragonboat-compatible interface for regular in-memory
// state machines. All operations are serialized by the framework. Update
// processes a single entry and returns its result.
type IStateMachine interface {
	// Update applies a single committed entry and returns its result.
	Update(entry Entry) (Result, error)

	// Lookup reads state without going through Raft consensus.
	Lookup(query interface{}) (interface{}, error)

	// SaveSnapshot writes the entire state to the writer. The fc parameter
	// allows registering external snapshot files (no-op in QuicRaft).
	// The stopper channel signals cancellation.
	SaveSnapshot(w io.Writer, fc ISnapshotFileCollection, stopper <-chan struct{}) error

	// RecoverFromSnapshot restores state from the reader, replacing all
	// existing state. The files parameter contains external snapshot files
	// (always nil in QuicRaft). The stopper channel signals cancellation.
	RecoverFromSnapshot(r io.Reader, files []SnapshotFile, stopper <-chan struct{}) error

	// Close releases all resources held by the state machine.
	Close() error
}

// IConcurrentStateMachine is the dragonboat-compatible interface for state
// machines that support concurrent read access during writes. Lookup may
// be called from any goroutine while Update runs.
type IConcurrentStateMachine interface {
	// Update applies a batch of committed entries. The implementation
	// populates each Entry.Result field in-place and returns the updated
	// entries slice.
	Update(entries []Entry) ([]Entry, error)

	// Lookup reads state. May be called concurrently with Update.
	Lookup(query interface{}) (interface{}, error)

	// PrepareSnapshot captures a point-in-time snapshot context.
	PrepareSnapshot() (interface{}, error)

	// SaveSnapshot writes the state captured by PrepareSnapshot to the writer.
	// The ctx parameter is the value returned by PrepareSnapshot.
	SaveSnapshot(ctx interface{}, w io.Writer, fc ISnapshotFileCollection, stopper <-chan struct{}) error

	// RecoverFromSnapshot restores state from the reader, replacing all
	// existing state.
	RecoverFromSnapshot(r io.Reader, files []SnapshotFile, stopper <-chan struct{}) error

	// Close releases all resources held by the state machine.
	Close() error
}

// IOnDiskStateMachine is the dragonboat-compatible interface for state
// machines backed by their own persistent storage.
type IOnDiskStateMachine interface {
	// Open initializes the on-disk state machine and returns the last
	// applied Raft log index. The stopper channel signals cancellation.
	Open(stopper <-chan struct{}) (uint64, error)

	// Update applies a batch of committed entries. The implementation
	// populates each Entry.Result field in-place and returns the updated
	// entries slice.
	Update(entries []Entry) ([]Entry, error)

	// Lookup reads state. May be called concurrently with Update.
	Lookup(query interface{}) (interface{}, error)

	// Sync flushes pending writes to durable storage.
	Sync() error

	// PrepareSnapshot captures a point-in-time snapshot context.
	PrepareSnapshot() (interface{}, error)

	// SaveSnapshot writes the on-disk state to the writer using the
	// context from PrepareSnapshot. The stopper channel signals cancellation.
	SaveSnapshot(ctx interface{}, w io.Writer, stopper <-chan struct{}) error

	// RecoverFromSnapshot restores the on-disk state from the reader.
	RecoverFromSnapshot(r io.Reader, stopper <-chan struct{}) error

	// Close releases resources and flushes any pending writes.
	Close() error
}

// IHash is an optional interface that state machines can implement to
// provide a hash of their current state for consistency verification.
type IHash interface {
	// GetHash returns a hash of the current state machine state.
	GetHash() (uint64, error)
}

// IExtended is an optional interface that state machines can implement to
// support linearizable reads without going through Raft consensus.
type IExtended interface {
	// NALookup performs a read that does not require agreement from the
	// Raft cluster.
	NALookup([]byte) ([]byte, error)
}

// CreateStateMachineFunc creates an IStateMachine instance for the given
// shard and replica.
type CreateStateMachineFunc func(shardID, replicaID uint64) IStateMachine

// CreateConcurrentStateMachineFunc creates an IConcurrentStateMachine
// instance for the given shard and replica.
type CreateConcurrentStateMachineFunc func(shardID, replicaID uint64) IConcurrentStateMachine

// CreateOnDiskStateMachineFunc creates an IOnDiskStateMachine instance
// for the given shard and replica.
type CreateOnDiskStateMachineFunc func(shardID, replicaID uint64) IOnDiskStateMachine
