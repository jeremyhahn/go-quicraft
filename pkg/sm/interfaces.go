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

package sm

import (
	"context"
	"io"
)

// StateMachine is the basic in-memory state machine interface. All state
// fits in memory and snapshots serialize the full state. QuicRaft serializes
// all method calls -- Update and Lookup are never called concurrently.
//
// All methods accept context.Context for cancellation propagation, deadline
// enforcement, and distributed tracing.
type StateMachine interface {
	// Update applies committed entries to the state machine. The results
	// slice is pre-allocated by QuicRaft with len(results) == len(entries).
	// The state machine MUST write results[i] for each entries[i].
	//
	// Update MUST be all-or-nothing: either all entries are applied and all
	// results written, or the state machine reverts to its pre-call state
	// and returns an error. Partial application is not supported.
	//
	// Returns error only for fatal, non-recoverable failures.
	Update(ctx context.Context, entries []Entry, results []Result) error

	// Lookup reads state without going through Raft (local read).
	// For StateMachine, Lookup is serialized with Update by QuicRaft.
	//
	// Performance note: use pointer types as queries to avoid heap
	// allocation from interface boxing on hot paths.
	Lookup(ctx context.Context, query interface{}) (interface{}, error)

	// SaveSnapshot writes the entire state machine state to the writer.
	// The stopper channel signals cancellation; the implementation should
	// periodically check it and return early if closed.
	SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error

	// RecoverFromSnapshot restores the state machine state from the reader,
	// replacing all existing state. The stopper channel signals cancellation.
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error

	// Close releases all resources held by the state machine.
	Close(ctx context.Context) error
}

// ConcurrentStateMachine is for state machines that support concurrent
// read access during writes. Lookup may be called from any goroutine
// while Update runs on the apply worker. The state machine is responsible
// for its own internal synchronization (e.g., sync.RWMutex, COW snapshots).
//
// PrepareSnapshot captures a point-in-time snapshot context that is later
// passed to SaveSnapshot. This two-phase approach allows the snapshot to
// proceed without blocking Update.
type ConcurrentStateMachine interface {
	// Update applies committed entries (all-or-nothing semantics).
	// Must be safe to call concurrently with Lookup.
	// The results slice is pre-allocated (len == len(entries)).
	Update(ctx context.Context, entries []Entry, results []Result) error

	// Lookup reads state. May be called concurrently with Update.
	// The state machine must handle its own synchronization.
	Lookup(ctx context.Context, query interface{}) (interface{}, error)

	// PrepareSnapshot returns an opaque context capturing a point-in-time
	// view of the state machine for snapshot purposes. The returned value
	// is passed to SaveSnapshot. This method is called on the apply worker
	// goroutine while Update is paused.
	PrepareSnapshot() (interface{}, error)

	// SaveSnapshot writes the state captured by PrepareSnapshot to the writer.
	// The snapshotCtx parameter is the value returned by PrepareSnapshot.
	// The stopper channel signals cancellation.
	SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error

	// RecoverFromSnapshot restores state from the reader, replacing all
	// existing state. The stopper channel signals cancellation.
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error

	// Close releases all resources held by the state machine.
	Close(ctx context.Context) error
}

// DiskStateMachine is for state machines backed by their own persistent
// storage (e.g., PebbleDB, SQLite). Snapshots transfer the database files.
// The state machine manages its own persistence; QuicRaft manages consensus.
//
// On-disk state machines may receive entries that were already applied before
// a crash. The implementation MUST handle this idempotently.
type DiskStateMachine interface {
	// Open initializes the state machine and returns the last applied index.
	// If this is a fresh start, returns 0. The initialDir is the working
	// directory assigned by QuicRaft for persistent storage. The stopper
	// channel signals cancellation.
	Open(ctx context.Context, initialDir string, stopper <-chan struct{}) (uint64, error)

	// Update applies committed entries (all-or-nothing semantics).
	// The results slice is pre-allocated (len == len(entries)).
	Update(ctx context.Context, entries []Entry, results []Result) error

	// Lookup reads state. May be called concurrently with Update.
	// The backing database must handle concurrent reads during writes.
	Lookup(ctx context.Context, query interface{}) (interface{}, error)

	// Sync flushes pending writes to durable storage. Called before
	// snapshot operations to ensure consistency.
	Sync() error

	// PrepareSnapshot returns an opaque context capturing a point-in-time
	// view for snapshot purposes. Called after Sync.
	PrepareSnapshot() (interface{}, error)

	// SaveSnapshot writes the on-disk state to the writer using the
	// context from PrepareSnapshot. For file-based databases, this
	// typically copies or hardlinks the database files.
	SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error

	// RecoverFromSnapshot restores the on-disk state from the reader,
	// replacing the current database files. The stopper channel signals
	// cancellation.
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error

	// Close releases resources and flushes any pending writes.
	Close(ctx context.Context) error
}

// Hasher computes a deterministic hash of state machine state for
// cross-replica consistency verification. Detected via type assertion.
type Hasher interface {
	GetHash() (uint64, error)
}

// NALookup provides zero-allocation byte-slice lookup for hot paths.
// When implemented, the RSM adapter uses this instead of Lookup to
// avoid heap allocation from interface{} boxing.
type NALookup interface {
	BytesLookup(key []byte) ([]byte, error)
}

// NALookupInto is an optional extension to NALookup that allows the caller
// to provide a destination buffer. The implementation MUST copy the value
// into dst (growing it via append if necessary) and return the populated
// sub-slice. Ownership of the returned slice belongs to the caller; the
// implementation MUST NOT retain a reference to it after returning.
//
// When implemented alongside NALookup, the RSM adapter prefers this method
// on the pooled read path to eliminate per-call heap allocation.
type NALookupInto interface {
	NALookup
	BytesLookupInto(key, dst []byte) ([]byte, error)
}
