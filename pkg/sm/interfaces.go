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

// StateMachine is the in-memory state machine interface; calls are serialized, and Update MUST be deterministic, since Raft cannot detect divergence (see Hasher).
type StateMachine interface {
	// Update applies committed entries all-or-nothing, writing results[i] for each entries[i]; it returns an error only for fatal failures.
	Update(ctx context.Context, entries []Entry, results []Result) error

	// Lookup reads state locally without Raft, serialized with Update; pointer queries avoid boxing allocations.
	Lookup(ctx context.Context, query interface{}) (interface{}, error)

	// SaveSnapshot writes the full state to w; ignoring stopper delays StopShard and Host.Close.
	SaveSnapshot(ctx context.Context, w io.Writer, stopper <-chan struct{}) error

	// RecoverFromSnapshot restores the state machine state from the reader,
	// replacing all existing state. The stopper channel signals cancellation.
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error

	// Close releases all resources held by the state machine.
	Close(ctx context.Context) error
}

// ConcurrentStateMachine allows Lookup during Update, with its own synchronization; PrepareSnapshot captures the state SaveSnapshot writes.
type ConcurrentStateMachine interface {
	// Update applies committed entries (all-or-nothing semantics).
	// Must be safe to call concurrently with Lookup.
	// The results slice is pre-allocated (len == len(entries)).
	Update(ctx context.Context, entries []Entry, results []Result) error

	// Lookup reads state. May be called concurrently with Update.
	// The state machine must handle its own synchronization.
	Lookup(ctx context.Context, query interface{}) (interface{}, error)

	// PrepareSnapshot returns a point-in-time context for SaveSnapshot, called on the apply worker while Update is paused.
	PrepareSnapshot() (interface{}, error)

	// SaveSnapshot writes the state PrepareSnapshot captured; ignoring stopper delays StopShard and Host.Close.
	SaveSnapshot(ctx context.Context, snapshotCtx interface{}, w io.Writer, stopper <-chan struct{}) error

	// RecoverFromSnapshot restores state from the reader, replacing all
	// existing state. The stopper channel signals cancellation.
	RecoverFromSnapshot(ctx context.Context, r io.Reader, stopper <-chan struct{}) error

	// Close releases all resources held by the state machine.
	Close(ctx context.Context) error
}

// DiskStateMachine manages its own persistent storage and must apply idempotently; its shards support no client sessions (ErrSessionsUnsupportedOnDisk).
type DiskStateMachine interface {
	// Open initializes the state machine in initialDir and returns the last applied index, 0 for a fresh start.
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

// Hasher computes an order-independent hash of whole-batch state; concurrent and on-disk implementations must synchronize GetHash with Update.
type Hasher interface {
	// GetHash returns a hash of the state machine's current state.
	GetHash() (uint64, error)
}

// NALookup provides zero-allocation byte-slice lookup for hot paths.
// When implemented, the RSM adapter uses this instead of Lookup to
// avoid heap allocation from interface{} boxing.
type NALookup interface {
	// BytesLookup returns the value stored under key.
	BytesLookup(key []byte) ([]byte, error)
}

// NALookupInto is NALookup into a caller buffer: copy into dst, return the sub-slice, retain nothing.
type NALookupInto interface {
	NALookup
	// BytesLookupInto copies the value stored under key into dst and
	// returns the populated slice.
	BytesLookupInto(key, dst []byte) ([]byte, error)
}
