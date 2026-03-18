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

// Package memdb provides an in-memory implementation of the logdb.LogDB
// interface. It is suitable for testing, development, and scenarios where
// durability is not required (e.g., ephemeral nodes, benchmarks).
//
// Data is sharded by (shardID, replicaID) with per-node read-write locks
// for fine-grained concurrency. A top-level read-write lock protects the
// node map itself, while each node's data has its own lock to allow
// concurrent access to different nodes.
//
// Entries are stored in sorted slices and looked up via binary search.
// The closed state uses sync/atomic for lock-free checking on the fast path.
package memdb

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// Compile-time interface assertion.
var _ logdb.LogDB = (*MemDB)(nil)

// entryHeaderSize is the fixed overhead per entry: Index(8) + Term(8) + Type(8).
const entryHeaderSize = 24

// nodeKey uniquely identifies a replica within a shard.
type nodeKey struct {
	shardID   uint64
	replicaID uint64
}

// nodeData holds all persisted state for a single (shardID, replicaID) pair.
type nodeData struct {
	mu         sync.RWMutex
	state      logdb.State
	hasState   bool
	entries    []logdb.Entry
	snapshot   logdb.Snapshot
	hasSnap    bool
	bootstrap  logdb.Bootstrap
	hasBoot    bool
	membership logdb.Membership
}

// MemDB is an in-memory implementation of logdb.LogDB.
// It is safe for concurrent use from multiple goroutines.
type MemDB struct {
	closed atomic.Bool
	mu     sync.RWMutex
	nodes  map[nodeKey]*nodeData
}

// New creates a ready-to-use in-memory LogDB instance.
func New() *MemDB {
	return &MemDB{
		nodes: make(map[nodeKey]*nodeData),
	}
}

// Name returns the name of this LogDB implementation.
func (m *MemDB) Name() string {
	return "memory"
}

// Close marks the database as closed. All subsequent operations return
// ErrClosed. Close is idempotent.
func (m *MemDB) Close() error {
	m.closed.Store(true)
	return nil
}

// SaveState persists a batch of Updates atomically. Each Update may
// contain entries and/or hard state for a single (shardID, replicaID).
func (m *MemDB) SaveState(updates []logdb.Update) error {
	if m.closed.Load() {
		return logdb.ErrClosed
	}
	for i := range updates {
		u := &updates[i]
		nd := m.getOrCreateNode(u.ShardID, u.ReplicaID)
		nd.mu.Lock()
		m.applyUpdate(nd, u)
		nd.mu.Unlock()
	}
	return nil
}

// applyUpdate applies a single Update to a node. Caller must hold nd.mu.Lock.
func (m *MemDB) applyUpdate(nd *nodeData, u *logdb.Update) {
	// Persist hard state if non-zero.
	if u.State != (logdb.State{}) {
		nd.state = u.State
		nd.hasState = true
	}

	// Persist membership from snapshot if present.
	if u.Snapshot.Index > 0 {
		nd.membership = u.Snapshot.Membership
	}

	// Append or overwrite entries.
	if len(u.Entries) > 0 {
		nd.entries = mergeEntries(nd.entries, u.Entries)
	}
}

// mergeEntries merges new entries into existing sorted entries. If the first
// new entry overlaps with existing entries, the existing entries from that
// index onward are replaced (truncate-and-append semantics matching Raft).
func mergeEntries(existing, incoming []logdb.Entry) []logdb.Entry {
	if len(existing) == 0 {
		result := make([]logdb.Entry, len(incoming))
		copy(result, incoming)
		return result
	}

	firstNew := incoming[0].Index

	// Find the insertion point via binary search.
	insertAt := sort.Search(len(existing), func(i int) bool {
		return existing[i].Index >= firstNew
	})

	// Truncate existing entries at the insertion point and append new ones.
	// Reuses existing slice capacity when possible (common case: sequential append).
	existing = existing[:insertAt]
	return append(existing, incoming...)
}

// IterateEntries returns log entries in the range [low, high) for the given
// node, up to maxSize bytes total. Results are appended to the provided
// entries slice. Returns the modified entries slice and the cumulative size.
//
// Entry size is calculated as: 24 bytes (3 uint64 header fields) + len(Cmd).
// A maxSize of 0 means unlimited.
func (m *MemDB) IterateEntries(
	entries []logdb.Entry,
	size uint64,
	shardID uint64,
	replicaID uint64,
	low uint64,
	high uint64,
	maxSize uint64,
) ([]logdb.Entry, uint64, error) {
	if m.closed.Load() {
		return entries, size, logdb.ErrClosed
	}
	if low >= high {
		return entries, size, nil
	}

	nd := m.getNode(shardID, replicaID)
	if nd == nil {
		return entries, size, logdb.ErrUnavailable
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()

	if len(nd.entries) == 0 {
		return entries, size, logdb.ErrUnavailable
	}

	// Binary search for the start position.
	startIdx := sort.Search(len(nd.entries), func(i int) bool {
		return nd.entries[i].Index >= low
	})

	// Check if low is in the compacted range.
	if startIdx >= len(nd.entries) || nd.entries[startIdx].Index > low {
		// The exact entry at 'low' was not found. If low is below the
		// first available entry, it has been compacted.
		if startIdx == 0 && nd.entries[0].Index > low {
			return entries, size, logdb.ErrCompacted
		}
		return entries, size, logdb.ErrUnavailable
	}

	for i := startIdx; i < len(nd.entries); i++ {
		e := nd.entries[i]
		if e.Index >= high {
			break
		}

		entryBytes := uint64(entryHeaderSize + len(e.Cmd))

		// Enforce maxSize: always include at least one entry, then stop
		// if adding the next entry would exceed the budget.
		if maxSize > 0 && size+entryBytes > maxSize && len(entries) > 0 {
			break
		}

		entries = append(entries, e)
		size += entryBytes
	}

	return entries, size, nil
}

// ReadState reads the persisted hard state and membership for the given node.
// Returns ErrNoState if no state has been saved.
func (m *MemDB) ReadState(shardID uint64, replicaID uint64) (logdb.State, logdb.Membership, error) {
	if m.closed.Load() {
		return logdb.State{}, logdb.Membership{}, logdb.ErrClosed
	}

	nd := m.getNode(shardID, replicaID)
	if nd == nil {
		return logdb.State{}, logdb.Membership{}, logdb.ErrNoState
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()

	if !nd.hasState {
		return logdb.State{}, logdb.Membership{}, logdb.ErrNoState
	}

	return nd.state, nd.membership, nil
}

// EntryRange returns the first index and total count of available log
// entries for the given node. Returns (0, 0, nil) when no entries exist.
func (m *MemDB) EntryRange(shardID uint64, replicaID uint64) (uint64, uint64, error) {
	if m.closed.Load() {
		return 0, 0, logdb.ErrClosed
	}

	nd := m.getNode(shardID, replicaID)
	if nd == nil {
		return 0, 0, nil
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()

	if len(nd.entries) == 0 {
		return 0, 0, nil
	}
	return nd.entries[0].Index, uint64(len(nd.entries)), nil
}

// RemoveEntriesTo removes log entries up to and including the given index
// for the specified node. Used for log compaction.
func (m *MemDB) RemoveEntriesTo(shardID uint64, replicaID uint64, index uint64) error {
	if m.closed.Load() {
		return logdb.ErrClosed
	}

	nd := m.getNode(shardID, replicaID)
	if nd == nil {
		return nil
	}

	nd.mu.Lock()
	defer nd.mu.Unlock()

	if len(nd.entries) == 0 {
		return nil
	}

	// Find the first entry with Index > index.
	cutIdx := sort.Search(len(nd.entries), func(i int) bool {
		return nd.entries[i].Index > index
	})

	if cutIdx == 0 {
		return nil
	}

	// Compact: copy remaining entries to a new slice to release old memory.
	remaining := len(nd.entries) - cutIdx
	if remaining == 0 {
		nd.entries = nil
		return nil
	}
	compacted := make([]logdb.Entry, remaining)
	copy(compacted, nd.entries[cutIdx:])
	nd.entries = compacted

	return nil
}

// Compact is a no-op for the in-memory implementation since there is
// no underlying storage to compact.
func (m *MemDB) Compact(_ uint64, _ uint64) error {
	if m.closed.Load() {
		return logdb.ErrClosed
	}
	return nil
}

// SaveSnapshot persists snapshot metadata for the given node. Returns
// ErrSnapshotOutOfDate if the snapshot is older than the existing one.
func (m *MemDB) SaveSnapshot(shardID uint64, replicaID uint64, snapshot logdb.Snapshot) error {
	if m.closed.Load() {
		return logdb.ErrClosed
	}

	nd := m.getOrCreateNode(shardID, replicaID)

	nd.mu.Lock()
	defer nd.mu.Unlock()

	if nd.hasSnap && snapshot.Index <= nd.snapshot.Index {
		return logdb.ErrSnapshotOutOfDate
	}

	nd.snapshot = snapshot
	nd.hasSnap = true
	return nil
}

// GetSnapshot returns the most recent snapshot metadata for the given node.
// Returns ErrNoSnapshot if no snapshot has been saved.
func (m *MemDB) GetSnapshot(shardID uint64, replicaID uint64) (logdb.Snapshot, error) {
	if m.closed.Load() {
		return logdb.Snapshot{}, logdb.ErrClosed
	}

	nd := m.getNode(shardID, replicaID)
	if nd == nil {
		return logdb.Snapshot{}, logdb.ErrNoSnapshot
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()

	if !nd.hasSnap {
		return logdb.Snapshot{}, logdb.ErrNoSnapshot
	}

	return nd.snapshot, nil
}

// RemoveNodeData removes all data for the given node.
func (m *MemDB) RemoveNodeData(shardID uint64, replicaID uint64) error {
	if m.closed.Load() {
		return logdb.ErrClosed
	}

	key := nodeKey{shardID: shardID, replicaID: replicaID}

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.nodes, key)
	return nil
}

// ListNodeInfo returns all NodeInfo entries in the LogDB.
func (m *MemDB) ListNodeInfo() ([]logdb.NodeInfo, error) {
	if m.closed.Load() {
		return nil, logdb.ErrClosed
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]logdb.NodeInfo, 0, len(m.nodes))
	for k := range m.nodes {
		result = append(result, logdb.NodeInfo{
			ShardID:   k.shardID,
			ReplicaID: k.replicaID,
		})
	}
	return result, nil
}

// SaveBootstrap persists bootstrap information for a node.
func (m *MemDB) SaveBootstrap(shardID uint64, replicaID uint64, bootstrap logdb.Bootstrap) error {
	if m.closed.Load() {
		return logdb.ErrClosed
	}

	nd := m.getOrCreateNode(shardID, replicaID)

	nd.mu.Lock()
	defer nd.mu.Unlock()

	nd.bootstrap = bootstrap
	nd.hasBoot = true
	return nil
}

// GetBootstrap returns the bootstrap information for a node. The bool
// return value indicates whether bootstrap info was found.
func (m *MemDB) GetBootstrap(shardID uint64, replicaID uint64) (logdb.Bootstrap, bool, error) {
	if m.closed.Load() {
		return logdb.Bootstrap{}, false, logdb.ErrClosed
	}

	nd := m.getNode(shardID, replicaID)
	if nd == nil {
		return logdb.Bootstrap{}, false, nil
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()

	if !nd.hasBoot {
		return logdb.Bootstrap{}, false, nil
	}

	return nd.bootstrap, true, nil
}

// getNode returns the nodeData for the given key, or nil if it does not exist.
func (m *MemDB) getNode(shardID, replicaID uint64) *nodeData {
	key := nodeKey{shardID: shardID, replicaID: replicaID}

	m.mu.RLock()
	nd := m.nodes[key]
	m.mu.RUnlock()

	return nd
}

// getOrCreateNode returns the nodeData for the given key, creating it if
// it does not exist.
func (m *MemDB) getOrCreateNode(shardID, replicaID uint64) *nodeData {
	key := nodeKey{shardID: shardID, replicaID: replicaID}

	// Fast path: check under read lock.
	m.mu.RLock()
	nd := m.nodes[key]
	m.mu.RUnlock()

	if nd != nil {
		return nd
	}

	// Slow path: create under write lock.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock.
	nd = m.nodes[key]
	if nd != nil {
		return nd
	}

	nd = &nodeData{}
	m.nodes[key] = nd
	return nd
}
