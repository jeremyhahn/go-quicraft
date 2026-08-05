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

// entryOverheadBytes is the fixed per-entry byte overhead charged against the
// IterateEntries maxSize budget. It is pinned to logdb.EntryOverheadBytes so
// size budgets behave identically across LogDB implementations.
const entryOverheadBytes = logdb.EntryOverheadBytes

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
	// compactedTo is the highest index removed by RemoveEntriesTo. It is the
	// node's compaction floor and outlives the entry slice: after a full
	// compaction the slice is empty but reads below the floor must still
	// report ErrCompacted so raft switches to InstallSnapshot instead of
	// retrying an ErrUnavailable range forever. waldb keeps the same floor
	// durably; this mirrors it.
	compactedTo uint64
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

	// Persist the snapshot metadata (and its membership) carried by the
	// Update, exactly as SaveSnapshot would: a follower's InstallSnapshot
	// lands here via SaveState, and dropping the metadata would make
	// GetSnapshot report ErrNoSnapshot after the install. Stale snapshots
	// are ignored, mirroring the SaveSnapshot/waldb staleness check.
	if u.Snapshot.Index > 0 && (!nd.hasSnap || u.Snapshot.Index > nd.snapshot.Index) {
		// The membership maps are deep-copied for the same reason Entry.Cmd
		// is: the caller owns them and may mutate them after the call, and the
		// stored state must never alias caller memory.
		m := copyMembership(u.Snapshot.Membership)
		nd.snapshot = u.Snapshot
		nd.snapshot.Membership = m
		nd.hasSnap = true
		nd.membership = m
	}

	// Append or overwrite entries.
	if len(u.Entries) > 0 {
		nd.entries = mergeEntries(nd.entries, u.Entries)
	}
}

// copyMembership returns a deep copy of m: the four maps are cloned so the
// stored membership never aliases the caller's. A nil map stays nil so a
// round-trip does not manufacture empty maps where the caller had none.
func copyMembership(m logdb.Membership) logdb.Membership {
	m.Addresses = copyAddrMap(m.Addresses)
	m.Observers = copyAddrMap(m.Observers)
	m.Witnesses = copyAddrMap(m.Witnesses)
	if m.Removed != nil {
		removed := make(map[uint64]bool, len(m.Removed))
		for k, v := range m.Removed {
			removed[k] = v
		}
		m.Removed = removed
	}
	return m
}

// copyAddrMap clones a replica-address map, preserving nil.
func copyAddrMap(src map[uint64]string) map[uint64]string {
	if src == nil {
		return nil
	}
	dst := make(map[uint64]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// mergeEntries merges new entries into existing sorted entries. If the first
// new entry overlaps with existing entries, the existing entries from that
// index onward are replaced (truncate-and-append semantics matching Raft).
//
// Incoming Cmd payloads are deep-copied: SaveState callers (Host.Propose)
// build entries directly over application-owned buffers and may reuse them
// after the call, so retaining the caller's backing arrays would let later
// mutations rewrite the persisted log and diverge replicas.
func mergeEntries(existing, incoming []logdb.Entry) []logdb.Entry {
	if len(existing) == 0 {
		return appendCopied(make([]logdb.Entry, 0, len(incoming)), incoming)
	}

	firstNew := incoming[0].Index

	// Find the insertion point via binary search.
	insertAt := sort.Search(len(existing), func(i int) bool {
		return existing[i].Index >= firstNew
	})

	// Truncate existing entries at the insertion point and append new ones.
	// Reuses existing slice capacity when possible (common case: sequential append).
	return appendCopied(existing[:insertAt], incoming)
}

// appendCopied appends incoming entries to dst with each Cmd deep-copied so
// the stored log never aliases the caller's buffers.
func appendCopied(dst, incoming []logdb.Entry) []logdb.Entry {
	for i := range incoming {
		e := incoming[i]
		if len(e.Cmd) > 0 {
			cmd := make([]byte, len(e.Cmd))
			copy(cmd, e.Cmd)
			e.Cmd = cmd
		}
		dst = append(dst, e)
	}
	return dst
}

// IterateEntries returns log entries in the range [low, high) for the given
// node, up to maxSize bytes total. Results are appended to the provided
// entries slice. Returns the modified entries slice and the cumulative size.
//
// Entry size is calculated as logdb.EntryOverheadBytes + len(Cmd), and the
// error semantics follow the LogDB interface contract (see logdb.LogDB).
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

	// The compaction floor is checked FIRST, before the entry slice, exactly
	// as waldb does. After a full compaction the slice is empty, and reporting
	// ErrUnavailable there would make raft retry the range forever instead of
	// falling back to InstallSnapshot.
	if low <= nd.compactedTo {
		return entries, size, logdb.ErrCompacted
	}

	if len(nd.entries) == 0 {
		return entries, size, logdb.ErrUnavailable
	}

	// Binary search for the start position.
	startIdx := sort.Search(len(nd.entries), func(i int) bool {
		return nd.entries[i].Index >= low
	})

	// Nothing stored inside [low, high): the range is simply not held.
	if startIdx >= len(nd.entries) || nd.entries[startIdx].Index >= high {
		return entries, size, logdb.ErrUnavailable
	}

	// The range holds entries but not the one at low, and low is at or above
	// the compaction floor (checked before the slice): the prefix is MISSING,
	// not compacted. The contract — and waldb — require the loud error;
	// reporting ErrCompacted told raft the range had been legitimately
	// truncated and hid the hole.
	if nd.entries[startIdx].Index != low {
		return entries, size, logdb.ErrLogGapDetected
	}

	expected := low
	for i := startIdx; i < len(nd.entries); i++ {
		e := nd.entries[i]
		if e.Index >= high {
			break
		}

		// A hole inside the requested range must fail loud, never be
		// served as a silently shortened result (Log Matching).
		if e.Index != expected {
			return entries, size, logdb.ErrLogGapDetected
		}

		entryBytes := uint64(entryOverheadBytes + len(e.Cmd))

		// Enforce maxSize: always include at least one entry, then stop
		// if adding the next entry would exceed the budget.
		if maxSize > 0 && size+entryBytes > maxSize && len(entries) > 0 {
			break
		}

		entries = append(entries, e)
		size += entryBytes
		expected++
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

// EntryRange returns the first index and the contiguous span of available
// log entries for the given node. Returns (0, 0, nil) when no entries exist.
// A stored entry count that does not match the first..last index span is a
// log hole and returns ErrLogGapDetected instead of a short length.
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
	first := nd.entries[0].Index
	last := nd.entries[len(nd.entries)-1].Index
	span := last - first + 1
	if span != uint64(len(nd.entries)) {
		return 0, 0, logdb.ErrLogGapDetected
	}
	return first, span, nil
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

	// Record the compaction floor before touching the slice, and never let it
	// go backwards. IterateEntries consults it ahead of the slice so a fully
	// compacted node still reports ErrCompacted rather than ErrUnavailable.
	if index > nd.compactedTo {
		nd.compactedTo = index
	}

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

	// The snapshot's membership becomes the node's membership, exactly as it
	// does on the SaveState path and in waldb (whose ReadState serves the
	// latest snapshot's membership); leaving it untouched made ReadState
	// report an empty membership after a snapshot install. The maps are
	// deep-copied for the same reason Entry.Cmd is: the caller owns them.
	membership := copyMembership(snapshot.Membership)
	nd.snapshot = snapshot
	nd.snapshot.Membership = membership
	nd.hasSnap = true
	nd.membership = membership
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
