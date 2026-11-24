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

// Package memdb provides an in-memory logdb.LogDB for tests, benchmarks and ephemeral nodes, with per-node locks and sorted entry slices.
package memdb

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// Compile-time interface assertions.
var _ logdb.LogDB = (*MemDB)(nil)
var _ logdb.AtRestProtectedLogDB = (*MemDB)(nil)

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
	// compactedTo is the node's compaction floor; reads below it return ErrCompacted even after the slice empties.
	compactedTo uint64
}

// MemDB is an in-memory implementation of logdb.LogDB.
// It is safe for concurrent use from multiple goroutines.
type MemDB struct {
	closed atomic.Bool
	// removalMu keeps each write whole against RemoveNodeData and Close. Lock order: removalMu, a node's mu, then mu.
	removalMu sync.RWMutex
	mu        sync.RWMutex
	nodes     map[nodeKey]*nodeData
	removed   map[nodeKey]struct{} // retired by RemoveNodeData; see NodeRemoved
}

// beginWrite read-locks removalMu and checks the database is open; the caller releases with removalMu.RUnlock.
func (m *MemDB) beginWrite() error {
	m.removalMu.RLock()
	if m.closed.Load() {
		m.removalMu.RUnlock()
		return logdb.ErrClosed
	}
	return nil
}

// New creates a ready-to-use in-memory LogDB instance.
func New() *MemDB {
	return &MemDB{
		nodes:   make(map[nodeKey]*nodeData),
		removed: make(map[nodeKey]struct{}),
	}
}

// Name returns the name of this LogDB implementation.
func (m *MemDB) Name() string {
	return "memory"
}

// AtRestProtection reports that this LogDB writes nothing to disk, so the at-rest guarantee holds vacuously.
func (m *MemDB) AtRestProtection() logdb.AtRestProtection {
	return logdb.AtRestNonDurable
}

// Close marks the database closed and releases every node's data; later operations return ErrClosed. Close is idempotent.
func (m *MemDB) Close() error {
	m.closed.Store(true)
	// Wait out every write already in flight so none is split by the release
	// below; any write that starts now sees the closed mark (beginWrite).
	m.removalMu.Lock()
	defer m.removalMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = make(map[nodeKey]*nodeData)
	return nil
}

// SaveState persists a batch of Updates atomically. Each Update may
// contain entries and/or hard state for a single (shardID, replicaID).
func (m *MemDB) SaveState(updates []logdb.Update) error {
	if m.closed.Load() {
		return logdb.ErrClosed
	}
	if err := m.beginWrite(); err != nil {
		return err
	}
	defer m.removalMu.RUnlock()
	// Validate the whole batch before applying any of it, keeping SaveState atomic.
	for i := range updates {
		u := &updates[i]
		if m.retired(u.ShardID, u.ReplicaID) {
			return logdb.ErrReplicaRetired
		}
		// A snapshot travels through SaveState as well as SaveSnapshot, and
		// the protocol bound has to hold on both doors (see SaveSnapshot).
		if u.Snapshot.Index > 0 {
			if err := u.Snapshot.CheckBounds(); err != nil {
				return err
			}
		}
		if len(u.Entries) == 0 {
			continue
		}
		if nd := m.getNode(u.ShardID, u.ReplicaID); nd != nil {
			nd.mu.RLock()
			floor := nd.compactedTo
			nd.mu.RUnlock()
			if first := u.Entries[0].Index; first <= floor {
				return &logdb.EntriesBelowCompactionFloorError{
					ShardID:     u.ShardID,
					ReplicaID:   u.ReplicaID,
					FirstIndex:  first,
					CompactedTo: floor,
				}
			}
		}
	}

	for i := range updates {
		u := &updates[i]
		// The error reports the node was removed before its lock was taken; it must be returned.
		if err := m.withNode(u.ShardID, u.ReplicaID, func(nd *nodeData) error {
			m.applyUpdate(nd, u)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// withNode runs fn under the node's lock after confirming the map still holds that node; a removed node returns logdb.ErrReplicaRetired. Lock order: nd.mu then m.mu.
func (m *MemDB) withNode(shardID, replicaID uint64, fn func(nd *nodeData) error) error {
	key := nodeKey{shardID: shardID, replicaID: replicaID}
	for {
		if m.retired(shardID, replicaID) {
			return logdb.ErrReplicaRetired
		}
		nd := m.getOrCreateNode(shardID, replicaID)
		nd.mu.Lock()
		m.mu.RLock()
		live := m.nodes[key] == nd
		_, retired := m.removed[key]
		m.mu.RUnlock()
		if retired {
			// RemoveNodeData retired the node while this write waited for its
			// lock: the removal wins, and the write is refused rather than
			// stored for a replica ID that is never used again.
			nd.mu.Unlock()
			return logdb.ErrReplicaRetired
		}
		if !live {
			nd.mu.Unlock()
			continue
		}
		err := fn(nd)
		nd.mu.Unlock()
		return err
	}
}

// applyUpdate applies a single Update to a node. Caller must hold nd.mu.Lock.
func (m *MemDB) applyUpdate(nd *nodeData, u *logdb.Update) {
	// Persist hard state if non-zero.
	if u.State != (logdb.State{}) {
		nd.state = u.State
		nd.hasState = true
	}

	// Persist the Update's snapshot metadata and membership as SaveSnapshot would, ignoring stale snapshots.
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

// copyMembership returns a deep copy of m, cloning all maps including Identities; nil maps stay nil.
func copyMembership(m logdb.Membership) logdb.Membership {
	m.Addresses = copyAddrMap(m.Addresses)
	m.Observers = copyAddrMap(m.Observers)
	m.Witnesses = copyAddrMap(m.Witnesses)
	m.Identities = copyAddrMap(m.Identities)
	m.RemovedIdentities = copyAddrMap(m.RemovedIdentities)
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

// mergeEntries merges new entries into sorted existing ones with truncate-and-append semantics, deep-copying Cmd.
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
	merged := appendCopied(existing[:insertAt], incoming)

	// Clear the slots past the new length so dropped entries' payloads can be collected.
	if n := len(merged); n < len(existing) {
		clear(existing[n:])
	}
	return merged
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

// IterateEntries appends entries in [low, high) up to maxSize bytes (0 is unlimited) and returns them with the total size.
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

	nd, err := m.readNode(shardID, replicaID)
	if err != nil {
		return entries, size, err
	}
	if nd == nil {
		return entries, size, logdb.ErrUnavailable
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()

	// Check the compaction floor first, as waldb does.
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

	// A missing prefix at or above the floor is a gap, not compaction.
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

		// Copy each entry and its payload out of the live log (see logdb.CopyEntryCmd).
		entries = append(entries, logdb.CopyEntryCmd(e))
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

	nd, err := m.readNode(shardID, replicaID)
	if err != nil {
		return logdb.State{}, logdb.Membership{}, err
	}
	if nd == nil {
		return logdb.State{}, logdb.Membership{}, logdb.ErrNoState
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()

	if !nd.hasState {
		return logdb.State{}, logdb.Membership{}, logdb.ErrNoState
	}

	// A copy, as in GetSnapshot: the stored membership is shared with the
	// stored snapshot, so an edit through it would rewrite both.
	return nd.state, copyMembership(nd.membership), nil
}

// EntryRange returns the first index and contiguous length of stored entries, (0, 0, nil) when empty, or ErrLogGapDetected on a hole.
func (m *MemDB) EntryRange(shardID uint64, replicaID uint64) (uint64, uint64, error) {
	if m.closed.Load() {
		return 0, 0, logdb.ErrClosed
	}

	nd, err := m.readNode(shardID, replicaID)
	if err != nil {
		return 0, 0, err
	}
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
	if err := m.beginWrite(); err != nil {
		return err
	}
	defer m.removalMu.RUnlock()
	if m.retired(shardID, replicaID) {
		return logdb.ErrReplicaRetired
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
	if err := m.beginWrite(); err != nil {
		return err
	}
	defer m.removalMu.RUnlock()

	// A snapshot at index 0 covers nothing; refuse it.
	if snapshot.Index == 0 {
		return logdb.ErrSnapshotOutOfDate
	}
	// Refuse a position at or above proto.MaxLogIndex or proto.MaxTerm before it becomes durable.
	if err := snapshot.CheckBounds(); err != nil {
		return err
	}

	return m.withNode(shardID, replicaID, func(nd *nodeData) error {
		if nd.hasSnap && snapshot.Index <= nd.snapshot.Index {
			return logdb.ErrSnapshotOutOfDate
		}

		// The snapshot's membership becomes the node's membership, deep-copied.
		membership := copyMembership(snapshot.Membership)
		nd.snapshot = snapshot
		nd.snapshot.Membership = membership
		nd.hasSnap = true
		nd.membership = membership
		return nil
	})
}

// GetSnapshot returns the most recent snapshot metadata for the given node.
// Returns ErrNoSnapshot if no snapshot has been saved.
func (m *MemDB) GetSnapshot(shardID uint64, replicaID uint64) (logdb.Snapshot, error) {
	if m.closed.Load() {
		return logdb.Snapshot{}, logdb.ErrClosed
	}

	nd, err := m.readNode(shardID, replicaID)
	if err != nil {
		return logdb.Snapshot{}, err
	}
	if nd == nil {
		return logdb.Snapshot{}, logdb.ErrNoSnapshot
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()

	if !nd.hasSnap {
		return logdb.Snapshot{}, logdb.ErrNoSnapshot
	}

	// The maps are copied on the way out for the same reason they are copied
	// on the way in: what the caller gets is the caller's to change, and an
	// edit must never reach the stored record (as in waldb).
	snap := nd.snapshot
	snap.Membership = copyMembership(nd.snapshot.Membership)
	return snap, nil
}

// RemoveNodeData removes all data for the given node and retires its replica
// ID (NodeRemoved).
func (m *MemDB) RemoveNodeData(shardID uint64, replicaID uint64) error {
	if m.closed.Load() {
		return logdb.ErrClosed
	}

	key := nodeKey{shardID: shardID, replicaID: replicaID}

	// Exclusive against every writer (removalMu), so no write is half-applied
	// across the removal.
	m.removalMu.Lock()
	defer m.removalMu.Unlock()
	if m.closed.Load() {
		return logdb.ErrClosed
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.nodes, key)
	m.removed[key] = struct{}{}
	return nil
}

// NodeRemoved reports whether RemoveNodeData has removed the node, and so
// retired its replica ID.
func (m *MemDB) NodeRemoved(shardID uint64, replicaID uint64) (bool, error) {
	if m.closed.Load() {
		return false, logdb.ErrClosed
	}
	return m.retired(shardID, replicaID), nil
}

// retired reports whether RemoveNodeData retired the node. Every writer
// refuses a retired node (logdb.ErrReplicaRetired).
func (m *MemDB) retired(shardID, replicaID uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, removed := m.removed[nodeKey{shardID: shardID, replicaID: replicaID}]
	return removed
}

// ListNodeInfo returns all NodeInfo entries in the LogDB.
func (m *MemDB) ListNodeInfo() ([]logdb.NodeInfo, error) {
	if m.closed.Load() {
		return nil, logdb.ErrClosed
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	// Again under mu: a Close between the check above and the lock has
	// released the map, and listing it would report no nodes (see readNode).
	if m.closed.Load() {
		return nil, logdb.ErrClosed
	}

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
	if err := m.beginWrite(); err != nil {
		return err
	}
	defer m.removalMu.RUnlock()

	// Deep-copy the address map; the caller owns it.
	bootstrap.Addresses = copyAddrMap(bootstrap.Addresses)

	return m.withNode(shardID, replicaID, func(nd *nodeData) error {
		nd.bootstrap = bootstrap
		nd.hasBoot = true
		return nil
	})
}

// GetBootstrap returns the bootstrap information for a node. The bool
// return value indicates whether bootstrap info was found.
func (m *MemDB) GetBootstrap(shardID uint64, replicaID uint64) (logdb.Bootstrap, bool, error) {
	if m.closed.Load() {
		return logdb.Bootstrap{}, false, logdb.ErrClosed
	}

	nd, err := m.readNode(shardID, replicaID)
	if err != nil {
		return logdb.Bootstrap{}, false, err
	}
	if nd == nil {
		return logdb.Bootstrap{}, false, nil
	}

	nd.mu.RLock()
	defer nd.mu.RUnlock()

	if !nd.hasBoot {
		return logdb.Bootstrap{}, false, nil
	}

	// A copy, as in GetSnapshot: the caller owns the returned Addresses.
	boot := nd.bootstrap
	boot.Addresses = copyAddrMap(nd.bootstrap.Addresses)
	return boot, true, nil
}

// getNode returns the nodeData for the given key, or nil if it does not exist.
func (m *MemDB) getNode(shardID, replicaID uint64) *nodeData {
	key := nodeKey{shardID: shardID, replicaID: replicaID}

	m.mu.RLock()
	nd := m.nodes[key]
	m.mu.RUnlock()

	return nd
}

// readNode is getNode for readers, rechecking the closed flag under mu so a reader racing Close returns ErrClosed.
func (m *MemDB) readNode(shardID, replicaID uint64) (*nodeData, error) {
	key := nodeKey{shardID: shardID, replicaID: replicaID}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed.Load() {
		return nil, logdb.ErrClosed
	}
	return m.nodes[key], nil
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
