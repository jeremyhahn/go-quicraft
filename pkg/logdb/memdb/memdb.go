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

// AtRestProtection reports that this LogDB has no at-rest artifact at all.
//
// Every record lives in the nodes map and nothing is ever written to a file,
// so there is no on-disk copy of a Raft log entry for an attacker with the
// disk to read, and none survives the process. The at-rest guarantee therefore
// holds VACUOUSLY -- which is a different fact from "encrypted", and is
// reported as its own state so a caller cannot mistake one for the other.
func (m *MemDB) AtRestProtection() logdb.AtRestProtection {
	return logdb.AtRestNonDurable
}

// Close marks the database as closed and releases every node's data. All
// subsequent operations return ErrClosed. Close is idempotent.
//
// Releasing the map is the whole point of closing an in-memory store: this
// backend holds the entire Raft log of every shard on the heap, so a Close that
// only flipped a flag left all of it alive for as long as the caller kept the
// handle — which, in a host that closes one store and opens another, is the
// full log twice. No operation can observe the released map: every exported
// method checks the closed flag before it reaches the nodes.
func (m *MemDB) Close() error {
	m.closed.Store(true)
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
	// Validate the WHOLE batch before applying any of it. SaveState is
	// specified to be atomic, and a floor violation found half-way through
	// would otherwise leave the updates before it stored and the ones after it
	// not — the caller's error return would then describe a batch that had
	// partly happened.
	for i := range updates {
		u := &updates[i]
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
		// The error is the whole point of withNode: it reports that the node
		// was unlinked (RemoveNodeData) between resolving it and taking its
		// lock. Discarding it here would return nil to a caller whose batch was
		// silently dropped — precisely the race withNode exists to close.
		if err := m.withNode(u.ShardID, u.ReplicaID, func(nd *nodeData) error {
			m.applyUpdate(nd, u)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// withNode runs fn against a node's data under that node's own lock, having
// first confirmed the node is still the one the map holds.
//
// The re-check is what makes a write and a concurrent RemoveNodeData
// linearizable. getOrCreateNode resolves the *nodeData under the map lock and
// releases it before the node lock is taken, so RemoveNodeData can unlink the
// node in that window; writing into the unlinked value then stored the batch
// somewhere no reader can ever reach while still returning nil, which is the
// one thing a durability API must never do — the caller was told the write
// happened. Neither ordering of the two calls explains that outcome.
//
// Re-checking makes the write win: the node is recreated and the result is
// exactly what "remove, then save" produces. The alternative ordering, "save,
// then remove", is also legal and is what happens when RemoveNodeData gets the
// map lock second. Both are honest; only the lost write was not.
//
// The retry cannot spin indefinitely against anything but an unbroken stream of
// RemoveNodeData calls for the same node, which is not a workload — each
// iteration is bounded and makes the same progress the first attempt would.
//
// Lock order is nd.mu then m.mu, and it is the ONLY order in this package:
// getNode and getOrCreateNode release m.mu before any caller touches nd.mu, and
// RemoveNodeData/ListNodeInfo take m.mu alone.
func (m *MemDB) withNode(shardID, replicaID uint64, fn func(nd *nodeData) error) error {
	key := nodeKey{shardID: shardID, replicaID: replicaID}
	for {
		nd := m.getOrCreateNode(shardID, replicaID)
		nd.mu.Lock()
		m.mu.RLock()
		live := m.nodes[key] == nd
		m.mu.RUnlock()
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

// copyMembership returns a deep copy of m: the five maps are cloned so the
// stored membership never aliases the caller's. A nil map stays nil so a
// round-trip does not manufacture empty maps where the caller had none.
//
// Identities is cloned like the rest, and dropping it here would be worse
// than an aliasing bug: it is the authorization key a replica recovering
// from a snapshot has no other source for. See logdb.Membership.Identities.
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
	merged := appendCopied(existing[:insertAt], incoming)

	// Reusing the capacity is what makes the append cheap, and it is also what
	// keeps the DROPPED entries alive: an overwrite that replaces a long
	// uncommitted suffix with a short one leaves the difference sitting in the
	// backing array past len, each element still holding a pointer to its Cmd
	// payload. Nothing reads those elements again, so the retention is
	// invisible until it is measured — and under leader churn it is unbounded,
	// because every overwrite adds its own orphaned tail and none is ever
	// reclaimed. Clear the region so the payloads become collectable the moment
	// they leave the log.
	//
	// When append reallocated, this clears the abandoned array instead, which
	// costs a pass over memory that is about to be collected anyway and keeps
	// the rule free of a special case that could silently stop applying.
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

		// Copied out with its own payload: the slice held here is this store's
		// live log, and a caller writing through the returned Cmd would be
		// rewriting the entries this replica serves and replays. See
		// logdb.CopyEntryCmd.
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

	// A snapshot at index 0 is not a snapshot: index 0 is the empty log, so it
	// covers nothing and can never be newer than what is stored. Storing it
	// makes GetSnapshot report a snapshot that exists at no index, and the
	// caller's next SaveSnapshot at a real index is the only thing that clears
	// it. The staleness rule below already rejects it whenever a snapshot IS
	// stored — this extends the same rule to the fresh node it slipped through
	// on, and matches the SaveState path, which has always required Index > 0.
	if snapshot.Index == 0 {
		return logdb.ErrSnapshotOutOfDate
	}

	return m.withNode(shardID, replicaID, func(nd *nodeData) error {
		if nd.hasSnap && snapshot.Index <= nd.snapshot.Index {
			return logdb.ErrSnapshotOutOfDate
		}

		// The snapshot's membership becomes the node's membership, exactly as
		// it does on the SaveState path and in waldb (whose ReadState serves
		// the latest snapshot's membership); leaving it untouched made
		// ReadState report an empty membership after a snapshot install. The
		// maps are deep-copied for the same reason Entry.Cmd is: the caller
		// owns them.
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

	// A record stored by this version always carries an explicit DataDir, so
	// mark it recorded regardless of what the caller passed: DataDirRecorded
	// describes the RECORD, not the request, and a reader must be able to tell
	// "the shard has no DataDir" from "nothing was ever written here".
	bootstrap.DataDirRecorded = true
	// Deep-copy the address map for exactly the reason Entry.Cmd and every
	// Membership map are copied: the caller owns it and may reuse or mutate it
	// after the call. This was the ONE write path that did not, so a caller
	// that recycled its bootstrap map silently rewrote the stored initial
	// membership — the set a recovering replica dials and authorizes against.
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
