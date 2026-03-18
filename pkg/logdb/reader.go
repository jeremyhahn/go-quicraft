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

package logdb

import (
	"sync"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// maxEntrySliceSize limits the capacity of entry slices allocated during
// GetRange to prevent excessive memory allocation when the requested
// range is extremely large. With proto.Entry at ~120 bytes each, 4MB
// allows roughly 34,000 entries per allocation.
const maxEntrySliceSize uint64 = 4 * 1024 * 1024

// entrySize is the approximate size in bytes of a single proto.Entry
// struct (header fields only, excludes Cmd payload). Used with
// maxEntrySliceSize to cap slice pre-allocation.
const entrySize uint64 = 120

// maxTermCacheEntries is the capacity of the term ring buffer.
// Once the buffer is full, the oldest entries are evicted automatically.
// This prevents unbounded memory growth during sustained high-throughput
// operation while providing O(1) append and O(log n) lookup.
const maxTermCacheEntries = 100_000

// termRingEntry stores a single (index, term) pair in the ring buffer.
type termRingEntry struct {
	index uint64
	term  uint64
}

// termRingBuffer is a fixed-capacity circular buffer for (index, term) pairs.
// Entries are appended in monotonically increasing index order and trimmed
// from the low end, making a ring buffer the ideal data structure: O(1)
// append with automatic eviction when full, O(log n) lookup via binary
// search over the ordered entries, and O(log n) trim.
type termRingBuffer struct {
	buf   []termRingEntry
	head  int // index of first valid entry in buf
	count int // number of valid entries
}

// newTermRingBuffer creates a ring buffer with the given capacity.
func newTermRingBuffer(capacity int) *termRingBuffer {
	return &termRingBuffer{
		buf: make([]termRingEntry, capacity),
	}
}

// append adds an (index, term) pair at the tail of the ring buffer.
// If the buffer is full, the oldest entry (at head) is evicted.
func (r *termRingBuffer) append(index, term uint64) {
	bufLen := len(r.buf)
	tail := (r.head + r.count) % bufLen
	r.buf[tail] = termRingEntry{index: index, term: term}
	if r.count == bufLen {
		// Buffer full: advance head to evict oldest.
		r.head = (r.head + 1) % bufLen
	} else {
		r.count++
	}
}

// lookup returns the term for the given index using binary search.
// Returns (term, true) if found, (0, false) otherwise.
func (r *termRingBuffer) lookup(index uint64) (uint64, bool) {
	if r.count == 0 {
		return 0, false
	}
	bufLen := len(r.buf)
	lo, hi := 0, r.count-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		entry := r.buf[(r.head+mid)%bufLen]
		if entry.index == index {
			return entry.term, true
		}
		if entry.index < index {
			lo = mid + 1
		} else {
			if mid == 0 {
				break
			}
			hi = mid - 1
		}
	}
	return 0, false
}

// trimTo removes all entries with index <= the given cutoff.
// Uses binary search to find the new head position in O(log n).
func (r *termRingBuffer) trimTo(index uint64) {
	if r.count == 0 {
		return
	}
	bufLen := len(r.buf)
	// Binary search for the first entry with index > cutoff.
	lo, hi := 0, r.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry := r.buf[(r.head+mid)%bufLen]
		if entry.index <= index {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// lo is the number of entries to remove.
	r.head = (r.head + lo) % bufLen
	r.count -= lo
}

// clear resets the buffer to empty without deallocating.
func (r *termRingBuffer) clear() {
	r.head = 0
	r.count = 0
}

// len returns the number of valid entries in the ring buffer.
//
//nolint:unused // used by tests for assertion readability
func (r *termRingBuffer) len() int {
	return r.count
}

// LogReader provides a cached view of entry ranges available in LogDB.
// It acts as a coordination point between the step worker (which reads
// entries and terms) and the commit worker (which updates the range
// after persisting entries). The cached markerIndex and length track
// which entries are available without querying LogDB every time.
//
// Concurrency model: LogReader uses sync.RWMutex. NodeState acquires
// RLock for concurrent read access. GetRange and Term acquire a full
// Lock because they mutate reusable entry buffers (logdbEntryBuf,
// protoEntryBuf) to avoid per-call allocation on the hot path. Write
// operations (SetRange, SetState, Append, CreateSnapshot,
// ApplySnapshot, Compact) also acquire Lock. In production, GetRange
// and Term are called from the step worker (single goroutine per
// shard), so full lock contention is negligible.
//
// The markerIndex represents the boundary between compacted entries and
// available entries. Entries at index <= markerIndex have been compacted.
// The first available entry is at markerIndex+1. The markerTerm caches
// the term at markerIndex to avoid a LogDB lookup on the common fast path.
type LogReader struct {
	mu sync.RWMutex

	// logdb is the underlying persistent log storage.
	logdb LogDB

	// shardID identifies the Raft shard this reader belongs to.
	shardID uint64

	// replicaID identifies the replica within the shard.
	replicaID uint64

	// markerIndex is the boundary index. Entries at or below this index
	// have been compacted. The first available entry is markerIndex+1.
	markerIndex uint64

	// markerTerm is the cached term at markerIndex, avoiding a LogDB
	// lookup when Term(markerIndex) is called.
	markerTerm uint64

	// length is the count of logical positions tracked, including the
	// marker position itself. The last available index is
	// markerIndex + length - 1. Initialized to 1 (marker only).
	length uint64

	// state is the cached hard state from LogDB.
	state proto.State

	// membership is the cached cluster membership, typically from the
	// latest snapshot.
	membership proto.Membership

	// snapshot is the most recent snapshot metadata. Stored by
	// CreateSnapshot and ApplySnapshot so the raft layer can send
	// InstallSnapshot to followers that are behind the compacted region.
	snapshot proto.Snapshot

	// termBuf caches entry terms indexed by log index in a ring buffer.
	// Populated by Append when entries are registered, checked by Term
	// before querying LogDB. This allows term lookups to succeed even
	// when LogDB is nil (testing) or entries have not been indexed yet.
	// The ring buffer auto-evicts the oldest entries when full, providing
	// O(1) append and O(log n) lookup via binary search.
	termBuf *termRingBuffer

	// entryCache stores full entries when LogDB is nil (testing mode).
	// This allows GetRange to serve entry data for indices that have
	// been truncated from inmem but are still needed by sendAppend
	// for slow followers. Only populated when logdb is nil.
	// Cleaned by Compact and ApplySnapshot.
	entryCache map[uint64]proto.Entry

	// logdbEntryBuf is a reusable buffer for logdb.Entry slices passed
	// to IterateEntries. Avoids allocating a new []Entry per GetRange
	// call. The buffer capacity grows to the high-water mark and is
	// reused across calls, eliminating per-call allocation overhead
	// on the hot path (sendAppend, once per follower per tick).
	logdbEntryBuf []Entry

	// protoEntryBuf is a reusable buffer for the []proto.Entry result
	// of converting logdb entries to proto entries. Avoids allocating
	// a new slice per GetRange call. Callers must not retain the
	// returned slice past the next GetRange/Term call on the same
	// LogReader, which matches the step worker's single-goroutine
	// access pattern.
	protoEntryBuf []proto.Entry
}

// NewLogReader creates a LogReader for the specified shard and replica,
// backed by the given LogDB. The reader starts with no entries available
// (length=1, representing only the marker position). Call SetRange or
// ApplySnapshot to initialize the available range.
func NewLogReader(shardID, replicaID uint64, logdb LogDB) *LogReader {
	return &LogReader{
		logdb:     logdb,
		shardID:   shardID,
		replicaID: replicaID,
		length:    1,
	}
}

// firstIndex returns the index of the first available entry.
// Caller must hold at least mu.RLock.
func (lr *LogReader) firstIndex() uint64 {
	return lr.markerIndex + 1
}

// lastIndex returns the index of the last available entry.
// Caller must hold at least mu.RLock.
func (lr *LogReader) lastIndex() uint64 {
	return lr.markerIndex + lr.length - 1
}

// IndexRange returns the first and last available entry indices. This is
// used by the raft layer to initialize the in-memory log boundary and
// to determine the first available entry for re-apply after restart.
// Returns (0, 0) when no entries are available.
func (lr *LogReader) IndexRange() (uint64, uint64) {
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	if lr.length <= 1 && lr.markerIndex == 0 {
		return 0, 0
	}
	return lr.firstIndex(), lr.lastIndex()
}

// GetRange returns entries in [low, high) up to maxSize bytes total.
// Returns ErrCompacted if low is at or below the compacted marker.
// Returns ErrUnavailable if high exceeds the available range.
//
// GetRange acquires a write lock because it mutates reusable entry
// buffers (logdbEntryBuf, protoEntryBuf) to avoid per-call allocation.
// In production, GetRange is called from the step worker (single
// goroutine per shard), so write lock contention is negligible.
func (lr *LogReader) GetRange(low, high, maxSize uint64) ([]proto.Entry, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	return lr.getRangeLocked(low, high, maxSize)
}

// getRangeLocked performs the entry retrieval with the read lock held.
func (lr *LogReader) getRangeLocked(low, high, maxSize uint64) ([]proto.Entry, error) {
	if low > high {
		return nil, ErrUnavailable
	}
	if low <= lr.markerIndex {
		return nil, ErrCompacted
	}
	if high > lr.lastIndex()+1 {
		return nil, ErrUnavailable
	}

	// Cap the number of entries to prevent unbounded allocation.
	maxEntries := maxEntrySliceSize / entrySize
	if high-low > maxEntries {
		high = low + maxEntries
	}

	// When LogDB is nil (testing mode), serve from the entry cache.
	if lr.logdb == nil {
		if lr.entryCache == nil {
			return nil, ErrUnavailable
		}
		result := make([]proto.Entry, 0, high-low)
		for idx := low; idx < high; idx++ {
			e, ok := lr.entryCache[idx]
			if !ok {
				return nil, ErrUnavailable
			}
			result = append(result, e)
		}
		return result, nil
	}

	// Reuse the logdb entry buffer to avoid per-call allocation.
	ents := lr.logdbEntryBuf[:0]
	var size uint64
	var err error
	ents, size, err = lr.logdb.IterateEntries(
		ents, 0, lr.shardID, lr.replicaID, low, high, maxSize,
	)
	if err != nil {
		return nil, err
	}
	lr.logdbEntryBuf = ents

	// Reuse the proto entry buffer to avoid per-call conversion allocation.
	lr.protoEntryBuf = logdbEntriesToProtoReuse(lr.protoEntryBuf, ents)

	result := lr.protoEntryBuf

	// Apply maxSize limiting: if we exceeded maxSize and have more than
	// one entry, trim the last entry that pushed us over the limit.
	// A maxSize of 0 means unlimited (no size restriction).
	if maxSize > 0 && size > maxSize && len(result) > 1 {
		result = result[:len(result)-1]
	}

	return result, nil
}

// Term returns the term for the entry at the given index.
// Returns ErrCompacted if the index is below the compacted marker.
// Returns ErrUnavailable if the index is beyond the available range.
// If index equals markerIndex, the cached markerTerm is returned
// without a LogDB lookup.
//
// Term acquires a write lock because its fallback path through
// getRangeLocked mutates reusable entry buffers.
func (lr *LogReader) Term(index uint64) (uint64, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	return lr.termLocked(index)
}

// termLocked returns the term for the given index. Caller must hold
// at least mu.RLock.
func (lr *LogReader) termLocked(index uint64) (uint64, error) {
	if index == lr.markerIndex {
		return lr.markerTerm, nil
	}
	// Check term cache (works even without LogDB backing).
	if lr.termBuf != nil {
		if t, ok := lr.termBuf.lookup(index); ok {
			return t, nil
		}
	}
	ents, err := lr.getRangeLocked(index, index+1, 0)
	if err != nil {
		return 0, err
	}
	if len(ents) == 0 {
		return 0, nil
	}
	return ents[0].Term, nil
}

// SetRange updates the LogReader to reflect newly available entries
// starting at firstIndex with the given length. This is called by the
// commit worker after entries have been persisted to LogDB.
//
// SetRange handles three cases:
//   - Overlap: firstIndex falls within the existing range, extending it.
//   - Contiguous: firstIndex is exactly at the end of the existing range.
//   - Gap: firstIndex is beyond the existing range, returning
//     ErrLogGapDetected to indicate a bug in the entry persistence pipeline.
func (lr *LogReader) SetRange(firstIndex, length uint64) error {
	if length == 0 {
		return nil
	}
	lr.mu.Lock()
	defer lr.mu.Unlock()
	return lr.setRangeLocked(firstIndex, length)
}

// setRangeLocked performs the range update. Caller must hold mu.Lock.
// Returns ErrLogGapDetected if firstIndex is beyond the existing range,
// indicating a bug in the entry persistence pipeline.
func (lr *LogReader) setRangeLocked(firstIndex, length uint64) error {
	first := lr.firstIndex()
	last := firstIndex + length - 1

	if last < first {
		return nil
	}

	if first > firstIndex {
		cut := first - firstIndex
		firstIndex = first
		length -= cut
	}

	offset := firstIndex - lr.markerIndex
	switch {
	case lr.length > offset:
		lr.length = offset + length
	case lr.length == offset:
		lr.length += length
	default:
		return ErrLogGapDetected
	}
	return nil
}

// SetState updates the cached hard state. Called by the commit worker
// after persisting the hard state to LogDB.
func (lr *LogReader) SetState(state proto.State) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	lr.state = state
}

// Append records that the given entries have been persisted to LogDB.
// It updates the cached range to include these entries. The entries
// must be contiguous (no gaps in index values). Returns
// ErrLogGapDetected if the entries are not contiguous with the
// existing range.
func (lr *LogReader) Append(entries []proto.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	lr.mu.Lock()
	defer lr.mu.Unlock()
	// Cache terms for entries being registered. The ring buffer
	// auto-evicts the oldest entries when full (O(1) per append).
	if lr.termBuf == nil {
		lr.termBuf = newTermRingBuffer(maxTermCacheEntries)
	}
	for i := range entries {
		lr.termBuf.append(entries[i].Index, entries[i].Term)
	}
	// When LogDB is nil (testing mode), also cache full entries so
	// GetRange can serve them after inmem truncation.
	if lr.logdb == nil {
		if lr.entryCache == nil {
			lr.entryCache = make(map[uint64]proto.Entry, len(entries))
		}
		for i := range entries {
			lr.entryCache[entries[i].Index] = entries[i]
		}
	}
	// Update the tracked range.
	return lr.setRangeLocked(entries[0].Index, uint64(len(entries)))
}

// NodeState returns the cached hard state and membership.
func (lr *LogReader) NodeState() (proto.State, proto.Membership) {
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	return lr.state, lr.membership
}

// SetMembership sets the initial cluster membership. This must be called
// before the Peer is created from this LogReader, so that the Raft state
// machine knows about all cluster members for quorum calculations.
func (lr *LogReader) SetMembership(m proto.Membership) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	lr.membership = m
}

// CreateSnapshot records snapshot metadata without changing the
// available entry range. Returns ErrSnapshotOutOfDate if the snapshot
// is older than the currently recorded snapshot.
func (lr *LogReader) CreateSnapshot(ss proto.Snapshot) error {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	return lr.setSnapshot(ss)
}

// ApplySnapshot applies a snapshot, resetting the available range to
// start at the snapshot index. All prior entries are considered
// compacted. Returns ErrSnapshotOutOfDate if the snapshot is older
// than the currently recorded snapshot.
func (lr *LogReader) ApplySnapshot(ss proto.Snapshot) error {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	if err := lr.setSnapshot(ss); err != nil {
		return err
	}
	lr.markerIndex = ss.Index
	lr.markerTerm = ss.Term
	lr.length = 1
	// Clear caches since all entries are compacted.
	if lr.termBuf != nil {
		lr.termBuf.clear()
	}
	clear(lr.entryCache)
	return nil
}

// setSnapshot stores snapshot metadata if it is newer than the current
// snapshot. Caller must hold mu.Lock.
func (lr *LogReader) setSnapshot(ss proto.Snapshot) error {
	if lr.membership.ConfigChangeID != 0 || lr.state.Commit != 0 {
		// We have existing state. Only accept newer snapshots.
		if ss.Index <= lr.markerIndex && ss.Index > 0 {
			return ErrSnapshotOutOfDate
		}
	}
	lr.membership = ss.Membership
	lr.snapshot = ss
	return nil
}

// Snapshot returns the most recent snapshot metadata known to the
// LogReader. Returns a zero-value Snapshot (Index==0) if no snapshot
// has been recorded.
func (lr *LogReader) Snapshot() proto.Snapshot {
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	return lr.snapshot
}

// Compact removes entries up to and including the given index by
// advancing the marker. After compaction, entries at or below index
// are no longer available and requests for them return ErrCompacted.
//
// Compact atomically updates both markerIndex and markerTerm within
// a single Lock scope to maintain the invariant that markerTerm always
// corresponds to markerIndex.
func (lr *LogReader) Compact(index uint64) error {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	if index < lr.markerIndex {
		return ErrCompacted
	}
	if index > lr.lastIndex() {
		return ErrUnavailable
	}

	// Look up the term at the compaction point before updating the marker.
	// We need to read the term while the current state is still valid.
	term, err := lr.termLocked(index)
	if err != nil {
		return err
	}

	// Atomically advance the marker and update the cached term.
	i := index - lr.markerIndex
	lr.length -= i
	lr.markerIndex = index
	lr.markerTerm = term

	// Clean caches for compacted entries.
	if lr.termBuf != nil {
		lr.termBuf.trimTo(index)
	}
	for k := range lr.entryCache {
		if k <= index {
			delete(lr.entryCache, k)
		}
	}
	return nil
}

// logdbEntriesToProtoReuse converts logdb entries to proto entries using
// the provided buffer to avoid allocation. The buffer is grown if its
// capacity is insufficient, otherwise it is resliced to len(entries).
// Returns the (potentially reallocated) buffer. Callers must not retain
// the returned slice past the next call.
func logdbEntriesToProtoReuse(buf []proto.Entry, entries []Entry) []proto.Entry {
	if len(entries) == 0 {
		return buf[:0]
	}
	if cap(buf) < len(entries) {
		buf = make([]proto.Entry, len(entries))
	} else {
		buf = buf[:len(entries)]
	}
	for i := range entries {
		buf[i] = proto.Entry{
			Term:  entries[i].Term,
			Index: entries[i].Index,
			Type:  entries[i].Type,
			Cmd:   entries[i].Cmd,
		}
	}
	return buf
}
