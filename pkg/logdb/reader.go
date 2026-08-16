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
//
// lookup binary-searches the buffer, so the stored indices must stay
// strictly increasing. An index at or below the current tail would break
// that ordering, so the superseded suffix is dropped first (trimFrom) and
// the pair is appended over it. This repairs — rather than corrupts — the
// ring when a caller re-appends an overwritten index.
func (r *termRingBuffer) append(index, term uint64) {
	if r.count > 0 {
		bufLen := len(r.buf)
		last := r.buf[(r.head+r.count-1)%bufLen]
		if index <= last.index {
			r.trimFrom(index)
		}
	}
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

// trimFrom removes all entries with index >= the given cutoff, dropping
// them from the tail. Uses binary search to find the new count in O(log n).
//
// This is the truncation counterpart of trimTo: a follower conflict
// truncation replaces a suffix of the log, and every cached term at or
// above the new first index is superseded. Leaving them cached would let
// termLocked serve a stale term for a re-written index (Log Matching
// violation) and would bake a wrong markerTerm into Compact.
func (r *termRingBuffer) trimFrom(index uint64) {
	if r.count == 0 {
		return
	}
	bufLen := len(r.buf)
	// Binary search for the first entry with entry.index >= index.
	lo, hi := 0, r.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry := r.buf[(r.head+mid)%bufLen]
		if entry.index < index {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// lo is the number of entries to keep.
	r.count = lo
}

// clear resets the buffer to empty without deallocating.
func (r *termRingBuffer) clear() {
	r.head = 0
	r.count = 0
}

// len returns the number of valid entries in the ring buffer.
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
	// a new slice per GetRange call.
	//
	// It is RETAINED by GetRange's caller past the lock, so it is confined to
	// the ONE goroutine that owns GetRange: the raft step worker on a running
	// shard, and StartShard before the shard's step worker exists (they never
	// overlap on the same per-shard LogReader). Every OTHER reader uses a
	// separate path — termLocked has its own term buffers, and GetRangeCopy
	// copies under the lock into a private buffer — so no goroutine can overwrite
	// this buffer while GetRange's caller is still reading it.
	protoEntryBuf []proto.Entry

	// termLogdbBuf / termProtoBuf back termLocked's single-entry reads. termLocked
	// is reachable from goroutines OTHER than the step worker (the snapshot pool's
	// Compact -> termLocked), so it must NOT share the GetRange buffers the step
	// worker retains past the lock. It never retains its result (it returns only a
	// term) and always holds lr.mu, so these buffers are safe to share across its
	// callers: every access is serialized and none outlives the lock.
	termLogdbBuf []Entry
	termProtoBuf []proto.Entry
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

// getRangeLocked performs the entry retrieval with the lock held, reusing the
// GetRange buffers. Its result is backed by protoEntryBuf, which the caller
// retains past the lock — so getRangeLocked is only for the step worker's own
// GetRange (and StartShard reconstruction, which never overlaps it). termLocked
// and GetRangeCopy use getRangeLockedInto with their own buffers.
func (lr *LogReader) getRangeLocked(low, high, maxSize uint64) ([]proto.Entry, error) {
	return lr.getRangeLockedInto(low, high, maxSize, &lr.logdbEntryBuf, &lr.protoEntryBuf)
}

// getRangeLockedInto performs the entry retrieval with the lock held, into the
// caller-supplied reuse buffers. Separating the buffers is what makes the reader
// safe under its real multi-goroutine access: the step worker's GetRange retains
// its result, so no other goroutine may write the buffers backing it. termLocked
// (reachable from the snapshot pool) and GetRangeCopy (an arbitrary API caller)
// therefore pass DIFFERENT buffers than GetRange.
func (lr *LogReader) getRangeLockedInto(low, high, maxSize uint64, logdbBuf *[]Entry, protoBuf *[]proto.Entry) ([]proto.Entry, error) {
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

	// Reuse the caller's logdb entry buffer to avoid per-call allocation. The
	// maxSize budget is enforced by IterateEntries itself: every LogDB backend
	// stops BEFORE appending an entry that would exceed it, except for the first
	// entry which is always included so a single oversized entry can still make
	// progress. No post-trim is therefore possible or needed here.
	ents := (*logdbBuf)[:0]
	var err error
	ents, _, err = lr.logdb.IterateEntries(
		ents, 0, lr.shardID, lr.replicaID, low, high, maxSize,
	)
	if err != nil {
		return nil, err
	}
	*logdbBuf = ents

	// Reuse the caller's proto entry buffer to avoid per-call conversion alloc.
	*protoBuf = logdbEntriesToProtoReuse(*protoBuf, ents)

	return *protoBuf, nil
}

// GetRangeCopy returns entries in [low, high) as freshly-allocated proto entries
// the caller fully owns, safe to read on any goroutine at any time.
//
// Unlike GetRange it does NOT hand out a reused buffer: it copies each entry
// (including Cmd) UNDER the lock into private storage, so an arbitrary API-caller
// goroutine (Host.QueryRaftLog) never touches the buffers the step worker retains
// past the lock. Use this from any goroutine that is not the step worker.
func (lr *LogReader) GetRangeCopy(low, high, maxSize uint64) ([]proto.Entry, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	// Private per-call buffers: never the GetRange buffers, so this call cannot
	// race a step-worker GetRange result that is being read without the lock.
	var logdbBuf []Entry
	var protoBuf []proto.Entry
	ents, err := lr.getRangeLockedInto(low, high, maxSize, &logdbBuf, &protoBuf)
	if err != nil {
		return nil, err
	}
	out := make([]proto.Entry, len(ents))
	for i := range ents {
		out[i] = ents[i]
		if len(ents[i].Cmd) > 0 {
			out[i].Cmd = append([]byte(nil), ents[i].Cmd...)
		} else {
			out[i].Cmd = nil
		}
	}
	return out, nil
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
	// Use termLocked's OWN buffers, never GetRange's: termLocked is reachable
	// from the snapshot pool (Compact) as well as the step worker, and writing
	// GetRange's protoEntryBuf here would race a step-worker GetRange result that
	// is being read without the lock. termLocked returns only a term, so nothing
	// retains these buffers past the lock.
	ents, err := lr.getRangeLockedInto(index, index+1, 0, &lr.termLogdbBuf, &lr.termProtoBuf)
	if err != nil {
		return 0, err
	}
	// Term 0 is Raft's "no term" sentinel: propagated as prevLogTerm it
	// makes followers reject appends forever, and as the local last-log
	// term it grants votes to shorter logs (Election Safety violation). A
	// missing or mismatched entry must therefore surface as ErrUnavailable,
	// never as a fabricated zero term.
	if len(ents) == 0 || ents[0].Index != index {
		return 0, ErrUnavailable
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
		// Overwrite: the existing range already covers firstIndex, so every
		// cached term and entry at or above it is superseded. Purge them
		// before the caller re-populates the new suffix, otherwise
		// termLocked — which consults termBuf ahead of the range check —
		// keeps serving the old terms for re-written indices and keeps
		// answering for indices the truncation removed entirely.
		lr.truncateCachesFrom(firstIndex)
		lr.length = offset + length
	case lr.length == offset:
		lr.length += length
	default:
		return ErrLogGapDetected
	}
	return nil
}

// truncateCachesFrom drops every cached term and entry with index >=
// fromIndex. Caller must hold mu.Lock.
func (lr *LogReader) truncateCachesFrom(fromIndex uint64) {
	if lr.termBuf != nil {
		lr.termBuf.trimFrom(fromIndex)
	}
	for k := range lr.entryCache {
		if k >= fromIndex {
			delete(lr.entryCache, k)
		}
	}
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
	// Drop any head that sits at or below the marker. setRangeLocked clamps
	// the RANGE the same way, but the caches below are populated from this
	// slice: caching the trimmed prefix records terms for indices the log no
	// longer holds, and termLocked consults termBuf ahead of the range check,
	// so those indices answer with a term instead of ErrCompacted. A leader
	// resolving term(prevLogIndex) for a lagging follower would then get a
	// plausible term rather than the ErrCompacted that triggers the
	// InstallSnapshot fallback, and the follower could never catch up.
	first := lr.firstIndex()
	if entries[len(entries)-1].Index < first {
		return nil
	}
	if entries[0].Index < first {
		entries = entries[first-entries[0].Index:]
	}
	// Update the tracked range FIRST. A truncating append (follower
	// conflict resolution) takes setRangeLocked's overwrite branch, which
	// purges the superseded suffix from the term and entry caches; the new
	// terms below are then written over a clean tail. Doing it the other
	// way round would drop the terms just cached.
	if err := lr.setRangeLocked(entries[0].Index, uint64(len(entries))); err != nil {
		return err
	}
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
	return nil
}

// PersistBootstrap durably writes a shard's initial log entries and hard state,
// then registers them in the reader's in-memory tracking. It is called exactly
// once, at first StartShard of a founding member, to lay down the
// EntryInitialMembership entry at index 1 (config-in-log bootstrap) so that a
// replica's membership is a function of its durable log rather than an
// out-of-band seed.
//
// The durable write happens FIRST: if it fails, nothing is registered in memory,
// so the reader never reports a range the LogDB does not hold. With a nil LogDB
// (testing) only the in-memory registration runs, which Append already backs
// with an entry cache.
func (lr *LogReader) PersistBootstrap(entries []proto.Entry, state proto.State) error {
	if len(entries) == 0 {
		return nil
	}
	if lr.logdb != nil {
		dbEntries := make([]Entry, len(entries))
		for i := range entries {
			dbEntries[i] = Entry{
				Index:       entries[i].Index,
				Term:        entries[i].Term,
				Type:        entries[i].Type,
				Key:         entries[i].Key,
				ClientID:    entries[i].ClientID,
				SeriesID:    entries[i].SeriesID,
				RespondedTo: entries[i].RespondedTo,
				OwnerID:     entries[i].OwnerID,
				Cmd:         entries[i].Cmd,
			}
		}
		if err := lr.logdb.SaveState([]Update{{
			ShardID:   lr.shardID,
			ReplicaID: lr.replicaID,
			State:     State{Term: state.Term, Vote: state.Vote, Commit: state.Commit},
			Entries:   dbEntries,
		}}); err != nil {
			return err
		}
	}
	if err := lr.Append(entries); err != nil {
		return err
	}
	lr.SetState(state)
	return nil
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
	// Each guard is conditioned on the state IT compares against, not on a
	// proxy for "this reader has been used".
	//
	// Both used to sit behind `lr.membership.ConfigChangeID != 0 ||
	// lr.state.Commit != 0`, which is a guess: ConfigChangeID 0 and Commit 0
	// are perfectly legal values for a replica that has recovered from a
	// snapshot and not yet committed anything of its own, and for that replica
	// the guess switched BOTH guards off. A stale snapshot then overwrote the
	// recorded one and, with it, lr.membership — the quorum set. The proxy also
	// bought nothing: on a genuinely fresh reader markerIndex and
	// snapshot.Index are both 0, which makes each guard inert on its own terms.
	//
	// The marker check alone is insufficient for CreateSnapshot: compaction
	// retains CompactionOverhead entries below the snapshot, so markerIndex
	// normally TRAILS the recorded snapshot index, and an ss with
	// markerIndex < ss.Index < lr.snapshot.Index would pass it. The legitimate
	// advance (ApplySnapshot/CreateSnapshot to a higher index) clears both.
	if ss.Index > 0 && ss.Index <= lr.markerIndex {
		return ErrSnapshotOutOfDate
	}
	if lr.snapshot.Index > 0 && ss.Index <= lr.snapshot.Index {
		return ErrSnapshotOutOfDate
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
			Term:        entries[i].Term,
			Index:       entries[i].Index,
			Type:        entries[i].Type,
			Key:         entries[i].Key,
			ClientID:    entries[i].ClientID,
			SeriesID:    entries[i].SeriesID,
			RespondedTo: entries[i].RespondedTo,
			OwnerID:     entries[i].OwnerID,
			Cmd:         entries[i].Cmd,
		}
	}
	return buf
}
