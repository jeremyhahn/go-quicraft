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

// maxEntrySliceSize caps the capacity of entry slices GetRange allocates.
const maxEntrySliceSize uint64 = 4 * 1024 * 1024

// entrySize is the approximate size in bytes of a single proto.Entry
// struct (header fields only, excludes Cmd payload). Used with
// maxEntrySliceSize to cap slice pre-allocation.
const entrySize uint64 = 120

// maxTermCacheEntries is the capacity of the term ring buffer; the oldest entries are evicted when full.
const maxTermCacheEntries = 100_000

// termRingEntry stores a single (index, term) pair in the ring buffer.
type termRingEntry struct {
	index uint64
	term  uint64
}

// termRingBuffer is a fixed-capacity ring of (index, term) pairs in increasing index order, with O(1) append and O(log n) lookup and trim.
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

// append adds an (index, term) pair at the tail, evicting the oldest when full; an index at or below the tail first drops the superseded suffix.
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

// trimFrom removes every entry with index >= cutoff, so a truncated suffix never serves a stale term.
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

// LogReader caches the entry range available in LogDB, shared by the step and commit workers; entries at or below markerIndex are compacted.
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

	// termBuf caches entry terms by index, filled by Append and checked by Term before LogDB.
	termBuf *termRingBuffer

	// entryCache holds full entries when LogDB is nil (testing); cleaned by Compact and ApplySnapshot.
	entryCache map[uint64]proto.Entry

	// logdbEntryBuf is GetRange's reusable buffer for IterateEntries.
	logdbEntryBuf []Entry

	// protoEntryBuf is GetRange's reusable result buffer, retained by its caller, so only the goroutine owning GetRange may use it.
	protoEntryBuf []proto.Entry

	// termLogdbBuf and termProtoBuf back termLocked's reads, separate from the GetRange buffers.
	termLogdbBuf []Entry
	termProtoBuf []proto.Entry
}

// NewLogReader creates a LogReader for the shard and replica with no entries available; call SetRange or ApplySnapshot to initialize it.
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

// IndexRange returns the first and last available entry indices, or (0, 0) when none are available.
func (lr *LogReader) IndexRange() (uint64, uint64) {
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	if lr.length <= 1 && lr.markerIndex == 0 {
		return 0, 0
	}
	return lr.firstIndex(), lr.lastIndex()
}

// GetRange returns entries in [low, high) up to maxSize bytes; ErrCompacted below the marker, ErrUnavailable past the range.
func (lr *LogReader) GetRange(low, high, maxSize uint64) ([]proto.Entry, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	return lr.getRangeLocked(low, high, maxSize)
}

// getRangeLocked retrieves entries with the lock held into the GetRange buffers; only for GetRange's owner.
func (lr *LogReader) getRangeLocked(low, high, maxSize uint64) ([]proto.Entry, error) {
	return lr.getRangeLockedInto(low, high, maxSize, &lr.logdbEntryBuf, &lr.protoEntryBuf)
}

// getRangeLockedInto retrieves entries with the lock held into caller-supplied buffers.
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

	// IterateEntries enforces maxSize, always including the first entry.
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

// GetRangeCopy returns entries in [low, high) copied into caller-owned memory; use it from any goroutine other than the step worker.
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

// Term returns the term at index; ErrCompacted below the marker, ErrUnavailable past the range.
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
	// Use termLocked's own buffers, never GetRange's.
	ents, err := lr.getRangeLockedInto(index, index+1, 0, &lr.termLogdbBuf, &lr.termProtoBuf)
	if err != nil {
		return 0, err
	}
	// A missing or mismatched entry is ErrUnavailable, never term 0.
	if len(ents) == 0 || ents[0].Index != index {
		return 0, ErrUnavailable
	}
	return ents[0].Term, nil
}

// SetRange extends the range with entries persisted from firstIndex; a gap returns ErrLogGapDetected.
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
		// Overwrite: purge cached terms and entries at or above firstIndex.
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

// Append records persisted contiguous entries in the range; a gap returns ErrLogGapDetected.
func (lr *LogReader) Append(entries []proto.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	lr.mu.Lock()
	defer lr.mu.Unlock()
	// Drop entries at or below the marker before caching their terms.
	first := lr.firstIndex()
	if entries[len(entries)-1].Index < first {
		return nil
	}
	if entries[0].Index < first {
		entries = entries[first-entries[0].Index:]
	}
	// Update the range first, so a truncating append purges the old suffix before new terms are cached.
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

// PersistBootstrap durably writes a founding member's initial entries and hard state, then registers them; with a nil LogDB only the registration runs.
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
			State:     State{Term: state.Term, Vote: state.Vote, Commit: state.Commit, VoteWindow: state.VoteWindow},
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

// ApplySnapshot resets the range to start at the snapshot index; an older snapshot returns ErrSnapshotOutOfDate.
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
	// Each guard compares against its own state; markerIndex trails the recorded snapshot, so CreateSnapshot also checks the snapshot index.
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

// RefreshSnapshotFileSize sets the FileSize of the snapshot recorded at index, for a rebuilt artifact; any other index is ErrSnapshotOutOfDate.
func (lr *LogReader) RefreshSnapshotFileSize(index, fileSize uint64) error {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	if index == 0 || lr.snapshot.Index != index {
		return ErrSnapshotOutOfDate
	}
	lr.snapshot.FileSize = fileSize
	return nil
}

// Compact removes entries up to and including index, updating markerIndex and markerTerm together.
func (lr *LogReader) Compact(index uint64) error {
	_, err := lr.CompactTo(index)
	return err
}

// CompactTo is Compact that also reports, under the same lock, whether the marker moved.
func (lr *LogReader) CompactTo(index uint64) (bool, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	if index < lr.markerIndex {
		return false, ErrCompacted
	}
	if index > lr.lastIndex() {
		return false, ErrUnavailable
	}

	// Look up the term at the compaction point before updating the marker.
	// We need to read the term while the current state is still valid.
	term, err := lr.termLocked(index)
	if err != nil {
		return false, err
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
	return i > 0, nil
}

// logdbEntriesToProtoReuse converts entries into buf, growing it if needed; do not retain the result past the next call.
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
