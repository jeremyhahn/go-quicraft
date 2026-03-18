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

package waldb

import (
	"log/slog"
	"sort"
	"sync"
)

// indexEntry stores the location of a single Raft log entry within a WAL
// segment file. The position and size fields enable O(1) pread access.
type indexEntry struct {
	// Index is the Raft log index of this entry.
	Index uint64
	// Term is the Raft term when this entry was proposed.
	Term uint64
	// SegmentID identifies which WAL segment file contains this entry.
	SegmentID uint64
	// Offset is the byte offset within the segment file.
	Offset int64
	// Size is the total byte length of the encoded record.
	Size int64
}

// index provides an in-memory sorted index of Raft log entries for a single
// (shardID, replicaID) pair. Entries are kept in ascending Index order,
// enabling O(log n) lookup and O(log n) range queries via binary search.
//
// Thread safety is provided by an embedded sync.RWMutex.
type index struct {
	mu      sync.RWMutex
	entries []indexEntry
}

// newIndex creates an empty index with a pre-allocated backing slice.
func newIndex() *index {
	return &index{
		entries: make([]indexEntry, 0, 64),
	}
}

// Append adds one or more entries to the index. Entries must have
// monotonically increasing Index values. If an entry's Index is less than
// or equal to the last entry in the index, it is treated as an overwrite:
// all entries from that Index onward are truncated before appending.
func (idx *index) Append(entries ...indexEntry) {
	if len(entries) == 0 {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, e := range entries {
		idx.appendLocked(e)
	}
}

// appendLocked adds a single entry while holding the write lock.
// If the new entry's Index already exists or is earlier than the tail,
// all entries from that position onward are truncated first. This handles
// Raft leader changes where a new leader overwrites uncommitted entries.
func (idx *index) appendLocked(e indexEntry) {
	n := len(idx.entries)
	if n == 0 || e.Index > idx.entries[n-1].Index {
		// Fast path: append in order.
		idx.entries = append(idx.entries, e)
		return
	}

	// Overwrite path: find insertion point and truncate.
	pos := sort.Search(n, func(i int) bool {
		return idx.entries[i].Index >= e.Index
	})
	truncatedCount := n - pos
	slog.Info("WAL index truncated: leader overwrote uncommitted entries",
		"truncated_from", pos,
		"truncated_count", truncatedCount,
		"new_index", e.Index,
		"new_term", e.Term,
	)
	idx.entries = append(idx.entries[:pos], e)
}

// Lookup finds the indexEntry for the given Raft log index using binary
// search. Returns the entry and true if found, or a zero-valued entry
// and false if no entry with that index exists.
func (idx *index) Lookup(raftIndex uint64) (indexEntry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	pos := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Index >= raftIndex
	})
	if pos < len(idx.entries) && idx.entries[pos].Index == raftIndex {
		return idx.entries[pos], true
	}
	return indexEntry{}, false
}

// Range returns all indexEntry values whose Raft log index is in [low, high).
// Returns nil if no entries fall within the range.
//
// The returned slice is a copy of the internal entries, safe to retain and
// use after the read lock is released.
func (idx *index) Range(low, high uint64) []indexEntry {
	if low >= high {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return nil
	}

	// Find the first entry >= low.
	start := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Index >= low
	})

	// Find the first entry >= high.
	end := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Index >= high
	})

	if start >= end {
		return nil
	}

	result := make([]indexEntry, end-start)
	copy(result, idx.entries[start:end])
	return result
}

// Compact removes all entries with Index <= upTo. This supports Raft log
// compaction where entries below the snapshot index are no longer needed.
func (idx *index) Compact(upTo uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(idx.entries) == 0 {
		return
	}

	// Find the first entry with Index > upTo.
	pos := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].Index > upTo
	})

	if pos == 0 {
		return
	}

	// Shift remaining entries to the front and release old memory.
	remaining := len(idx.entries) - pos
	copy(idx.entries, idx.entries[pos:])
	idx.entries = idx.entries[:remaining]
}

// Len returns the number of entries currently in the index.
func (idx *index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// First returns the first (lowest-index) entry or false if the index is empty.
func (idx *index) First() (indexEntry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if len(idx.entries) == 0 {
		return indexEntry{}, false
	}
	return idx.entries[0], true
}

// Last returns the last (highest-index) entry or false if the index is empty.
func (idx *index) Last() (indexEntry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if len(idx.entries) == 0 {
		return indexEntry{}, false
	}
	return idx.entries[len(idx.entries)-1], true
}

// AllSegmentIDs returns the set of all unique segment IDs referenced by
// any entry in the index.
func (idx *index) AllSegmentIDs() map[uint64]bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make(map[uint64]bool)
	for _, e := range idx.entries {
		result[e.SegmentID] = true
	}
	return result
}
