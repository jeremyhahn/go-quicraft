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

package raft

import (
	"sync/atomic"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/invariant"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/server"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

const (
	// entrySliceSize is the default initial capacity of the in-memory
	// entries slice. Pre-allocation reduces GC pressure during early
	// log growth.
	entrySliceSize = 512

	// minEntrySliceFreeSize is the minimum free capacity required
	// before a resize is triggered after shrinking.
	minEntrySliceFreeSize = 64
)

// getEntrySliceInMemSize returns the total in-memory size of the given
// entry slice. This includes the fixed header (68 bytes per entry) and
// the variable-length Cmd payload.
func getEntrySliceInMemSize(entries []proto.Entry) uint64 {
	if len(entries) == 0 {
		return 0
	}
	var sz uint64
	for i := range entries {
		sz += uint64(entries[i].Size())
	}
	return sz
}

// inMemory caches contiguous log entries in RAM, including entries not yet persisted: entries[i].Index == markerIndex + i.
type inMemory struct {
	// entries is the contiguous slice of in-memory log entries.
	entries []proto.Entry
	// snapshot is the pending snapshot that has not yet been persisted.
	snapshot *proto.Snapshot
	// markerIndex is the log index of the first entry in the entries
	// slice. When entries is empty, markerIndex == lastIndex + 1.
	markerIndex uint64
	// savedTo tracks the highest index that has been persisted to LogDB.
	savedTo uint64
	// appliedTo tracks the highest index that has been applied.
	appliedTo uint64
	// appliedToTerm is the term of the entry at appliedTo; atomic because restore writes it without raftMu.
	appliedToTerm atomic.Uint64
	// wastedPrefix counts entries truncated from the front of the backing array without compaction.
	wastedPrefix uint64
	// shrunk is set when entries have been truncated from the front,
	// indicating the slice may benefit from reallocation on the next
	// resize opportunity.
	shrunk bool
	// rl is the per-shard in-memory rate limiter. Nil when rate limiting
	// is disabled (MaxInMemLogSize == 0).
	rl *server.InMemRateLimiter
	// globalRL is the global (cross-shard) rate limiter. Nil when global
	// rate limiting is disabled (MaxTotalInMemLogSize == 0).
	globalRL *server.RateLimiter
	// globalContribution is this shard's size counted in the shared globalRL.
	globalContribution uint64
}

// setGlobalSize reports sz as this shard's total contribution to the shared
// global limiter, applying the change as a delta so the other shards' shares
// are preserved.
func (im *inMemory) setGlobalSize(sz uint64) {
	if im.globalRL == nil {
		return
	}
	switch {
	case sz > im.globalContribution:
		im.globalRL.Increase(sz - im.globalContribution)
	case sz < im.globalContribution:
		im.globalRL.Decrease(im.globalContribution - sz)
	}
	im.globalContribution = sz
}

// addGlobalSize adds sz to this shard's contribution to the global limiter.
func (im *inMemory) addGlobalSize(sz uint64) {
	if im.globalRL == nil {
		return
	}
	im.globalRL.Increase(sz)
	im.globalContribution += sz
}

// subGlobalSize removes sz from this shard's global contribution, clamped to what it contributed.
func (im *inMemory) subGlobalSize(sz uint64) {
	if im.globalRL == nil {
		return
	}
	if sz > im.globalContribution {
		sz = im.globalContribution
	}
	im.globalRL.Decrease(sz)
	im.globalContribution -= sz
}

// newInMemory creates an in-memory log starting after the given lastIndex.
// lastIndex is the highest index known to exist in LogDB.
func newInMemory(lastIndex uint64) inMemory {
	return inMemory{
		markerIndex: lastIndex + 1,
		savedTo:     lastIndex,
	}
}

// checkMarkerIndex validates the invariant that the first entry's index
// matches markerIndex. This is a programming error check.
func (im *inMemory) checkMarkerIndex() {
	if len(im.entries) > 0 {
		invariant.Assert(
			im.entries[0].Index == im.markerIndex,
			"marker index mismatch with first entry index",
		)
	}
}

// getEntries returns entries in [low, high), or logdb.ErrCompacted or logdb.ErrUnavailable outside the cached range.
func (im *inMemory) getEntries(low, high uint64) ([]proto.Entry, error) {
	upperBound := im.markerIndex + uint64(len(im.entries))
	if low < im.markerIndex {
		return nil, logdb.ErrCompacted
	}
	if high > upperBound {
		return nil, logdb.ErrUnavailable
	}
	if low > high {
		return nil, logdb.ErrUnavailable
	}
	if low == high {
		return nil, nil
	}
	return im.entries[low-im.markerIndex : high-im.markerIndex], nil
}

// getSnapshotIndex returns the snapshot index if a snapshot is present.
func (im *inMemory) getSnapshotIndex() (uint64, bool) {
	if im.snapshot != nil {
		return im.snapshot.Index, true
	}
	return 0, false
}

// getLastIndex returns the last index and true if in-memory has entries
// or a snapshot. Returns (0, false) when only the marker is set.
func (im *inMemory) getLastIndex() (uint64, bool) {
	if len(im.entries) > 0 {
		return im.entries[len(im.entries)-1].Index, true
	}
	if idx, ok := im.getSnapshotIndex(); ok {
		return idx, true
	}
	return 0, false
}

// lastIndex returns the index of the last entry. If entries is empty,
// returns the snapshot index if available, otherwise returns markerIndex-1.
func (im *inMemory) lastIndex() uint64 {
	if idx, ok := im.getLastIndex(); ok {
		return idx
	}
	// No entries and no snapshot: markerIndex is lastIndex+1, so
	// lastIndex is markerIndex-1.
	if im.markerIndex > 0 {
		return im.markerIndex - 1
	}
	return 0
}

// firstIndex returns the index of the first available entry. If a snapshot
// is present, returns snapshot.Index + 1. Otherwise returns markerIndex.
func (im *inMemory) firstIndex() uint64 {
	if idx, ok := im.getSnapshotIndex(); ok {
		return idx + 1
	}
	return im.markerIndex
}

// term returns the term for the entry at the given index. Returns the
// term and true if found, 0 and false otherwise.
func (im *inMemory) term(index uint64) (uint64, bool) {
	// Check the applied-to cache first.
	if at := im.appliedToTerm.Load(); index > 0 && index == im.appliedTo && at > 0 {
		return at, true
	}
	// Check if index is before our in-memory range.
	if index < im.markerIndex {
		if idx, ok := im.getSnapshotIndex(); ok && idx == index {
			return im.snapshot.Term, true
		}
		return 0, false
	}
	// Check if index is within our entries slice.
	if len(im.entries) > 0 && index <= im.entries[len(im.entries)-1].Index {
		offset := index - im.markerIndex
		if offset < uint64(len(im.entries)) {
			return im.entries[offset].Term, true
		}
	}
	return 0, false
}

// entriesToSave returns entries that have not yet been persisted to LogDB.
// These are entries with index > savedTo.
func (im *inMemory) entriesToSave() []proto.Entry {
	idx := im.savedTo + 1
	offset := idx - im.markerIndex
	if offset > uint64(len(im.entries)) {
		return nil
	}
	return im.entries[offset:]
}

// savedLogTo records that entries up to (index, term) have been persisted.
// The term is checked to guard against stale persistence notifications.
func (im *inMemory) savedLogTo(index, term uint64) {
	if index < im.markerIndex {
		return
	}
	if len(im.entries) == 0 {
		return
	}
	lastIdx := im.entries[len(im.entries)-1].Index
	if index > lastIdx {
		return
	}
	if im.entries[index-im.markerIndex].Term != term {
		return
	}
	im.savedTo = index
}

// commitUpdate processes a persistence notification, updating savedTo and
// snapshot state.
func (im *inMemory) commitUpdate(up proto.UpdateCommit) {
	if up.StableLogTo > 0 {
		im.savedLogTo(up.StableLogTo, up.StableLogTerm)
	}
	if up.StableSnapshotTo > 0 {
		im.savedSnapshot(up.StableSnapshotTo)
	}
}

// savedSnapshot records that the snapshot at the given index has been
// persisted. If the index matches, the pending snapshot is cleared.
func (im *inMemory) savedSnapshot(index uint64) {
	if idx, ok := im.getSnapshotIndex(); ok && idx == index {
		im.snapshot = nil
	}
}

// appliedLogTo advances the applied marker and truncates applied entries.
func (im *inMemory) appliedLogTo(index uint64) {
	if index < im.markerIndex {
		return
	}
	if len(im.entries) == 0 {
		return
	}
	lastIdx := im.entries[len(im.entries)-1].Index
	if index > lastIdx {
		return
	}
	offset := index - im.markerIndex
	if offset >= uint64(len(im.entries)) {
		return
	}
	entry := im.entries[offset]
	invariant.Assert(entry.Index == index, "appliedLogTo: entry index mismatch")

	im.appliedTo = entry.Index
	im.appliedToTerm.Store(entry.Term)

	// Decrease the per-shard and global limiters independently.
	if im.rl != nil || im.globalRL != nil {
		appliedEntries := im.entries[:index+1-im.markerIndex]
		sz := getEntrySliceInMemSize(appliedEntries)
		if im.rl != nil {
			im.rl.Decrease(sz)
		}
		im.subGlobalSize(sz)
	}

	newMarkerIndex := index + 1
	truncated := newMarkerIndex - im.markerIndex
	im.entries = im.entries[truncated:]
	im.wastedPrefix += truncated
	im.markerIndex = newMarkerIndex
	im.shrunk = true
	im.resizeIfNeeded()
	im.checkMarkerIndex()
}

// restore resets the in-memory log from a snapshot, discarding all
// existing entries.
func (im *inMemory) restore(ss proto.Snapshot) {
	im.snapshot = &ss
	im.markerIndex = ss.Index + 1
	im.appliedTo = ss.Index
	im.appliedToTerm.Store(ss.Term)
	im.shrunk = false
	im.wastedPrefix = 0
	im.entries = nil
	im.savedTo = ss.Index
	if im.rl != nil {
		im.rl.Set(0)
	}
	im.setGlobalSize(0)
}

// merge incorporates entries by appending, replacing everything, or truncating a conflicting suffix and appending.
func (im *inMemory) merge(entries []proto.Entry) {
	if len(entries) == 0 {
		return
	}
	firstNewIndex := entries[0].Index
	im.resizeIfNeeded()

	switch {
	case firstNewIndex == im.markerIndex+uint64(len(im.entries)):
		// Case 1: new entries are contiguous with existing entries.
		im.entries = append(im.entries, entries...)
		if im.rl != nil || im.globalRL != nil {
			sz := getEntrySliceInMemSize(entries)
			if im.rl != nil {
				im.rl.Increase(sz)
			}
			im.addGlobalSize(sz)
		}

	case firstNewIndex <= im.markerIndex:
		// Case 2: new entries replace all existing entries.
		im.markerIndex = firstNewIndex
		im.shrunk = false
		im.wastedPrefix = 0
		im.entries = im.newEntrySlice(entries)
		im.savedTo = firstNewIndex - 1
		if im.rl != nil || im.globalRL != nil {
			sz := getEntrySliceInMemSize(im.entries)
			if im.rl != nil {
				im.rl.Set(sz)
			}
			im.setGlobalSize(sz)
		}

	default:
		// Case 3: new entries overlap with existing. Keep the prefix
		// up to firstNewIndex, discard the rest, then append new.
		offset := firstNewIndex - im.markerIndex
		im.shrunk = false
		im.wastedPrefix = 0
		if offset > uint64(len(im.entries)) {
			// Only legal when there are no buffered entries (post
			// snapshot restore); with a non-empty buffer this would
			// silently create a hole in the log.
			invariant.Assert(len(im.entries) == 0,
				"gapped merge past in-memory tail")
			// After a snapshot restore there is no prefix to keep; adopt the new entries.
			im.markerIndex = firstNewIndex
			im.savedTo = firstNewIndex - 1
			im.entries = im.newEntrySlice(entries)
		} else {
			existing := im.entries[:offset]
			im.entries = im.newEntrySlice(existing)
			im.entries = append(im.entries, entries...)
		}
		if im.savedTo >= firstNewIndex {
			im.savedTo = firstNewIndex - 1
		}
		if im.rl != nil || im.globalRL != nil {
			sz := getEntrySliceInMemSize(im.entries)
			if im.rl != nil {
				im.rl.Set(sz)
			}
			im.setGlobalSize(sz)
		}
	}
	im.checkMarkerIndex()
}

// resize reallocates the entries slice, right-sized to the live entry count, to release capacity.
func (im *inMemory) resize() {
	im.shrunk = false
	im.wastedPrefix = 0
	n := len(im.entries)
	newCap := n + minEntrySliceFreeSize
	if newCap < minEntrySliceFreeSize*2 {
		newCap = minEntrySliceFreeSize * 2
	}
	result := make([]proto.Entry, n, newCap)
	copy(result, im.entries)
	im.entries = result
}

// resizeIfNeeded compacts the entries slice when the wasted prefix justifies it.
func (im *inMemory) resizeIfNeeded() {
	if !im.shrunk {
		return
	}
	n := uint64(len(im.entries))
	// Fast path: no live entries remain. Release the backing array
	// without any allocation.
	if n == 0 {
		im.entries = nil
		im.shrunk = false
		im.wastedPrefix = 0
		return
	}
	// Only compact when wasted prefix exceeds the absolute threshold.
	if im.wastedPrefix < entrySliceSize {
		return
	}
	// Proportional gate: only compact when waste significantly exceeds
	// live entries. This prevents a 512-entry allocation when, e.g.,
	// 512 entries were wasted but 500 are still live.
	if im.wastedPrefix < 2*n {
		return
	}
	im.resize()
}

// newEntrySlice creates a new slice with at least entrySliceSize capacity,
// copying the provided entries into it.
func (im *inMemory) newEntrySlice(entries []proto.Entry) []proto.Entry {
	sz := entrySliceSize
	if len(entries) > sz {
		sz = len(entries)
	}
	result := make([]proto.Entry, 0, sz)
	result = append(result, entries...)
	return result
}
