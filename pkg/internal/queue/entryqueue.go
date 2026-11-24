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

package queue

import (
	"sync"
	"sync/atomic"
)

// EntryQueue is a multi-producer single-consumer double-buffered queue.
// Multiple producers call Add to append items to the active write buffer
// under a brief mutex. The single consumer calls Swap to atomically
// exchange the write buffer with the caller-provided read buffer,
// returning all accumulated items in one batch.
//
// This design minimizes contention on the hot path: producers hold the
// lock only for a slice append (typically <50ns), and the consumer
// acquires the lock once per drain cycle regardless of item count.
//
// The maxLen parameter provides backpressure: Add returns false when the
// write buffer has reached capacity, signaling the caller to apply
// backpressure (e.g., return ErrSystemBusy). Unlike SwapBuffer, this
// queue is bounded to prevent unbounded memory growth under load.
type EntryQueue[T any] struct {
	mu       sync.Mutex
	writeBuf []T
	maxLen   int
	// batch is the id of the batch being filled, starting at 1. Items added
	// while it reads b are taken by the Swap that takes batch b, which then
	// moves it past b. AddTracked records it.
	batch uint64
	// handedOff is the highest batch id the consumer has declared handed on
	// (HandOff). A tracked item is handed off once its batch id is at or
	// below it, which is what Taken reports.
	handedOff uint64
	// closed is set once by Close; from then on nothing is added or taken,
	// and nothing more is handed off.
	closed bool
	// nonEmpty mirrors "a Swap now would take something": len(writeBuf) > 0
	// and not closed. It is written only under mu, and only on the
	// transitions (an add into an empty buffer, a Swap that takes the
	// buffer, Close), so a steady stream of adds pays a branch, not an
	// atomic store. Empty reads it without mu.
	nonEmpty atomic.Bool
}

// NewEntryQueue creates an EntryQueue with the given initial capacity
// and maximum length. The initial capacity is a pre-allocation hint;
// maxLen is the hard limit enforced by Add.
func NewEntryQueue[T any](initCap, maxLen int) *EntryQueue[T] {
	return &EntryQueue[T]{
		writeBuf: make([]T, 0, initCap),
		maxLen:   maxLen,
		batch:    1,
	}
}

// Add appends items to the write buffer. Returns true if the items were
// accepted, false if the queue is at capacity (len >= maxLen). When
// false is returned, no items are added.
//
// Safe for concurrent use by multiple producer goroutines.
func (q *EntryQueue[T]) Add(items []T) bool {
	return q.AddTracked(items, nil)
}

// AddTracked is Add that also records, in *at, the generation the items were
// added under (a non-zero value; *at is left untouched when the add is
// refused). The write happens under the queue's lock, so Taken can never
// observe the items taken before *at is set. at may be nil.
func (q *EntryQueue[T]) AddTracked(items []T, at *uint64) bool {
	q.mu.Lock()
	if q.closed || len(q.writeBuf)+len(items) > q.maxLen {
		q.mu.Unlock()
		return false
	}
	if len(q.writeBuf) == 0 && len(items) > 0 {
		q.nonEmpty.Store(true)
	}
	q.writeBuf = append(q.writeBuf, items...)
	if at != nil {
		*at = q.batch
	}
	q.mu.Unlock()
	return true
}

// Taken reports whether the items whose batch AddTracked recorded in *at have
// been handed off — taken by SwapBatch AND declared handed on by HandOff —
// and whether the queue is closed. A zero *at (never added) is not taken.
// Once closed is true the answer is final: nothing is handed off after Close.
func (q *EntryQueue[T]) Taken(at *uint64) (taken, closed bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return *at != 0 && *at <= q.handedOff, q.closed
}

// HandOff declares the batch SwapBatch returned as id handed on by the
// consumer — the moment it is given to its destination — and reports true. It
// reports false, declaring nothing, once the queue is closed: the consumer
// must then not hand the batch on, since Taken has already reported its items
// final and untaken. A batch that is taken but never handed off stays
// untaken.
func (q *EntryQueue[T]) HandOff(id uint64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if id > q.handedOff {
		q.handedOff = id
	}
	return true
}

// Close stops the queue: every later Add is refused and every later Swap
// takes nothing. Items still queued are never taken. Idempotent.
func (q *EntryQueue[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.nonEmpty.Store(false)
	q.mu.Unlock()
}

// AddOne appends a single item to the write buffer. Returns true if the
// item was accepted, false if the queue is at capacity. This avoids the
// overhead of slice creation for single-item producers (e.g., transport
// message delivery).
//
// Safe for concurrent use by multiple producer goroutines.
func (q *EntryQueue[T]) AddOne(item T) bool {
	q.mu.Lock()
	if q.closed || len(q.writeBuf) >= q.maxLen {
		q.mu.Unlock()
		return false
	}
	if len(q.writeBuf) == 0 {
		q.nonEmpty.Store(true)
	}
	q.writeBuf = append(q.writeBuf, item)
	q.mu.Unlock()
	return true
}

// Swap exchanges the write buffer with the provided read buffer and
// returns the accumulated items. The caller should pass the previously
// returned slice (reset to length 0) to reuse its backing array and
// avoid allocation. If no items have been added, returns buf unchanged
// (length 0).
//
// Must only be called by the single consumer (step worker).
func (q *EntryQueue[T]) Swap(buf []T) []T {
	items, _ := q.SwapBatch(buf)
	return items
}

// SwapBatch is Swap that also returns the id of the batch it took, for
// HandOff; 0 when it took nothing.
func (q *EntryQueue[T]) SwapBatch(buf []T) ([]T, uint64) {
	q.mu.Lock()
	if q.closed || len(q.writeBuf) == 0 {
		q.mu.Unlock()
		return buf[:0], 0
	}
	id := q.batch
	q.batch++
	result := q.writeBuf
	q.nonEmpty.Store(false)
	// Reuse the caller's buffer as the new write buffer. Reset length
	// to 0 but keep the backing array to avoid allocation. Zero the
	// recycled backing array first: producers only overwrite the slots
	// they append into, so if a later burst is shorter than a prior one
	// the tail slots retain prior-generation values whose references
	// (e.g. proto.Message.Entries -> Entry.Cmd aliasing per-frame bulk
	// allocations) would stay reachable and pin that memory for the
	// process lifetime. clear() releases those references for GC,
	// matching the element-zeroing contract of proto.PutEntrySlice.
	if cap(buf) > 0 {
		clear(buf[:cap(buf)])
		q.writeBuf = buf[:0]
	} else {
		q.writeBuf = make([]T, 0, cap(result))
	}
	q.mu.Unlock()
	return result, id
}

// Len returns the number of items currently in the write buffer.
// Safe for concurrent use.
func (q *EntryQueue[T]) Len() int {
	q.mu.Lock()
	n := len(q.writeBuf)
	q.mu.Unlock()
	return n
}

// Empty reports, without taking the lock, whether a Swap now would take
// nothing: the buffer is empty or the queue is closed. It is exact at the
// moment of the read; an item added after it is taken by the next Swap, so a
// caller that skips work on Empty must already have a later Swap that picks
// it up. It is the hot-path form of Len() == 0.
func (q *EntryQueue[T]) Empty() bool {
	return !q.nonEmpty.Load()
}
