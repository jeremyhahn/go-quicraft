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

// EntryQueue is a bounded multi-producer single-consumer double-buffered queue: producers Add under a short lock, the consumer Swaps buffers; Add returns false when full.
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
	// nonEmpty mirrors len(writeBuf) > 0 && !closed; written under mu on transitions only, read by Empty without mu.
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

// Add appends items and reports false, adding nothing, when the queue is full; safe for concurrent producers.
func (q *EntryQueue[T]) Add(items []T) bool {
	return q.AddTracked(items, nil)
}

// AddTracked is Add that records the non-zero generation in *at under the lock; at may be nil.
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

// Taken reports whether the batch recorded in *at was taken and handed off, and whether the queue is closed (then the answer is final).
func (q *EntryQueue[T]) Taken(at *uint64) (taken, closed bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return *at != 0 && *at <= q.handedOff, q.closed
}

// HandOff declares batch id handed on and reports true; once closed it reports false and the consumer must not hand the batch on.
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

// AddOne appends one item and reports false when full; safe for concurrent producers.
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

// Swap exchanges the write buffer with buf and returns the queued items; single consumer only.
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
	// Reuse buf as the write buffer, cleared so stale tail slots pin no memory.
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

// Empty reports without locking whether a Swap now would take nothing.
func (q *EntryQueue[T]) Empty() bool {
	return !q.nonEmpty.Load()
}
