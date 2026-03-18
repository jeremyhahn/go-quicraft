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

import "sync"

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
}

// NewEntryQueue creates an EntryQueue with the given initial capacity
// and maximum length. The initial capacity is a pre-allocation hint;
// maxLen is the hard limit enforced by Add.
func NewEntryQueue[T any](initCap, maxLen int) *EntryQueue[T] {
	return &EntryQueue[T]{
		writeBuf: make([]T, 0, initCap),
		maxLen:   maxLen,
	}
}

// Add appends items to the write buffer. Returns true if the items were
// accepted, false if the queue is at capacity (len >= maxLen). When
// false is returned, no items are added.
//
// Safe for concurrent use by multiple producer goroutines.
func (q *EntryQueue[T]) Add(items []T) bool {
	q.mu.Lock()
	if len(q.writeBuf)+len(items) > q.maxLen {
		q.mu.Unlock()
		return false
	}
	q.writeBuf = append(q.writeBuf, items...)
	q.mu.Unlock()
	return true
}

// AddOne appends a single item to the write buffer. Returns true if the
// item was accepted, false if the queue is at capacity. This avoids the
// overhead of slice creation for single-item producers (e.g., transport
// message delivery).
//
// Safe for concurrent use by multiple producer goroutines.
func (q *EntryQueue[T]) AddOne(item T) bool {
	q.mu.Lock()
	if len(q.writeBuf) >= q.maxLen {
		q.mu.Unlock()
		return false
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
	q.mu.Lock()
	if len(q.writeBuf) == 0 {
		q.mu.Unlock()
		return buf[:0]
	}
	result := q.writeBuf
	// Reuse the caller's buffer as the new write buffer. Reset length
	// to 0 but keep the backing array to avoid allocation.
	if cap(buf) > 0 {
		q.writeBuf = buf[:0]
	} else {
		q.writeBuf = make([]T, 0, cap(result))
	}
	q.mu.Unlock()
	return result
}

// Len returns the number of items currently in the write buffer.
// Safe for concurrent use.
func (q *EntryQueue[T]) Len() int {
	q.mu.Lock()
	n := len(q.writeBuf)
	q.mu.Unlock()
	return n
}
