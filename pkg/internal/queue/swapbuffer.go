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

// Package queue provides high-performance concurrent data structures
// for buffering and notification in the Raft pipeline.
package queue

import "sync"

// SwapBuffer is a double-buffered queue for single-producer,
// single-consumer patterns. Items are added to the write buffer and
// swapped to the read buffer atomically. The producer calls Add,
// and the consumer calls Swap to collect all accumulated items.
//
// After the first Swap, the previously returned slice's backing array is
// recycled as the new write buffer (cleared via [:0]), eliminating
// steady-state allocations. The caller MUST NOT retain the returned
// slice beyond the next Swap call.
type SwapBuffer[T any] struct {
	mu       sync.Mutex
	writeBuf []T
	readBuf  []T
	initCap  int
	maxLen   int
}

// NewSwapBuffer creates a SwapBuffer with the given initial capacity and
// maximum length. If maxLen is 0, the buffer is unbounded. When maxLen
// is set, Add returns false when the write buffer is at capacity,
// providing backpressure to the producer.
func NewSwapBuffer[T any](capacity int, maxLen int) *SwapBuffer[T] {
	return &SwapBuffer[T]{
		writeBuf: make([]T, 0, capacity),
		initCap:  capacity,
		maxLen:   maxLen,
	}
}

// Add appends an item to the write buffer. Returns true if the item was
// added, false if the write buffer has reached maxLen (backpressure).
// When maxLen is 0, Add always succeeds.
// Thread-safe for a single producer (serialized by mutex).
func (sb *SwapBuffer[T]) Add(item T) bool {
	sb.mu.Lock()
	if sb.maxLen > 0 && len(sb.writeBuf) >= sb.maxLen {
		sb.mu.Unlock()
		return false
	}
	sb.writeBuf = append(sb.writeBuf, item)
	sb.mu.Unlock()
	return true
}

// Swap atomically swaps the write buffer with the recycled read buffer,
// returning the accumulated items. If no items have been added, returns nil.
//
// The returned slice is valid until the next Swap call. After the first
// cycle, Swap reuses the previous read buffer's backing array (cleared
// via [:0]) instead of allocating, achieving zero allocations in steady
// state.
func (sb *SwapBuffer[T]) Swap() []T {
	sb.mu.Lock()
	if len(sb.writeBuf) == 0 {
		sb.mu.Unlock()
		return nil
	}
	items := sb.writeBuf
	// Recycle the old read buffer's backing array as the new write buffer.
	// If readBuf is nil (first Swap), fall back to a fresh allocation.
	if sb.readBuf != nil {
		sb.writeBuf = sb.readBuf[:0]
	} else {
		sb.writeBuf = make([]T, 0, sb.initCap)
	}
	sb.readBuf = items
	sb.mu.Unlock()
	return items
}

// Len returns the number of items currently in the write buffer.
func (sb *SwapBuffer[T]) Len() int {
	sb.mu.Lock()
	n := len(sb.writeBuf)
	sb.mu.Unlock()
	return n
}
