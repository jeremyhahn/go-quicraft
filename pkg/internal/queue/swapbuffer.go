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

// SwapBuffer is a single-producer, single-consumer double buffer; a slice returned by Swap is valid only until the next Swap.
type SwapBuffer[T any] struct {
	mu       sync.Mutex
	writeBuf []T
	readBuf  []T
	initCap  int
	maxLen   int
}

// NewSwapBuffer creates a SwapBuffer with an initial capacity and a maximum length; 0 means unbounded.
func NewSwapBuffer[T any](capacity int, maxLen int) *SwapBuffer[T] {
	return &SwapBuffer[T]{
		writeBuf: make([]T, 0, capacity),
		initCap:  capacity,
		maxLen:   maxLen,
	}
}

// Add appends item and returns false when the write buffer is at maxLen.
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

// Swap returns the accumulated items, or nil, recycling the previous read buffer.
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
