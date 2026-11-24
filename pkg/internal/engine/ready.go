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

// Package engine implements the worker pool that drives Raft shards
// through their lifecycle. This file provides ReadyMap, an optimized
// double-buffered ready map using Dragonboat-style single-lock design.
package engine

import "sync"

// ReadyMap is a double-buffered set of shards with pending work: Set marks a shard, Swap collects and resets; a swapped map is valid until the next Swap.
type ReadyMap struct {
	mu    sync.Mutex
	ready map[uint64]struct{}
	maps  [2]map[uint64]struct{}
	index uint8
}

// NewReadyMap creates a ReadyMap with pre-allocated double buffers.
func NewReadyMap() *ReadyMap {
	return &ReadyMap{
		ready: make(map[uint64]struct{}),
		maps: [2]map[uint64]struct{}{
			make(map[uint64]struct{}),
			make(map[uint64]struct{}),
		},
		index: 0,
	}
}

// Set marks a shard as having pending work.
// Safe for concurrent use by multiple goroutines.
func (r *ReadyMap) Set(shardID uint64) {
	r.mu.Lock()
	r.ready[shardID] = struct{}{}
	r.mu.Unlock()
}

// Swap returns the shards marked since the last swap, owned by the caller until the next Swap; single goroutine only.
func (r *ReadyMap) Swap() map[uint64]struct{} {
	// Prepare the alternate map by clearing it.
	// This happens outside the lock since we own this map.
	nextIndex := (r.index + 1) % 2
	alternate := r.maps[nextIndex]
	clear(alternate)

	// Atomic pointer swap under lock.
	r.mu.Lock()
	result := r.ready
	r.ready = alternate
	r.mu.Unlock()

	// Update index for next cycle.
	r.index = nextIndex

	return result
}
