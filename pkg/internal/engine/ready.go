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

// ReadyMap is a double-buffered notification map for signaling which
// shards have pending work. Writers call Set() to mark a shard;
// consumers call Swap() to atomically collect all marked shards and
// reset the map.
//
// This implementation uses a single mutex with pointer swap, inspired
// by Dragonboat's readyCluster design. The key insight is that swapping
// pointers is O(1) and the single lock has negligible contention because:
// 1. The critical section is extremely short (one map insert)
// 2. Swap() only holds the lock during pointer swap, not during iteration
//
// The returned map from Swap() MUST NOT be retained beyond the next
// Swap() call; it is recycled via two-map rotation.
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

// Swap atomically swaps the ready map and returns the previous one
// containing all shards marked since the last swap. The returned map
// is owned by the caller until the NEXT Swap() call, after which it
// may be recycled.
//
// This is O(1) for the swap operation itself. The caller iterates
// the returned map outside any lock.
//
// Swap is NOT safe for concurrent use; it must be called from a single
// goroutine (the owning step worker).
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
