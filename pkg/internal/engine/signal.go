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

package engine

import "sync/atomic"

// WorkSignal routes shard work notifications to the correct worker.
// Each worker has its own ReadyMap and notification channel. Shards
// are assigned to workers by fixed hash: shardID % numWorkers.
//
// The atomic pre-filter on each worker avoids redundant channel sends
// when the worker is already notified. A Load-before-Swap optimization
// on the notified flag provides a two-level fast path: the Load (~1ns)
// avoids the Swap write barrier (~5ns), and both avoid the channel
// send (~10-15ns). Under contention, most Notify() calls hit the Load
// fast path since workers stay notified between processing cycles.
type WorkSignal struct {
	readyMaps []*ReadyMap
	channels  []chan struct{}
	notified  []atomic.Bool
}

// NewWorkSignal creates a WorkSignal with the given number of workers.
// Each worker gets its own ReadyMap and a buffered notification channel
// (capacity 1). numWorkers must be greater than zero.
func NewWorkSignal(numWorkers int) *WorkSignal {
	ws := &WorkSignal{
		readyMaps: make([]*ReadyMap, numWorkers),
		channels:  make([]chan struct{}, numWorkers),
		notified:  make([]atomic.Bool, numWorkers),
	}
	for i := range numWorkers {
		ws.readyMaps[i] = NewReadyMap()
		ws.channels[i] = make(chan struct{}, 1)
	}
	return ws
}

// Notify marks a shard as having pending work and notifies the owning
// worker. The shard is routed to worker shardID % numWorkers.
//
// Uses a two-level atomic pre-filter:
//  1. Load: if already notified, return immediately (~1ns, no write barrier).
//  2. Swap: atomically set to true; if it was false, send on channel.
//
// The Load fast path avoids the Swap write barrier under contention
// when multiple goroutines notify the same worker concurrently. This
// is safe because the step worker iterates ALL nodes in processReady(),
// not just the ready set, so no work is missed by the early return.
//
// Safe for concurrent use by multiple goroutines.
func (ws *WorkSignal) Notify(shardID uint64) {
	workerID := shardID % uint64(len(ws.readyMaps))
	ws.readyMaps[workerID].Set(shardID)
	// Fast path: if already notified, skip the Swap (write barrier).
	if ws.notified[workerID].Load() {
		return
	}
	if !ws.notified[workerID].Swap(true) {
		ws.channels[workerID] <- struct{}{}
	}
}

// WaitC returns the read-only notification channel for the given
// worker. Workers select on this channel to wake up when work is
// available.
func (ws *WorkSignal) WaitC(workerID int) <-chan struct{} {
	return ws.channels[workerID]
}

// Swap returns the set of ready shards for the given worker by
// atomically swapping the worker's ReadyMap. The returned map must
// not be retained beyond the current processing cycle.
func (ws *WorkSignal) Swap(workerID int) map[uint64]struct{} {
	return ws.readyMaps[workerID].Swap()
}

// HasWork returns true if the worker has been notified of pending work.
// This is a cheap atomic load (~1ns) used by the adaptive spin-wait
// loop in the step worker to detect new work without the overhead of
// a channel receive (~500us-1ms goroutine wake-up on high-core-count
// machines). It does not consume the notification; the caller must
// still call ClearNotified after processing.
func (ws *WorkSignal) HasWork(workerID int) bool {
	return ws.notified[workerID].Load()
}

// ClearNotified resets the notification flag for a worker. Called at
// the top of the worker's processing loop, before consuming the
// ReadyMap, to re-arm the notification mechanism for the next cycle.
func (ws *WorkSignal) ClearNotified(workerID int) {
	ws.notified[workerID].Store(false)
}

// NumWorkers returns the number of workers managed by this WorkSignal.
func (ws *WorkSignal) NumWorkers() int {
	return len(ws.readyMaps)
}
