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

// Package server provides the admin HTTP server for node management.
package server

import "sync/atomic"

// gcTick is the number of ticks after which a follower state entry
// is considered stale and eligible for garbage collection.
const gcTick uint64 = 3

// ChangeTickThreshold is the minimum number of ticks between rate limit
// state transitions, preventing thrashing when load oscillates around
// the threshold.
const ChangeTickThreshold uint64 = 10

// RateLimiter provides simple atomic size tracking with a hard limit.
// It is used for global (cross-shard) rate limiting where hysteresis
// is not needed. All methods are safe for concurrent access.
type RateLimiter struct {
	size    atomic.Uint64
	maxSize uint64
}

// NewRateLimiter creates a new RateLimiter with the given maximum size.
// A maxSize of 0 disables rate limiting.
func NewRateLimiter(maxSize uint64) *RateLimiter {
	return &RateLimiter{maxSize: maxSize}
}

// Enabled returns true if rate limiting is active (maxSize > 0).
func (r *RateLimiter) Enabled() bool {
	return r.maxSize > 0
}

// Increase atomically adds sz to the tracked size.
func (r *RateLimiter) Increase(sz uint64) {
	r.size.Add(sz)
}

// Decrease atomically subtracts sz from the tracked size. If the
// subtraction would underflow (sz >= current), the size saturates at 0.
func (r *RateLimiter) Decrease(sz uint64) {
	for {
		current := r.size.Load()
		var next uint64
		if sz >= current {
			next = 0
		} else {
			next = current - sz
		}
		if r.size.CompareAndSwap(current, next) {
			return
		}
	}
}

// Set atomically stores the tracked size.
func (r *RateLimiter) Set(sz uint64) {
	r.size.Store(sz)
}

// Get atomically loads the current tracked size.
func (r *RateLimiter) Get() uint64 {
	return r.size.Load()
}

// RateLimited returns true if the current size exceeds maxSize.
func (r *RateLimiter) RateLimited() bool {
	return r.Enabled() && r.Get() > r.maxSize
}

// MaxSize returns the configured maximum size.
func (r *RateLimiter) MaxSize() uint64 {
	return r.maxSize
}

// followerState tracks a follower's in-memory log size and the tick
// at which the state was last updated. Used for leader-side follower
// backpressure tracking.
type followerState struct {
	tick         uint64
	inMemLogSize uint64
}

// InMemRateLimiter provides per-shard in-memory log rate limiting with
// hysteresis and follower feedback. The mutable state (limited,
// tickLimited, followerSizes) is owned by the single raft state machine
// goroutine and must only be mutated through RateLimited(), Tick(),
// SetFollowerState(), and Reset().
//
// Client goroutines that need to check the rate-limited state must call
// IsLimited(), which reads an atomic cache updated by the owning raft
// goroutine each time RateLimited() is evaluated.
//
// Hysteresis prevents thrashing: the rate limiter becomes limited when
// any tracked size exceeds 100% of maxSize, and becomes unlimited only
// when all tracked sizes drop below 70% of maxSize. State transitions
// are debounced by ChangeTickThreshold ticks.
type InMemRateLimiter struct {
	rl            RateLimiter
	followerSizes map[uint64]followerState
	tick          uint64
	tickLimited   uint64
	limited       bool
	// cachedLimited is an atomic snapshot of the limited field, written
	// by the single-writer raft goroutine in RateLimited() and read by
	// concurrent client goroutines via IsLimited().
	cachedLimited atomic.Bool
}

// NewInMemRateLimiter creates a new InMemRateLimiter with the given
// maximum in-memory log size. A maxSize of 0 disables rate limiting.
func NewInMemRateLimiter(maxSize uint64) *InMemRateLimiter {
	return &InMemRateLimiter{
		rl:            RateLimiter{maxSize: maxSize},
		followerSizes: make(map[uint64]followerState),
	}
}

// Enabled returns true if rate limiting is active (maxSize > 0).
func (r *InMemRateLimiter) Enabled() bool {
	return r.rl.Enabled()
}

// Increase adds sz to the local in-memory log size.
func (r *InMemRateLimiter) Increase(sz uint64) {
	r.rl.Increase(sz)
}

// Decrease subtracts sz from the local in-memory log size.
func (r *InMemRateLimiter) Decrease(sz uint64) {
	r.rl.Decrease(sz)
}

// Set stores the local in-memory log size.
func (r *InMemRateLimiter) Set(sz uint64) {
	r.rl.Set(sz)
}

// Get returns the current local in-memory log size.
func (r *InMemRateLimiter) Get() uint64 {
	return r.rl.Get()
}

// MaxSize returns the configured maximum size.
func (r *InMemRateLimiter) MaxSize() uint64 {
	return r.rl.MaxSize()
}

// Tick advances the logical clock and garbage-collects stale follower
// states. Called once per leader tick.
func (r *InMemRateLimiter) Tick() {
	r.tick++
	r.gc()
}

// GetTick returns the current logical tick.
func (r *InMemRateLimiter) GetTick() uint64 {
	return r.tick
}

// Reset clears all follower states and resets the rate limit state.
// Called on leadership transitions.
func (r *InMemRateLimiter) Reset() {
	r.followerSizes = make(map[uint64]followerState)
	r.limited = false
	r.tickLimited = 0
	r.cachedLimited.Store(false)
}

// SetFollowerState records a follower's reported in-memory log size.
// Called when the leader receives a RateLimit message from a follower.
func (r *InMemRateLimiter) SetFollowerState(replicaID, sz uint64) {
	r.followerSizes[replicaID] = followerState{
		tick:         r.tick,
		inMemLogSize: sz,
	}
}

// RateLimited returns whether proposals should be rate-limited. Uses
// hysteresis with tick-based debouncing to prevent thrashing.
//
// This method is NOT safe for concurrent access. It must only be called
// by the single owning raft goroutine. Client goroutines should call
// IsLimited() instead.
func (r *InMemRateLimiter) RateLimited() bool {
	if !r.Enabled() {
		return false
	}
	limited := r.limitedByInMemSize()
	if limited != r.limited {
		if r.tickLimited == 0 || r.tick-r.tickLimited > ChangeTickThreshold {
			r.limited = limited
			r.tickLimited = r.tick
		}
	}
	r.cachedLimited.Store(r.limited)
	return r.limited
}

// IsLimited returns the cached rate-limited state. Safe for concurrent
// access from multiple goroutines. The cached state is updated by
// RateLimited() which must be called periodically by the owning
// (single-writer) raft goroutine.
func (r *InMemRateLimiter) IsLimited() bool {
	return r.cachedLimited.Load()
}

// limitedByInMemSize performs the raw threshold check with asymmetric
// thresholds (100% to become limited, 70% to become unlimited).
func (r *InMemRateLimiter) limitedByInMemSize() bool {
	maxSz := r.rl.Get()
	for _, fs := range r.followerSizes {
		if fs.inMemLogSize > maxSz {
			maxSz = fs.inMemLogSize
		}
	}
	if r.limited {
		// To exit rate limiting: require all sizes below 70%.
		return maxSz >= r.rl.maxSize*7/10
	}
	// To enter rate limiting: any size above 100%.
	return maxSz > r.rl.maxSize
}

// gc removes stale follower states that haven't been updated within
// gcTick ticks.
func (r *InMemRateLimiter) gc() {
	for id, fs := range r.followerSizes {
		if r.tick-fs.tick > gcTick {
			delete(r.followerSizes, id)
		}
	}
}
