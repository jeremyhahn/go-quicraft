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

package server

import (
	"math"
	"testing"
)

// TestRateLimiter_IncreaseSaturatesAtMaxUint64 verifies that an Increase
// whose sum overflows clamps to MaxUint64 rather than wrapping, keeps the
// limiter tripped, and that a further Increase keeps MaxUint64.
func TestRateLimiter_IncreaseSaturatesAtMaxUint64(t *testing.T) {
	r := NewRateLimiter(100)
	r.Set(math.MaxUint64 - 5)
	r.Increase(10)
	if got := r.Get(); got != math.MaxUint64 {
		t.Fatalf("Get() after overflowing Increase = %d, want MaxUint64", got)
	}
	if !r.RateLimited() {
		t.Fatal("RateLimited() = false after saturation, want true")
	}
	r.Increase(1)
	if got := r.Get(); got != math.MaxUint64 {
		t.Fatalf("Get() after Increase on saturated size = %d, want MaxUint64", got)
	}
	if !r.RateLimited() {
		t.Fatal("RateLimited() = false after second Increase, want true")
	}
}

// TestInMemRateLimiter_IncreaseSaturatesAtMaxUint64 verifies the same
// saturation through the InMemRateLimiter wrapper.
func TestInMemRateLimiter_IncreaseSaturatesAtMaxUint64(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.Set(math.MaxUint64 - 5)
	r.Increase(10)
	if got := r.Get(); got != math.MaxUint64 {
		t.Fatalf("Get() after overflowing Increase = %d, want MaxUint64", got)
	}
	r.Increase(1)
	if got := r.Get(); got != math.MaxUint64 {
		t.Fatalf("Get() after Increase on saturated size = %d, want MaxUint64", got)
	}
}

// TestRateLimiter_DecreaseSaturatesAtZero verifies that Decrease by exactly
// the current size, by more than it, and on zero all give exactly zero.
func TestRateLimiter_DecreaseSaturatesAtZero(t *testing.T) {
	r := NewRateLimiter(100)
	r.Set(10)
	r.Decrease(10)
	if got := r.Get(); got != 0 {
		t.Fatalf("Set(10); Decrease(10): Get() = %d, want 0", got)
	}
	r.Set(10)
	r.Decrease(11)
	if got := r.Get(); got != 0 {
		t.Fatalf("Set(10); Decrease(11): Get() = %d, want 0", got)
	}
	r.Decrease(1)
	if got := r.Get(); got != 0 {
		t.Fatalf("Decrease(1) on 0: Get() = %d, want 0", got)
	}
}

// TestInMemRateLimiter_DecreaseSaturatesAtZero verifies the same floor
// through the InMemRateLimiter wrapper.
func TestInMemRateLimiter_DecreaseSaturatesAtZero(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.Set(10)
	r.Decrease(10)
	if got := r.Get(); got != 0 {
		t.Fatalf("Set(10); Decrease(10): Get() = %d, want 0", got)
	}
	r.Set(10)
	r.Decrease(11)
	if got := r.Get(); got != 0 {
		t.Fatalf("Set(10); Decrease(11): Get() = %d, want 0", got)
	}
	r.Decrease(1)
	if got := r.Get(); got != 0 {
		t.Fatalf("Decrease(1) on 0: Get() = %d, want 0", got)
	}
}

// TestInMemRateLimiter_FollowerGCBoundary verifies that a follower state is
// kept for exactly gcTick ticks after it is recorded and is removed by the
// next Tick.
func TestInMemRateLimiter_FollowerGCBoundary(t *testing.T) {
	r := NewInMemRateLimiter(100)
	r.SetFollowerState(2, 101)
	for i := uint64(1); i <= gcTick; i++ {
		r.Tick()
		if _, ok := r.followerSizes[2]; !ok {
			t.Fatalf("follower state for replica 2 removed after %d ticks, want kept for %d", i, gcTick)
		}
	}
	r.Tick()
	if _, ok := r.followerSizes[2]; ok {
		t.Fatalf("follower state for replica 2 still present after %d ticks, want removed", gcTick+1)
	}
}
