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
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// RateLimiter tests
// ---------------------------------------------------------------------------

func TestRateLimiter_Disabled(t *testing.T) {
	rl := NewRateLimiter(0)
	if rl.Enabled() {
		t.Fatal("expected disabled")
	}
	if rl.RateLimited() {
		t.Fatal("disabled limiter should never be rate limited")
	}
	rl.Increase(100)
	if rl.RateLimited() {
		t.Fatal("disabled limiter should never be rate limited after increase")
	}
}

func TestRateLimiter_Enabled(t *testing.T) {
	rl := NewRateLimiter(1000)
	if !rl.Enabled() {
		t.Fatal("expected enabled")
	}
	if rl.MaxSize() != 1000 {
		t.Fatalf("MaxSize = %d, want 1000", rl.MaxSize())
	}
}

func TestRateLimiter_IncreaseDecrease(t *testing.T) {
	rl := NewRateLimiter(1000)
	rl.Increase(500)
	if rl.Get() != 500 {
		t.Fatalf("Get = %d, want 500", rl.Get())
	}
	rl.Increase(300)
	if rl.Get() != 800 {
		t.Fatalf("Get = %d, want 800", rl.Get())
	}
	rl.Decrease(200)
	if rl.Get() != 600 {
		t.Fatalf("Get = %d, want 600", rl.Get())
	}
}

func TestRateLimiter_RateLimited(t *testing.T) {
	rl := NewRateLimiter(100)
	rl.Set(100)
	if rl.RateLimited() {
		t.Fatal("at limit should not be rate limited")
	}
	rl.Set(101)
	if !rl.RateLimited() {
		t.Fatal("above limit should be rate limited")
	}
	rl.Set(50)
	if rl.RateLimited() {
		t.Fatal("below limit should not be rate limited")
	}
}

func TestRateLimiter_DecreaseConcurrentUnderflow(t *testing.T) {
	rl := NewRateLimiter(1000)
	// 55 is not a multiple of the step, so whatever the interleaving one
	// decrease runs from 5 and must cross zero from a positive value.
	rl.Set(55)

	// Multiple concurrent decreases that collectively exceed the value.
	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			rl.Decrease(10)
		}()
	}
	wg.Wait()

	// Total decrease is 1000 but started at 55: must be 0 (not wrapped).
	val := rl.Get()
	if val != 0 {
		t.Fatalf("Get = %d, want 0 (concurrent underflow saturation)", val)
	}
}

func TestRateLimiter_ConcurrentAccess(t *testing.T) {
	rl := NewRateLimiter(1000000)
	var wg sync.WaitGroup
	n := 100
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			rl.Increase(100)
		}()
		go func() {
			defer wg.Done()
			_ = rl.Get()
			_ = rl.RateLimited()
		}()
	}
	wg.Wait()
	// Final size should be n * 100 = 10000
	if rl.Get() != uint64(n)*100 {
		t.Fatalf("Get = %d, want %d", rl.Get(), uint64(n)*100)
	}
}

// ---------------------------------------------------------------------------
// InMemRateLimiter tests
// ---------------------------------------------------------------------------

func TestInMemRateLimiter_Disabled(t *testing.T) {
	rl := NewInMemRateLimiter(0)
	if rl.Enabled() {
		t.Fatal("expected disabled")
	}
	if rl.RateLimited() {
		t.Fatal("disabled limiter should never be rate limited")
	}
	if rl.IsLimited() {
		t.Fatal("disabled limiter: IsLimited should always be false")
	}
	rl.Increase(999)
	if rl.RateLimited() {
		t.Fatal("disabled limiter should never be rate limited after increase")
	}
}

func TestInMemRateLimiter_Enabled(t *testing.T) {
	rl := NewInMemRateLimiter(1000)
	if !rl.Enabled() {
		t.Fatal("expected enabled")
	}
	if rl.MaxSize() != 1000 {
		t.Fatalf("MaxSize = %d, want 1000", rl.MaxSize())
	}
}

func TestInMemRateLimiter_BasicIncreaseDecrease(t *testing.T) {
	rl := NewInMemRateLimiter(1000)
	rl.Increase(500)
	if rl.Get() != 500 {
		t.Fatalf("Get = %d, want 500", rl.Get())
	}
	rl.Decrease(200)
	if rl.Get() != 300 {
		t.Fatalf("Get = %d, want 300", rl.Get())
	}
}

func TestInMemRateLimiter_TickDebouncing(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	// Advance past initial debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Exceed limit: becomes limited.
	rl.Set(101)
	if !rl.RateLimited() {
		t.Fatal("expected rate limited")
	}

	// Immediately drop below: should NOT change due to debouncing.
	rl.Set(50)
	if !rl.RateLimited() {
		t.Fatal("expected still rate limited due to debouncing")
	}

	// Advance one tick less than threshold: still debounced.
	for i := 0; i < int(ChangeTickThreshold); i++ {
		rl.Tick()
	}
	rl.Set(50)
	if !rl.RateLimited() {
		t.Fatal("expected still rate limited within debounce window")
	}

	// Advance one more tick past threshold.
	rl.Tick()
	rl.Set(50)
	if rl.RateLimited() {
		t.Fatal("expected not rate limited after debounce window")
	}
}

func TestInMemRateLimiter_FollowerGC(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	rl.SetFollowerState(2, 101)
	rl.SetFollowerState(3, 50)

	// Advance past gcTick to trigger GC.
	for i := 0; i <= int(gcTick)+1; i++ {
		rl.Tick()
	}

	// Both follower states should be GC'd (stale).
	// Update one to keep it alive.
	rl.SetFollowerState(3, 50)
	rl.Tick()

	// Follower 2 is gone (stale), follower 3 is alive.
	// Set local to 0 and verify follower 2's high value doesn't affect us.
	rl.Set(0)

	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
		rl.SetFollowerState(3, 50) // keep alive
	}

	if rl.RateLimited() {
		t.Fatal("expected not rate limited after follower 2 GC'd")
	}
}

func TestInMemRateLimiter_Reset(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	rl.Set(101)
	rl.SetFollowerState(2, 200)
	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
		rl.SetFollowerState(2, 200) // keep alive past GC
	}
	if !rl.RateLimited() {
		t.Fatal("expected rate limited before reset")
	}
	tick := rl.GetTick()

	// Reset clears follower states, the limited flag, and the local size.
	// The local size belongs to the previous term, so backpressure
	// accounting starts clean after a leadership transition.
	rl.Reset()
	if rl.Get() != 0 {
		t.Fatalf("Get = %d, want 0 after reset", rl.Get())
	}
	if rl.IsLimited() {
		t.Fatal("IsLimited should be false after Reset")
	}
	if rl.RateLimited() {
		t.Fatal("expected not rate limited after reset")
	}

	// The tick is an opaque counter that keeps advancing across a reset.
	rl.Tick()
	if rl.GetTick() != tick+1 {
		t.Fatalf("GetTick = %d, want %d (tick continues after reset)", rl.GetTick(), tick+1)
	}
}

func TestInMemRateLimiter_AtExactLimit(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// At exactly 100%: should NOT be limited (> not >=).
	rl.Set(100)
	if rl.RateLimited() {
		t.Fatal("at exact limit should not be rate limited")
	}
}

func TestInMemRateLimiter_MultipleFollowers(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// All followers and local below limit.
	rl.Set(30)
	rl.SetFollowerState(2, 40)
	rl.SetFollowerState(3, 50)
	rl.SetFollowerState(4, 60)
	if rl.RateLimited() {
		t.Fatal("expected not rate limited with all below limit")
	}

	// One follower exceeds limit.
	rl.SetFollowerState(5, 101)
	if !rl.RateLimited() {
		t.Fatal("expected rate limited when one follower exceeds limit")
	}
}

// ---------------------------------------------------------------------------
// IsLimited (concurrent-safe cache) tests
// ---------------------------------------------------------------------------

func TestInMemRateLimiter_IsLimited_ReflectsRateLimited(t *testing.T) {
	rl := NewInMemRateLimiter(100)

	// Initially not limited.
	rl.RateLimited() // seed the cache
	if rl.IsLimited() {
		t.Fatal("IsLimited should be false initially")
	}

	// Push above threshold and advance past debounce window.
	rl.Set(200)
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}
	rl.RateLimited() // update cache
	if !rl.IsLimited() {
		t.Fatal("IsLimited should be true after exceeding threshold")
	}

	// Drop below hysteresis exit (70%) and advance past debounce.
	rl.Set(50)
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}
	rl.RateLimited() // update cache
	if rl.IsLimited() {
		t.Fatal("IsLimited should be false after dropping below hysteresis exit")
	}
}

// ---------------------------------------------------------------------------
// Additional InMemRateLimiter edge case tests
// ---------------------------------------------------------------------------

func TestInMemRateLimiter_FollowerOverridesLocalInHysteresisCheck(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	// Advance past debounce threshold.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Local is above limit, making us limited.
	rl.Set(101)
	if !rl.RateLimited() {
		t.Fatal("expected rate limited with local above limit")
	}

	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Local drops below 70%, but a follower is still at 80%.
	// The follower's size should keep us in the limited state (hysteresis
	// requires ALL sizes below 70%).
	rl.Set(30)
	rl.SetFollowerState(2, 80)
	if !rl.RateLimited() {
		t.Fatal("expected still rate limited: follower at 80% prevents exit (hysteresis requires < 70%)")
	}

	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
		rl.SetFollowerState(2, 80) // keep alive past GC
	}

	// Now follower drops below 70%.
	rl.SetFollowerState(2, 50)
	if rl.RateLimited() {
		t.Fatal("expected not rate limited: all sizes below 70%")
	}
}

func TestInMemRateLimiter_HysteresisExitAtExact70Percent(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Enter rate limited state.
	rl.Set(101)
	if !rl.RateLimited() {
		t.Fatal("expected rate limited at 101%")
	}

	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// At exactly 70: limitedByInMemSize returns maxSz >= maxSize*7/10
	// which is 70 >= 70 = true, so we should still be limited.
	rl.Set(70)
	if !rl.RateLimited() {
		t.Fatal("expected still rate limited at exactly 70% (>= threshold)")
	}

	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// At 69: 69 >= 70 is false, so we should exit rate limiting.
	rl.Set(69)
	if rl.RateLimited() {
		t.Fatal("expected not rate limited at 69%")
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkRateLimiterIncrease(b *testing.B) {
	b.ReportAllocs()
	rl := NewRateLimiter(1000000)
	for b.Loop() {
		rl.Increase(1)
	}
}

func BenchmarkRateLimiterRateLimited(b *testing.B) {
	b.ReportAllocs()
	rl := NewRateLimiter(1000000)
	rl.Set(500)
	for b.Loop() {
		rl.RateLimited()
	}
}

func BenchmarkRateLimiterIncrease_Concurrent(b *testing.B) {
	b.ReportAllocs()
	rl := NewRateLimiter(1000000)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rl.Increase(1)
		}
	})
}

func BenchmarkInMemRateLimiterRateLimited(b *testing.B) {
	b.ReportAllocs()
	rl := NewInMemRateLimiter(1000000)
	rl.Set(500)
	rl.SetFollowerState(2, 400)
	rl.SetFollowerState(3, 300)
	// Advance past debounce threshold so hysteresis logic is fully exercised.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
		rl.SetFollowerState(2, 400)
		rl.SetFollowerState(3, 300)
	}
	for b.Loop() {
		rl.RateLimited()
	}
}

func BenchmarkInMemRateLimiterIsLimited_Concurrent(b *testing.B) {
	b.ReportAllocs()
	rl := NewInMemRateLimiter(1000000)
	rl.Set(500)
	rl.RateLimited() // seed the cache
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rl.IsLimited()
		}
	})
}
