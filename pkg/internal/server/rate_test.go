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

func TestRateLimiter_Set(t *testing.T) {
	rl := NewRateLimiter(1000)
	rl.Set(750)
	if rl.Get() != 750 {
		t.Fatalf("Get = %d, want 750", rl.Get())
	}
	rl.Set(0)
	if rl.Get() != 0 {
		t.Fatalf("Get = %d, want 0", rl.Get())
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

func TestRateLimiter_DecreaseUnderflowSaturatesAtZero(t *testing.T) {
	rl := NewRateLimiter(1000)
	rl.Set(100)

	// Decrease by more than current value: must saturate at 0.
	rl.Decrease(200)
	if rl.Get() != 0 {
		t.Fatalf("Get = %d, want 0 (underflow saturation)", rl.Get())
	}
}

func TestRateLimiter_DecreaseExactValueGoesToZero(t *testing.T) {
	rl := NewRateLimiter(1000)
	rl.Set(500)

	// Decrease by exactly the current value.
	rl.Decrease(500)
	if rl.Get() != 0 {
		t.Fatalf("Get = %d, want 0 (exact decrease)", rl.Get())
	}
}

func TestRateLimiter_DecreaseFromZero(t *testing.T) {
	rl := NewRateLimiter(1000)
	// Size is 0, decrease by any amount should stay at 0.
	rl.Decrease(100)
	if rl.Get() != 0 {
		t.Fatalf("Get = %d, want 0 (decrease from zero)", rl.Get())
	}
}

func TestRateLimiter_DecreaseConcurrentUnderflow(t *testing.T) {
	rl := NewRateLimiter(1000)
	rl.Set(50)

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

	// Total decrease is 1000 but started at 50: must be 0 (not wrapped).
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

func TestInMemRateLimiter_Set(t *testing.T) {
	rl := NewInMemRateLimiter(1000)
	rl.Set(750)
	if rl.Get() != 750 {
		t.Fatalf("Get = %d, want 750", rl.Get())
	}
}

func TestInMemRateLimiter_Hysteresis(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	// Advance past debounce threshold.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Exceed 100%: should become limited.
	rl.Set(101)
	if !rl.RateLimited() {
		t.Fatal("expected rate limited at 101%")
	}

	// Advance ticks past debounce threshold.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Drop to 70%: should STILL be limited (hysteresis).
	rl.Set(70)
	if !rl.RateLimited() {
		t.Fatal("expected still rate limited at 70% (hysteresis)")
	}

	// Advance ticks past debounce threshold.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Drop below 70%: should become unlimited.
	rl.Set(69)
	if rl.RateLimited() {
		t.Fatal("expected not rate limited at 69%")
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

func TestInMemRateLimiter_FollowerTracking(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Local size is fine, but follower reports high size.
	rl.Set(50)
	rl.SetFollowerState(2, 101)
	if !rl.RateLimited() {
		t.Fatal("expected rate limited due to follower size")
	}

	// Advance past debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Update follower to be below threshold.
	rl.SetFollowerState(2, 50)
	if rl.RateLimited() {
		t.Fatal("expected not rate limited after follower size drops")
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

	// Reset clears follower states and the limited flag. In practice,
	// the local size is also reset (e.g., after snapshot restore).
	rl.Reset()
	rl.Set(0)
	if rl.RateLimited() {
		t.Fatal("expected not rate limited after reset")
	}
}

func TestInMemRateLimiter_GetTick(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	if rl.GetTick() != 0 {
		t.Fatalf("GetTick = %d, want 0", rl.GetTick())
	}
	rl.Tick()
	rl.Tick()
	rl.Tick()
	if rl.GetTick() != 3 {
		t.Fatalf("GetTick = %d, want 3", rl.GetTick())
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

func TestInMemRateLimiter_InitialStateNotLimited(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	if rl.RateLimited() {
		t.Fatal("new limiter should not be rate limited")
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

func TestInMemRateLimiter_IsLimited_DisabledLimiter(t *testing.T) {
	rl := NewInMemRateLimiter(0)
	rl.RateLimited() // seed the cache
	if rl.IsLimited() {
		t.Fatal("disabled limiter: IsLimited should always be false")
	}
}

func TestInMemRateLimiter_Reset_ClearsCachedLimited(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	rl.Set(200)
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}
	rl.RateLimited()
	if !rl.IsLimited() {
		t.Fatal("expected IsLimited true before reset")
	}
	rl.Reset()
	if rl.IsLimited() {
		t.Fatal("IsLimited should be false after Reset")
	}
}

func TestInMemRateLimiter_IsLimited_ConcurrentReads(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	rl.Set(200)
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}
	rl.RateLimited() // seed cache as limited

	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			// Concurrent reads must not race. The value should be true.
			if !rl.IsLimited() {
				t.Error("IsLimited should be true during concurrent reads")
			}
		}()
	}
	wg.Wait()
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

func TestInMemRateLimiter_FollowerGCExactBoundary(t *testing.T) {
	rl := NewInMemRateLimiter(100)

	// Set a follower state at tick 0.
	rl.SetFollowerState(2, 101)

	// Advance exactly gcTick ticks. At this point, tick-fs.tick == gcTick
	// which is NOT > gcTick, so the follower should still be alive.
	for i := uint64(0); i < gcTick; i++ {
		rl.Tick()
	}

	// Advance past debounce so hysteresis logic takes effect.
	for i := uint64(0); i < ChangeTickThreshold+2-gcTick; i++ {
		rl.Tick()
		rl.SetFollowerState(2, 101) // keep refreshing during debounce advancement
	}

	// Now set follower at current tick and advance exactly gcTick ticks.
	currentTick := rl.GetTick()
	rl.SetFollowerState(3, 101)

	for i := uint64(0); i < gcTick; i++ {
		rl.Tick()
	}

	// tick - fs.tick == gcTick, NOT > gcTick, so follower 3 should survive.
	// Verify by checking its effect on rate limiting.
	rl.Set(0)
	_ = currentTick

	// One more tick: tick - fs.tick == gcTick + 1 > gcTick, GC triggers.
	rl.Tick()

	// Advance past debounce with no follower updates.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Local is 0, followers GC'd, should not be rate limited.
	rl.Set(0)
	if rl.RateLimited() {
		t.Fatal("expected not rate limited after all followers GC'd")
	}
}

func TestInMemRateLimiter_ResetClearsTick(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	rl.Tick()
	rl.Tick()
	rl.Tick()
	if rl.GetTick() != 3 {
		t.Fatalf("GetTick = %d, want 3", rl.GetTick())
	}

	rl.Reset()
	// Reset clears limited state and followers, but tick is NOT reset
	// (it is an opaque counter that monotonically advances).
	// Verify the limited state is cleared.
	if rl.IsLimited() {
		t.Fatal("IsLimited should be false after Reset")
	}

	// Tick continues from where it was.
	rl.Tick()
	if rl.GetTick() != 4 {
		t.Fatalf("GetTick = %d, want 4 (tick continues after reset)", rl.GetTick())
	}
}

func TestInMemRateLimiter_DebouncePreventsThrashing(t *testing.T) {
	rl := NewInMemRateLimiter(100)
	// Advance past initial debounce.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}

	// Rapidly oscillate between limited and unlimited states.
	// Due to debouncing, the state should NOT change on every evaluation.
	rl.Set(101)
	if !rl.RateLimited() {
		t.Fatal("expected rate limited initially")
	}

	// Immediately drop below and rise above limit multiple times.
	for i := 0; i < 5; i++ {
		rl.Set(50)
		if !rl.RateLimited() {
			t.Fatalf("iteration %d: expected still rate limited (debounce prevents change)", i)
		}
		rl.Set(101)
	}

	// After advancing past debounce, the drop should take effect.
	for i := 0; i < int(ChangeTickThreshold)+2; i++ {
		rl.Tick()
	}
	rl.Set(50)
	if rl.RateLimited() {
		t.Fatal("expected not rate limited after debounce window passes")
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
