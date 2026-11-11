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

package perfresult

import (
	"sync"
	"testing"
	"time"
)

func TestNewLatencyRecorderMinCapacity(t *testing.T) {
	rec := NewLatencyRecorder(0)
	if rec == nil {
		t.Fatal("expected non-nil recorder")
	}
	// Even with 0 expected samples, capacity should be at least 64.
	if cap(rec.samples) < 64 {
		t.Fatalf("expected minimum capacity 64, got %d", cap(rec.samples))
	}
}

func TestResetRetainsAllocation(t *testing.T) {
	rec := NewLatencyRecorder(100)
	for i := 0; i < 50; i++ {
		rec.Record(time.Duration(i) * time.Microsecond)
	}

	capBefore := cap(rec.samples)
	rec.Reset()
	capAfter := cap(rec.samples)

	if rec.Count() != 0 {
		t.Fatalf("expected count 0 after reset, got %d", rec.Count())
	}
	if capAfter < capBefore {
		t.Fatalf("expected capacity retained after reset, before=%d, after=%d", capBefore, capAfter)
	}
}

func TestEmptyRecorderReturnsZero(t *testing.T) {
	rec := NewLatencyRecorder(10)

	tests := []struct {
		name string
		get  func() time.Duration
	}{
		{name: "p50", get: func() time.Duration { return rec.Percentile(0.50) }},
		{name: "p99", get: func() time.Duration { return rec.Percentile(0.99) }},
		{name: "min", get: rec.Min},
		{name: "max", get: rec.Max},
		{name: "mean", get: rec.Mean},
		{name: "sum", get: rec.Sum},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.get(); got != 0 {
				t.Fatalf("expected 0 for %s on empty recorder, got %v", tt.name, got)
			}
		})
	}
}

func TestPercentileSingleSample(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(42 * time.Microsecond)

	// With a single sample, all percentiles should return that sample.
	if rec.P50() != 42*time.Microsecond {
		t.Fatalf("expected p50=42us, got %v", rec.P50())
	}
	if rec.P90() != 42*time.Microsecond {
		t.Fatalf("expected p90=42us, got %v", rec.P90())
	}
	if rec.P95() != 42*time.Microsecond {
		t.Fatalf("expected p95=42us, got %v", rec.P95())
	}
	if rec.P99() != 42*time.Microsecond {
		t.Fatalf("expected p99=42us, got %v", rec.P99())
	}
	if rec.P999() != 42*time.Microsecond {
		t.Fatalf("expected p999=42us, got %v", rec.P999())
	}
}

func TestPercentileP0Edge(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(1 * time.Millisecond)
	rec.Record(2 * time.Millisecond)
	rec.Record(3 * time.Millisecond)

	// p=0.0 should clamp rank to 1, returning the smallest element.
	got := rec.Percentile(0.0)
	if got != 1*time.Millisecond {
		t.Fatalf("expected p0=1ms, got %v", got)
	}
}

func TestSum(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(10 * time.Millisecond)
	rec.Record(20 * time.Millisecond)
	rec.Record(30 * time.Millisecond)

	if rec.Sum() != 60*time.Millisecond {
		t.Fatalf("expected sum=60ms, got %v", rec.Sum())
	}
}

func TestStatsEmpty(t *testing.T) {
	rec := NewLatencyRecorder(10)
	stats := rec.Stats()

	if stats.Count != 0 {
		t.Fatalf("expected count 0, got %d", stats.Count)
	}
	if stats.P50Ns != 0 || stats.P99Ns != 0 || stats.P999Ns != 0 || stats.MinNs != 0 || stats.MaxNs != 0 || stats.MeanNs != 0 {
		t.Fatal("expected all stats to be 0 on empty recorder")
	}
}

func TestStatsPopulated(t *testing.T) {
	rec := NewLatencyRecorder(100)
	for i := 1; i <= 100; i++ {
		rec.Record(time.Duration(i) * time.Microsecond)
	}

	stats := rec.Stats()

	if stats.Count != 100 {
		t.Fatalf("expected count 100, got %d", stats.Count)
	}
	if stats.MinNs != (1 * time.Microsecond).Nanoseconds() {
		t.Fatalf("expected min=1us, got %dns", stats.MinNs)
	}
	if stats.MaxNs != (100 * time.Microsecond).Nanoseconds() {
		t.Fatalf("expected max=100us, got %dns", stats.MaxNs)
	}
	if stats.P50Ns != (50 * time.Microsecond).Nanoseconds() {
		t.Fatalf("expected p50=50us, got %dns", stats.P50Ns)
	}
	// Nearest-rank: rank = ceil(p * n).
	if stats.P90Ns != (90 * time.Microsecond).Nanoseconds() {
		t.Fatalf("expected p90=90us, got %dns", stats.P90Ns)
	}
	if stats.P95Ns != (95 * time.Microsecond).Nanoseconds() {
		t.Fatalf("expected p95=95us, got %dns", stats.P95Ns)
	}
	if stats.P99Ns != (99 * time.Microsecond).Nanoseconds() {
		t.Fatalf("expected p99=99us, got %dns", stats.P99Ns)
	}
	// P999: ceil(0.999 * 100) = 100 -> samples[99] = 100us
	if stats.P999Ns != (100 * time.Microsecond).Nanoseconds() {
		t.Fatalf("expected p999=100us, got %dns", stats.P999Ns)
	}
	// Mean of 1..100 = 50.5us, integer division truncation -> 50500ns
	expectedMean := int64(50500)
	if stats.MeanNs != expectedMean {
		t.Fatalf("expected mean=%dns, got %dns", expectedMean, stats.MeanNs)
	}
}

func TestConcurrentRecordAndRead(t *testing.T) {
	rec := NewLatencyRecorder(1000)
	const writers = 10
	const samplesPerWriter = 100

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < samplesPerWriter; i++ {
				rec.Record(time.Duration(i+1) * time.Microsecond)
			}
		}()
	}
	wg.Wait()

	expectedCount := writers * samplesPerWriter
	if rec.Count() != expectedCount {
		t.Fatalf("expected count %d, got %d", expectedCount, rec.Count())
	}

	// Verify percentile and stats are accessible without panicking.
	_ = rec.P50()
	_ = rec.P99()
	_ = rec.Min()
	_ = rec.Max()
	_ = rec.Mean()
	_ = rec.Stats()
}

func TestConcurrentRecordAndReset(t *testing.T) {
	rec := NewLatencyRecorder(100)

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer goroutine.
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			rec.Record(time.Duration(i+1) * time.Microsecond)
		}
	}()

	// Resetter goroutine.
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			rec.Reset()
		}
	}()

	wg.Wait()

	// Just verify no panic or race; count may vary.
	_ = rec.Count()
}

func TestRecordInvalidatesSorted(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(3 * time.Millisecond)
	rec.Record(1 * time.Millisecond)

	// Force a sort by calling Percentile.
	_ = rec.P50()

	// Now recording again should invalidate the sorted flag.
	rec.Record(2 * time.Millisecond)

	// Min should still work correctly after re-sort.
	if rec.Min() != 1*time.Millisecond {
		t.Fatalf("expected min=1ms after re-record, got %v", rec.Min())
	}
	if rec.Max() != 3*time.Millisecond {
		t.Fatalf("expected max=3ms after re-record, got %v", rec.Max())
	}
}

func TestPercentileAboveOneClampedToMax(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(5 * time.Millisecond)
	rec.Record(10 * time.Millisecond)

	// Percentile > 1.0 should clamp rank to n.
	got := rec.Percentile(1.5)
	if got != 10*time.Millisecond {
		t.Fatalf("expected max value for percentile > 1.0, got %v", got)
	}
}
