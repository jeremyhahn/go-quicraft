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

func TestNewLatencyRecorderLargeCapacity(t *testing.T) {
	rec := NewLatencyRecorder(10000)
	if cap(rec.samples) < 10000 {
		t.Fatalf("expected capacity >= 10000, got %d", cap(rec.samples))
	}
}

func TestRecordAndCount(t *testing.T) {
	rec := NewLatencyRecorder(10)

	if rec.Count() != 0 {
		t.Fatalf("expected count 0, got %d", rec.Count())
	}

	rec.Record(5 * time.Millisecond)
	rec.Record(10 * time.Millisecond)
	rec.Record(1 * time.Millisecond)

	if rec.Count() != 3 {
		t.Fatalf("expected count 3, got %d", rec.Count())
	}
}

func TestReset(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(5 * time.Millisecond)
	rec.Record(10 * time.Millisecond)

	rec.Reset()

	if rec.Count() != 0 {
		t.Fatalf("expected count 0 after reset, got %d", rec.Count())
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

	if capAfter < capBefore {
		t.Fatalf("expected capacity retained after reset, before=%d, after=%d", capBefore, capAfter)
	}
}

func TestPercentileEmptyRecorder(t *testing.T) {
	rec := NewLatencyRecorder(10)

	if rec.Percentile(0.50) != 0 {
		t.Fatal("expected 0 for p50 on empty recorder")
	}
	if rec.Percentile(0.99) != 0 {
		t.Fatal("expected 0 for p99 on empty recorder")
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

func TestPercentileP1Edge(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(1 * time.Millisecond)
	rec.Record(2 * time.Millisecond)
	rec.Record(3 * time.Millisecond)

	// p=1.0 should return the largest element.
	got := rec.Percentile(1.0)
	if got != 3*time.Millisecond {
		t.Fatalf("expected p100=3ms, got %v", got)
	}
}

func TestPercentileKnownDistribution(t *testing.T) {
	rec := NewLatencyRecorder(100)
	// Record 1ms through 100ms.
	for i := 1; i <= 100; i++ {
		rec.Record(time.Duration(i) * time.Millisecond)
	}

	// Nearest-rank: rank = ceil(p * n)
	// P50: ceil(0.50 * 100) = 50 -> samples[49] = 50ms
	if rec.P50() != 50*time.Millisecond {
		t.Fatalf("expected p50=50ms, got %v", rec.P50())
	}
	// P90: ceil(0.90 * 100) = 90 -> samples[89] = 90ms
	if rec.P90() != 90*time.Millisecond {
		t.Fatalf("expected p90=90ms, got %v", rec.P90())
	}
	// P95: ceil(0.95 * 100) = 95 -> samples[94] = 95ms
	if rec.P95() != 95*time.Millisecond {
		t.Fatalf("expected p95=95ms, got %v", rec.P95())
	}
	// P99: ceil(0.99 * 100) = 99 -> samples[98] = 99ms
	if rec.P99() != 99*time.Millisecond {
		t.Fatalf("expected p99=99ms, got %v", rec.P99())
	}
}

func TestMinEmpty(t *testing.T) {
	rec := NewLatencyRecorder(10)
	if rec.Min() != 0 {
		t.Fatal("expected 0 for Min on empty recorder")
	}
}

func TestMaxEmpty(t *testing.T) {
	rec := NewLatencyRecorder(10)
	if rec.Max() != 0 {
		t.Fatal("expected 0 for Max on empty recorder")
	}
}

func TestMinMax(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(50 * time.Microsecond)
	rec.Record(10 * time.Microsecond)
	rec.Record(100 * time.Microsecond)
	rec.Record(1 * time.Microsecond)

	if rec.Min() != 1*time.Microsecond {
		t.Fatalf("expected min=1us, got %v", rec.Min())
	}
	if rec.Max() != 100*time.Microsecond {
		t.Fatalf("expected max=100us, got %v", rec.Max())
	}
}

func TestMeanEmpty(t *testing.T) {
	rec := NewLatencyRecorder(10)
	if rec.Mean() != 0 {
		t.Fatal("expected 0 for Mean on empty recorder")
	}
}

func TestMean(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(10 * time.Millisecond)
	rec.Record(20 * time.Millisecond)
	rec.Record(30 * time.Millisecond)

	// Mean = (10+20+30)/3 = 20ms
	if rec.Mean() != 20*time.Millisecond {
		t.Fatalf("expected mean=20ms, got %v", rec.Mean())
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

func TestP999(t *testing.T) {
	rec := NewLatencyRecorder(1000)
	for i := 1; i <= 1000; i++ {
		rec.Record(time.Duration(i) * time.Microsecond)
	}
	p999 := rec.P999()
	// 99.9th percentile of 1-1000us should be 999us or 1000us
	if p999 < 999*time.Microsecond || p999 > 1000*time.Microsecond {
		t.Fatalf("expected P999 ~999-1000us, got %v", p999)
	}
}

func TestP999Empty(t *testing.T) {
	rec := NewLatencyRecorder(10)
	if rec.P999() != 0 {
		t.Fatal("expected 0 for P999 on empty recorder")
	}
}

func TestP999SingleSample(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(42 * time.Microsecond)
	if rec.P999() != 42*time.Microsecond {
		t.Fatalf("expected P999=42us with single sample, got %v", rec.P999())
	}
}

func TestPercentileNegativeClampedToMin(t *testing.T) {
	rec := NewLatencyRecorder(10)
	rec.Record(5 * time.Millisecond)
	rec.Record(10 * time.Millisecond)

	// Negative percentile should clamp rank to 1.
	got := rec.Percentile(-0.5)
	if got != 5*time.Millisecond {
		t.Fatalf("expected min value for negative percentile, got %v", got)
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
