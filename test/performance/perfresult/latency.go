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
	"math"
	"sort"
	"sync"
	"time"
)

// LatencyRecorder accumulates latency observations for percentile analysis.
// It is safe for concurrent use.
type LatencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
	sorted  bool
}

// NewLatencyRecorder creates a LatencyRecorder pre-allocated for the
// expected number of observations.
func NewLatencyRecorder(expectedSamples int) *LatencyRecorder {
	cap := expectedSamples
	if cap < 64 {
		cap = 64
	}
	return &LatencyRecorder{
		samples: make([]time.Duration, 0, cap),
	}
}

// Record appends a single latency observation.
func (r *LatencyRecorder) Record(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d)
	r.sorted = false
	r.mu.Unlock()
}

// Count returns the number of recorded observations.
func (r *LatencyRecorder) Count() int {
	r.mu.Lock()
	n := len(r.samples)
	r.mu.Unlock()
	return n
}

// Reset clears all recorded observations, retaining the backing allocation.
func (r *LatencyRecorder) Reset() {
	r.mu.Lock()
	r.samples = r.samples[:0]
	r.sorted = true
	r.mu.Unlock()
}

// ensureSorted sorts the sample slice if it has not been sorted since
// the last Record call. Must be called with r.mu held.
func (r *LatencyRecorder) ensureSorted() {
	if r.sorted {
		return
	}
	sort.Slice(r.samples, func(i, j int) bool {
		return r.samples[i] < r.samples[j]
	})
	r.sorted = true
}

// Percentile returns the latency at the given percentile (0.0 to 1.0)
// using the nearest-rank method. Returns 0 when no observations have
// been recorded.
func (r *LatencyRecorder) Percentile(p float64) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := len(r.samples)
	if n == 0 {
		return 0
	}

	r.ensureSorted()

	rank := int(math.Ceil(p * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return r.samples[rank-1]
}

// P50 returns the median latency.
func (r *LatencyRecorder) P50() time.Duration {
	return r.Percentile(0.50)
}

// P90 returns the 90th percentile latency.
func (r *LatencyRecorder) P90() time.Duration {
	return r.Percentile(0.90)
}

// P95 returns the 95th percentile latency.
func (r *LatencyRecorder) P95() time.Duration {
	return r.Percentile(0.95)
}

// P99 returns the 99th percentile latency.
func (r *LatencyRecorder) P99() time.Duration {
	return r.Percentile(0.99)
}

// P999 returns the 99.9th percentile latency.
func (r *LatencyRecorder) P999() time.Duration {
	return r.Percentile(0.999)
}

// Min returns the minimum observed latency.
func (r *LatencyRecorder) Min() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.samples) == 0 {
		return 0
	}
	r.ensureSorted()
	return r.samples[0]
}

// Max returns the maximum observed latency.
func (r *LatencyRecorder) Max() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.samples) == 0 {
		return 0
	}
	r.ensureSorted()
	return r.samples[len(r.samples)-1]
}

// Mean returns the arithmetic mean of all observations.
// Returns 0 when no observations have been recorded.
func (r *LatencyRecorder) Mean() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := len(r.samples)
	if n == 0 {
		return 0
	}

	var total time.Duration
	for _, d := range r.samples {
		total += d
	}
	return total / time.Duration(n)
}

// Stats returns a LatencyStats snapshot from the recorded observations.
func (r *LatencyRecorder) Stats() LatencyStats {
	count := r.Count()
	if count == 0 {
		return LatencyStats{}
	}
	return LatencyStats{
		P50Ns:  r.P50().Nanoseconds(),
		P90Ns:  r.P90().Nanoseconds(),
		P95Ns:  r.P95().Nanoseconds(),
		P99Ns:  r.P99().Nanoseconds(),
		P999Ns: r.P999().Nanoseconds(),
		MinNs:  r.Min().Nanoseconds(),
		MaxNs:  r.Max().Nanoseconds(),
		MeanNs: r.Mean().Nanoseconds(),
		Count:  int64(count),
	}
}
