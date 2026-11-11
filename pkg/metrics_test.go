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

package quicraft

import (
	"testing"
	"time"
)

// panickingCollector panics on every method. It stands in for an embedder's
// metrics backend that fails on a closed registry, a full buffer, or a nil
// map — ordinary bugs a library must survive.
type panickingCollector struct{ NoOpMetricsCollector }

func (panickingCollector) ObserveProposalBatchSize(uint64, int) {
	panic("metrics backend exploded")
}
func (panickingCollector) IncElection(uint64)               { panic("metrics backend exploded") }
func (panickingCollector) RegisterShard(uint64)             { panic("metrics backend exploded") }
func (panickingCollector) IncWriteBytes(int64)              { panic("metrics backend exploded") }
func (panickingCollector) ObserveSendLatency(time.Duration) { panic("metrics backend exploded") }

// TestSafeMetricsCollector_ContainsPanics pins the barrier that keeps an
// embedder bug from deadlocking the host.
//
// The engine calls the collector between node.raftMu.Lock() and a plain
// node.raftMu.Unlock() in the step and commit workers. A panic escaping there
// leaves raftMu held forever; the worker's recover-and-restart loop then
// blocks on it, and with it every shard that worker owns and Host.Close. The
// symptom is a hung process with no error anywhere.
//
// The test asserts the wrapper swallows the panic AND records it, so a broken
// backend is visible rather than merely survived.
func TestSafeMetricsCollector_ContainsPanics(t *testing.T) {
	wrapped := newSafeMetricsCollector(panickingCollector{})
	safe, ok := wrapped.(*safeMetricsCollector)
	if !ok {
		t.Fatalf("newSafeMetricsCollector returned %T, want *safeMetricsCollector", wrapped)
	}

	// Each of these would take the host down if the panic escaped.
	safe.ObserveProposalBatchSize(1, 10)
	safe.IncElection(1)
	safe.RegisterShard(1)
	safe.IncWriteBytes(64)
	safe.ObserveSendLatency(time.Millisecond)

	if got := safe.PanicCount(); got != 5 {
		t.Fatalf("PanicCount() = %d, want 5: every panicking call must be "+
			"contained AND counted", got)
	}
}

// TestSafeMetricsCollector_WrappingIsIdempotentAndSkipsNoOp keeps the wrapper
// off paths that cannot panic: NoOpMetricsCollector's methods are empty by
// construction, and double-wrapping would add a second defer to every
// hot-path metric call.
func TestSafeMetricsCollector_WrappingIsIdempotentAndSkipsNoOp(t *testing.T) {
	if got := newSafeMetricsCollector(nil); got != nil {
		t.Errorf("newSafeMetricsCollector(nil) = %v, want nil", got)
	}
	noop := NoOpMetricsCollector{}
	if _, wrapped := newSafeMetricsCollector(noop).(*safeMetricsCollector); wrapped {
		t.Error("NoOpMetricsCollector was wrapped; its methods cannot panic")
	}
	once := newSafeMetricsCollector(panickingCollector{})
	if twice := newSafeMetricsCollector(once); twice != once {
		t.Error("wrapping is not idempotent: a collector was wrapped twice")
	}
}
