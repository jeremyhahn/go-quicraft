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

package stopper

import (
	"sync/atomic"
	"testing"
	"time"
)

// pollUntil polls pred at 1ms intervals until it returns true or deadline expires.
func pollUntil(t *testing.T, timeout time.Duration, desc string, pred func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	for {
		if pred() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("timed out after %v: %s", timeout, desc)
		}
	}
}

func TestRunWorker_RunsAndStopsCleanly(t *testing.T) {
	s := New()
	var executed atomic.Bool
	s.RunWorker(func() {
		executed.Store(true)
		<-s.ShouldStop()
	})
	pollUntil(t, 5*time.Second, "worker started", executed.Load)
	s.Stop()
	if !executed.Load() {
		t.Fatal("worker was never executed")
	}
}

func TestRunWorker_MultipleWorkersStopCleanly(t *testing.T) {
	s := New()
	const n = 10
	var count atomic.Int64
	for range n {
		s.RunWorker(func() {
			count.Add(1)
			<-s.ShouldStop()
		})
	}
	pollUntil(t, 5*time.Second, "all workers started", func() bool {
		return count.Load() == n
	})
	s.Stop()
	if got := count.Load(); got != n {
		t.Fatalf("expected %d workers to run, got %d", n, got)
	}
}

func TestStop_Idempotent(t *testing.T) {
	s := New()
	var count atomic.Int64
	s.RunWorker(func() {
		count.Add(1)
		<-s.ShouldStop()
	})
	pollUntil(t, 5*time.Second, "worker started", func() bool {
		return count.Load() == 1
	})
	// Call Stop multiple times — must not panic or deadlock.
	s.Stop()
	s.Stop()
	s.Stop()
	if got := count.Load(); got != 1 {
		t.Fatalf("expected worker to run once, got %d", got)
	}
}

func TestShouldStop_ClosedAfterStop(t *testing.T) {
	s := New()
	s.Stop()
	select {
	case <-s.ShouldStop():
		// Channel is closed — correct behavior.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ShouldStop channel was not closed after Stop")
	}
}

func TestClose_ReturnsNilError(t *testing.T) {
	s := New()
	var started atomic.Bool
	s.RunWorker(func() {
		started.Store(true)
		<-s.ShouldStop()
	})
	pollUntil(t, 5*time.Second, "worker started", started.Load)
	err := s.Close()
	if err != nil {
		t.Fatalf("expected nil error from Close, got %v", err)
	}
}

func TestClose_StopsWorkers(t *testing.T) {
	s := New()
	var started atomic.Bool
	var done atomic.Bool
	s.RunWorker(func() {
		started.Store(true)
		<-s.ShouldStop()
		done.Store(true)
	})
	pollUntil(t, 5*time.Second, "worker started", started.Load)
	_ = s.Close()
	if !done.Load() {
		t.Fatal("worker did not complete after Close")
	}
}

func TestStop_NoWorkers(t *testing.T) {
	// Stop with no workers must not panic or block.
	s := New()
	s.Stop()
}
