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

package transport

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestShutdownTracker_TryAdd_Success verifies that TryAdd succeeds
// when shutdown has not been initiated.
func TestShutdownTracker_TryAdd_Success(t *testing.T) {
	var st shutdownTracker
	if !st.TryAdd() {
		t.Fatal("TryAdd should succeed before shutdown")
	}
	if st.InFlight() != 1 {
		t.Fatalf("InFlight: got %d, want 1", st.InFlight())
	}
	st.Done()
	if st.InFlight() != 0 {
		t.Fatalf("InFlight after Done: got %d, want 0", st.InFlight())
	}
}

// TestShutdownTracker_TryAdd_AfterShutdown verifies that TryAdd returns
// false after ShutdownAndWait has been called.
func TestShutdownTracker_TryAdd_AfterShutdown(t *testing.T) {
	var st shutdownTracker
	st.ShutdownAndWait()
	if st.TryAdd() {
		t.Fatal("TryAdd should fail after shutdown")
	}
	if !st.IsShutdown() {
		t.Fatal("IsShutdown should return true after shutdown")
	}
}

// TestShutdownTracker_ShutdownAndWait_NoInFlight verifies that
// ShutdownAndWait returns immediately when no operations are in flight.
func TestShutdownTracker_ShutdownAndWait_NoInFlight(t *testing.T) {
	var st shutdownTracker
	done := make(chan struct{})
	go func() {
		st.ShutdownAndWait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ShutdownAndWait should return immediately with no in-flight ops")
	}
}

// TestShutdownTracker_ShutdownAndWait_WaitsForInFlight verifies that
// ShutdownAndWait blocks until all in-flight operations complete.
func TestShutdownTracker_ShutdownAndWait_WaitsForInFlight(t *testing.T) {
	var st shutdownTracker

	// Start an operation.
	if !st.TryAdd() {
		t.Fatal("TryAdd should succeed")
	}

	shutdownDone := make(chan struct{})
	go func() {
		st.ShutdownAndWait()
		close(shutdownDone)
	}()

	// ShutdownAndWait should block while an operation is in flight.
	select {
	case <-shutdownDone:
		t.Fatal("ShutdownAndWait returned while operation is still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	// Complete the operation -- ShutdownAndWait should now return.
	st.Done()

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ShutdownAndWait should return after last in-flight op completes")
	}
}

// TestShutdownTracker_ConcurrentTryAddDuringShutdown verifies that
// concurrent TryAdd calls during shutdown are properly rejected without
// any races. This exercises the core race condition that shutdownTracker
// is designed to prevent.
func TestShutdownTracker_ConcurrentTryAddDuringShutdown(t *testing.T) {
	for iter := 0; iter < 100; iter++ {
		var st shutdownTracker
		var succeeded atomic.Int64
		var wg sync.WaitGroup

		// Launch goroutines that all try to add concurrently.
		const goroutines = 32
		start := make(chan struct{})
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if st.TryAdd() {
					succeeded.Add(1)
					// Simulate some work.
					time.Sleep(time.Microsecond)
					st.Done()
				}
			}()
		}

		// Release all goroutines simultaneously and start shutdown.
		close(start)
		st.ShutdownAndWait()

		// After shutdown, InFlight must be zero.
		if st.InFlight() != 0 {
			t.Fatalf("iter %d: InFlight after shutdown: %d", iter, st.InFlight())
		}

		// Wait for all goroutines to finish.
		wg.Wait()
	}
}

// TestShutdownTracker_MultipleTryAddAndDone verifies correct counting
// with multiple concurrent TryAdd/Done cycles.
func TestShutdownTracker_MultipleTryAddAndDone(t *testing.T) {
	var st shutdownTracker

	const ops = 100
	for i := 0; i < ops; i++ {
		if !st.TryAdd() {
			t.Fatalf("TryAdd %d should succeed", i)
		}
	}
	if st.InFlight() != ops {
		t.Fatalf("InFlight: got %d, want %d", st.InFlight(), ops)
	}

	for i := 0; i < ops; i++ {
		st.Done()
	}
	if st.InFlight() != 0 {
		t.Fatalf("InFlight after all Done: got %d, want 0", st.InFlight())
	}
}

// TestShutdownTracker_IsShutdown_BeforeAndAfter verifies IsShutdown
// returns the correct value before and after shutdown.
func TestShutdownTracker_IsShutdown_BeforeAndAfter(t *testing.T) {
	var st shutdownTracker
	if st.IsShutdown() {
		t.Fatal("IsShutdown should be false before shutdown")
	}
	st.ShutdownAndWait()
	if !st.IsShutdown() {
		t.Fatal("IsShutdown should be true after shutdown")
	}
}

// TestShutdownTracker_ZeroValueIsReady verifies that a zero-value
// shutdownTracker is ready to use without any initialization.
func TestShutdownTracker_ZeroValueIsReady(t *testing.T) {
	var st shutdownTracker
	if st.IsShutdown() {
		t.Fatal("zero-value should not be in shutdown state")
	}
	if st.InFlight() != 0 {
		t.Fatal("zero-value should have 0 in-flight")
	}
	if !st.TryAdd() {
		t.Fatal("TryAdd on zero-value should succeed")
	}
	st.Done()
}

// TestShutdownTracker_ShutdownAndWait_MultipleInFlight verifies that
// ShutdownAndWait waits for all N operations, not just the first.
func TestShutdownTracker_ShutdownAndWait_MultipleInFlight(t *testing.T) {
	var st shutdownTracker

	const ops = 10
	for i := 0; i < ops; i++ {
		if !st.TryAdd() {
			t.Fatal("TryAdd should succeed")
		}
	}

	shutdownDone := make(chan struct{})
	go func() {
		st.ShutdownAndWait()
		close(shutdownDone)
	}()

	// Complete all but one operation.
	for i := 0; i < ops-1; i++ {
		st.Done()
		select {
		case <-shutdownDone:
			t.Fatalf("ShutdownAndWait returned with %d ops remaining", ops-1-i)
		case <-time.After(time.Millisecond):
		}
	}

	// Complete the last operation.
	st.Done()

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ShutdownAndWait should return after all ops complete")
	}
}

func TestShutdownTracker_ShutdownAndWaitWithInFlight(t *testing.T) {
	var tracker shutdownTracker

	// Start an in-flight operation.
	if !tracker.TryAdd() {
		t.Fatal("TryAdd should succeed before shutdown")
	}

	// Start ShutdownAndWait in a goroutine - it will block until Done().
	done := make(chan struct{})
	go func() {
		tracker.ShutdownAndWait()
		close(done)
	}()

	// Give time for ShutdownAndWait to set the flag and start waiting.
	time.Sleep(50 * time.Millisecond)

	// New TryAdd should fail after shutdown.
	if tracker.TryAdd() {
		t.Fatal("TryAdd should fail after shutdown initiated")
	}

	// Complete the in-flight operation.
	tracker.Done()

	// ShutdownAndWait should return.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ShutdownAndWait did not return after in-flight completed")
	}
}

// ---------------------------------------------------------------------------
// getOrCreateSendQueue stopped during write lock (quic.go:562-565)
// ---------------------------------------------------------------------------

// TestGetOrCreateSendQueue_StoppedWhileWaitingForWriteLock exercises
// the stopped check inside the write lock (quic.go:562-565). It holds
// the sendQueuesMu write lock, stops the transport, then releases the
// lock. A concurrent getOrCreateSendQueue call blocks at Lock(), and
// when it acquires the lock, stopped=true.
