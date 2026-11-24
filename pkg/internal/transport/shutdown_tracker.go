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
	"sync/atomic"
)

// shutdownBit is the flag bit in the int64 state, indicating shutdown.
// When set, TryAdd returns false and no new operations can begin.
const shutdownBit int64 = 1 << 62

// shutdownTracker is a lock-free reference counter that supports atomic
// rejection of new operations once shutdown is initiated. It solves the
// sync.WaitGroup race where Add(1) and Wait() cannot safely overlap.
//
// The counter uses an atomic.Int64 with the following layout:
//   - Bits 0-61: active operation count (up to ~4.6 quintillion)
//   - Bit 62:    shutdown flag
//
// TryAdd atomically increments the count only if the shutdown bit is not
// set. Done decrements the count and wakes the shutdown waiter when the
// count reaches zero. ShutdownAndWait sets the shutdown bit and blocks
// until all in-flight operations complete.
//
// Zero value is ready to use. ShutdownAndWait must be called at most
// once per instance.
//
// shutdownTracker is safe for concurrent use by multiple goroutines.
type shutdownTracker struct {
	state atomic.Int64

	// done is atomically published by ShutdownAndWait before setting
	// the shutdown bit, ensuring Done() sees a valid channel when it
	// observes the shutdown flag.
	done atomic.Pointer[chan struct{}]
}

// TryAdd attempts to register a new in-flight operation. It returns true
// if the operation was registered successfully, or false if shutdown has
// been initiated. When TryAdd returns true, the caller MUST call Done
// when the operation completes.
func (s *shutdownTracker) TryAdd() bool {
	for {
		old := s.state.Load()
		if old&shutdownBit != 0 {
			return false
		}
		if s.state.CompareAndSwap(old, old+1) {
			return true
		}
	}
}

// Done marks an in-flight operation as complete. It must be called
// exactly once for each successful TryAdd call. If this was the last
// in-flight operation and shutdown has been initiated, Done wakes the
// ShutdownAndWait caller.
func (s *shutdownTracker) Done() {
	newVal := s.state.Add(-1)
	count := newVal &^ shutdownBit
	if count == 0 && newVal&shutdownBit != 0 {
		// Last operation completed after shutdown. The done channel
		// is guaranteed to be non-nil here because ShutdownAndWait
		// publishes it via atomic store BEFORE setting the shutdown
		// bit (CAS). The CAS in ShutdownAndWait happens-before the
		// Load in this Add(-1) that observed the shutdown bit.
		ch := s.done.Load()
		select {
		case *ch <- struct{}{}:
		default:
		}
	}
}

// ShutdownAndWait sets the shutdown flag and blocks until all in-flight
// operations complete. After ShutdownAndWait returns, all future TryAdd
// calls return false. ShutdownAndWait must be called at most once.
func (s *shutdownTracker) ShutdownAndWait() {
	// Allocate and publish the done channel BEFORE setting the shutdown
	// bit. This ensures any Done() call that observes the shutdown bit
	// also sees a valid done channel via the atomic load.
	ch := make(chan struct{}, 1)
	s.done.Store(&ch)

	// Atomically set the shutdown bit.
	for {
		old := s.state.Load()
		if old&shutdownBit != 0 {
			// Already shut down (defensive; single caller contract).
			break
		}
		if s.state.CompareAndSwap(old, old|shutdownBit) {
			// Shutdown bit set. If count was already zero, no
			// in-flight operations exist -- return immediately.
			if old == 0 {
				return
			}
			break
		}
	}

	// Wait for all in-flight operations to complete.
	<-ch
}

// InFlight returns the number of currently active operations.
// This is intended for testing and monitoring only.
func (s *shutdownTracker) InFlight() int64 {
	return s.state.Load() &^ shutdownBit
}

// IsShutdown returns true if the shutdown flag has been set.
func (s *shutdownTracker) IsShutdown() bool {
	return s.state.Load()&shutdownBit != 0
}
