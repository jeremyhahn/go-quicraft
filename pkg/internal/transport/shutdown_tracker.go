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

// shutdownTracker is a lock-free counter (bits 0-61 count, bit 62 shutdown) that refuses new operations after shutdown; the zero value is ready, ShutdownAndWait is called at most once.
type shutdownTracker struct {
	state atomic.Int64

	// done is atomically published by ShutdownAndWait before setting
	// the shutdown bit, ensuring Done() sees a valid channel when it
	// observes the shutdown flag.
	done atomic.Pointer[chan struct{}]
}

// TryAdd registers an in-flight operation unless shutdown has begun; on true the caller must call Done.
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

// Done completes one TryAdd and wakes ShutdownAndWait when the last finishes.
func (s *shutdownTracker) Done() {
	newVal := s.state.Add(-1)
	count := newVal &^ shutdownBit
	if count == 0 && newVal&shutdownBit != 0 {
		// done is non-nil: ShutdownAndWait publishes it before setting the shutdown bit.
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
	// Publish done before setting the shutdown bit.
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
