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

package rsm

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// rlockPollInterval bounds how long a registered reader sleeps between
// TryRLock attempts when no unlockApply wakes it: writers other than the
// apply stream (snapshot recovery, Close) do not signal readWait.
const rlockPollInterval = time.Millisecond

// readWaitQueue lets context-aware readers take readGuard fairly without a
// goroutine per waiter. A reader whose TryRLock fails registers (join), and
// until it leaves, lockApply waits for the registered set to drain before it
// queues for the write lock, and every unlockApply wakes the registered
// readers to retry. Between two applies no apply writer is pending, so the
// retry succeeds; RWMutex's own reader hand-off is therefore kept without a
// blocking RLock that a cancelled read could not abandon.
type readWaitQueue struct {
	// waiters mirrors count for the lock-free fast path of pending/signal.
	waiters atomic.Int32
	mu      sync.Mutex
	count   int
	// wake is closed by signal; readers fetch it before each TryRLock.
	wake chan struct{}
	// drained is closed when count drops to zero.
	drained chan struct{}
}

func (q *readWaitQueue) join() {
	q.mu.Lock()
	if q.count == 0 {
		q.drained = make(chan struct{})
	}
	q.count++
	q.waiters.Store(int32(q.count))
	q.mu.Unlock()
}

func (q *readWaitQueue) leave() {
	q.mu.Lock()
	q.count--
	q.waiters.Store(int32(q.count))
	if q.count == 0 {
		close(q.drained)
		q.drained = nil
	}
	q.mu.Unlock()
}

// wakeChan returns the channel the next signal closes.
func (q *readWaitQueue) wakeChan() <-chan struct{} {
	q.mu.Lock()
	if q.wake == nil {
		q.wake = make(chan struct{})
	}
	w := q.wake
	q.mu.Unlock()
	return w
}

// pending returns a channel closed once the readers registered now have all
// left, or nil when none is registered.
func (q *readWaitQueue) pending() <-chan struct{} {
	if q.waiters.Load() == 0 {
		return nil
	}
	q.mu.Lock()
	d := q.drained
	q.mu.Unlock()
	return d
}

// signal wakes every registered reader to retry TryRLock.
func (q *readWaitQueue) signal() {
	if q.waiters.Load() == 0 {
		return
	}
	q.mu.Lock()
	if q.wake != nil {
		close(q.wake)
		q.wake = nil
	}
	q.mu.Unlock()
}

// rlockContext takes the read guard. When the guard is free it takes it at
// once without checking ctx, so a context that is already done does not stop
// the read; it returns ctx.Err() only when ctx is done while the read is
// waiting for the guard. Update holds the guard's write side for the whole
// user SM Update call, so a plain RLock would keep a cancelled read waiting
// for as long as that Update runs.
//
// When the guard is not free at once the caller registers in readWait and
// retries TryRLock on its own goroutine, woken by each unlockApply (or every
// rlockPollInterval): nothing is left blocked in RLock behind an abandoned
// read, so repeated cancelled reads against a stalled Update cost no
// goroutines. lockApply yields to the registered readers, so a continuous
// Update stream cannot starve the read either, and the readers it yields to
// were registered before it, so reads cannot starve Update. A queued read
// rechecks ctx after each acquisition and releases the guard when it was
// cancelled, so a cancelled read never reaches the lookup.
func (s *StateMachine) rlockContext(ctx context.Context) error {
	if s.readGuard.TryRLock() {
		return nil
	}
	done := ctx.Done()
	if done == nil {
		s.readGuard.RLock()
		return nil
	}
	s.readWait.join()
	defer s.readWait.leave()
	timer := time.NewTimer(rlockPollInterval)
	defer timer.Stop()
	for {
		// Fetch the wake channel before the attempt, so an unlockApply
		// after a failed TryRLock always closes the channel waited on.
		wake := s.readWait.wakeChan()
		if s.readGuard.TryRLock() {
			// The context may have been cancelled while this reader was
			// queued and the guard released before it resumed: the select
			// below can then pick wake or the timer over done. A queued read
			// that was cancelled must not run the lookup.
			if err := ctx.Err(); err != nil {
				s.readGuard.RUnlock()
				return err
			}
			return nil
		}
		select {
		case <-done:
			return ctx.Err()
		case <-wake:
		case <-timer.C:
			timer.Reset(rlockPollInterval)
		}
	}
}
