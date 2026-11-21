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
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// newAuditStateMachine returns a regular-tier StateMachine over testKV.
func newAuditStateMachine(t *testing.T) *StateMachine {
	t.Helper()
	createFn := sm.NewCreateFunc(func(shardID, replicaID uint64) sm.StateMachine {
		return newTestKV()
	})
	s, err := NewStateMachine(1, 1, createFn, t.TempDir(), testPolicy(100, 100, 0), nil, false)
	if err != nil {
		t.Fatalf("NewStateMachine = %v", err)
	}
	return s
}

// TestAudit_RLockContext_ParkedReaderSeesCloseNotStaleState checks the
// Close interaction of rlockContext: Close takes the guard's write side
// directly and never signals readWait, so a context-aware reader parked
// behind an Update must still be woken (by its poll) once Close releases the
// guard, and must return ErrClosed rather than run against the closed state
// machine. Both read entry points are covered.
func TestAudit_RLockContext_ParkedReaderSeesCloseNotStaleState(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(s *StateMachine, ctx context.Context) error
	}{
		{"Lookup", func(s *StateMachine, ctx context.Context) error {
			_, err := s.Lookup(ctx, "key")
			return err
		}},
		{"NALookupBufContext", func(s *StateMachine, ctx context.Context) error {
			_, rel, _, err := s.NALookupBufContext(ctx, []byte("key"))
			rel()
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newAuditStateMachine(t)
			s.lockApply()
			// A cancellable context that is never cancelled: the reader takes
			// the registered (queued) path and waits for the guard alone.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errs := make(chan error, 1)
			go func() { errs <- tc.read(s, ctx) }()
			waitFor(t, 2*time.Second, "the reader to register in readWait", func() bool {
				return s.readWait.waiters.Load() > 0
			})

			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			// Close queues as a writer behind the held Update; releasing the
			// Update lets Close take the guard ahead of the woken reader.
			time.Sleep(5 * time.Millisecond)
			s.unlockApply()

			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("Close = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not return with a reader parked in readWait")
			}
			select {
			case err := <-errs:
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Fatalf("read after Close = %v, want ErrClosed or a read that completed before it", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("a reader parked in readWait was never woken after Close released the guard")
			}
			waitFor(t, 2*time.Second, "readWait to drain", func() bool {
				return s.readWait.waiters.Load() == 0
			})
		})
	}
}

// TestAudit_RLockContext_DeadlineWhileParkedLeavesGuardFree checks the
// request-deadline interaction: a reader whose deadline expires while it is
// parked behind an Update returns context.DeadlineExceeded, leaves readWait,
// and the next Update does not wait on it.
func TestAudit_RLockContext_DeadlineWhileParkedLeavesGuardFree(t *testing.T) {
	s := newAuditStateMachine(t)
	defer s.Close()

	s.lockApply()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := s.Lookup(ctx, "key")
	if !errors.Is(err, context.DeadlineExceeded) {
		s.unlockApply()
		t.Fatalf("Lookup past its deadline = %v, want context.DeadlineExceeded", err)
	}
	if n := s.readWait.waiters.Load(); n != 0 {
		s.unlockApply()
		t.Fatalf("readWait holds %d waiters after the reader's deadline, want 0", n)
	}
	if d := s.readWait.pending(); d != nil {
		s.unlockApply()
		t.Fatal("readWait reports pending readers after the reader left")
	}
	s.unlockApply()

	// The next apply takes the guard without yielding to a departed reader.
	done := make(chan struct{})
	go func() {
		s.lockApply()
		s.unlockApply()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lockApply waited on a reader that had already left")
	}
}

// TestAudit_RLockContext_RecoveryDoesNotParkReaders checks the snapshot
// recovery interaction: RecoverFromSnapshot holds the guard's read side, so
// a context-aware read during a recovery is not queued behind it (only
// applyMu excludes Apply), and no reader is left registered.
func TestAudit_RLockContext_RecoveryDoesNotParkReaders(t *testing.T) {
	s := newAuditStateMachine(t)
	defer s.Close()

	// Hold the read side as RecoverFromSnapshot does for its whole span.
	s.readGuard.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := s.Lookup(ctx, "key")
	s.readGuard.RUnlock()
	if err != nil {
		t.Fatalf("Lookup during a recovery's read hold = %v, want nil", err)
	}
	if n := s.readWait.waiters.Load(); n != 0 {
		t.Fatalf("readWait holds %d waiters, want 0", n)
	}
}
