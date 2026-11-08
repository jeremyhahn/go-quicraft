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
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// shardLockRefs reports how many callers hold or wait on shardID's lock.
func shardLockRefs(l *shardLocks, shardID uint64) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if rec, ok := l.locks[shardID]; ok {
		return rec.refs
	}
	return 0
}

// waitForShardLockRefs waits until shardID's lock has want holders and
// waiters, so a test knows a goroutine is parked in acquire.
func waitForShardLockRefs(t *testing.T, l *shardLocks, shardID uint64, want int) {
	t.Helper()
	if !waitUntil(5*time.Second, func() bool { return shardLockRefs(l, shardID) == want }) {
		t.Fatalf("shard %d lock refs = %d, want %d", shardID, shardLockRefs(l, shardID), want)
	}
}

// TestShardLocks_Excludes: many concurrent acquirers of one shard never hold
// its lock together, and none of them leaves a record behind.
func TestShardLocks_Excludes(t *testing.T) {
	var l shardLocks
	var inside, maxInside atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				release, err := l.acquire(context.Background(), 7)
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				n := inside.Add(1)
				for {
					m := maxInside.Load()
					if n <= m || maxInside.CompareAndSwap(m, n) {
						break
					}
				}
				inside.Add(-1)
				release()
			}
		}()
	}
	wg.Wait()
	if m := maxInside.Load(); m != 1 {
		t.Fatalf("%d callers held the lock at once, want 1", m)
	}
	if n := l.count(); n != 0 {
		t.Fatalf("%d lock records remain, want 0", n)
	}
}

// TestShardLocks_TryAcquire: tryAcquire is refused while the lock is held,
// the refusal leaves no reference, and the lock is free again on release.
func TestShardLocks_TryAcquire(t *testing.T) {
	var l shardLocks
	release, ok := l.tryAcquire(3)
	if !ok {
		t.Fatal("tryAcquire of a free lock was refused")
	}
	if again, ok := l.tryAcquire(3); ok || again != nil {
		t.Fatal("tryAcquire of a held lock succeeded")
	}
	if refs := shardLockRefs(&l, 3); refs != 1 {
		t.Fatalf("refs after a refused tryAcquire = %d, want 1 (the holder)", refs)
	}
	other, ok := l.tryAcquire(4)
	if !ok {
		t.Fatal("another shard's lock was refused")
	}
	other()
	release()
	if n := l.count(); n != 0 {
		t.Fatalf("%d lock records remain, want 0", n)
	}
	release, ok = l.tryAcquire(3)
	if !ok {
		t.Fatal("tryAcquire after the release was refused")
	}
	release()
}

// TestShardLocks_CancelledWaiterReleasesItsReference: a caller whose context
// ends while it waits returns the mapped context error and drops its
// reference, so the record is gone once the holder releases.
func TestShardLocks_CancelledWaiterReleasesItsReference(t *testing.T) {
	var l shardLocks
	release, err := l.acquire(context.Background(), 9)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errC := make(chan error, 1)
	go func() {
		_, err := l.acquire(ctx, 9)
		errC <- err
	}()
	waitForShardLockRefs(t, &l, 9, 2)
	cancel()
	if err := <-errC; !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancelled acquire = %v, want ErrCanceled", err)
	}
	if refs := shardLockRefs(&l, 9); refs != 1 {
		t.Fatalf("refs after the waiter gave up = %d, want 1 (the holder)", refs)
	}

	expired, cancelExpired := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelExpired()
	if _, err := l.acquire(expired, 9); !errors.Is(err, ErrTimeout) {
		t.Fatalf("acquire past its deadline = %v, want ErrTimeout", err)
	}

	release()
	if n := l.count(); n != 0 {
		t.Fatalf("%d lock records remain, want 0", n)
	}
}

// TestShardLocks_NoLostWakeup: a waiter that gives up does not take another
// waiter's turn or strand it on a channel the set has forgotten — the
// remaining waiter acquires the lock as soon as the holder releases it.
func TestShardLocks_NoLostWakeup(t *testing.T) {
	var l shardLocks
	release, err := l.acquire(context.Background(), 11)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	quitter, cancelQuitter := context.WithCancel(context.Background())
	quitErrC := make(chan error, 1)
	go func() {
		_, err := l.acquire(quitter, 11)
		quitErrC <- err
	}()
	waitForShardLockRefs(t, &l, 11, 2)

	acquiredC := make(chan func(), 1)
	go func() {
		r, err := l.acquire(context.Background(), 11)
		if err != nil {
			t.Errorf("waiter acquire: %v", err)
			close(acquiredC)
			return
		}
		acquiredC <- r
	}()
	waitForShardLockRefs(t, &l, 11, 3)

	cancelQuitter()
	if err := <-quitErrC; !errors.Is(err, ErrCanceled) {
		t.Fatalf("quitter acquire = %v, want ErrCanceled", err)
	}
	select {
	case <-acquiredC:
		t.Fatal("waiter acquired while the holder still held the lock")
	default:
	}

	release()
	select {
	case r, ok := <-acquiredC:
		if !ok {
			t.FailNow()
		}
		// A newcomer must contend with the waiter's lock, not a fresh one.
		if _, ok := l.tryAcquire(11); ok {
			t.Fatal("a newcomer acquired the lock the woken waiter holds")
		}
		r()
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never acquired the released lock")
	}
	if n := l.count(); n != 0 {
		t.Fatalf("%d lock records remain, want 0", n)
	}
}
