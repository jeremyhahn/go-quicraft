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
	"sync"
)

// shardLocks is a set of exclusive per-shard locks whose bookkeeping lives
// only while a shard's lock is held or waited on. The zero value is ready to
// use.
//
// Each lock is a buffered channel of capacity one: it admits exactly one
// holder and, unlike a sync.Mutex, lets a caller abandon the wait when its
// context is cancelled.
//
// A shard's record carries a count of the callers that hold or wait on its
// channel, and is deleted only when that count reaches zero. Deleting it on
// unlock instead would be an ABA bug: callers still waiting on the old
// channel and a newcomer that installed a fresh one would both "hold" the
// shard's lock at once.
type shardLocks struct {
	mu    sync.Mutex
	locks map[uint64]*shardLock
}

// shardLock is one shard's lock record.
type shardLock struct {
	ch   chan struct{} // capacity one; a send takes the lock, a receive frees it
	refs int           // callers holding or waiting on ch; guarded by shardLocks.mu
}

// ref returns shardID's record, creating it when absent, and counts the
// caller as one of its holders or waiters.
func (l *shardLocks) ref(shardID uint64) *shardLock {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.locks[shardID]
	if !ok {
		if l.locks == nil {
			l.locks = make(map[uint64]*shardLock)
		}
		rec = &shardLock{ch: make(chan struct{}, 1)}
		l.locks[shardID] = rec
	}
	rec.refs++
	return rec
}

// unref drops the caller's reference to rec, deleting the record once no
// caller holds or waits on it.
func (l *shardLocks) unref(shardID uint64, rec *shardLock) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec.refs--
	if rec.refs == 0 {
		delete(l.locks, shardID)
	}
}

// release returns the function that frees rec's lock and drops the holder's
// reference to it.
func (l *shardLocks) release(shardID uint64, rec *shardLock) func() {
	return func() {
		<-rec.ch
		l.unref(shardID, rec)
	}
}

// acquire takes shardID's lock, waiting until it is free or ctx is done. The
// returned function releases it.
func (l *shardLocks) acquire(ctx context.Context, shardID uint64) (func(), error) {
	rec := l.ref(shardID)
	select {
	case rec.ch <- struct{}{}:
		return l.release(shardID, rec), nil
	case <-ctx.Done():
		l.unref(shardID, rec)
		return nil, mapContextError(ctx.Err())
	}
}

// tryAcquire takes shardID's lock if it is free, reporting whether it did. The
// returned function releases it and is nil when the lock was not taken.
func (l *shardLocks) tryAcquire(shardID uint64) (func(), bool) {
	rec := l.ref(shardID)
	select {
	case rec.ch <- struct{}{}:
		return l.release(shardID, rec), true
	default:
		l.unref(shardID, rec)
		return nil, false
	}
}

// count reports how many shards have a lock record, that is, a lock held or
// waited on.
func (l *shardLocks) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.locks)
}
