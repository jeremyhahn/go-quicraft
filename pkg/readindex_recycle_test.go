package quicraft

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPendingReadNeverCompletesRecycledRequestState replays, step by step,
// the interleaving behind #831. A ReadIndex request is registered in
// pendingReads (acquireRef). The completer (confirmReadyReads) completes it
// and drops the map's reference (releaseRef), which takes the pending count
// to zero while the caller still holds the request. Before the completer's
// recycle check finishes, the caller takes the result and releases
// (Release), the object goes back to the pool, and the next ReadIndex takes
// it and resets it (newRequestState). The completer's late check must not
// then return that live object to the pool: the pool would hand it to a
// third request while the second one's pendingReads entry still points at
// it, and that entry would complete the wrong request.
func TestPendingReadNeverCompletesRecycledRequestState(t *testing.T) {
	first := newRequestState(NewRequestStatePool(), 1, time.Time{}, true)
	first.acquireRef() // the pendingReads entry
	// The pool the next ReadIndex draws from; it hands out first's object.
	reuse := &sync.Pool{New: func() interface{} { return first }}

	var next *RequestState
	callerReleased := false
	testHookTryRecycle = func(rs *RequestState, stage int) {
		if rs != first {
			return
		}
		switch stage {
		case recycleStageEnter:
			if !callerReleased {
				// The caller takes the result and releases.
				callerReleased = true
				if got := <-first.ResultC(); got.Value != 1 {
					t.Errorf("first read result = %d, want 1", got.Value)
				}
				first.Release()
			}
		case recycleStageDecide:
			if next == nil && first.recycled.Load() {
				// The next ReadIndex takes the recycled object.
				next = newRequestState(reuse, 2, time.Time{}, true)
			}
		}
	}
	defer func() { testHookTryRecycle = nil }()

	// The completer: complete the read, remove its entry, drop the
	// map's reference.
	first.complete(RequestResult{Value: 1})
	first.releaseRef()
	testHookTryRecycle = nil

	if !callerReleased {
		if got := <-first.ResultC(); got.Value != 1 {
			t.Fatalf("first read result = %d, want 1", got.Value)
		}
		first.Release()
	}
	if next == nil {
		next = newRequestState(reuse, 2, time.Time{}, true)
	}
	next.acquireRef() // the next read's pendingReads entry

	if next.recycled.Load() {
		t.Fatal("a live ReadIndex request's RequestState was returned to the pool: " +
			"a late recycle check of the previous request recycled it after its reset")
	}
	if next.key != 2 {
		t.Fatalf("next request key = %d, want 2", next.key)
	}

	// The next read completes normally and returns to the pool once.
	next.complete(RequestResult{Value: 2})
	next.releaseRef()
	if got := <-next.ResultC(); got.Value != 2 {
		t.Fatalf("next read result = %d, want 2", got.Value)
	}
	next.Release()
	if !next.recycled.Load() {
		t.Fatal("the next request was not recycled after its last reference")
	}
}

// TestPendingReadRecycleUnderContention drives the same lifecycle
// concurrently on one shared pool: a completer goroutine's complete and
// releaseRef race the caller's Release, and the object goes straight back
// into use. Each request checks, until it releases, that the object is still
// its own: the result it received and its key are its own, and it is not
// marked recycled. Under -race a double return to the pool also shows as a
// data race between two requests sharing one object.
func TestPendingReadRecycleUnderContention(t *testing.T) {
	pool := NewRequestStatePool()
	workers := 4 * runtime.GOMAXPROCS(0)
	if workers < 16 {
		workers = 16
	}
	stop := time.Now().Add(300 * time.Millisecond)

	var nextKey atomic.Uint64
	var failures atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) && failures.Load() == 0 {
				key := nextKey.Add(1)
				rs := newRequestState(pool, key, time.Time{}, true)
				rs.acquireRef()
				go func() {
					rs.complete(RequestResult{Value: key})
					rs.releaseRef()
				}()
				result := <-rs.ResultC()
				if result.Value != key || rs.key != key || rs.recycled.Load() {
					failures.Add(1)
				}
				rs.Release()
			}
		}()
	}
	wg.Wait()
	if n := failures.Load(); n != 0 {
		t.Fatalf("%d requests found their RequestState in use by another request", n)
	}
}
