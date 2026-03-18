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

package engine

import (
	"sync"
	"testing"
	"time"
)

func TestWorkSignal_NotifySendsToCorrectWorker(t *testing.T) {
	const numWorkers = 4
	ws := NewWorkSignal(numWorkers)

	// Shard 5 should route to worker 5 % 4 = 1.
	ws.Notify(5)

	select {
	case <-ws.WaitC(1):
		// Expected: worker 1 received notification.
	case <-time.After(time.Second):
		t.Fatal("worker 1 did not receive notification")
	}

	// Verify the shard appears in the correct worker's ReadyMap.
	got := ws.Swap(1)
	if _, ok := got[5]; !ok {
		t.Fatal("expected shard 5 in worker 1 ready map")
	}
}

func TestWorkSignal_NotifyNoNotificationToOtherWorkers(t *testing.T) {
	const numWorkers = 4
	ws := NewWorkSignal(numWorkers)

	// Shard 5 routes to worker 1.
	ws.Notify(5)

	// Workers 0, 2, 3 should NOT have received notifications.
	for _, wid := range []int{0, 2, 3} {
		select {
		case <-ws.WaitC(wid):
			t.Fatalf("worker %d should not have received notification", wid)
		default:
			// Expected: no notification.
		}
	}
}

func TestWorkSignal_WaitCReturnsChannel(t *testing.T) {
	ws := NewWorkSignal(3)
	ch := ws.WaitC(0)
	if ch == nil {
		t.Fatal("WaitC returned nil channel")
	}
	ch2 := ws.WaitC(2)
	if ch2 == nil {
		t.Fatal("WaitC returned nil channel for worker 2")
	}
}

func TestWorkSignal_SwapReturnsReadyShardsForCorrectWorker(t *testing.T) {
	const numWorkers = 4
	ws := NewWorkSignal(numWorkers)

	// Shard 0 -> worker 0, shard 4 -> worker 0, shard 8 -> worker 0.
	ws.Notify(0)
	ws.Notify(4)
	ws.Notify(8)

	// Shard 1 -> worker 1.
	ws.Notify(1)

	// Drain worker 0 notifications.
	<-ws.WaitC(0)

	got0 := ws.Swap(0)
	if len(got0) != 3 {
		t.Fatalf("worker 0: expected 3 shards, got %d", len(got0))
	}
	for _, id := range []uint64{0, 4, 8} {
		if _, ok := got0[id]; !ok {
			t.Fatalf("worker 0: expected shard %d", id)
		}
	}

	// Drain worker 1 notification.
	<-ws.WaitC(1)

	got1 := ws.Swap(1)
	if len(got1) != 1 {
		t.Fatalf("worker 1: expected 1 shard, got %d", len(got1))
	}
	if _, ok := got1[1]; !ok {
		t.Fatal("worker 1: expected shard 1")
	}
}

func TestWorkSignal_ClearNotifiedResetsFlag(t *testing.T) {
	ws := NewWorkSignal(2)
	ws.Notify(0) // Worker 0.
	<-ws.WaitC(0)

	// Clear notification flag.
	ws.ClearNotified(0)

	// Notify again should succeed and send to channel.
	ws.Notify(0)
	select {
	case <-ws.WaitC(0):
		// Expected: second notification received.
	case <-time.After(time.Second):
		t.Fatal("expected second notification after ClearNotified")
	}
}

func TestWorkSignal_ConcurrentNotify(t *testing.T) {
	const numWorkers = 8
	const shardsPerGoroutine = 100
	const goroutines = 50
	ws := NewWorkSignal(numWorkers)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(base int) {
			defer wg.Done()
			for i := range shardsPerGoroutine {
				ws.Notify(uint64(base*shardsPerGoroutine + i))
			}
		}(g)
	}
	wg.Wait()

	// Drain all notifications and collect shards.
	collected := make(map[uint64]struct{})
	for wid := range numWorkers {
		// Drain channel.
		select {
		case <-ws.WaitC(wid):
		default:
		}
		for id := range ws.Swap(wid) {
			collected[id] = struct{}{}
		}
	}

	expected := goroutines * shardsPerGoroutine
	if len(collected) != expected {
		t.Fatalf("expected %d unique shards, got %d", expected, len(collected))
	}
}

func TestWorkSignal_AtomicPreFilterAvoidsDoubleChannelSend(t *testing.T) {
	ws := NewWorkSignal(1)

	// Notify the same worker many times. Only the first should send
	// to the channel (capacity 1). The rest should hit the atomic
	// pre-filter fast path.
	for range 100 {
		ws.Notify(0)
	}

	// Channel should have exactly 1 item.
	select {
	case <-ws.WaitC(0):
	case <-time.After(time.Second):
		t.Fatal("expected exactly one notification in channel")
	}

	// Channel should now be empty.
	select {
	case <-ws.WaitC(0):
		t.Fatal("expected channel to be empty after single drain")
	default:
		// Expected.
	}
}

func TestWorkSignal_MultipleWorkersIndependent(t *testing.T) {
	const numWorkers = 4
	ws := NewWorkSignal(numWorkers)

	// Notify worker 2 only.
	ws.Notify(2) // 2 % 4 = 2.

	// Worker 2 should be notified.
	select {
	case <-ws.WaitC(2):
	case <-time.After(time.Second):
		t.Fatal("worker 2 should have received notification")
	}

	// Other workers should have no notifications and empty ready maps.
	for _, wid := range []int{0, 1, 3} {
		select {
		case <-ws.WaitC(wid):
			t.Fatalf("worker %d should not have a notification", wid)
		default:
		}
		got := ws.Swap(wid)
		if len(got) != 0 {
			t.Fatalf("worker %d: expected empty ready map, got %d", wid, len(got))
		}
	}
}

func TestWorkSignal_NumWorkers(t *testing.T) {
	ws := NewWorkSignal(16)
	if ws.NumWorkers() != 16 {
		t.Fatalf("expected 16 workers, got %d", ws.NumWorkers())
	}
}

func TestWorkSignal_NumWorkersOne(t *testing.T) {
	ws := NewWorkSignal(1)
	if ws.NumWorkers() != 1 {
		t.Fatalf("expected 1 worker, got %d", ws.NumWorkers())
	}
}

func TestWorkSignal_NotifyAndSwapCycle(t *testing.T) {
	ws := NewWorkSignal(2)

	// Full lifecycle: notify, wait, clear, swap, process.
	ws.Notify(0) // Worker 0.
	ws.Notify(2) // Worker 0 (2 % 2 = 0).
	ws.Notify(1) // Worker 1 (1 % 2 = 1).

	// Worker 0 cycle.
	<-ws.WaitC(0)
	ws.ClearNotified(0)
	got0 := ws.Swap(0)
	if len(got0) != 2 {
		t.Fatalf("worker 0: expected 2 shards, got %d", len(got0))
	}

	// Worker 1 cycle.
	<-ws.WaitC(1)
	ws.ClearNotified(1)
	got1 := ws.Swap(1)
	if len(got1) != 1 {
		t.Fatalf("worker 1: expected 1 shard, got %d", len(got1))
	}

	// Second cycle: no notifications, swap should be empty.
	got0 = ws.Swap(0)
	if len(got0) != 0 {
		t.Fatalf("worker 0 second cycle: expected 0 shards, got %d", len(got0))
	}
}

func TestWorkSignal_ClearNotifiedWithoutPriorNotify(t *testing.T) {
	ws := NewWorkSignal(2)

	// Clearing without prior notification should not panic.
	ws.ClearNotified(0)
	ws.ClearNotified(1)

	// Subsequent notify should work normally.
	ws.Notify(0)
	select {
	case <-ws.WaitC(0):
	case <-time.After(time.Second):
		t.Fatal("expected notification after clearing non-notified worker")
	}
}

func BenchmarkWorkSignal_Notify(b *testing.B) {
	const numWorkers = 8
	ws := NewWorkSignal(numWorkers)

	// Drain channels in background so Notify doesn't block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-done:
				return
			default:
				for wid := range numWorkers {
					select {
					case <-ws.WaitC(wid):
						ws.ClearNotified(wid)
						ws.Swap(wid)
					default:
					}
				}
			}
		}
	}()

	b.ResetTimer()
	for i := range b.N {
		ws.Notify(uint64(i))
	}
	b.StopTimer()
}

func BenchmarkWorkSignal_NotifyContended(b *testing.B) {
	const numWorkers = 8
	ws := NewWorkSignal(numWorkers)

	// Drain channels in background.
	stopDrain := make(chan struct{})
	var drainWg sync.WaitGroup
	drainWg.Add(1)
	go func() {
		defer drainWg.Done()
		for {
			select {
			case <-stopDrain:
				return
			default:
				for wid := range numWorkers {
					select {
					case <-ws.WaitC(wid):
						ws.ClearNotified(wid)
						ws.Swap(wid)
					default:
					}
				}
			}
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := uint64(0)
		for pb.Next() {
			ws.Notify(id)
			id++
		}
	})
	b.StopTimer()
	close(stopDrain)
	drainWg.Wait()
}
