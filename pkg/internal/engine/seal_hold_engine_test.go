package engine

import (
	"errors"
	"sync"
	"testing"
)

// TestEngineSetSealHold pins that Engine.SetSealHold installs the hold on
// EVERY commit worker, not only some, and that a zero SealHold restores the
// fail-the-shard behaviour on every worker. A shard on a worker the hold
// missed would latch failed on a sealed barrier.
func TestEngineSetSealHold(t *testing.T) {
	cfg := newTestHostConfig()
	cfg.NumWorkers = 4
	e := NewEngine(cfg, newTestLogDB(), newTestSender(), nil, nil, nil)

	if got := len(e.commitWorkers); got != 4 {
		t.Fatalf("len(e.commitWorkers) = %d, want 4", got)
	}

	sentinel := errors.New("sealed refusal")
	other := errors.New("disk fault")

	var mu sync.Mutex
	var asked []uint64
	unsealed := make(chan struct{})
	close(unsealed)

	e.SetSealHold(SealHold{
		IsSealed: func(err error) bool { return errors.Is(err, sentinel) },
		UnsealedC: func(shardID uint64) <-chan struct{} {
			mu.Lock()
			asked = append(asked, shardID)
			mu.Unlock()
			return unsealed
		},
	})

	for i, w := range e.commitWorkers {
		if !w.isSealedSaveErr(sentinel) {
			t.Fatalf("worker %d: isSealedSaveErr(sentinel) = false after SetSealHold", i)
		}
		if w.isSealedSaveErr(other) {
			t.Fatalf("worker %d: isSealedSaveErr(other) = true, want false", i)
		}
		if w.sealHold.UnsealedC == nil {
			t.Fatalf("worker %d: sealHold.UnsealedC is nil after SetSealHold", i)
		}
		mu.Lock()
		before := len(asked)
		mu.Unlock()
		if ch := w.sealHold.UnsealedC(7); ch != unsealed {
			t.Fatalf("worker %d: UnsealedC(7) returned a different channel", i)
		}
		mu.Lock()
		n := len(asked)
		last := uint64(0)
		if n > 0 {
			last = asked[n-1]
		}
		mu.Unlock()
		if n != before+1 || last != 7 {
			t.Fatalf("worker %d: UnsealedC(7) recorded %d calls (last %d), want one call for shard 7", i, n-before, last)
		}
	}

	e.SetSealHold(SealHold{})

	for i, w := range e.commitWorkers {
		if w.isSealedSaveErr(sentinel) {
			t.Fatalf("worker %d: isSealedSaveErr(sentinel) = true after a zero SealHold; the shard would not fail", i)
		}
		if w.sealHold.UnsealedC != nil {
			t.Fatalf("worker %d: sealHold.UnsealedC is not nil after a zero SealHold", i)
		}
	}
}
