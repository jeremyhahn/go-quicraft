package batch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// heldProposer signals when the final flush reaches SyncPropose, then holds
// the call until the test releases it, ignoring ctx so the caller's deadline
// expires while the batch is still in flight. It then reports success: the
// batch commits.
type heldProposer struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *heldProposer) SyncPropose(_ context.Context, _ uint64, _ []byte) error {
	p.once.Do(func() { close(p.entered) })
	<-p.release
	return nil
}

// TestSubmit_CloseThenDeadlineIsIndeterminate pins #1225: a caller parked in
// awaitResult's Close branch whose deadline expires during the final flush's
// SyncPropose must see an indeterminate FlushError, not a bare
// context.DeadlineExceeded, because the close path still proposes (and here
// commits) its batch.
func TestSubmit_CloseThenDeadlineIsIndeterminate(t *testing.T) {
	t.Parallel()

	hp := &heldProposer{entered: make(chan struct{}), release: make(chan struct{})}
	agg, err := NewAggregator(hp,
		WithConfig(Config{
			// Neither size nor timer flushes: only the close path does.
			MaxBatchSize:  1000,
			FlushInterval: 10 * time.Second,
			QueueSize:     100,
		}),
		WithRegistry(newTestRegistry(t)),
	)
	if err != nil {
		t.Fatalf("NewAggregator failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	const shardID = 7
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- agg.Submit(ctx, shardID, []byte("close-then-deadline"))
	}()

	// Wait until the proposal is enqueued or batched, so Close cannot win
	// against the enqueue.
	waitUntil := time.Now().Add(5 * time.Second)
	for len(agg.queue) == 0 && agg.BatchLen() == 0 {
		if time.Now().After(waitUntil) {
			t.Fatal("proposal never reached the queue")
		}
		time.Sleep(time.Millisecond)
	}

	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		_ = agg.Close()
	}()

	select {
	case <-hp.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("final flush never reached SyncPropose")
	}

	// The caller's deadline expires while the final flush is in flight.
	var submitErr error
	select {
	case submitErr = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Submit did not return after its deadline")
	}
	close(hp.release)
	<-closeDone

	var fe *FlushError
	if !errors.As(submitErr, &fe) {
		t.Fatalf("Submit error = %v (%T), want *FlushError", submitErr, submitErr)
	}
	if !fe.Indeterminate {
		t.Fatalf("FlushError.Indeterminate = false, want true")
	}
	if fe.ShardID != shardID {
		t.Fatalf("FlushError.ShardID = %d, want %d", fe.ShardID, shardID)
	}
	if !errors.Is(submitErr, context.DeadlineExceeded) {
		t.Fatalf("errors.Is(err, context.DeadlineExceeded) = false for %v", submitErr)
	}
}
