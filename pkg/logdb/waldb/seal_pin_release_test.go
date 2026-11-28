package waldb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// TestSealPinReleaseDoesNotHoldDataMu is the regression for the seal-pin
// release ordering (#1246). Barrier.PurgeEpochsBefore holds the barrier's
// inFlightMu across the synchronous StatePersister call, and
// DeregisterInFlightEpoch waits for inFlightMu. A commit whose record's epoch
// the shard already pins hands its own pin back; if it did so while holding
// dataMu, every reader of the shard would wait on the purge's I/O. The test
// blocks the persister mid-purge, commits such a record, and requires a
// shard read to complete while the purge is still blocked.
func TestSealPinReleaseDoesNotHoldDataMu(t *testing.T) {
	ctx := context.Background()
	_ = seal.AllowWeakKDFParams()
	strategy, err := newTestSealStrategy([]byte("test-passphrase"))
	if err != nil {
		t.Fatalf("newTestSealStrategy: %v", err)
	}
	t.Cleanup(func() { strategy.Close() })

	var blockPersist atomic.Bool
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce atomic.Bool
	unblock := func() {
		if releaseOnce.CompareAndSwap(false, true) {
			close(release)
		}
	}
	t.Cleanup(unblock)

	cfg := seal.DefaultBarrierConfig(1)
	cfg.StatePersister = func(*seal.SealedRootKey) error {
		if blockPersist.Load() {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	}
	barrier := seal.MustNewBarrier(cfg)
	if err := barrier.Initialize(ctx, strategy, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Initialize: %v", err)
	}

	db, err := Open(t.TempDir(),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		unblock()
		_ = db.Close()
	})
	shard := db.getShard(1)

	// Epoch 1 seals nothing, so a purge below 2 has an epoch to drop and
	// reaches the persister. Both records are sealed under epoch 2.
	rotateBarrierTo(t, db, 2)

	write := func() recordLanding {
		t.Helper()
		if err := shard.ensureActiveSegment(); err != nil {
			t.Fatalf("ensureActiveSegment: %v", err)
		}
		shard.batchMode = true
		landing, err := shard.writeRecord([]byte{payloadTypeCompact, 0})
		shard.batchMode = false
		if err != nil {
			t.Fatalf("writeRecord: %v", err)
		}
		if landing.epoch != 2 || landing.pinnedEpoch != 2 {
			t.Fatalf("landing epoch=%d pinned=%d, want 2/2", landing.epoch, landing.pinnedEpoch)
		}
		return landing
	}

	// The first record is committed: the shard now holds the epoch-2 pin.
	// The second is sealed and durable but not yet committed; its own pin
	// is still in its landing.
	shard.mu.Lock()
	first := write()
	var committed pendingMemUpdates
	committed.stageEpoch(first)
	committed.applyTo(shard)
	second := write()
	shard.mu.Unlock()

	// Rotate past epoch 2 so the in-flight bookkeeping is the only thing
	// keeping it, then block the purge inside the persister call, which
	// runs with inFlightMu held.
	rotateBarrierTo(t, db, 2) // one more rotation: current epoch 3
	blockPersist.Store(true)
	purgeDone := make(chan error, 1)
	go func() {
		_, err := barrier.PurgeEpochsBefore(ctx, 2)
		purgeDone <- err
	}()
	select {
	case <-entered:
	case err := <-purgeDone:
		t.Fatalf("PurgeEpochsBefore(2) returned (%v) without calling the persister", err)
	case <-time.After(10 * time.Second):
		t.Fatal("PurgeEpochsBefore(2) never reached the persister")
	}

	// Commit the second record while the purge is blocked. Its pin is a
	// duplicate of the one the shard holds, so the commit hands it back.
	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		shard.mu.Lock()
		defer shard.mu.Unlock()
		var pending pendingMemUpdates
		pending.stageEpoch(second)
		pending.applyTo(shard)
	}()

	// Give the commit time to apply its in-memory state and, if it releases
	// its pin under dataMu, to block in DeregisterInFlightEpoch.
	time.Sleep(100 * time.Millisecond)

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _, _ = db.ReadState(1, 1)
	}()
	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		unblock()
		<-readDone
		t.Fatal("ReadState blocked while a purge held inFlightMu: " +
			"the commit released its seal pin under dataMu")
	}

	unblock()
	if err := <-purgeDone; err != nil {
		t.Fatalf("PurgeEpochsBefore: %v", err)
	}
	<-commitDone
}
