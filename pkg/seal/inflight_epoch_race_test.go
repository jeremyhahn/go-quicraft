package seal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// inFlightEpochRaceFrame is one ciphertext sealed during the race together
// with the epoch it was sealed under and whether its pin is still held.
type inFlightEpochRaceFrame struct {
	plaintext  []byte
	ciphertext []byte
	epoch      uint64
	pinned     bool
}

const (
	inFlightEpochRaceWorkers   = 8
	inFlightEpochRaceFrames    = 24
	inFlightEpochRaceRotations = 40
	inFlightEpochRacePurges    = 200
)

// inFlightEpochRaceSealPinned seals plaintext with EncryptWithEpochPinned,
// which pins the epoch atomically with the seal.
func inFlightEpochRaceSealPinned(ctx context.Context, b *Barrier, plaintext []byte) (inFlightEpochRaceFrame, error) {
	if err := ctx.Err(); err != nil {
		return inFlightEpochRaceFrame{}, err
	}
	ct, epoch, err := b.EncryptWithEpochPinned(nil, plaintext)
	if err != nil {
		return inFlightEpochRaceFrame{}, err
	}
	return inFlightEpochRaceFrame{plaintext: plaintext, ciphertext: ct, epoch: epoch, pinned: true}, nil
}

// inFlightEpochRaceSealRegistered pins the current epoch by hand with
// RegisterInFlightEpoch and then encrypts. A Rotate can land between the
// two calls, so the frame is kept only when it was sealed under the epoch
// that was pinned; otherwise the pin is released and the seal retried.
func inFlightEpochRaceSealRegistered(ctx context.Context, b *Barrier, plaintext []byte) (inFlightEpochRaceFrame, error) {
	for {
		if err := ctx.Err(); err != nil {
			return inFlightEpochRaceFrame{}, err
		}
		pinned := b.CurrentEpoch()
		b.RegisterInFlightEpoch(pinned)
		ct, epoch, err := b.EncryptWithEpoch(nil, plaintext)
		if err != nil {
			b.DeregisterInFlightEpoch(pinned)
			return inFlightEpochRaceFrame{}, err
		}
		if epoch == pinned {
			return inFlightEpochRaceFrame{plaintext: plaintext, ciphertext: ct, epoch: epoch, pinned: true}, nil
		}
		b.DeregisterInFlightEpoch(pinned)
	}
}

// inFlightEpochRaceCheckDecrypt asserts a frame decrypts under its epoch.
func inFlightEpochRaceCheckDecrypt(ctx context.Context, b *Barrier, f inFlightEpochRaceFrame) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pt, err := b.DecryptForEpoch(nil, f.ciphertext, f.epoch)
	if err != nil {
		return fmt.Errorf("pinned epoch %d (current %d): DecryptForEpoch: %w", f.epoch, b.CurrentEpoch(), err)
	}
	if !bytes.Equal(pt, f.plaintext) {
		return fmt.Errorf("pinned epoch %d: plaintext mismatch", f.epoch)
	}
	return nil
}

// inFlightEpochRaceInFlightLen reads the in-flight table size under its lock.
func inFlightEpochRaceInFlightLen(ctx context.Context, t *testing.T, b *Barrier) map[uint64]int {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	b.inFlightMu.Lock()
	defer b.inFlightMu.Unlock()
	out := make(map[uint64]int, len(b.inFlightEpochs))
	for e, n := range b.inFlightEpochs {
		out[e] = int(n)
	}
	return out
}

// inFlightEpochRaceWaitGoroutines waits for the goroutine count to fall back
// to baseline, failing the test if it does not within the deadline.
func inFlightEpochRaceWaitGoroutines(ctx context.Context, t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("goroutines left running: %d, baseline %d", n, baseline)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// inFlightEpochRaceSealFunc seals one frame and holds an in-flight pin on
// the epoch it was sealed under.
type inFlightEpochRaceSealFunc func(ctx context.Context, b *Barrier, plaintext []byte) (inFlightEpochRaceFrame, error)

// TestBarrier_InFlightEpochRace_PinsSurviveRotateAndPurge runs the race once
// per pinning API, each on its own barrier with only that API's workers, so
// neither API's pins can protect the other's ciphertexts.
func TestBarrier_InFlightEpochRace_PinsSurviveRotateAndPurge(t *testing.T) {
	for _, tc := range []struct {
		name string
		seal inFlightEpochRaceSealFunc
	}{
		{name: "EncryptWithEpochPinned", seal: inFlightEpochRaceSealPinned},
		{name: "RegisterInFlightEpoch", seal: inFlightEpochRaceSealRegistered},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			inFlightEpochRaceRun(ctx, t, tc.seal)
		})
	}
}

// inFlightEpochRaceRun races workers sealing with seal against a rotator and
// a background purger on a fresh barrier, then checks that the pins seal
// holds, and only those, kept the old epoch across a completed purge.
func inFlightEpochRaceRun(ctx context.Context, t *testing.T, seal inFlightEpochRaceSealFunc) {
	t.Helper()
	b, _ := testInitializedBarrier(t)
	defer func() { _ = b.Seal() }()

	baseline := runtime.NumGoroutine()

	var (
		racing   sync.WaitGroup // rotator and purger
		workers  sync.WaitGroup
		started  sync.WaitGroup // each worker holds a pin sealed before any rotation
		sealed   sync.WaitGroup // each worker has stopped appending to frames
		raceDone = make(chan struct{})
		raceOnce sync.Once
		errMu    sync.Mutex
		errs     []error
	)
	report := func(err error) {
		errMu.Lock()
		errs = append(errs, err)
		errMu.Unlock()
	}
	// Release the workers on every exit path, so a t.Fatal before the
	// explicit release does not leave them blocked.
	releaseWorkers := func() { raceOnce.Do(func() { close(raceDone) }) }
	defer releaseWorkers()

	frames := make([][]inFlightEpochRaceFrame, inFlightEpochRaceWorkers)
	started.Add(inFlightEpochRaceWorkers)
	sealed.Add(inFlightEpochRaceWorkers)
	startedCh := make(chan struct{})
	go func() {
		started.Wait()
		close(startedCh)
	}()
	// firstRotated closes after the first successful Rotate, or when the
	// rotator stops without one, so no goroutine waits on it forever. The
	// purger and the second half of every worker's seals wait on it, so
	// purges run, and seals and pins continue, after an old epoch exists.
	firstRotated := make(chan struct{})
	var firstRotatedOnce sync.Once
	closeFirstRotated := func() { firstRotatedOnce.Do(func() { close(firstRotated) }) }
	// initialEpoch is the epoch every worker's first-half pins are sealed
	// under: no rotation runs before every worker has sealed them.
	initialEpoch := b.CurrentEpoch()
	// overlapPurges counts background purges that ran with the current
	// epoch past initialEpoch while the old-epoch pins were held.
	var overlapPurges atomic.Int64

	racing.Add(2)
	go func() {
		defer racing.Done()
		defer closeFirstRotated()
		// Rotate only once every worker keeps a pin sealed under the
		// initial epoch, so the API under test holds old-epoch pins
		// across the purges.
		select {
		case <-startedCh:
		case <-ctx.Done():
			report(fmt.Errorf("rotator waiting for workers to start: %w", ctx.Err()))
			return
		}
		for i := 0; i < inFlightEpochRaceRotations; i++ {
			if _, err := b.Rotate(ctx); err != nil {
				report(fmt.Errorf("Rotate %d: %w", i, err))
				return
			}
			if i == 0 {
				closeFirstRotated()
			}
			runtime.Gosched()
		}
	}()
	go func() {
		defer racing.Done()
		select {
		case <-firstRotated:
		case <-ctx.Done():
			report(fmt.Errorf("purger waiting for the first rotation: %w", ctx.Err()))
			return
		}
		for i := 0; i < inFlightEpochRacePurges; i++ {
			current := b.CurrentEpoch()
			if _, err := b.PurgeEpochsBefore(ctx, current); err != nil {
				report(fmt.Errorf("PurgeEpochsBefore %d: %w", i, err))
				return
			}
			if current > initialEpoch {
				// The workers hold pins on initialEpoch until raceDone,
				// which follows racing.Wait: this purge had an old,
				// pinned epoch to skip, and must have kept it.
				b.mu.RLock()
				_, kept := b.deks[initialEpoch]
				b.mu.RUnlock()
				if !kept {
					report(fmt.Errorf("PurgeEpochsBefore %d (current %d) removed pinned epoch %d", i, current, initialEpoch))
				}
				overlapPurges.Add(1)
			}
			runtime.Gosched()
		}
	}()

	for w := 0; w < inFlightEpochRaceWorkers; w++ {
		workers.Add(1)
		go func(w int) {
			defer workers.Done()
			signalled := false
			signalStarted := func() {
				if !signalled {
					signalled = true
					started.Done()
				}
			}
			for i := 0; i < inFlightEpochRaceFrames; i++ {
				if i == inFlightEpochRaceFrames/2 {
					// The first half holds pins under initialEpoch; the
					// second half seals after a rotation, while the
					// purger runs.
					signalStarted()
					select {
					case <-firstRotated:
					case <-ctx.Done():
						report(fmt.Errorf("worker %d waiting for the first rotation: %w", w, ctx.Err()))
					}
				}
				pt := []byte(fmt.Sprintf("worker %d frame %d", w, i))
				f, err := seal(ctx, b, pt)
				if err != nil {
					report(fmt.Errorf("worker %d frame %d seal: %w", w, i, err))
					break
				}
				runtime.Gosched()
				if err := inFlightEpochRaceCheckDecrypt(ctx, b, f); err != nil {
					report(fmt.Errorf("worker %d frame %d during race: %w", w, i, err))
				}
				// Release every third pin during the race so releases run
				// concurrently with purges; the rest stay held until the
				// rotator and purger stop.
				if i%3 == 0 {
					b.DeregisterInFlightEpoch(f.epoch)
					f.pinned = false
				}
				frames[w] = append(frames[w], f)
			}
			// On an early seal error the worker still signals, so the
			// rotator and the main goroutine never wait on it.
			signalStarted()
			sealed.Done()
			<-raceDone
			for i := range frames[w] {
				f := &frames[w][i]
				if !f.pinned {
					continue
				}
				if err := inFlightEpochRaceCheckDecrypt(ctx, b, *f); err != nil {
					report(fmt.Errorf("worker %d frame %d after race: %w", w, i, err))
				}
				b.DeregisterInFlightEpoch(f.epoch)
				f.pinned = false
			}
		}(w)
	}

	flushErrs := func() {
		errMu.Lock()
		for _, err := range errs {
			t.Error(err)
		}
		errs = nil
		errMu.Unlock()
	}

	racing.Wait()
	if overlapPurges.Load() == 0 {
		flushErrs()
		t.Fatalf("no background purge ran after a rotation with the epoch-%d pins held", initialEpoch)
	}
	// sealed.Wait orders every worker's appends to frames before the reads
	// below.
	sealed.Wait()
	// One more purge with the remaining pins still held: pinned epochs stay.
	afterPurge := b.CurrentEpoch()
	if afterPurge <= initialEpoch {
		flushErrs()
		t.Fatalf("CurrentEpoch = %d, want past the pinned epoch %d before the purge", afterPurge, initialEpoch)
	}
	if _, err := b.PurgeEpochsBefore(ctx, afterPurge); err != nil {
		t.Fatalf("PurgeEpochsBefore with pins held: %v", err)
	}
	// (a) the in-flight table holds exactly the references these workers
	// still hold, at least one of them on the old initial epoch, and every
	// frame on an old epoch still decrypts: reference protection by this
	// API alone, not currency, kept those epochs.
	held := make(map[uint64]int)
	for w := range frames {
		for i, f := range frames[w] {
			if !f.pinned {
				continue
			}
			held[f.epoch]++
			if f.epoch >= afterPurge {
				continue
			}
			if err := inFlightEpochRaceCheckDecrypt(ctx, b, f); err != nil {
				t.Errorf("worker %d frame %d pinned on old epoch %d after purge: %v", w, i, f.epoch, err)
			}
		}
	}
	inFlight := inFlightEpochRaceInFlightLen(ctx, t, b)
	if held[initialEpoch] == 0 {
		flushErrs()
		t.Fatalf("no pin held on old epoch %d across the purge (current epoch %d)", initialEpoch, afterPurge)
	}
	if inFlight[initialEpoch] != held[initialEpoch] {
		t.Errorf("in-flight references on old epoch %d = %d, want %d held pins", initialEpoch, inFlight[initialEpoch], held[initialEpoch])
	}
	for e, n := range held {
		if inFlight[e] != n {
			t.Errorf("in-flight references on epoch %d = %d, want %d held pins", e, inFlight[e], n)
		}
	}
	for e, n := range inFlight {
		if held[e] == 0 {
			t.Errorf("in-flight references on epoch %d = %d, want none held", e, n)
		}
	}
	b.mu.RLock()
	_, kept := b.deks[initialEpoch]
	b.mu.RUnlock()
	if !kept {
		t.Errorf("purge at current epoch %d removed the pinned epoch %d", afterPurge, initialEpoch)
	}
	if t.Failed() {
		flushErrs()
		t.FailNow()
	}
	releaseWorkers()
	workers.Wait()

	for _, err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	// (c) every pin is released: the in-flight table is empty.
	if left := inFlightEpochRaceInFlightLen(ctx, t, b); len(left) != 0 {
		t.Fatalf("in-flight epochs left after all releases: %v", left)
	}

	// (b) one more purge removes every old epoch.
	current := b.CurrentEpoch()
	if current < 2 {
		t.Fatalf("CurrentEpoch = %d, want rotations to have advanced it", current)
	}
	if _, err := b.PurgeEpochsBefore(ctx, current); err != nil {
		t.Fatalf("final PurgeEpochsBefore: %v", err)
	}
	b.mu.RLock()
	remaining := len(b.deks)
	_, keepsCurrent := b.deks[current]
	b.mu.RUnlock()
	if remaining != 1 || !keepsCurrent {
		t.Fatalf("after final purge: %d DEKs, current %d kept=%v; want only the current epoch", remaining, current, keepsCurrent)
	}

	var old, live int
	for w := range frames {
		for i, f := range frames[w] {
			_, err := b.DecryptForEpoch(nil, f.ciphertext, f.epoch)
			if f.epoch == current {
				live++
				if err != nil {
					t.Errorf("worker %d frame %d under current epoch %d: %v", w, i, f.epoch, err)
				}
				continue
			}
			old++
			if !errors.Is(err, ErrDecryptionFailed) {
				t.Errorf("worker %d frame %d under purged epoch %d: err = %v, want ErrDecryptionFailed", w, i, f.epoch, err)
			}
		}
	}
	t.Logf("frames sealed under purged epochs: %d, under current epoch: %d", old, live)

	if left := inFlightEpochRaceInFlightLen(ctx, t, b); len(left) != 0 {
		t.Fatalf("in-flight epochs after final purge: %v", left)
	}
	inFlightEpochRaceWaitGoroutines(ctx, t, baseline)
}
