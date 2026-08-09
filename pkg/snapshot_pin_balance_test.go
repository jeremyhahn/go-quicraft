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
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Register/Deregister-counting barrier
// ---------------------------------------------------------------------------

// countingBarrier wraps a Barrier and records the RAW net in-flight reference
// count per epoch, INCLUDING negatives.
//
// The production seal.Barrier deletes an epoch's entry once its count drops to
// <= 0 (DeregisterInFlightEpoch, pkg/seal/barrier.go), which makes a
// double-free look identical to a balanced release. An audit of the snapshot
// encrypt loops' pin accounting must not inherit that erasure:
//
//   - net[e] > 0 after a full cycle  => LEAK. The epoch is pinned forever, so
//     PurgeEpochsBefore skips it for the life of the process and the DEK is
//     never retired.
//   - net[e] < 0 at any point        => DOUBLE-FREE / under-count. One holder's
//     extra release can drop a count another concurrent writer still needs from
//     1 to 0, letting a purge drop a live DEK and brick that snapshot.
type countingBarrier struct {
	Barrier
	mu     sync.Mutex
	net    map[uint64]int
	low    map[uint64]int
	regs   int
	deregs int
}

func newCountingBarrier(inner Barrier) *countingBarrier {
	return &countingBarrier{
		Barrier: inner,
		net:     make(map[uint64]int),
		low:     make(map[uint64]int),
	}
}

func (b *countingBarrier) RegisterInFlightEpoch(epoch uint64) {
	b.mu.Lock()
	b.net[epoch]++
	b.regs++
	b.mu.Unlock()
	b.Barrier.RegisterInFlightEpoch(epoch)
}

func (b *countingBarrier) DeregisterInFlightEpoch(epoch uint64) {
	b.mu.Lock()
	b.net[epoch]--
	if b.net[epoch] < b.low[epoch] {
		b.low[epoch] = b.net[epoch]
	}
	b.deregs++
	b.mu.Unlock()
	b.Barrier.DeregisterInFlightEpoch(epoch)
}

func (b *countingBarrier) held(epoch uint64) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.net[epoch]
}

func (b *countingBarrier) registrations() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.regs
}

func (b *countingBarrier) countsLocked() string {
	epochs := make([]uint64, 0, len(b.net))
	for e := range b.net {
		epochs = append(epochs, e)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	out := fmt.Sprintf("regs=%d deregs=%d", b.regs, b.deregs)
	for _, e := range epochs {
		out += fmt.Sprintf(" [epoch %d net=%d low=%d]", e, b.net[e], b.low[e])
	}
	return out
}

func (b *countingBarrier) counts() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.countsLocked()
}

// assertBalanced asserts the net in-flight count is exactly ZERO for every
// epoch and that no epoch ever dipped below zero.
func (b *countingBarrier) assertBalanced(t *testing.T, what string) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for e, n := range b.net {
		if n > 0 {
			t.Errorf("%s: epoch %d LEAKED %d in-flight pin(s); its DEK can never be purged (%s)",
				what, e, n, b.countsLocked())
		}
		if n < 0 {
			t.Errorf("%s: epoch %d was DEREGISTERED %d time(s) more than registered (%s)",
				what, e, -n, b.countsLocked())
		}
	}
	for e, l := range b.low {
		if l < 0 {
			t.Errorf("%s: epoch %d transiently reached net=%d — a DOUBLE-FREE can let a "+
				"purge drop a DEK another holder still needs (%s)", what, e, l, b.countsLocked())
		}
	}
}

// rotatingMockBarrier seals every frame under a NEW epoch, modelling a key
// rotation landing between each chunk — the multi-epoch case the pin set exists
// for. Encryption is identity so the accounting, not the crypto, is under test.
type rotatingMockBarrier struct {
	mockBarrier
	emu     sync.Mutex
	epoch   uint64
	rotate  bool
	failAt  int
	seals   int
	used    map[uint64]bool
	sealErr error
}

func newRotatingMockBarrier(start uint64, rotate bool) *rotatingMockBarrier {
	return &rotatingMockBarrier{epoch: start, rotate: rotate, used: map[uint64]bool{}, failAt: -1}
}

func (b *rotatingMockBarrier) CurrentEpoch() uint64 {
	b.emu.Lock()
	defer b.emu.Unlock()
	return b.epoch
}

func (b *rotatingMockBarrier) EncryptWithEpoch(_, pt []byte) ([]byte, uint64, error) {
	b.emu.Lock()
	defer b.emu.Unlock()
	b.seals++
	if b.failAt >= 0 && b.seals > b.failAt {
		return nil, 0, b.sealErr
	}
	e := b.epoch
	b.used[e] = true
	if b.rotate {
		b.epoch++
	}
	return append([]byte(nil), pt...), e, nil
}

var _ Barrier = (*rotatingMockBarrier)(nil)

// pinErrWriter fails the Nth Write, so both dst.Write error returns of
// encryptSnapshotFrames (the 4-byte frame length and the ciphertext) can be
// driven independently.
type pinErrWriter struct {
	failOn int
	n      int
	err    error
}

func (w *pinErrWriter) Write(p []byte) (int, error) {
	w.n++
	if w.n >= w.failOn {
		return 0, w.err
	}
	return len(p), nil
}

// pinErrReader yields one full chunk then fails, so the loop is holding a pin when
// the read error arrives.
type pinErrReader struct {
	remaining int
	err       error
}

func (r *pinErrReader) Read(p []byte) (int, error) {
	if r.remaining > 0 {
		n := len(p)
		if n > r.remaining {
			n = r.remaining
		}
		r.remaining -= n
		return n, nil
	}
	return 0, r.err
}

// renameFailFS is the real filesystem with Rename refused, the one
// reencryptReceivedSnapshot / reencryptSnapshotFile failure the shared
// faultySnapshotFS does not inject.
type renameFailFS struct {
	osSnapshotFS
}

var errTestRenameRefused = errors.New("test fs: rename refused")

func (renameFailFS) Rename(string, string) error { return errTestRenameRefused }

var _ snapshotFS = renameFailFS{}

// ---------------------------------------------------------------------------
// encryptSnapshotFrames
// ---------------------------------------------------------------------------

// TestPinBalance_EncryptSnapshotFrames_SuccessHandsOffExactlyOnePinPerEpoch
// proves the SUCCESS contract: on return every epoch a frame was sealed under
// is held with a net count of exactly ONE — not zero (the caller must be able
// to rely on the pin until its durable record exists) and not two (the caller
// releases each element once, so a duplicate registration leaks forever).
func TestPinBalance_EncryptSnapshotFrames_SuccessHandsOffExactlyOnePinPerEpoch(t *testing.T) {
	inner := newRotatingMockBarrier(5, true)
	b := newCountingBarrier(inner)

	plain := make([]byte, snapshotEncryptChunkSize*3+100)
	var out bytes.Buffer
	_, pins, err := encryptSnapshotFrames(b, bytes.NewReader(plain), &out)
	if err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	if len(inner.used) < 3 {
		t.Fatalf("fixture: %d frame epochs used, want the multi-epoch path", len(inner.used))
	}

	seen := map[uint64]bool{}
	for _, e := range pins {
		if seen[e] {
			t.Fatalf("epoch %d appears twice in the returned pin set; the caller releases "+
				"once per element, so the duplicate registration leaks: %v", e, pins)
		}
		seen[e] = true
		if got := b.held(e); got != 1 {
			t.Fatalf("epoch %d held %d times on return, want exactly 1: %s", e, got, b.counts())
		}
	}
	// Every epoch actually sealed must be in the set (the property the commit adds).
	for e := range inner.used {
		if !seen[e] {
			t.Fatalf("frame epoch %d was sealed but not returned as pinned: %v", e, pins)
		}
	}

	for _, e := range pins {
		b.DeregisterInFlightEpoch(e)
	}
	b.assertBalanced(t, "encryptSnapshotFrames success + caller release")
}

// atomicPinCountingBarrier is a countingBarrier that ALSO implements the
// optional atomicEpochPinner capability, so the encrypt loop takes the atomic
// seal-and-pin path. The barrier itself takes the in-flight reference (as the
// real seal.Barrier does under its own lock), which is exactly the path whose
// accounting differs: the pinner must NOT re-register, and must release a
// duplicate reference when the same epoch is sealed twice.
type atomicPinCountingBarrier struct {
	*countingBarrier
	rotateEvery int // seal a new epoch every N frames (0 = never rotate)
	frames      int
	epoch       uint64
}

func (b *atomicPinCountingBarrier) EncryptWithEpochPinned(_, pt []byte) ([]byte, uint64, error) {
	b.frames++
	if b.rotateEvery > 0 && b.frames%b.rotateEvery == 0 {
		b.epoch++
	}
	// The barrier takes the reference itself, atomically with the seal.
	b.RegisterInFlightEpoch(b.epoch)
	return append([]byte(nil), pt...), b.epoch, nil
}

// TestPinBalance_AtomicPath_BalancesAndDedups covers the ATOMIC seal-and-pin
// path, which the other balance tests miss because their barrier does not
// implement the capability. Two properties: every distinct sealed epoch is
// returned exactly once, and a repeated epoch does not leak the extra reference
// the barrier took (the pinner releases the duplicate immediately).
func TestPinBalance_AtomicPath_BalancesAndDedups(t *testing.T) {
	for _, tc := range []struct {
		name        string
		rotateEvery int
	}{
		{name: "single_epoch_repeated", rotateEvery: 0}, // same epoch every frame
		{name: "rotation_every_frame", rotateEvery: 1},  // a new epoch per frame
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := createTestBarrier(t)
			cb := newCountingBarrier(inner)
			b := &atomicPinCountingBarrier{countingBarrier: cb, rotateEvery: tc.rotateEvery, epoch: 7}

			var out bytes.Buffer
			plain := make([]byte, snapshotEncryptChunkSize*3+11) // several frames
			_, pins, err := encryptSnapshotFrames(b, bytes.NewReader(plain), &out)
			if err != nil {
				t.Fatalf("encryptSnapshotFrames: %v", err)
			}
			if len(pins) == 0 {
				t.Fatal("no epochs returned as pinned")
			}
			// No duplicates in the returned set.
			seen := map[uint64]bool{}
			for _, e := range pins {
				if seen[e] {
					t.Fatalf("epoch %d returned twice: the caller would release it twice "+
						"and under-count the in-flight refcount, letting a live DEK be purged", e)
				}
				seen[e] = true
				if got := cb.held(e); got != 1 {
					t.Fatalf("epoch %d held %d references, want exactly 1 (a duplicate the "+
						"barrier took was not released)", e, got)
				}
			}
			// Caller releases each exactly once; accounting must return to zero.
			for _, e := range pins {
				b.DeregisterInFlightEpoch(e)
			}
			cb.assertBalanced(t, "atomic seal-and-pin "+tc.name)
		})
	}
}

// TestPinBalance_EncryptSnapshotFrames_EveryErrorPathReleases walks all four
// error returns of the loop — EncryptWithEpoch, the frame-length write, the
// ciphertext write, and the source read — each of which has its own inline
// unpinAll(). Each must leave the accounting flat AND return a nil pin set, so
// the caller has nothing to release a second time.
func TestPinBalance_EncryptSnapshotFrames_EveryErrorPathReleases(t *testing.T) {
	errInjected := errors.New("injected")

	cases := []struct {
		name string
		run  func(b Barrier) (uint64, []uint64, error)
	}{
		{
			name: "encrypt_error",
			run: func(b Barrier) (uint64, []uint64, error) {
				var out bytes.Buffer
				plain := make([]byte, snapshotEncryptChunkSize*2+10)
				return encryptSnapshotFrames(b, bytes.NewReader(plain), &out)
			},
		},
		{
			name: "write_frame_length_error",
			run: func(b Barrier) (uint64, []uint64, error) {
				plain := make([]byte, snapshotEncryptChunkSize*2+10)
				return encryptSnapshotFrames(b, bytes.NewReader(plain),
					&pinErrWriter{failOn: 1, err: errInjected})
			},
		},
		{
			name: "write_ciphertext_error",
			run: func(b Barrier) (uint64, []uint64, error) {
				plain := make([]byte, snapshotEncryptChunkSize*2+10)
				return encryptSnapshotFrames(b, bytes.NewReader(plain),
					&pinErrWriter{failOn: 2, err: errInjected})
			},
		},
		{
			name: "read_error",
			run: func(b Barrier) (uint64, []uint64, error) {
				var out bytes.Buffer
				return encryptSnapshotFrames(b,
					&pinErrReader{remaining: snapshotEncryptChunkSize, err: errInjected}, &out)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := newRotatingMockBarrier(9, true)
			if tc.name == "encrypt_error" {
				// Seal one frame (pinning its epoch) before failing, so the loop
				// is holding a pin at the moment of the error.
				inner.failAt = 1
				inner.sealErr = errInjected
			}
			b := newCountingBarrier(inner)

			_, pins, err := tc.run(b)
			if !errors.Is(err, errInjected) {
				t.Fatalf("error = %v, want the injected failure", err)
			}
			if pins != nil {
				t.Errorf("error return handed back pins %v; the loop already released them, "+
					"so a caller that releases the set again DOUBLE-FREES", pins)
			}
			if b.registrations() == 0 {
				t.Fatalf("fixture: nothing was ever pinned, so the release is untested: %s", b.counts())
			}
			b.assertBalanced(t, "encryptSnapshotFrames "+tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// reencryptReceivedSnapshot / reencryptImportPinned
// ---------------------------------------------------------------------------

func writePlainSnapshot(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "snapshot.dat")
	data := make([]byte, snapshotEncryptChunkSize*2+64)
	for i := range data {
		data[i] = byte(i*17 + 1)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write plaintext snapshot: %v", err)
	}
	return path
}

// TestPinBalance_ReencryptReceivedSnapshot_PostEncryptFailuresRelease drives
// each failure that can land AFTER encryptSnapshotFrames has already taken its
// pins but BEFORE reencryptReceivedSnapshot commits — Sync, the temp Close, the
// source Close, and Rename. The caller never receives the set on those paths,
// so the helper itself must release it or the epochs are pinned forever.
func TestPinBalance_ReencryptReceivedSnapshot_PostEncryptFailuresRelease(t *testing.T) {
	cases := []struct {
		name string
		fsys snapshotFS
		want error
	}{
		{"sync_failure", faultySnapshotFS{failSync: true}, errTestSyncFailed},
		{"temp_close_failure", faultySnapshotFS{failCloseTemp: true}, errTestCloseFailed},
		{"source_close_failure", faultySnapshotFS{failCloseSource: true}, errTestCloseFailed},
		{"rename_failure", renameFailFS{}, errTestRenameRefused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newCountingBarrier(newRotatingMockBarrier(4, true))
			path := writePlainSnapshot(t, t.TempDir())

			_, pins, err := reencryptReceivedSnapshot(tc.fsys, path, b)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if pins != nil {
				t.Errorf("error return handed back pins %v after already releasing them "+
					"(double-free if the caller honors the contract)", pins)
			}
			if b.registrations() == 0 {
				t.Fatalf("fixture: nothing was pinned, so the release is untested: %s", b.counts())
			}
			b.assertBalanced(t, "reencryptReceivedSnapshot "+tc.name)
		})
	}
}

// TestPinBalance_ReencryptImportPinned covers the import handoff: on SUCCESS
// the pins must still be held when the function returns (ImportSnapshot's
// SaveSnapshot has not run yet) and must drop to zero once the returned cleanup
// runs; on ERROR the returned cleanup must be inert and the accounting already
// flat — releasing there would double-free what the helper already released.
func TestPinBalance_ReencryptImportPinned(t *testing.T) {
	t.Run("success_holds_then_releases", func(t *testing.T) {
		b := newCountingBarrier(newRotatingMockBarrier(11, true))
		path := writePlainSnapshot(t, t.TempDir())

		epoch, cleanup, err := reencryptImportPinned(osSnapshotFS{}, path, b)
		if err != nil {
			t.Fatalf("reencryptImportPinned: %v", err)
		}
		if epoch == 0 {
			t.Fatal("epoch = 0 for an encrypted import")
		}
		if b.registrations() < 2 {
			t.Fatalf("fixture: only %d registrations, want the multi-epoch path: %s",
				b.registrations(), b.counts())
		}
		if b.held(epoch) != 1 {
			t.Fatalf("recorded epoch %d held %d times before cleanup, want 1 — the import's "+
				"SaveSnapshot has not recorded it yet: %s", epoch, b.held(epoch), b.counts())
		}
		cleanup()
		b.assertBalanced(t, "reencryptImportPinned success")
	})

	t.Run("error_cleanup_is_inert", func(t *testing.T) {
		b := newCountingBarrier(newRotatingMockBarrier(11, true))
		path := writePlainSnapshot(t, t.TempDir())

		_, cleanup, err := reencryptImportPinned(renameFailFS{}, path, b)
		if !errors.Is(err, errTestRenameRefused) {
			t.Fatalf("error = %v, want %v", err, errTestRenameRefused)
		}
		b.assertBalanced(t, "reencryptImportPinned error (before cleanup)")
		// ImportSnapshot defers cleanup unconditionally, so it also runs on the
		// error path; it must not release anything a second time.
		cleanup()
		b.assertBalanced(t, "reencryptImportPinned error (after deferred cleanup)")
	})

	t.Run("nil_barrier_is_a_noop", func(t *testing.T) {
		path := writePlainSnapshot(t, t.TempDir())
		epoch, cleanup, err := reencryptImportPinned(osSnapshotFS{}, path, nil)
		if err != nil {
			t.Fatalf("nil barrier: %v", err)
		}
		if epoch != 0 {
			t.Fatalf("epoch = %d, want 0 for a plaintext deployment", epoch)
		}
		// Must not dereference the nil barrier.
		cleanup()
	})
}

// ---------------------------------------------------------------------------
// reencryptSnapshotFile (the migrate path behind ReencryptSnapshotFile)
// ---------------------------------------------------------------------------

// TestPinBalance_ReencryptSnapshotFile covers the migrate path in both
// directions that pin the DESTINATION barrier, on success and on a failure
// landing after the encrypt loop took its pins.
func TestPinBalance_ReencryptSnapshotFile(t *testing.T) {
	t.Run("plaintext_to_framed_success", func(t *testing.T) {
		dst := newCountingBarrier(newRotatingMockBarrier(3, true))
		dir := t.TempDir()
		srcPath := writePlainSnapshot(t, dir)
		dstPath := filepath.Join(dir, "out", "snapshot.dat")

		if _, err := reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath, nil, dst); err != nil {
			t.Fatalf("reencryptSnapshotFile: %v", err)
		}
		if dst.registrations() < 2 {
			t.Fatalf("fixture: %d registrations, want the multi-epoch path: %s",
				dst.registrations(), dst.counts())
		}
		dst.assertBalanced(t, "reencryptSnapshotFile plaintext->framed success")
	})

	t.Run("plaintext_to_framed_rename_failure", func(t *testing.T) {
		dst := newCountingBarrier(newRotatingMockBarrier(3, true))
		dir := t.TempDir()
		srcPath := writePlainSnapshot(t, dir)
		dstPath := filepath.Join(dir, "out", "snapshot.dat")

		_, err := reencryptSnapshotFile(renameFailFS{}, srcPath, dstPath, nil, dst)
		if !errors.Is(err, errTestRenameRefused) {
			t.Fatalf("error = %v, want %v", err, errTestRenameRefused)
		}
		if dst.registrations() == 0 {
			t.Fatalf("fixture: nothing pinned: %s", dst.counts())
		}
		dst.assertBalanced(t, "reencryptSnapshotFile rename failure")
	})

	t.Run("plaintext_to_framed_encrypt_failure", func(t *testing.T) {
		// The encrypt loop fails after pinning, having already released its own
		// pins. reencryptSnapshotFile's deferred release then runs over whatever
		// the loop handed back: a non-nil set here would DOUBLE-FREE.
		inner := newRotatingMockBarrier(3, true)
		inner.failAt = 1
		inner.sealErr = errors.New("injected seal failure")
		dst := newCountingBarrier(inner)
		dir := t.TempDir()
		srcPath := writePlainSnapshot(t, dir)
		dstPath := filepath.Join(dir, "out", "snapshot.dat")

		if _, err := reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath, nil, dst); err == nil {
			t.Fatal("expected the injected seal failure")
		}
		if dst.registrations() == 0 {
			t.Fatalf("fixture: nothing pinned: %s", dst.counts())
		}
		dst.assertBalanced(t, "reencryptSnapshotFile encrypt failure")
	})

	t.Run("framed_to_framed_success", func(t *testing.T) {
		src := newCountingBarrier(createTestBarrier(t))
		dst := newCountingBarrier(createTestBarrier(t))
		dir := t.TempDir()
		plain := snapshotPlaintext()
		srcPath := filepath.Join(dir, "src.dat")
		writeEncryptedSnapshotFile(t, src, srcPath, plain)
		dstPath := filepath.Join(dir, "out", "snapshot.dat")

		if _, err := reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath, src, dst); err != nil {
			t.Fatalf("reencryptSnapshotFile: %v", err)
		}
		src.assertBalanced(t, "reencryptSnapshotFile framed->framed src")
		dst.assertBalanced(t, "reencryptSnapshotFile framed->framed dst")
		if dst.registrations() == 0 {
			t.Fatalf("fixture: dest barrier pinned nothing: %s", dst.counts())
		}
	})

	t.Run("framed_to_plaintext_nil_dst_barrier", func(t *testing.T) {
		// dstBarrier is nil, so the deferred dstPins release must never
		// dereference it. A panic here is the finding.
		src := newCountingBarrier(createTestBarrier(t))
		dir := t.TempDir()
		plain := snapshotPlaintext()
		srcPath := filepath.Join(dir, "src.dat")
		writeEncryptedSnapshotFile(t, src, srcPath, plain)
		dstPath := filepath.Join(dir, "out", "snapshot.dat")

		if _, err := reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath, src, nil); err != nil {
			t.Fatalf("reencryptSnapshotFile: %v", err)
		}
		got, err := os.ReadFile(dstPath)
		if err != nil {
			t.Fatalf("read dst: %v", err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatal("plaintext destination mismatch")
		}
		src.assertBalanced(t, "reencryptSnapshotFile framed->plaintext src")
	})
}

// ---------------------------------------------------------------------------
// HandleSnapshot (the receive path)
// ---------------------------------------------------------------------------

// TestPinBalance_HandleSnapshot_FullReceiveCycleReturnsToZero drives a real
// receive — encrypt-on-write, snapshot.meta, the directory fsync chain and the
// LogDB record — and asserts the barrier's in-flight accounting is flat when
// the handler returns. A leak here pins an epoch for the life of the process.
func TestPinBalance_HandleSnapshot_FullReceiveCycleReturnsToZero(t *testing.T) {
	inner := createTestBarrier(t)
	b := newCountingBarrier(inner)
	f := newSnapshotReceiveFixture(t, b)
	authorizeSnapshotPath(t, f.engine, f.host, 1, 2, 1)

	payload := make([]byte, snapshotEncryptChunkSize*2+256)
	for i := range payload {
		payload[i] = byte(i * 5)
	}
	chunks := snapshotChunkFor(1, 2, 1, 500, 10,
		map[uint64]string{1: "127.0.0.1:6002", 2: "127.0.0.1:6001"}, payload)

	if err := f.handler.HandleSnapshot(chunks, authorizeAllSenders); err != nil {
		t.Fatalf("HandleSnapshot: %v", err)
	}
	if _, err := f.logdb.GetSnapshot(1, 2); err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if b.registrations() == 0 {
		t.Fatalf("fixture: receive pinned nothing, so the release is untested: %s", b.counts())
	}
	b.assertBalanced(t, "HandleSnapshot full receive cycle")
}

// TestPinBalance_HandleSnapshot_WriteFailureReturnsToZero is the error twin:
// the encrypt fails mid-write, the handler bails out, and the accounting must
// still be flat — the loop released its own pins and the handler's deferred
// release must not take them again.
func TestPinBalance_HandleSnapshot_WriteFailureReturnsToZero(t *testing.T) {
	inner := newRotatingMockBarrier(6, true)
	inner.failAt = 1
	inner.sealErr = errors.New("injected seal failure")
	b := newCountingBarrier(inner)
	f := newSnapshotReceiveFixture(t, b)
	authorizeSnapshotPath(t, f.engine, f.host, 1, 2, 1)

	payload := make([]byte, snapshotEncryptChunkSize*2+256)
	chunks := snapshotChunkFor(1, 2, 1, 500, 10,
		map[uint64]string{1: "127.0.0.1:6002", 2: "127.0.0.1:6001"}, payload)

	err := f.handler.HandleSnapshot(chunks, authorizeAllSenders)
	if err == nil {
		t.Fatal("HandleSnapshot: expected the injected seal failure")
	}
	var recvErr *SnapshotReceiveError
	if !errors.As(err, &recvErr) {
		t.Fatalf("error = %T (%v), want *SnapshotReceiveError", err, err)
	}
	if b.registrations() == 0 {
		t.Fatalf("fixture: nothing pinned: %s", b.counts())
	}
	b.assertBalanced(t, "HandleSnapshot write failure")
}
