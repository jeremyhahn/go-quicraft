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
	"context"
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

// countingBarrier wraps a Barrier and records the raw net in-flight count per epoch, negatives included: above zero after a cycle is a leak, below zero a double release.
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

// assertBalanced asserts every epoch's net in-flight count is zero and never went negative.
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

func (b *rotatingMockBarrier) EncryptWithEpochContext(dst, pt, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpoch(dst, pt)
	if err != nil {
		return nil, 0, err
	}
	return appendTestAADTag(ct, contextAAD), epoch, nil
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
// reencryptSnapshotFile failure the shared faultySnapshotFS does not inject.
type renameFailFS struct {
	osSnapshotFS
}

var errTestRenameRefused = errors.New("test fs: rename refused")

func (renameFailFS) Rename(string, string) error { return errTestRenameRefused }

var _ snapshotFS = renameFailFS{}

// ---------------------------------------------------------------------------
// encryptSnapshotFrames
// ---------------------------------------------------------------------------

// TestPinBalance_EncryptSnapshotFrames_SuccessHandsOffExactlyOnePinPerEpoch verifies each sealed epoch is held with a net count of exactly one on return.
func TestPinBalance_EncryptSnapshotFrames_SuccessHandsOffExactlyOnePinPerEpoch(t *testing.T) {
	inner := newRotatingMockBarrier(5, true)
	b := newCountingBarrier(inner)

	plain := make([]byte, snapshotEncryptChunkSize*3+100)
	var out bytes.Buffer
	_, pins, err := encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(plain), &out)
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

// atomicPinCountingBarrier is a countingBarrier that pins atomically with each seal and rotates on a schedule, as seal.Barrier does.
type atomicPinCountingBarrier struct {
	*countingBarrier
	rotateEvery int // seal a new epoch every N frames (0 = never rotate)
	frames      int
	epoch       uint64
}

func (b *atomicPinCountingBarrier) EncryptWithEpochPinnedContext(_, pt, contextAAD []byte) ([]byte, uint64, error) {
	b.frames++
	if b.rotateEvery > 0 && b.frames%b.rotateEvery == 0 {
		b.epoch++
	}
	// The barrier takes the reference itself, atomically with the seal.
	b.RegisterInFlightEpoch(b.epoch)
	return appendTestAADTag(append([]byte(nil), pt...), contextAAD), b.epoch, nil
}

// TestPinBalance_AtomicPath_BalancesAndDedups verifies the atomic path returns each sealed epoch once and releases duplicate references.
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
			_, pins, err := encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(plain), &out)
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

// TestPinBalance_EncryptSnapshotFrames_EveryErrorPathReleases verifies each seal error path leaves the count flat and returns a nil pin set.
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
				return encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(plain), &out)
			},
		},
		{
			name: "write_frame_length_error",
			run: func(b Barrier) (uint64, []uint64, error) {
				plain := make([]byte, snapshotEncryptChunkSize*2+10)
				return encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(plain),
					&pinErrWriter{failOn: 1, err: errInjected})
			},
		},
		{
			name: "write_ciphertext_error",
			run: func(b Barrier) (uint64, []uint64, error) {
				plain := make([]byte, snapshotEncryptChunkSize*2+10)
				return encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(plain),
					&pinErrWriter{failOn: 2, err: errInjected})
			},
		},
		{
			name: "read_error",
			run: func(b Barrier) (uint64, []uint64, error) {
				var out bytes.Buffer
				// One byte past a chunk: the writer holds a full chunk back until
				// more plaintext proves it is not the terminal frame, so exactly
				// one chunk would seal nothing before the error arrived.
				return encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, &pinErrReader{remaining: snapshotEncryptChunkSize + 1, err: errInjected}, &out)
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
// writeImportedSnapshotData (the import's streamed encryption)
// ---------------------------------------------------------------------------

// plainImportPayload is a plaintext snapshot payload spanning several frames.
func plainImportPayload() []byte {
	data := make([]byte, snapshotEncryptChunkSize*2+64)
	for i := range data {
		data[i] = byte(i*17 + 1)
	}
	return data
}

// writePlainSnapshot writes plainImportPayload to dir/snapshot.dat.
func writePlainSnapshot(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "snapshot.dat")
	if err := os.WriteFile(path, plainImportPayload(), 0o600); err != nil {
		t.Fatalf("write plaintext snapshot: %v", err)
	}
	return path
}

// TestPinBalance_WriteImportedSnapshotData_PostEncryptFailuresRelease verifies Sync and Close failures after encryption release the pins.
func TestPinBalance_WriteImportedSnapshotData_PostEncryptFailuresRelease(t *testing.T) {
	cases := []struct {
		name string
		fsys snapshotFS
		want error
	}{
		{"sync_failure", importWriteFaultFS{failSync: true}, errTestSyncFailed},
		{"close_failure", importWriteFaultFS{failClose: true}, errTestCloseFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newCountingBarrier(newRotatingMockBarrier(4, true))
			path := filepath.Join(t.TempDir(), "snapshot.dat")
			h := &Host{}

			_, pins, err := h.writeImportedSnapshotData(context.Background(), tc.fsys,
				testSnapShardID, testSnapReplicaID, testSnapIndex, path, nil,
				trailedPayload(t, plainImportPayload()), b)
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
			b.assertBalanced(t, "writeImportedSnapshotData "+tc.name)
		})
	}
}

// TestPinBalance_WriteImportedSnapshotData verifies pins are held after a successful data write until released, and none remain on error.
func TestPinBalance_WriteImportedSnapshotData(t *testing.T) {
	t.Run("success_holds_then_releases", func(t *testing.T) {
		b := newCountingBarrier(newRotatingMockBarrier(11, true))
		path := filepath.Join(t.TempDir(), "snapshot.dat")
		h := &Host{}

		epoch, pins, err := h.writeImportedSnapshotData(context.Background(), osSnapshotFS{},
			testSnapShardID, testSnapReplicaID, testSnapIndex, path, nil,
			trailedPayload(t, plainImportPayload()), b)
		if err != nil {
			t.Fatalf("writeImportedSnapshotData: %v", err)
		}
		if epoch == 0 {
			t.Fatal("epoch = 0 for an encrypted import")
		}
		if b.registrations() < 2 {
			t.Fatalf("fixture: only %d registrations, want the multi-epoch path: %s",
				b.registrations(), b.counts())
		}
		if b.held(epoch) != 1 {
			t.Fatalf("recorded epoch %d held %d times before release, want 1 — the import's "+
				"SaveSnapshot has not recorded it yet: %s", epoch, b.held(epoch), b.counts())
		}
		for _, e := range pins {
			b.DeregisterInFlightEpoch(e)
		}
		b.assertBalanced(t, "writeImportedSnapshotData success")
	})

	t.Run("nil_barrier_writes_plaintext", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "snapshot.dat")
		h := &Host{}
		payload := plainImportPayload()
		epoch, pins, err := h.writeImportedSnapshotData(context.Background(), osSnapshotFS{},
			testSnapShardID, testSnapReplicaID, testSnapIndex, path, nil, trailedPayload(t, payload), nil)
		if err != nil {
			t.Fatalf("nil barrier: %v", err)
		}
		if epoch != 0 || pins != nil {
			t.Fatalf("(epoch %d, pins %v), want (0, nil) for a plaintext deployment", epoch, pins)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read back: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("a plaintext deployment's import did not store the payload as it is")
		}
	})
}

// ---------------------------------------------------------------------------
// reencryptSnapshotFile (the migrate path behind ReencryptSnapshotFile)
// ---------------------------------------------------------------------------

// TestPinBalance_ReencryptSnapshotFile verifies destination pins balance on success and on a failure after pinning.
func TestPinBalance_ReencryptSnapshotFile(t *testing.T) {
	t.Run("plaintext_to_framed_success", func(t *testing.T) {
		dst := newCountingBarrier(newRotatingMockBarrier(3, true))
		dir := t.TempDir()
		srcPath := writePlainSnapshot(t, dir)
		dstPath := filepath.Join(dir, "out", "snapshot.dat")

		if _, err := reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, nil, dst); err != nil {
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

		_, err := reencryptSnapshotFile(renameFailFS{}, srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, nil, dst)
		if !errors.Is(err, errTestRenameRefused) {
			t.Fatalf("error = %v, want %v", err, errTestRenameRefused)
		}
		if dst.registrations() == 0 {
			t.Fatalf("fixture: nothing pinned: %s", dst.counts())
		}
		dst.assertBalanced(t, "reencryptSnapshotFile rename failure")
	})

	t.Run("plaintext_to_framed_encrypt_failure", func(t *testing.T) {
		// The loop already released its pins, so the set it returns must be nil.
		inner := newRotatingMockBarrier(3, true)
		inner.failAt = 1
		inner.sealErr = errors.New("injected seal failure")
		dst := newCountingBarrier(inner)
		dir := t.TempDir()
		srcPath := writePlainSnapshot(t, dir)
		dstPath := filepath.Join(dir, "out", "snapshot.dat")

		if _, err := reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, nil, dst); err == nil {
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

		if _, err := reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, src, dst); err != nil {
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

		if _, err := reencryptSnapshotFile(osSnapshotFS{}, srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, src, nil); err != nil {
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

// EncryptWithEpochPinnedContext composes this fake's OWN seal and register, so
// whatever it injects or counts in either still takes effect on the pinned path
// the frame writer now always uses.
func (b *rotatingMockBarrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpochContext(dst, plaintext, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	b.RegisterInFlightEpoch(epoch)
	return ct, epoch, nil
}

// EncryptWithEpochPinnedContext composes this fake's OWN seal and register, so
// whatever it injects or counts in either still takes effect on the pinned path
// the frame writer now always uses.
func (b *countingBarrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpochContext(dst, plaintext, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	b.RegisterInFlightEpoch(epoch)
	return ct, epoch, nil
}
