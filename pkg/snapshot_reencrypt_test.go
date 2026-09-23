// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quicraft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// snapshotPlaintext returns a multi-chunk plaintext payload (>64KiB) so the
// frame loop is exercised across several frames.
func snapshotPlaintext() []byte {
	p := make([]byte, 64*1024*2+1234)
	for i := range p {
		p[i] = byte(i*7 + 3)
	}
	return p
}

// rotatingRecordBarrier assigns a NEW epoch to every frame (simulating a key
// rotation between each chunk) and records which epochs were pinned in-flight, so
// a test can prove the encrypt loop pins EVERY epoch it seals — not just the min.
type rotatingRecordBarrier struct {
	mockBarrier
	mu         sync.Mutex
	epoch      uint64
	used       map[uint64]bool
	registered map[uint64]bool
}

func (b *rotatingRecordBarrier) CurrentEpoch() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.epoch
}

func (b *rotatingRecordBarrier) EncryptWithEpoch(_, pt []byte) ([]byte, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.epoch
	b.epoch++ // a rotation lands between every frame
	if b.used == nil {
		b.used = map[uint64]bool{}
	}
	b.used[e] = true
	return append([]byte(nil), pt...), e, nil
}

func (b *rotatingRecordBarrier) EncryptWithEpochContext(dst, pt, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpoch(dst, pt)
	if err != nil {
		return nil, 0, err
	}
	return appendTestAADTag(ct, contextAAD), epoch, nil
}

func (b *rotatingRecordBarrier) RegisterInFlightEpoch(e uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.registered == nil {
		b.registered = map[uint64]bool{}
	}
	b.registered[e] = true
}

func (b *rotatingRecordBarrier) DeregisterInFlightEpoch(uint64) {}

// TestEncryptSnapshotFrames_PinsEveryFrameEpoch pins the DEK-lifetime property:
// when a snapshot's frames are sealed under multiple epochs (rotations landing
// mid-loop), EVERY such epoch must be pinned in-flight — not just the minimum —
// or a concurrent PurgeEpochsBefore could drop an intermediate frame's DEK and
// leave the snapshot permanently undecryptable.
func TestEncryptSnapshotFrames_PinsEveryFrameEpoch(t *testing.T) {
	b := &rotatingRecordBarrier{epoch: 5}
	// Several 64KiB chunks -> several frames, each sealed under a new epoch.
	plain := make([]byte, snapshotEncryptChunkSize*3+100)
	var out bytes.Buffer
	_, pinned, err := encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(plain), &out)
	if err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	if len(b.used) < 3 {
		t.Fatalf("fixture: only %d frame epochs used, want the multi-frame path", len(b.used))
	}
	pinnedSet := map[uint64]bool{}
	for _, e := range pinned {
		pinnedSet[e] = true
	}
	for e := range b.used {
		if !pinnedSet[e] {
			t.Fatalf("frame epoch %d was sealed but NOT returned as pinned (pinned=%v, used=%v): "+
				"a concurrent purge could drop its DEK and orphan that frame", e, pinned, b.used)
		}
		if !b.registered[e] {
			t.Fatalf("frame epoch %d was sealed but never RegisterInFlightEpoch'd", e)
		}
	}
}

// TestSnapshotFrames_EncryptDecryptRoundTrip proves encryptSnapshotFrames and
// decryptSnapshotFrames are inverses under the same barrier and that the recorded
// epoch is non-zero.
func TestSnapshotFrames_EncryptDecryptRoundTrip(t *testing.T) {
	b := createTestBarrier(t)
	plain := snapshotPlaintext()

	var enc bytes.Buffer
	epoch, _, err := encryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(plain), &enc)
	if err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	if epoch == 0 {
		t.Fatal("recorded epoch = 0, want non-zero for an encrypted snapshot")
	}
	var dec bytes.Buffer
	if _, err := decryptSnapshotFrames(b, testSnapShardID, testSnapReplicaID, testSnapIndex, &enc, &dec); err != nil {
		t.Fatalf("decryptSnapshotFrames: %v", err)
	}
	if !bytes.Equal(dec.Bytes(), plain) {
		t.Fatal("round-trip mismatch")
	}
}

// TestReceiveSnapshotReencryptsPerNode is the cross-node regression: a snapshot
// produced and encrypted by node A is streamed as PLAINTEXT (the sender decrypts
// before sending — doSendSnapshot), the receiver node B re-encrypts it under its
// OWN barrier, and B recovers the original data — while node A (a different
// per-node key) CANNOT decrypt B's at-rest file. This proves per-node snapshot
// encryption with no shared cross-node key.
func TestReceiveSnapshotReencryptsPerNode(t *testing.T) {
	nodeA := createTestBarrier(t) // sender
	nodeB := createTestBarrier(t) // receiver (independent random root key)

	plain := snapshotPlaintext()

	// Node A's at-rest encrypted snapshot.
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "a-snapshot.dat")
	writeEncryptedSnapshotFile(t, nodeA, srcPath, plain)

	// SEND: decrypt A's snapshot to plaintext (what goes on the wire).
	wirePath := filepath.Join(dir, "wire.dat")
	wf, err := os.Create(wirePath)
	if err != nil {
		t.Fatalf("create wire file: %v", err)
	}
	sf, err := os.Open(srcPath)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	if _, err := decryptSnapshotFrames(nodeA, testSnapShardID, testSnapReplicaID, testSnapIndex, sf, wf); err != nil {
		t.Fatalf("decrypt-for-send: %v", err)
	}
	_ = sf.Close()
	_ = wf.Close()
	if got, _ := os.ReadFile(wirePath); !bytes.Equal(got, plain) {
		t.Fatal("wire bytes are not the original plaintext")
	}

	// RECEIVE: node B writes the plaintext then re-encrypts under its OWN barrier.
	recvPath := filepath.Join(dir, "b-snapshot.dat")
	if err := os.WriteFile(recvPath, mustRead(t, wirePath), 0o600); err != nil {
		t.Fatalf("write received plaintext: %v", err)
	}
	recvEpoch, _, err := reencryptReceivedSnapshot(osSnapshotFS{}, recvPath, testSnapShardID, testSnapReplicaID, testSnapIndex, nodeB)
	if err != nil {
		t.Fatalf("reencryptReceivedSnapshot: %v", err)
	}
	if recvEpoch == 0 {
		t.Fatal("receiver recorded epoch 0 — re-encryption did not run")
	}

	// B recovers the original data from its at-rest snapshot.
	bf, err := os.Open(recvPath)
	if err != nil {
		t.Fatalf("open recv: %v", err)
	}
	defer func() { _ = bf.Close() }()
	var recovered bytes.Buffer
	if _, err := decryptSnapshotFrames(nodeB, testSnapShardID, testSnapReplicaID, testSnapIndex, bf, &recovered); err != nil {
		t.Fatalf("node B recover: %v", err)
	}
	if !bytes.Equal(recovered.Bytes(), plain) {
		t.Fatal("node B recovered data does not match original (DATA LOSS)")
	}

	// Node A (different per-node key) must NOT be able to decrypt B's file.
	bf2, err := os.Open(recvPath)
	if err != nil {
		t.Fatalf("reopen recv: %v", err)
	}
	defer func() { _ = bf2.Close() }()
	var crossAttempt bytes.Buffer
	if _, err := decryptSnapshotFrames(nodeA, testSnapShardID, testSnapReplicaID, testSnapIndex, bf2, &crossAttempt); err == nil && bytes.Equal(crossAttempt.Bytes(), plain) {
		t.Fatal("node A decrypted node B's snapshot — per-node keys are NOT isolated")
	}
}

// TestReceiveSnapshot_NilBarrierLeavesPlaintext proves the no-barrier path is a
// no-op (snapshot stays plaintext, epoch 0) — the plaintext deployment is
// unchanged.
func TestReceiveSnapshot_NilBarrierLeavesPlaintext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.dat")
	plain := snapshotPlaintext()
	if err := os.WriteFile(path, plain, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	epoch, _, err := reencryptReceivedSnapshot(osSnapshotFS{}, path, testSnapShardID, testSnapReplicaID, testSnapIndex, nil)
	if err != nil {
		t.Fatalf("reencryptReceivedSnapshot(nil): %v", err)
	}
	if epoch != 0 {
		t.Fatalf("nil-barrier epoch = %d, want 0", epoch)
	}
	if got := mustRead(t, path); !bytes.Equal(got, plain) {
		t.Fatal("nil-barrier path modified the snapshot")
	}
}

// TestReencryptSnapshotFile_RekeyAcrossBarriers proves the migration primitive
// re-keys an at-rest snapshot from one barrier to another: barrier A's framed
// snapshot is re-encrypted under barrier B, B recovers the exact original bytes,
// A (a different key) cannot read B's file, and the source is left untouched.
func TestReencryptSnapshotFile_RekeyAcrossBarriers(t *testing.T) {
	barA := createTestBarrier(t)
	barB := createTestBarrier(t)
	plain := snapshotPlaintext()

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src", "snapshot.dat")
	if err := os.MkdirAll(filepath.Dir(srcPath), 0o750); err != nil {
		t.Fatal(err)
	}
	writeEncryptedSnapshotFile(t, barA, srcPath, plain)

	dstPath := filepath.Join(dir, "dst", "snapshot.dat")
	epoch, err := ReencryptSnapshotFile(srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, barA, barB)
	if err != nil {
		t.Fatalf("ReencryptSnapshotFile: %v", err)
	}
	if epoch == 0 {
		t.Fatal("dst epoch = 0, want non-zero for a keyed destination")
	}

	// B recovers the original plaintext.
	df, err := os.Open(dstPath)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer func() { _ = df.Close() }()
	var recovered bytes.Buffer
	if _, err := decryptSnapshotFrames(barB, testSnapShardID, testSnapReplicaID, testSnapIndex, df, &recovered); err != nil {
		t.Fatalf("barrier B recover: %v", err)
	}
	if !bytes.Equal(recovered.Bytes(), plain) {
		t.Fatal("recovered data does not match original (DATA LOSS)")
	}

	// A must NOT decrypt B's re-keyed file.
	af, err := os.Open(dstPath)
	if err != nil {
		t.Fatalf("reopen dst: %v", err)
	}
	defer func() { _ = af.Close() }()
	var cross bytes.Buffer
	if _, err := decryptSnapshotFrames(barA, testSnapShardID, testSnapReplicaID, testSnapIndex, af, &cross); err == nil && bytes.Equal(cross.Bytes(), plain) {
		t.Fatal("barrier A decrypted barrier B's snapshot — keys not isolated")
	}

	// Source is left intact and still decryptable under A.
	sf, err := os.Open(srcPath)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer func() { _ = sf.Close() }()
	var srcRecovered bytes.Buffer
	if _, err := decryptSnapshotFrames(barA, testSnapShardID, testSnapReplicaID, testSnapIndex, sf, &srcRecovered); err != nil {
		t.Fatalf("source no longer decryptable under A — migration touched the source: %v", err)
	}
	if !bytes.Equal(srcRecovered.Bytes(), plain) {
		t.Fatal("source snapshot was modified")
	}
}

// TestReencryptSnapshotFile_PlaintextSides covers the none<->keyed paths an
// enable-encryption migration uses: plaintext source -> keyed dest, and keyed
// source -> plaintext dest, with data preserved both ways.
func TestReencryptSnapshotFile_PlaintextSides(t *testing.T) {
	bar := createTestBarrier(t)
	plain := snapshotPlaintext()
	dir := t.TempDir()

	plainSrc := filepath.Join(dir, "plain.dat")
	if err := os.WriteFile(plainSrc, plain, 0o600); err != nil {
		t.Fatal(err)
	}
	keyedDst := filepath.Join(dir, "keyed.dat")
	epoch, err := ReencryptSnapshotFile(plainSrc, keyedDst, testSnapShardID, testSnapReplicaID, testSnapIndex, nil, bar)
	if err != nil {
		t.Fatalf("plaintext->keyed: %v", err)
	}
	if epoch == 0 {
		t.Fatal("plaintext->keyed epoch = 0, want non-zero")
	}
	kf, err := os.Open(keyedDst)
	if err != nil {
		t.Fatalf("open keyed: %v", err)
	}
	defer func() { _ = kf.Close() }()
	var dec bytes.Buffer
	if _, err := decryptSnapshotFrames(bar, testSnapShardID, testSnapReplicaID, testSnapIndex, kf, &dec); err != nil {
		t.Fatalf("recover keyed: %v", err)
	}
	if !bytes.Equal(dec.Bytes(), plain) {
		t.Fatal("plaintext->keyed lost data")
	}

	plainDst := filepath.Join(dir, "back-to-plain.dat")
	if _, err := ReencryptSnapshotFile(keyedDst, plainDst, testSnapShardID, testSnapReplicaID, testSnapIndex, bar, nil); err != nil {
		t.Fatalf("keyed->plaintext: %v", err)
	}
	if got := mustRead(t, plainDst); !bytes.Equal(got, plain) {
		t.Fatal("keyed->plaintext lost data")
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

// pinRecordingBarrier wraps a real Barrier and records every in-flight epoch
// registration so tests can assert the L12 pinning contract.
type pinRecordingBarrier struct {
	Barrier
	mu                  sync.Mutex
	registered          []uint64
	deregistered        []uint64
	sawEncryptBeforePin bool
}

// EncryptWithEpochContext records whether a frame was ever encrypted BEFORE the
// first in-flight epoch was pinned. A path that pins only AFTER its encrypt loop
// (leaving the frames exposed to a concurrent rotate+purge mid-loop) trips this.
// It wraps the CONTEXT form because that is the one the snapshot encrypt loops
// call; the plain form would never be reached and the assertion would go
// vacuous.
func (p *pinRecordingBarrier) EncryptWithEpochContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	p.mu.Lock()
	if len(p.registered) == 0 {
		p.sawEncryptBeforePin = true
	}
	p.mu.Unlock()
	return p.Barrier.EncryptWithEpochContext(dst, plaintext, contextAAD)
}

func (p *pinRecordingBarrier) RegisterInFlightEpoch(epoch uint64) {
	p.mu.Lock()
	p.registered = append(p.registered, epoch)
	p.mu.Unlock()
	p.Barrier.RegisterInFlightEpoch(epoch)
}

func (p *pinRecordingBarrier) DeregisterInFlightEpoch(epoch uint64) {
	p.mu.Lock()
	p.deregistered = append(p.deregistered, epoch)
	p.mu.Unlock()
	p.Barrier.DeregisterInFlightEpoch(epoch)
}

// TestReencryptSnapshotFile_PinsSourceEpochs is the L12 regression test for
// the missing RegisterInFlightEpoch: while the source is decrypted, every
// epoch its frames reference must be pinned on the source barrier so a
// concurrent PurgeEpochsBefore cannot remove a DEK mid-migration — and the
// pins must be released when the migration finishes.
func TestReencryptSnapshotFile_PinsSourceEpochs(t *testing.T) {
	barA := createTestBarrier(t)
	barB := createTestBarrier(t)
	plain := snapshotPlaintext()

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "snapshot.dat")
	writeEncryptedSnapshotFile(t, barA, srcPath, plain)

	rec := &pinRecordingBarrier{Barrier: barA}
	dstPath := filepath.Join(dir, "dst", "snapshot.dat")
	if _, err := ReencryptSnapshotFile(srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, rec, barB); err != nil {
		t.Fatalf("ReencryptSnapshotFile: %v", err)
	}

	rec.mu.Lock()
	registered := append([]uint64(nil), rec.registered...)
	deregistered := append([]uint64(nil), rec.deregistered...)
	rec.mu.Unlock()

	if len(registered) == 0 {
		t.Fatal("source epochs were never pinned — a concurrent purge could remove a DEK mid-migration")
	}
	wantEpoch := barA.CurrentEpoch()
	found := false
	for _, e := range registered {
		if e == wantEpoch {
			found = true
		}
	}
	if !found {
		t.Fatalf("pinned epochs %v do not include the source frame epoch %d", registered, wantEpoch)
	}
	if len(deregistered) != len(registered) {
		t.Fatalf("pins leaked: registered %v but deregistered %v", registered, deregistered)
	}
}

// TestSnapshotFrameEpochs_ScansHeadersWithoutDecrypting verifies the frame
// epoch scanner returns the distinct epochs embedded in the frames' seal
// ciphertext headers, in first-seen order, without needing a barrier.
func TestSnapshotFrameEpochs_ScansHeadersWithoutDecrypting(t *testing.T) {
	// Real frames produced by a live barrier scan to that barrier's epoch.
	bar := createTestBarrier(t)
	var enc bytes.Buffer
	if _, _, err := encryptSnapshotFrames(bar, testSnapShardID, testSnapReplicaID, testSnapIndex, bytes.NewReader(snapshotPlaintext()), &enc); err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	epochs, err := snapshotFrameEpochs(bytes.NewReader(enc.Bytes()))
	if err != nil {
		t.Fatalf("snapshotFrameEpochs: %v", err)
	}
	if len(epochs) != 1 || epochs[0] != bar.CurrentEpoch() {
		t.Fatalf("epochs = %v, want [%d]", epochs, bar.CurrentEpoch())
	}

	// Synthetic multi-epoch frames: the scanner reads only the length
	// prefix and the 8-byte big-endian epoch header, so garbage ciphertext
	// bodies are fine.
	var synth bytes.Buffer
	writeFrame := func(epoch uint64, bodyLen int) {
		frame := make([]byte, 8+bodyLen)
		binary.BigEndian.PutUint64(frame[:8], epoch)
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(frame)))
		synth.Write(lenBuf[:])
		synth.Write(frame)
	}
	writeFrame(3, 40)
	writeFrame(4, 40)
	writeFrame(3, 40) // duplicate epoch: reported once
	epochs, err = snapshotFrameEpochs(bytes.NewReader(synth.Bytes()))
	if err != nil {
		t.Fatalf("snapshotFrameEpochs synthetic: %v", err)
	}
	if len(epochs) != 2 || epochs[0] != 3 || epochs[1] != 4 {
		t.Fatalf("epochs = %v, want [3 4]", epochs)
	}

	// Final frame truncated inside its epoch header: best-effort — the
	// epochs collected from the complete frames are returned with the error.
	truncated := append([]byte(nil), synth.Bytes()...)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], 48)
	truncated = append(truncated, lenBuf[:]...)
	truncated = append(truncated, 0xAA, 0xBB, 0xCC) // 3 of the declared 48 bytes
	epochs, err = snapshotFrameEpochs(bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("expected error for frame truncated inside its epoch header")
	}
	if len(epochs) != 2 {
		t.Fatalf("best-effort epochs = %v, want the 2 complete frames' epochs", epochs)
	}
}

// TestReencryptSnapshotFile_CreatesNestedDestination exercises the L12
// durability path: a destination several directories deep that does not
// exist yet must be created, populated, and dir-synced (SyncDirChain up to
// the deepest pre-existing ancestor) without error.
func TestReencryptSnapshotFile_CreatesNestedDestination(t *testing.T) {
	bar := createTestBarrier(t)
	plain := snapshotPlaintext()
	dir := t.TempDir()

	srcPath := filepath.Join(dir, "snapshot.dat")
	writeEncryptedSnapshotFile(t, bar, srcPath, plain)

	dstPath := filepath.Join(dir, "a", "b", "c", "snapshot.dat")
	if _, err := ReencryptSnapshotFile(srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, bar, nil); err != nil {
		t.Fatalf("ReencryptSnapshotFile into nested destination: %v", err)
	}
	if got := mustRead(t, dstPath); !bytes.Equal(got, plain) {
		t.Fatal("nested destination content mismatch")
	}
}

// TestReencryptSnapshotFile_SealedBarrierFailsClosed proves a CONFIGURED but
// sealed barrier is not treated as a plaintext side.
//
// The previous contract said "a nil or sealed barrier denotes a PLAINTEXT
// side". For the destination that is a silent downgrade. For the SOURCE it is
// corruption: the source's ciphertext would be read as though it were
// plaintext and written out recorded as plaintext, producing a destination
// nothing can decrypt, with no error reported.
func TestReencryptSnapshotFile_SealedBarrierFailsClosed(t *testing.T) {
	plain := snapshotPlaintext()

	newSrc := func(t *testing.T, bar Barrier) (string, string) {
		t.Helper()
		dir := t.TempDir()
		srcPath := filepath.Join(dir, "src", "snapshot.dat")
		if err := os.MkdirAll(filepath.Dir(srcPath), 0o750); err != nil {
			t.Fatal(err)
		}
		writeEncryptedSnapshotFile(t, bar, srcPath, plain)
		return srcPath, filepath.Join(dir, "dst", "snapshot.dat")
	}

	t.Run("sealed source", func(t *testing.T) {
		barA := createTestBarrier(t)
		barB := createTestBarrier(t)
		srcPath, dstPath := newSrc(t, barA)
		if err := barA.Seal(); err != nil {
			t.Fatalf("Seal: %v", err)
		}
		if _, err := ReencryptSnapshotFile(srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, barA, barB); !errors.Is(err, ErrSnapshotBarrierSealed) {
			t.Fatalf("error = %v, want ErrSnapshotBarrierSealed (a sealed source "+
				"must not have its ciphertext copied out as plaintext)", err)
		}
		if _, err := os.Stat(dstPath); !os.IsNotExist(err) {
			t.Error("destination was written despite the sealed source")
		}
	})

	t.Run("sealed destination", func(t *testing.T) {
		barA := createTestBarrier(t)
		barB := createTestBarrier(t)
		srcPath, dstPath := newSrc(t, barA)
		if err := barB.Seal(); err != nil {
			t.Fatalf("Seal: %v", err)
		}
		if _, err := ReencryptSnapshotFile(srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, barA, barB); !errors.Is(err, ErrSnapshotBarrierSealed) {
			t.Fatalf("error = %v, want ErrSnapshotBarrierSealed (a sealed "+
				"destination must not durably write plaintext)", err)
		}
		if _, err := os.Stat(dstPath); !os.IsNotExist(err) {
			t.Error("destination was written despite the sealed barrier")
		}
	})

	// A NIL barrier still means "this side is plaintext by configuration",
	// which is the behavior downstream migration callers depend on.
	t.Run("nil destination still writes plaintext", func(t *testing.T) {
		barA := createTestBarrier(t)
		srcPath, dstPath := newSrc(t, barA)
		epoch, err := ReencryptSnapshotFile(srcPath, dstPath, testSnapShardID, testSnapReplicaID, testSnapIndex, barA, nil)
		if err != nil {
			t.Fatalf("ReencryptSnapshotFile: %v", err)
		}
		if epoch != 0 {
			t.Errorf("epoch = %d, want 0 for a plaintext destination", epoch)
		}
		got, err := os.ReadFile(dstPath)
		if err != nil {
			t.Fatalf("read dst: %v", err)
		}
		if !bytes.Equal(got, plain) {
			t.Error("nil destination barrier did not write the plaintext through")
		}
	})
}

// EncryptWithEpochPinnedContext composes this fake's OWN seal and register, so
// whatever it injects or counts in either still takes effect on the pinned path
// the frame writer now always uses.
func (b *rotatingRecordBarrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := b.EncryptWithEpochContext(dst, plaintext, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	b.RegisterInFlightEpoch(epoch)
	return ct, epoch, nil
}

// EncryptWithEpochPinnedContext is the path every snapshot frame now takes:
// the real barrier seals and pins in ONE step under its own lock, so a frame
// can never exist unpinned. It records that pin for the assertions without
// registering it a second time — the real barrier already holds it, and the
// caller's release goes through DeregisterInFlightEpoch as before.
func (p *pinRecordingBarrier) EncryptWithEpochPinnedContext(dst, plaintext, contextAAD []byte) ([]byte, uint64, error) {
	ct, epoch, err := p.Barrier.EncryptWithEpochPinnedContext(dst, plaintext, contextAAD)
	if err != nil {
		return nil, 0, err
	}
	p.mu.Lock()
	p.registered = append(p.registered, epoch)
	p.mu.Unlock()
	return ct, epoch, nil
}
