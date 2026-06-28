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
	"os"
	"path/filepath"
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

// TestSnapshotFrames_EncryptDecryptRoundTrip proves encryptSnapshotFrames and
// decryptSnapshotFrames are inverses under the same barrier and that the recorded
// epoch is non-zero.
func TestSnapshotFrames_EncryptDecryptRoundTrip(t *testing.T) {
	b := createTestBarrier(t)
	plain := snapshotPlaintext()

	var enc bytes.Buffer
	epoch, err := encryptSnapshotFrames(b, bytes.NewReader(plain), &enc)
	if err != nil {
		t.Fatalf("encryptSnapshotFrames: %v", err)
	}
	if epoch == 0 {
		t.Fatal("recorded epoch = 0, want non-zero for an encrypted snapshot")
	}
	var dec bytes.Buffer
	if _, err := decryptSnapshotFrames(b, &enc, &dec); err != nil {
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
	if _, err := decryptSnapshotFrames(nodeA, sf, wf); err != nil {
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
	recvEpoch, err := reencryptReceivedSnapshot(recvPath, nodeB)
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
	if _, err := decryptSnapshotFrames(nodeB, bf, &recovered); err != nil {
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
	if _, err := decryptSnapshotFrames(nodeA, bf2, &crossAttempt); err == nil && bytes.Equal(crossAttempt.Bytes(), plain) {
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
	epoch, err := reencryptReceivedSnapshot(path, nil)
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
	epoch, err := ReencryptSnapshotFile(srcPath, dstPath, barA, barB)
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
	if _, err := decryptSnapshotFrames(barB, df, &recovered); err != nil {
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
	if _, err := decryptSnapshotFrames(barA, af, &cross); err == nil && bytes.Equal(cross.Bytes(), plain) {
		t.Fatal("barrier A decrypted barrier B's snapshot — keys not isolated")
	}

	// Source is left intact and still decryptable under A.
	sf, err := os.Open(srcPath)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer func() { _ = sf.Close() }()
	var srcRecovered bytes.Buffer
	if _, err := decryptSnapshotFrames(barA, sf, &srcRecovered); err != nil {
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
	epoch, err := ReencryptSnapshotFile(plainSrc, keyedDst, nil, bar)
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
	if _, err := decryptSnapshotFrames(bar, kf, &dec); err != nil {
		t.Fatalf("recover keyed: %v", err)
	}
	if !bytes.Equal(dec.Bytes(), plain) {
		t.Fatal("plaintext->keyed lost data")
	}

	plainDst := filepath.Join(dir, "back-to-plain.dat")
	if _, err := ReencryptSnapshotFile(keyedDst, plainDst, bar, nil); err != nil {
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
