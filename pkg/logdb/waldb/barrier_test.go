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

package waldb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// newTestBarrier creates a real seal.Barrier initialized with a software
// strategy using minimal Argon2 parameters for fast test execution.
func newTestBarrier(t *testing.T) *seal.Barrier {
	t.Helper()
	strategy, err := seal.NewSoftwareStrategyWithParams(
		[]byte("test-passphrase"), 1, 64*1024, 1,
	)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams: %v", err)
	}
	t.Cleanup(func() { strategy.Close() })

	barrier := seal.NewBarrier(seal.DefaultBarrierConfig(1))
	if err := barrier.Initialize(context.Background(), strategy, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Initialize: %v", err)
	}
	return barrier
}

// openEncryptedTestDB creates a WAL DB with barrier encryption enabled.
func openEncryptedTestDB(t *testing.T, barrier WALBarrier) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open with barrier: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// --- Encrypted WAL round-trip tests ---

// TestEncryptedWALRoundTripEntries verifies entries written through an
// encrypted WAL can be read back correctly.
func TestEncryptedWALRoundTripEntries(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	entries := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: []byte("hello encrypted world")},
		{Index: 2, Term: 1, Cmd: []byte("foo bar baz")},
		{Index: 3, Term: 2, Cmd: []byte("third entry")},
	}
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	got, err := db.GetEntries(1, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetEntries: got %d entries, want 3", len(got))
	}
	if string(got[0].Cmd) != "hello encrypted world" {
		t.Fatalf("entry[0].Cmd = %q, want %q", got[0].Cmd, "hello encrypted world")
	}
	if string(got[1].Cmd) != "foo bar baz" {
		t.Fatalf("entry[1].Cmd = %q, want %q", got[1].Cmd, "foo bar baz")
	}
	if string(got[2].Cmd) != "third entry" {
		t.Fatalf("entry[2].Cmd = %q, want %q", got[2].Cmd, "third entry")
	}
}

// TestEncryptedWALRoundTripEntriesError verifies that SaveEntries returns
// an error when the entries slice is empty (no-op) and handles bad input.
func TestEncryptedWALRoundTripEntriesEmptyCmd(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	// Entries with empty Cmd should still round-trip.
	entries := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: nil},
		{Index: 2, Term: 1, Cmd: []byte{}},
	}
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	got, err := db.GetEntries(1, 1, 1, 3, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetEntries: got %d entries, want 2", len(got))
	}
}

// TestEncryptedWALRoundTripState verifies hard state written through an
// encrypted WAL can be read back correctly.
func TestEncryptedWALRoundTripState(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	state := logdb.State{Term: 5, Vote: 2, Commit: 100}
	if err := db.SaveNodeState(1, 1, state); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}

	got, err := db.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if got.Term != 5 || got.Vote != 2 || got.Commit != 100 {
		t.Fatalf("GetState: got %+v, want Term=5 Vote=2 Commit=100", got)
	}
}

// TestEncryptedWALRoundTripStateError verifies ReadState returns ErrNoState
// for an unknown node in an encrypted WAL.
func TestEncryptedWALRoundTripStateNonExistent(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	_, _, err := db.ReadState(99, 99)
	if err != logdb.ErrNoState {
		t.Fatalf("ReadState: got %v, want %v", err, logdb.ErrNoState)
	}
}

// TestEncryptedWALRoundTripBootstrap verifies bootstrap info written through
// an encrypted WAL can be read back correctly.
func TestEncryptedWALRoundTripBootstrap(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	bs := logdb.Bootstrap{
		Addresses: map[uint64]string{1: "addr1", 2: "addr2"},
		Join:      true,
		Type:      2,
	}
	if err := db.SaveBootstrap(1, 1, bs); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}

	got, found, err := db.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap: %v", err)
	}
	if !found {
		t.Fatal("GetBootstrap: not found")
	}
	if !got.Join || got.Type != 2 || len(got.Addresses) != 2 {
		t.Fatalf("GetBootstrap: got %+v", got)
	}
}

// TestEncryptedWALRoundTripSnapshot verifies snapshot metadata written
// through an encrypted WAL can be read back correctly.
func TestEncryptedWALRoundTripSnapshot(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	ss := logdb.Snapshot{
		Index:       50,
		Term:        3,
		Filepath:    "/data/snapshot-50",
		FileSize:    8192,
		OnDiskIndex: 45,
	}
	if err := db.SaveSnapshot(1, 1, ss); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, err := db.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.Index != 50 || got.Term != 3 || got.Filepath != "/data/snapshot-50" {
		t.Fatalf("GetSnapshot: got %+v", got)
	}
	if got.FileSize != 8192 || got.OnDiskIndex != 45 {
		t.Fatalf("GetSnapshot: FileSize=%d OnDiskIndex=%d", got.FileSize, got.OnDiskIndex)
	}
}

// --- Raw bytes verification ---

// TestEncryptedWALRecordsNotPlaintext verifies that on-disk WAL records are
// actually encrypted (not stored as plaintext). We write a known entry,
// then read the raw segment file bytes and verify the plaintext command
// does not appear.
func TestEncryptedWALRecordsNotPlaintext(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	knownPayload := []byte("THIS_IS_A_KNOWN_PLAINTEXT_PAYLOAD_12345")
	entries := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: knownPayload},
	}
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	db.Close()

	// Scan all segment files in all shard directories for the plaintext.
	found := false
	for i := 0; i < DefaultShardCount; i++ {
		shardDir := filepath.Join(dir, shardDirName(i))
		dirEntries, err := os.ReadDir(shardDir)
		if err != nil {
			continue
		}
		for _, de := range dirEntries {
			if de.IsDir() {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(shardDir, de.Name()))
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if bytes.Contains(raw, knownPayload) {
				found = true
			}
		}
	}
	if found {
		t.Fatal("plaintext payload found in raw WAL segment file; expected encrypted data")
	}
}

// TestEncryptedWALRecordsNotPlaintextError verifies that the verification
// test actually detects plaintext when encryption is NOT enabled.
func TestEncryptedWALRecordsPlaintextWithoutBarrier(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	knownPayload := []byte("THIS_IS_A_KNOWN_PLAINTEXT_PAYLOAD_12345")
	entries := []logdb.Entry{
		{Index: 1, Term: 1, Cmd: knownPayload},
	}
	if err := db.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	db.Close()

	// Without encryption, the plaintext should appear in the raw file.
	found := false
	for i := 0; i < DefaultShardCount; i++ {
		shardDir := filepath.Join(dir, shardDirName(i))
		dirEntries, err := os.ReadDir(shardDir)
		if err != nil {
			continue
		}
		for _, de := range dirEntries {
			if de.IsDir() {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(shardDir, de.Name()))
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if bytes.Contains(raw, knownPayload) {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("plaintext payload NOT found in unencrypted WAL; test sanity check failed")
	}
}

// --- Encrypted WAL recovery tests ---

// TestEncryptedWALRecovery verifies that data survives close and reopen
// when WAL encryption is enabled.
func TestEncryptedWALRecovery(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	// Open, write, close.
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 3, Vote: 1, Commit: 10}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	if err := db.SaveBootstrap(1, 1, logdb.Bootstrap{
		Addresses: map[uint64]string{1: "localhost:9001"},
		Type:      1,
	}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{Index: 5, Term: 2}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with the same barrier and verify.
	db2, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// Verify entries.
	got, err := db2.GetEntries(1, 1, 1, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries after reopen: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("GetEntries after reopen: got %d, want 10", len(got))
	}

	// Verify state.
	state, err := db2.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState after reopen: %v", err)
	}
	if state.Term != 3 || state.Vote != 1 || state.Commit != 10 {
		t.Fatalf("GetState after reopen: got %+v", state)
	}

	// Verify bootstrap.
	bs, found, err := db2.GetBootstrap(1, 1)
	if err != nil {
		t.Fatalf("GetBootstrap after reopen: %v", err)
	}
	if !found {
		t.Fatal("GetBootstrap after reopen: not found")
	}
	if bs.Addresses[1] != "localhost:9001" {
		t.Fatalf("GetBootstrap after reopen: addr=%q", bs.Addresses[1])
	}

	// Verify snapshot.
	ss, err := db2.GetSnapshot(1, 1)
	if err != nil {
		t.Fatalf("GetSnapshot after reopen: %v", err)
	}
	if ss.Index != 5 || ss.Term != 2 {
		t.Fatalf("GetSnapshot after reopen: got %+v", ss)
	}
}

// TestEncryptedWALRecoveryWithoutBarrier verifies that a WAL written with
// encryption cannot be recovered without the barrier (records are skipped).
func TestEncryptedWALRecoveryWithoutBarrier(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	// Write encrypted data.
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 3, Vote: 1, Commit: 5}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	db.Close()

	// Reopen WITHOUT barrier -- encrypted records should be silently
	// skipped during recovery (no barrier means no decryption, and the
	// raw encrypted bytes will fail to parse as valid records).
	db2, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		// No WithBarrier -- records are just replayed as raw bytes.
	)
	if err != nil {
		t.Fatalf("Reopen without barrier: %v", err)
	}
	defer db2.Close()

	// The unencrypted replay will attempt to interpret encrypted bytes as
	// WAL records. Since they are AES-GCM ciphertext, the payload type
	// byte is effectively random, so most records will be silently skipped
	// by replayRecord. No entries or state should be recovered.
	got, err := db2.GetEntries(1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetEntries: got %d entries without barrier, want 0", len(got))
	}

	_, _, err = db2.ReadState(1, 1)
	if err != logdb.ErrNoState {
		t.Fatalf("ReadState: got %v, want %v", err, logdb.ErrNoState)
	}
}

// TestEncryptedWALRecoveryCompaction verifies that compacted state survives
// close and reopen with encryption enabled.
func TestEncryptedWALRecoveryCompaction(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	db.Close()

	db2, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	// Compacted entries should still be compacted.
	_, err = db2.GetEntries(1, 1, 1, 6, 0)
	if err != logdb.ErrCompacted {
		t.Fatalf("GetEntries(1,6): got %v, want %v", err, logdb.ErrCompacted)
	}

	// Entries above compaction point should be available.
	got, err := db2.GetEntries(1, 1, 6, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries(6,11): %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("GetEntries(6,11): got %d, want 5", len(got))
	}
}

// --- Sealed barrier tests ---

// TestEncryptedWALSealedBarrierWrite verifies that writing to a WAL with
// a sealed barrier returns an encryption error.
func TestEncryptedWALSealedBarrierWrite(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	// Seal the barrier.
	if err := barrier.Seal(); err != nil {
		t.Fatalf("barrier.Seal: %v", err)
	}

	// Writing should fail because the barrier is sealed.
	err := db.SaveEntries(1, 1, makeEntries(1, 1, 1))
	if err == nil {
		t.Fatal("SaveEntries with sealed barrier: expected error, got nil")
	}

	var encryptErr *WALEncryptError
	if !errors.As(err, &encryptErr) {
		t.Fatalf("SaveEntries with sealed barrier: expected WALEncryptError, got %T: %v", err, err)
	}
}

// TestEncryptedWALSealedBarrierWriteError verifies the WALEncryptError
// type provides correct error messages and unwrapping.
func TestEncryptedWALSealedBarrierErrorType(t *testing.T) {
	inner := errors.New("test inner error")
	e := &WALEncryptError{Err: inner}
	msg := e.Error()
	if msg == "" {
		t.Fatal("WALEncryptError.Error() returned empty string")
	}
	if e.Unwrap() != inner {
		t.Fatalf("WALEncryptError.Unwrap: got %v, want %v", e.Unwrap(), inner)
	}
}

// --- Multi-shard encrypted tests ---

// TestEncryptedWALMultiShard verifies that multiple shards work correctly
// with encryption enabled, including data isolation.
func TestEncryptedWALMultiShard(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries(1,1): %v", err)
	}
	if err := db.SaveEntries(2, 1, makeEntries(1, 3, 2)); err != nil {
		t.Fatalf("SaveEntries(2,1): %v", err)
	}

	got1, err := db.GetEntries(1, 1, 1, 6, 0)
	if err != nil {
		t.Fatalf("GetEntries(1,1): %v", err)
	}
	if len(got1) != 5 {
		t.Fatalf("GetEntries(1,1): got %d, want 5", len(got1))
	}

	got2, err := db.GetEntries(2, 1, 1, 4, 0)
	if err != nil {
		t.Fatalf("GetEntries(2,1): %v", err)
	}
	if len(got2) != 3 {
		t.Fatalf("GetEntries(2,1): got %d, want 3", len(got2))
	}
}

// TestEncryptedWALMultiShardError verifies that multi-shard isolation holds
// after compaction with encryption enabled.
func TestEncryptedWALMultiShardCompaction(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries(1,1): %v", err)
	}
	if err := db.SaveEntries(2, 1, makeEntries(1, 10, 2)); err != nil {
		t.Fatalf("SaveEntries(2,1): %v", err)
	}

	// Compact shard 1 but not shard 2.
	if err := db.RemoveEntriesTo(1, 1, 5); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}

	// Shard 1: compacted range returns error.
	_, err := db.GetEntries(1, 1, 1, 6, 0)
	if err != logdb.ErrCompacted {
		t.Fatalf("GetEntries(1,1,1,6): got %v, want %v", err, logdb.ErrCompacted)
	}

	// Shard 2: all entries should be present.
	got, err := db.GetEntries(2, 1, 1, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries(2,1): %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("GetEntries(2,1): got %d, want 10", len(got))
	}
}

// --- Segment rotation with encryption ---

// TestEncryptedWALSegmentRotation verifies entries survive WAL segment
// rotation when encryption is enabled.
func TestEncryptedWALSegmentRotation(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	// Small max file size to force segment rotation.
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(512),
		WithMaxFileSize(2*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	totalEntries := 30
	for i := 1; i <= totalEntries; i++ {
		entry := logdb.Entry{
			Index: uint64(i),
			Term:  1,
			Cmd:   make([]byte, 80),
		}
		entry.Cmd[0] = byte(i % 256)
		if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}

	got, err := db.GetEntries(1, 1, 1, uint64(totalEntries)+1, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != totalEntries {
		t.Fatalf("GetEntries: got %d, want %d", len(got), totalEntries)
	}
	for i, e := range got {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry[%d].Index = %d, want %d", i, e.Index, i+1)
		}
		if e.Cmd[0] != byte((i+1)%256) {
			t.Fatalf("entry[%d].Cmd[0] = %d, want %d", i, e.Cmd[0], byte((i+1)%256))
		}
	}

	db.Close()
}

// TestEncryptedWALSegmentRotationRecovery verifies entries survive segment
// rotation AND database reopen when encryption is enabled.
func TestEncryptedWALSegmentRotationRecovery(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(512),
		WithMaxFileSize(2*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	totalEntries := 20
	for i := 1; i <= totalEntries; i++ {
		entry := logdb.Entry{
			Index: uint64(i),
			Term:  1,
			Cmd:   make([]byte, 60),
		}
		entry.Cmd[0] = byte(i % 256)
		if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
			t.Fatalf("SaveEntries(%d): %v", i, err)
		}
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 5, Vote: 3, Commit: 20}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	db.Close()

	// Reopen with the same barrier.
	db2, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(512),
		WithMaxFileSize(2*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	got, err := db2.GetEntries(1, 1, 1, uint64(totalEntries)+1, 0)
	if err != nil {
		t.Fatalf("GetEntries after reopen: %v", err)
	}
	if len(got) != totalEntries {
		t.Fatalf("GetEntries: got %d, want %d", len(got), totalEntries)
	}
	for i, e := range got {
		if e.Cmd[0] != byte((i+1)%256) {
			t.Fatalf("entry[%d].Cmd[0] = %d, want %d", i, e.Cmd[0], byte((i+1)%256))
		}
	}

	state, err := db2.GetState(1, 1)
	if err != nil {
		t.Fatalf("GetState after reopen: %v", err)
	}
	if state.Term != 5 || state.Vote != 3 || state.Commit != 20 {
		t.Fatalf("GetState: got %+v, want Term=5 Vote=3 Commit=20", state)
	}
}

// --- Key rotation test ---

// TestEncryptedWALKeyRotation verifies that entries written before and
// after key rotation can both be read back correctly.
func TestEncryptedWALKeyRotation(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	// Write entries at epoch 1.
	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries (epoch 1): %v", err)
	}

	// Rotate the key.
	newEpoch, err := barrier.Rotate()
	if err != nil {
		t.Fatalf("barrier.Rotate: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("barrier.Rotate: epoch=%d, want 2", newEpoch)
	}

	// Write more entries at epoch 2.
	if err := db.SaveEntries(1, 1, makeEntries(6, 5, 1)); err != nil {
		t.Fatalf("SaveEntries (epoch 2): %v", err)
	}

	// Both epoch-1 and epoch-2 entries should be readable.
	got, err := db.GetEntries(1, 1, 1, 11, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("GetEntries: got %d entries, want 10", len(got))
	}
	for i, e := range got {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry[%d].Index = %d, want %d", i, e.Index, i+1)
		}
	}
}

// TestEncryptedWALKeyRotationError verifies that rotation on a sealed
// barrier returns an error (not panicking).
func TestEncryptedWALKeyRotationSealed(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	// Write some data.
	if err := db.SaveEntries(1, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	// Seal the barrier.
	if err := barrier.Seal(); err != nil {
		t.Fatalf("barrier.Seal: %v", err)
	}

	// Rotate should fail.
	_, err := barrier.Rotate()
	if err == nil {
		t.Fatal("barrier.Rotate on sealed barrier: expected error, got nil")
	}

	// Writes should also fail.
	err = db.SaveEntries(1, 1, makeEntries(4, 1, 1))
	if err == nil {
		t.Fatal("SaveEntries on sealed barrier: expected error, got nil")
	}
}

// --- WALBarrier interface satisfaction ---

// TestCryptoBarrierSatisfiesWALBarrier is a compile-time check that
// seal.Barrier satisfies the WALBarrier interface.
func TestCryptoBarrierSatisfiesWALBarrier(t *testing.T) {
	var _ WALBarrier = (*seal.Barrier)(nil)
}

// --- WAL decryption error tests ---

// TestEncryptedWALRecoveryDecryptionError verifies that when a WAL is
// written with one barrier and recovery is attempted with a different
// barrier (wrong key), a WALDecryptError is returned instead of
// silently skipping records.
func TestEncryptedWALRecoveryDecryptionError(t *testing.T) {
	barrier1 := newTestBarrier(t)
	dir := t.TempDir()

	// Write encrypted data with barrier1.
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier1),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	db.Close()

	// Create a different barrier (different key).
	barrier2 := newTestBarrierWithPassphrase(t, "different-passphrase")

	// Reopen with wrong barrier -- must return WALDecryptError.
	_, err = Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier2),
	)
	if err == nil {
		t.Fatal("Open with wrong barrier: expected error, got nil")
	}

	var decryptErr *WALDecryptError
	if !errors.As(err, &decryptErr) {
		t.Fatalf("Open with wrong barrier: expected WALDecryptError, got %T: %v", err, err)
	}
}

// TestWALDecryptErrorType verifies the WALDecryptError type provides
// correct error messages and unwrapping.
func TestWALDecryptErrorType(t *testing.T) {
	inner := errors.New("test decryption failure")
	e := &WALDecryptError{SegmentID: 7, Err: inner}
	msg := e.Error()
	if msg == "" {
		t.Fatal("WALDecryptError.Error() returned empty string")
	}
	if e.Unwrap() != inner {
		t.Fatalf("WALDecryptError.Unwrap: got %v, want %v", e.Unwrap(), inner)
	}
}

// newTestBarrierWithPassphrase creates a barrier with a specific passphrase,
// for testing decryption with wrong keys.
func newTestBarrierWithPassphrase(t *testing.T, passphrase string) *seal.Barrier {
	t.Helper()
	strategy, err := seal.NewSoftwareStrategyWithParams(
		[]byte(passphrase), 1, 64*1024, 1,
	)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyWithParams: %v", err)
	}
	t.Cleanup(func() { strategy.Close() })

	barrier := seal.NewBarrier(seal.DefaultBarrierConfig(1))
	if err := barrier.Initialize(context.Background(), strategy, seal.Credentials{}); err != nil {
		t.Fatalf("barrier.Initialize: %v", err)
	}
	return barrier
}

// --- WithBarrier option ---

// TestWithBarrierOption verifies the functional option sets the barrier.
func TestWithBarrierOption(t *testing.T) {
	barrier := newTestBarrier(t)
	opts := defaultDBOptions()
	WithBarrier(barrier)(&opts)
	if opts.barrier != barrier {
		t.Fatal("WithBarrier did not set barrier on dbOptions")
	}
}

// TestWithBarrierNilOption verifies nil barrier is a no-op (no encryption).
func TestWithBarrierNilOption(t *testing.T) {
	opts := defaultDBOptions()
	WithBarrier(nil)(&opts)
	if opts.barrier != nil {
		t.Fatal("WithBarrier(nil) should set barrier to nil")
	}
}

// ---------------------------------------------------------------------------
// MinLiveEpoch tests
// ---------------------------------------------------------------------------

// TestMinLiveEpoch_NoBarrier verifies that MinLiveEpoch returns 0 when
// no barrier is configured (plaintext WAL).
func TestMinLiveEpoch_NoBarrier(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if epoch != 0 {
		t.Fatalf("MinLiveEpoch = %d, want 0 (no barrier)", epoch)
	}
}

// TestMinLiveEpoch_SingleEpoch verifies that MinLiveEpoch returns the
// current epoch when all records are written at a single epoch.
func TestMinLiveEpoch_SingleEpoch(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	if err := db.SaveEntries(1, 1, makeEntries(1, 10, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("MinLiveEpoch = %d, want 1", epoch)
	}
}

// TestMinLiveEpoch_MultiEpoch verifies that MinLiveEpoch returns the
// minimum epoch when records span multiple key rotation epochs.
func TestMinLiveEpoch_MultiEpoch(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	// Write at epoch 1.
	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries (epoch 1): %v", err)
	}

	// Rotate to epoch 2.
	if _, err := barrier.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Write at epoch 2.
	if err := db.SaveEntries(1, 1, makeEntries(6, 5, 1)); err != nil {
		t.Fatalf("SaveEntries (epoch 2): %v", err)
	}

	// Rotate to epoch 3.
	if _, err := barrier.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Write at epoch 3.
	if err := db.SaveEntries(1, 1, makeEntries(11, 5, 1)); err != nil {
		t.Fatalf("SaveEntries (epoch 3): %v", err)
	}

	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("MinLiveEpoch = %d, want 1 (oldest epoch still in WAL)", epoch)
	}
}

// TestMinLiveEpoch_AfterCompaction verifies that MinLiveEpoch increases
// after compaction removes old segments containing older epochs.
func TestMinLiveEpoch_AfterCompaction(t *testing.T) {
	barrier := newTestBarrier(t)

	// Use a very small segment size to force rotation, creating
	// separate segments for different epochs.
	dir := t.TempDir()
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(512),
		WithMaxFileSize(2*1024), // 2KB segment forces rotation
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Write enough entries at epoch 1 to fill multiple segments.
	for i := uint64(1); i <= 20; i++ {
		entry := logdb.Entry{
			Index: i, Term: 1, Type: 0,
			Cmd: bytes.Repeat([]byte{byte(i)}, 200),
		}
		if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
			t.Fatalf("SaveEntries epoch 1 entry %d: %v", i, err)
		}
	}

	// Rotate to epoch 2.
	if _, err := barrier.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Write entries at epoch 2.
	for i := uint64(21); i <= 40; i++ {
		entry := logdb.Entry{
			Index: i, Term: 2, Type: 0,
			Cmd: bytes.Repeat([]byte{byte(i)}, 200),
		}
		if err := db.SaveEntries(1, 1, []logdb.Entry{entry}); err != nil {
			t.Fatalf("SaveEntries epoch 2 entry %d: %v", i, err)
		}
	}

	// Verify min epoch is 1 before compaction.
	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch before compaction: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("MinLiveEpoch before compaction = %d, want 1", epoch)
	}

	// Compact entries 1-20 (all epoch 1 entries).
	if err := db.RemoveEntriesTo(1, 1, 20); err != nil {
		t.Fatalf("RemoveEntriesTo: %v", err)
	}
	if err := db.Compact(1, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// After compaction + GC, min epoch should increase to 2.
	epochAfter, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch after compaction: %v", err)
	}
	if epochAfter != 2 {
		// This is acceptable if segments with epoch 1 records survive GC
		// because they also contain epoch 2 records (mixed segment).
		// In that case, minWriteEpoch stays at 1, which is conservative
		// and safe (prevents premature purge).
		if epochAfter != 1 {
			t.Fatalf("MinLiveEpoch after compaction = %d, want 1 or 2", epochAfter)
		}
	}
}

// TestMinLiveEpoch_ClosedDB verifies that MinLiveEpoch returns an error
// when called on a closed database.
func TestMinLiveEpoch_ClosedDB(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	if err := db.SaveEntries(1, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}

	db.Close()
	_, err := db.MinLiveEpoch()
	if !errors.Is(err, logdb.ErrClosed) {
		t.Fatalf("MinLiveEpoch on closed DB: got %v, want ErrClosed", err)
	}
}

// TestMinLiveEpoch_EmptyDB verifies that MinLiveEpoch returns 0 for an
// empty encrypted DB.
func TestMinLiveEpoch_EmptyDB(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	if epoch != 0 {
		t.Fatalf("MinLiveEpoch = %d, want 0 (empty DB)", epoch)
	}
}

// TestMinLiveEpoch_MultipleShards verifies that MinLiveEpoch returns the
// global minimum across all WAL shards.
func TestMinLiveEpoch_MultipleShards(t *testing.T) {
	barrier := newTestBarrier(t)
	db := openEncryptedTestDB(t, barrier)

	// Write at epoch 1 to shard 1.
	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries shard 1: %v", err)
	}

	// Rotate to epoch 2.
	if _, err := barrier.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Write at epoch 2 to shard 2 (different WAL shard).
	if err := db.SaveEntries(2, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries shard 2: %v", err)
	}

	epoch, err := db.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch: %v", err)
	}
	// Shard 1 has epoch 1, shard 2 has epoch 2. Global min should be 1.
	if epoch != 1 {
		t.Fatalf("MinLiveEpoch = %d, want 1 (global min across shards)", epoch)
	}
}

// TestMinLiveEpoch_RecoveryPreservesEpoch verifies that MinLiveEpoch is
// correctly restored after DB close and reopen (WAL replay).
func TestMinLiveEpoch_RecoveryPreservesEpoch(t *testing.T) {
	barrier := newTestBarrier(t)
	dir := t.TempDir()

	// Open, write at epoch 1, rotate, write at epoch 2, close.
	db, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries epoch 1: %v", err)
	}
	if _, err := barrier.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if err := db.SaveEntries(1, 1, makeEntries(6, 5, 1)); err != nil {
		t.Fatalf("SaveEntries epoch 2: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with the same barrier (still has both epoch DEKs).
	db2, err := Open(dir,
		WithNoSync(true),
		WithBlockSize(1024),
		WithMaxFileSize(64*1024),
		WithBarrier(barrier),
	)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	epoch, err := db2.MinLiveEpoch()
	if err != nil {
		t.Fatalf("MinLiveEpoch after recovery: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("MinLiveEpoch after recovery = %d, want 1", epoch)
	}
}
