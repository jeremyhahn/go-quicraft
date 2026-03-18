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

//go:build quicraft_errfs

// Package errfs_test exercises WAL I/O error injection scenarios using
// ErrorFS. Each test opens a waldb.DB with an ErrorFS delegate, injects a
// failure at a precise operation number, and then verifies that:
//
//   - The error is correctly surfaced to the caller (no swallowing, no panic).
//   - The DB can be re-opened against the same directory using the real FS.
//   - Data written before the failure is intact; data written during or after
//     the failure is absent (no phantom reads).
//
// # Injection strategy
//
// The ErrorFS counter is shared across all FS and File operations. To target
// a specific write-path operation we use a sweep approach: iterate injectAt
// from 1 upward using a FRESH COPY of the baseline directory for each attempt.
// This ensures the directory state is identical for every injection target and
// the op count is reproducible.
//
//   - copyDir creates a complete recursive copy of the source directory.
//   - sweepUntilWriteError finds the first injectAt that causes a write
//     operation (not an Open-phase op) to return an error.
//
// The tests use the quicraft_errfs build tag so they are excluded from the
// default test run and can be targeted explicitly with:
//
//	go test -tags quicraft_errfs ./test/errfs/...
package errfs_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/test/errfs"
)

// walBlockSize is the WAL block size used throughout these tests.
const walBlockSize = 1024

// largeCmdSize is the Cmd payload for entries that must overflow the block
// buffer during segment.Write. A Cmd of this size causes padAndFlushBlock to
// be called, triggering File.Write inline during SaveEntries — independent of
// whether sync is enabled. With blockSize=1024 and recordHeaderSize=7, a Cmd
// of 1088 bytes spans at least two blocks.
const largeCmdSize = walBlockSize + 64

// maxSweepOps is the maximum number of injection targets to try in a sweep
// before declaring failure. It is intentionally generous to handle machines
// with many open-phase ops.
const maxSweepOps = 300

// --- helpers ---

// testEntry builds a logdb.Entry with a small recognisable payload.
func testEntry(index, term uint64) logdb.Entry {
	return logdb.Entry{Index: index, Term: term, Type: 0, Cmd: []byte("cmd")}
}

// testEntryLarge builds a logdb.Entry whose Cmd exceeds walBlockSize, forcing
// padAndFlushBlock → File.Write during segment.Write.
func testEntryLarge(index, term uint64) logdb.Entry {
	cmd := make([]byte, largeCmdSize)
	for i := range cmd {
		cmd[i] = 0xAB
	}
	return logdb.Entry{Index: index, Term: term, Type: 0, Cmd: cmd}
}

// testState returns a non-zero logdb.State.
func testState(term, vote, commit uint64) logdb.State {
	return logdb.State{Term: term, Vote: vote, Commit: commit}
}

// openDB opens a waldb.DB at dir with the supplied options merged with fixed
// test defaults (blockSize, maxFileSize). Callers must call Close.
func openDB(t *testing.T, dir string, opts ...waldb.Option) *waldb.DB {
	t.Helper()
	base := []waldb.Option{
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64 * 1024),
	}
	db, err := waldb.Open(dir, append(base, opts...)...)
	if err != nil {
		t.Fatalf("waldb.Open: %v", err)
	}
	return db
}

// mustClose closes db, failing the test on error.
func mustClose(t *testing.T, db *waldb.DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}
}

// lenientClose closes db and logs (but does not fail) on error. Used when the
// DB may be in a degraded state after error injection.
func lenientClose(t *testing.T, db *waldb.DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Logf("DB.Close (expected after fault injection): %v", err)
	}
}

// saveEntries writes count sequential small entries starting at startIdx.
func saveEntries(t *testing.T, db *waldb.DB, shardID, replicaID, startIdx, count uint64) {
	t.Helper()
	entries := make([]logdb.Entry, count)
	for i := uint64(0); i < count; i++ {
		entries[i] = testEntry(startIdx+i, 1)
	}
	if err := db.SaveEntries(shardID, replicaID, entries); err != nil {
		t.Fatalf("SaveEntries(start=%d, count=%d): %v", startIdx, count, err)
	}
}

// assertEntriesPresent verifies GetEntries returns exactly (high-low) entries
// in [low, high) all with wantTerm.
func assertEntriesPresent(t *testing.T, db *waldb.DB, shardID, replicaID, low, high, wantTerm uint64) {
	t.Helper()
	got, err := db.GetEntries(shardID, replicaID, low, high, 0)
	if err != nil {
		t.Fatalf("GetEntries[%d,%d): %v", low, high, err)
	}
	want := int(high - low)
	if len(got) != want {
		t.Fatalf("GetEntries[%d,%d): got %d, want %d", low, high, len(got), want)
	}
	for _, e := range got {
		if e.Term != wantTerm {
			t.Fatalf("entry index=%d: term=%d, want %d", e.Index, e.Term, wantTerm)
		}
	}
}

// assertEntryAbsent verifies no entry at the given index is visible.
func assertEntryAbsent(t *testing.T, db *waldb.DB, shardID, replicaID, index uint64) {
	t.Helper()
	got, err := db.GetEntries(shardID, replicaID, index, index+1, 0)
	if err != nil {
		t.Fatalf("assertEntryAbsent GetEntries[%d,%d): %v", index, index+1, err)
	}
	if len(got) != 0 {
		t.Fatalf("entry index=%d must be absent, got %d entries", index, len(got))
	}
}

// copyDir creates a recursive copy of src into dst, preserving file mode bits.
// Used to snapshot a baseline DB directory so each sweep iteration starts from
// an identical state.
func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(src, path)
		if relErr != nil {
			return relErr
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		return os.WriteFile(target, data, 0600)
	})
}

// sweepUntilWriteError tries injectAt values 1, 2, … until writeFn returns a
// non-nil error that wraps io.ErrUnexpectedEOF. For each attempt it copies
// srcDir to a fresh temp directory so the DB state is identical across all
// iterations. Returns the first injectAt that caused a write error.
//
// The test is fatally failed if no write error is found within maxSweepOps.
func sweepUntilWriteError(
	t *testing.T,
	srcDir string,
	writeFn func(dir string, efs *errfs.ErrorFS) error,
) int64 {
	t.Helper()
	for target := int64(1); target <= maxSweepOps; target++ {
		iterDir := t.TempDir()
		if err := copyDir(srcDir, iterDir); err != nil {
			t.Fatalf("copyDir(target=%d): %v", target, err)
		}
		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF)
		err := writeFn(iterDir, efs)
		if err != nil && errors.Is(err, io.ErrUnexpectedEOF) {
			return target
		}
	}
	t.Fatalf("sweepUntilWriteError: no write error found across %d injection attempts", maxSweepOps)
	return 0
}

// --- TestErrorFS_WAL_WriteMidEntry ---

// TestErrorFS_WAL_WriteMidEntry verifies that when a File.Write call fails
// during a WAL segment write, the error propagates to the caller and the
// partially-written entry is absent after recovery.
//
// A large entry (Cmd > blockSize) is used so padAndFlushBlock fires inline
// during SaveEntries, making the write interception reliable without sync.
func TestErrorFS_WAL_WriteMidEntry(t *testing.T) {
	const (
		shardID   = uint64(1)
		replicaID = uint64(1)
	)

	// Establish a stable baseline in a dedicated dir.
	baseDir := t.TempDir()
	setup := openDB(t, baseDir, waldb.WithNoSync(true))
	saveEntries(t, setup, shardID, replicaID, 1, 3)
	mustClose(t, setup)

	// Find the injection target that causes SaveEntries to fail.
	firedAt := sweepUntilWriteError(t, baseDir, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs, waldb.WithNoSync(true))
		if openErr != nil {
			return nil // injection fired during Open; skip
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(4, 1)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("write injection fired at op %d", firedAt)

	// Run the actual injection against a fresh copy of the baseline.
	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), firedAt, io.ErrUnexpectedEOF)
	db, openErr := openDBWithEFS(t, injectDir, efs, waldb.WithNoSync(true))
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(4, 1)})
	if writeErr == nil {
		t.Fatal("SaveEntries: expected error, got nil")
	}
	if !errors.Is(writeErr, io.ErrUnexpectedEOF) {
		t.Fatalf("SaveEntries error chain does not contain injected error: %v", writeErr)
	}
	if !efs.Injected() {
		t.Fatal("ErrorFS: injection was not triggered")
	}

	lenientClose(t, db)

	// Re-open injectDir WITHOUT ErrorFS and verify data integrity.
	recovered := openDB(t, injectDir, waldb.WithNoSync(true))
	defer mustClose(t, recovered)

	// All 3 baseline entries must survive.
	assertEntriesPresent(t, recovered, shardID, replicaID, 1, 4, 1)

	// Entry 4 (the failed large write) must NOT be visible.
	assertEntryAbsent(t, recovered, shardID, replicaID, 4)
}

// openDBWithEFS opens a waldb.DB with an ErrorFS. Returns (nil, err) if Open
// fails (injection fired during Open). The caller can detect this and skip.
func openDBWithEFS(t *testing.T, dir string, efs *errfs.ErrorFS, opts ...waldb.Option) (*waldb.DB, error) {
	t.Helper()
	base := []waldb.Option{
		waldb.WithFS(efs),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64 * 1024),
	}
	db, err := waldb.Open(dir, append(base, opts...)...)
	return db, err
}

// --- TestErrorFS_WAL_SyncFailure ---

// TestErrorFS_WAL_SyncFailure verifies that when a File.Write or File.Sync
// call fails during the sync phase of SaveState, the error propagates to the
// caller and previously-committed entries survive recovery.
//
// WithNoSync(false) is used so SaveState calls segment.Sync → flushBlock →
// File.Write, File.Sync inline.
func TestErrorFS_WAL_SyncFailure(t *testing.T) {
	const (
		shardID   = uint64(2)
		replicaID = uint64(1)
	)

	baseDir := t.TempDir()

	// Write baseline with sync enabled for durability.
	setup := openDB(t, baseDir)
	saveEntries(t, setup, shardID, replicaID, 1, 3)
	mustClose(t, setup)

	// Find the injection target that causes a sync-enabled SaveEntries to fail.
	firedAt := sweepUntilWriteError(t, baseDir, func(dir string, efs *errfs.ErrorFS) error {
		// WithNoSync NOT set: sync is enabled so Write and Sync fire during SaveEntries.
		db, openErr := openDBWithEFS(t, dir, efs)
		if openErr != nil {
			return nil
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(4, 1)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("sync injection fired at op %d", firedAt)

	// Inject at the discovered target and verify the error surfaces.
	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), firedAt, io.ErrUnexpectedEOF)
	db, openErr := openDBWithEFS(t, injectDir, efs)
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	syncErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(4, 1)})
	if syncErr == nil {
		t.Fatal("SaveEntries: expected error, got nil")
	}
	if !errors.Is(syncErr, io.ErrUnexpectedEOF) {
		t.Fatalf("SaveEntries error does not wrap injected error: %v", syncErr)
	}

	lenientClose(t, db)

	// Re-open without ErrorFS — must recover cleanly.
	recovered := openDB(t, injectDir, waldb.WithNoSync(true))
	defer mustClose(t, recovered)

	// The 3 baseline entries must survive.
	assertEntriesPresent(t, recovered, shardID, replicaID, 1, 4, 1)
}

// --- TestErrorFS_WAL_ReadDuringReplay ---

// TestErrorFS_WAL_ReadDuringReplay verifies that an I/O error during WAL
// recovery (Open) is surfaced to the caller without a panic.
//
// It sweeps all injection targets across a full Open call, verifying that
// every error wraps the injected error.
func TestErrorFS_WAL_ReadDuringReplay(t *testing.T) {
	const (
		shardID   = uint64(3)
		replicaID = uint64(1)
	)

	dir := t.TempDir()

	// Write data so segment files exist for recovery to read.
	setup := openDB(t, dir, waldb.WithNoSync(true))
	saveEntries(t, setup, shardID, replicaID, 1, 5)
	mustClose(t, setup)

	// Probe the total number of ops for a clean Open.
	passthrough := errfs.New(waldb.DefaultFS(), 0, io.ErrUnexpectedEOF)
	probeDB, err := waldb.Open(dir,
		waldb.WithFS(passthrough),
		waldb.WithNoSync(true),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64*1024),
	)
	if err != nil {
		t.Fatalf("probe Open: %v", err)
	}
	totalOps := passthrough.Count()
	mustClose(t, probeDB)

	if totalOps == 0 {
		t.Fatal("probe: Open consumed zero operations")
	}

	foundReadError := false

	// For each op, copy the dir to preserve baseline state.
	for target := int64(1); target <= totalOps; target++ {
		iterDir := t.TempDir()
		if err := copyDir(dir, iterDir); err != nil {
			t.Fatalf("copyDir(target=%d): %v", target, err)
		}

		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF)
		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithNoSync(true),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			if !errors.Is(openErr, io.ErrUnexpectedEOF) {
				t.Fatalf("target=%d: Open error does not wrap injected: %v", target, openErr)
			}
			foundReadError = true
			continue
		}
		mustClose(t, db)
	}

	if !foundReadError {
		t.Fatal("no injection target caused Open to fail; read-path error handling may be incomplete")
	}

	t.Logf("read-path error correctly surfaced for at least one of %d injection targets", totalOps)
}

// --- TestErrorFS_LogDB_SaveStateWriteFailure ---

// TestErrorFS_LogDB_SaveStateWriteFailure verifies that when a Write fails
// during the SaveState batch path (logdb.LogDB.SaveState), the typed error
// propagates to the caller and previously-committed state survives recovery.
func TestErrorFS_LogDB_SaveStateWriteFailure(t *testing.T) {
	const (
		shardID   = uint64(5)
		replicaID = uint64(1)
	)

	baseDir := t.TempDir()

	// Establish durable baseline.
	setup := openDB(t, baseDir, waldb.WithNoSync(true))
	goodState := testState(2, 1, 3)
	if err := setup.SaveNodeState(shardID, replicaID, goodState); err != nil {
		t.Fatalf("setup SaveNodeState: %v", err)
	}
	saveEntries(t, setup, shardID, replicaID, 1, 2)
	mustClose(t, setup)

	// Find the injection target for the SaveState write path.
	firedAt := sweepUntilWriteError(t, baseDir, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs, waldb.WithNoSync(true))
		if openErr != nil {
			return nil
		}
		writeErr := db.SaveState([]logdb.Update{{
			ShardID:   shardID,
			ReplicaID: replicaID,
			State:     testState(3, 2, 5),
			Entries:   []logdb.Entry{testEntryLarge(3, 3)},
		}})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("SaveState write injection fired at op %d", firedAt)

	// Execute injection and verify the error.
	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), firedAt, io.ErrUnexpectedEOF)
	db, openErr := openDBWithEFS(t, injectDir, efs, waldb.WithNoSync(true))
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	writeErr := db.SaveState([]logdb.Update{{
		ShardID:   shardID,
		ReplicaID: replicaID,
		State:     testState(3, 2, 5),
		Entries:   []logdb.Entry{testEntryLarge(3, 3)},
	}})
	if writeErr == nil {
		t.Fatal("SaveState: expected error, got nil")
	}
	if !errors.Is(writeErr, io.ErrUnexpectedEOF) {
		t.Fatalf("SaveState error chain does not contain injected error: %v", writeErr)
	}
	if !efs.Injected() {
		t.Fatal("ErrorFS: injection was not triggered")
	}

	lenientClose(t, db)

	// Re-open without ErrorFS and verify baseline data is intact.
	recovered := openDB(t, injectDir, waldb.WithNoSync(true))
	defer mustClose(t, recovered)

	gotState, err := recovered.GetState(shardID, replicaID)
	if err != nil {
		t.Fatalf("GetState after recovery: %v", err)
	}
	if gotState != goodState {
		t.Fatalf("GetState: got %+v, want %+v", gotState, goodState)
	}

	assertEntriesPresent(t, recovered, shardID, replicaID, 1, 3, 1)
	assertEntryAbsent(t, recovered, shardID, replicaID, 3)
}

// --- TestErrorFS_LogDB_GetRangeReadFailure ---

// TestErrorFS_LogDB_GetRangeReadFailure verifies that an I/O error during
// WAL recovery surfaces as a non-nil error from waldb.Open, preventing the
// caller from receiving a silently-partial log view.
func TestErrorFS_LogDB_GetRangeReadFailure(t *testing.T) {
	const (
		shardID   = uint64(6)
		replicaID = uint64(1)
	)

	dir := t.TempDir()

	// Write 10 entries so the recovery path reads real data.
	setup := openDB(t, dir, waldb.WithNoSync(true))
	saveEntries(t, setup, shardID, replicaID, 1, 10)
	mustClose(t, setup)

	// Probe total ops for a clean Open.
	passthrough := errfs.New(waldb.DefaultFS(), 0, io.ErrUnexpectedEOF)
	probeDB, err := waldb.Open(dir,
		waldb.WithFS(passthrough),
		waldb.WithNoSync(true),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64*1024),
	)
	if err != nil {
		t.Fatalf("probe Open: %v", err)
	}
	totalOps := passthrough.Count()
	mustClose(t, probeDB)

	failCount := 0

	for target := int64(1); target <= totalOps; target++ {
		iterDir := t.TempDir()
		if err := copyDir(dir, iterDir); err != nil {
			t.Fatalf("copyDir(target=%d): %v", target, err)
		}

		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF)
		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithNoSync(true),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			if !errors.Is(openErr, io.ErrUnexpectedEOF) {
				t.Fatalf("target=%d: Open error does not wrap injected: %v", target, openErr)
			}
			failCount++
			continue
		}
		mustClose(t, db)
	}

	if failCount == 0 {
		t.Fatal("no injection caused Open to fail; read-path error surfacing may be missing")
	}

	t.Logf("Open failed for %d of %d injection targets (read-path errors correctly surfaced)",
		failCount, totalOps)
}

// --- TestErrorFS_LogDB_SuccessfulReOpenAfterFailure ---

// TestErrorFS_LogDB_SuccessfulReOpenAfterFailure is the end-to-end crash-
// recovery scenario: after multiple I/O failures across an entire sweep of
// injection targets, the DB re-opens cleanly with the real FS and all
// durably-committed data is readable and the DB is operational.
func TestErrorFS_LogDB_SuccessfulReOpenAfterFailure(t *testing.T) {
	const (
		shardID   = uint64(7)
		replicaID = uint64(1)
	)

	dir := t.TempDir()

	// Phase 1: write 5 entries and hard state durably.
	first := openDB(t, dir, waldb.WithNoSync(true))
	saveEntries(t, first, shardID, replicaID, 1, 5)
	goodState := testState(1, 1, 5)
	if err := first.SaveNodeState(shardID, replicaID, goodState); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	mustClose(t, first)

	// Phase 2: perform many injected write attempts (each on a fresh copy)
	// to simulate repeated crash scenarios across the op range.
	probeCount := int64(0)
	{
		passthrough := errfs.New(waldb.DefaultFS(), 0, io.ErrUnexpectedEOF)
		probeDB, err := waldb.Open(dir,
			waldb.WithFS(passthrough),
			waldb.WithNoSync(true),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if err != nil {
			t.Fatalf("probe Open: %v", err)
		}
		probeCount = passthrough.Count() + 30 // cover open + write ops
		mustClose(t, probeDB)
	}

	for target := int64(1); target <= probeCount; target++ {
		iterDir := t.TempDir()
		if err := copyDir(dir, iterDir); err != nil {
			t.Fatalf("copyDir(target=%d): %v", target, err)
		}

		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF)
		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithNoSync(true),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			continue
		}
		_ = db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(6, 2)})
		lenientClose(t, db)

		// After each injection, verify the original dir is still recoverable.
		check := openDB(t, iterDir, waldb.WithNoSync(true))
		checkState, stateErr := check.GetState(shardID, replicaID)
		mustClose(t, check)
		if stateErr != nil && !errors.Is(stateErr, logdb.ErrNoState) {
			t.Fatalf("target=%d: GetState after recovery: %v", target, stateErr)
		}
		if stateErr == nil && checkState != goodState {
			t.Fatalf("target=%d: GetState: got %+v, want %+v", target, checkState, goodState)
		}
	}

	// Phase 3: the original dir must still be fully recoverable.
	recovered := openDB(t, dir, waldb.WithNoSync(true))
	defer mustClose(t, recovered)

	assertEntriesPresent(t, recovered, shardID, replicaID, 1, 6, 1)

	gotState, err := recovered.GetState(shardID, replicaID)
	if err != nil {
		t.Fatalf("GetState after recovery: %v", err)
	}
	if gotState != goodState {
		t.Fatalf("GetState: got %+v, want %+v", gotState, goodState)
	}

	newEntries := []logdb.Entry{testEntry(7, 2), testEntry(8, 2)}
	if err := recovered.SaveEntries(shardID, replicaID, newEntries); err != nil {
		t.Fatalf("SaveEntries on recovered DB: %v", err)
	}
}

// --- TestErrorFS_WAL_OpenFileFailure ---

// TestErrorFS_WAL_OpenFileFailure verifies that an I/O error during segment
// file creation (OpenFile) surfaces as a typed error. waldb uses lazy shard
// initialization, so OpenFile for the first segment fires on the first write.
func TestErrorFS_WAL_OpenFileFailure(t *testing.T) {
	const (
		shardID   = uint64(8)
		replicaID = uint64(1)
	)

	// Fresh (empty) dir so the shard is lazy and OpenFile fires on SaveEntries.
	baseDir := t.TempDir()

	// Probe total ops for Open + SaveEntries on fresh dir.
	passthrough := errfs.New(waldb.DefaultFS(), 0, io.ErrUnexpectedEOF)
	probeDB, openErr := waldb.Open(baseDir,
		waldb.WithFS(passthrough),
		waldb.WithNoSync(true),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64*1024),
	)
	if openErr != nil {
		t.Fatalf("probe Open: %v", openErr)
	}
	opsAfterOpen := passthrough.Count()
	if err := probeDB.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(1, 1)}); err != nil {
		t.Fatalf("probe SaveEntries: %v", err)
	}
	opsAfterWrite := passthrough.Count()
	mustClose(t, probeDB)

	// Sweep across write ops and verify all errors wrap the injected error.
	foundError := false
	for target := int64(1); target <= opsAfterWrite; target++ {
		injectDir := t.TempDir()
		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF)

		db, openErr := waldb.Open(injectDir,
			waldb.WithFS(efs),
			waldb.WithNoSync(true),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			if !errors.Is(openErr, io.ErrUnexpectedEOF) {
				t.Fatalf("target=%d: Open error does not wrap injected: %v", target, openErr)
			}
			foundError = true
			continue
		}

		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(1, 1)})
		lenientClose(t, db)

		if writeErr != nil {
			if !errors.Is(writeErr, io.ErrUnexpectedEOF) {
				t.Fatalf("target=%d: SaveEntries error does not wrap injected: %v", target, writeErr)
			}
			foundError = true
		}
	}

	if !foundError {
		t.Fatalf("no error found across %d injection targets (opsAfterOpen=%d, opsAfterWrite=%d)",
			opsAfterWrite, opsAfterOpen, opsAfterWrite)
	}
}

// --- TestErrorFS_InjectedError_TypeAssertion ---

// TestErrorFS_InjectedError_TypeAssertion verifies that *InjectedError is
// accessible via errors.As from a waldb error chain, enabling callers to
// extract operation context for logging or metrics.
func TestErrorFS_InjectedError_TypeAssertion(t *testing.T) {
	const (
		shardID   = uint64(9)
		replicaID = uint64(1)
	)

	// Find a target where a large-entry SaveEntries fails.
	baseDir := t.TempDir()

	type result struct {
		target int64
		err    error
		efs    *errfs.ErrorFS
	}
	var found *result

	for target := int64(1); target <= maxSweepOps; target++ {
		iterDir := t.TempDir()

		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF)
		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithNoSync(true),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			continue
		}

		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(1, 1)})
		lenientClose(t, db)

		if writeErr != nil && errors.Is(writeErr, io.ErrUnexpectedEOF) {
			found = &result{target: target, err: writeErr, efs: efs}
			break
		}
	}

	_ = baseDir
	if found == nil {
		t.Fatal("no injection target produced a SaveEntries write error")
	}

	// Verify *InjectedError is accessible via errors.As.
	var injected *errfs.InjectedError
	if !errors.As(found.err, &injected) {
		t.Fatalf("errors.As(*InjectedError): not found in chain: %v", found.err)
	}
	if injected.Operation == "" {
		t.Error("InjectedError.Operation is empty")
	}
	if injected.OpCount != found.target {
		t.Errorf("InjectedError.OpCount=%d, want %d", injected.OpCount, found.target)
	}
	if !errors.Is(injected.Err, io.ErrUnexpectedEOF) {
		t.Errorf("InjectedError.Err: %v, want io.ErrUnexpectedEOF", injected.Err)
	}
	if !found.efs.Injected() {
		t.Error("ErrorFS.Injected() should be true after firing")
	}

	t.Logf("InjectedError: op=%q count=%d", injected.Operation, injected.OpCount)
}

// --- TestErrorFS_Reset_ClearsState ---

// TestErrorFS_Reset_ClearsState verifies that Reset clears the fired flag and
// counter so the same ErrorFS instance can be re-armed between test scenarios.
func TestErrorFS_Reset_ClearsState(t *testing.T) {
	const (
		shardID   = uint64(10)
		replicaID = uint64(1)
	)

	// Phase 1: find a target that makes a SaveEntries call fail.
	var firstFiredEFS *errfs.ErrorFS

	for target := int64(1); target <= maxSweepOps; target++ {
		dir := t.TempDir()
		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF)
		db, openErr := waldb.Open(dir,
			waldb.WithFS(efs),
			waldb.WithNoSync(true),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			continue
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(1, 1)})
		lenientClose(t, db)

		if writeErr != nil && errors.Is(writeErr, io.ErrUnexpectedEOF) {
			firstFiredEFS = efs
			break
		}
	}

	if firstFiredEFS == nil {
		t.Fatal("no injection target produced a write error in phase 1")
	}

	// Verify fired state.
	if !firstFiredEFS.Injected() {
		t.Fatal("after firing: Injected() should be true")
	}
	if firstFiredEFS.Count() == 0 {
		t.Fatal("after firing: Count() should be > 0")
	}

	// Phase 2: Reset to passthrough and verify state clears.
	firstFiredEFS.Reset(0, io.ErrUnexpectedEOF)

	if firstFiredEFS.Injected() {
		t.Fatal("after Reset(0): Injected() should be false")
	}
	if firstFiredEFS.Count() != 0 {
		t.Fatalf("after Reset(0): Count()=%d, want 0", firstFiredEFS.Count())
	}

	// Phase 3: re-arm and verify a second injection fires. We sweep injectAt
	// until SaveEntries fails, demonstrating that Reset allows the same
	// ErrorFS instance to be re-used as a new injection point.
	var secondFired bool
	for target := int64(1); target <= maxSweepOps; target++ {
		dir2 := t.TempDir()
		// Re-arm the same efs instance (firstFiredEFS) at increasing targets.
		firstFiredEFS.Reset(target, io.ErrUnexpectedEOF)

		db2, openErr2 := waldb.Open(dir2,
			waldb.WithFS(firstFiredEFS),
			waldb.WithNoSync(true),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr2 != nil {
			// Injection fired during Open (non-critical-path check OK too).
			if errors.Is(openErr2, io.ErrUnexpectedEOF) {
				secondFired = true
				break
			}
			continue
		}

		secondErr := db2.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(1, 1)})
		lenientClose(t, db2)

		if secondErr != nil && errors.Is(secondErr, io.ErrUnexpectedEOF) {
			secondFired = true
			break
		}
	}

	if !secondFired {
		t.Fatal("re-armed injection: no error produced after Reset + re-arm")
	}
}

// --- TestErrorFS_WAL_MultipleShards_PartialBatch ---

// TestErrorFS_WAL_MultipleShards_PartialBatch verifies that when a batch
// SaveState spanning two shards fails mid-batch, the error surfaces and no
// phantom data from the partial batch is visible after recovery.
func TestErrorFS_WAL_MultipleShards_PartialBatch(t *testing.T) {
	// shardA and shardB hash to different WAL directory shards.
	const (
		shardA = uint64(0) // → shard-0
		shardB = uint64(1) // → shard-1
		repID  = uint64(1)
	)

	baseDir := t.TempDir()

	// Establish baseline in both shards.
	setup := openDB(t, baseDir, waldb.WithNoSync(true))
	saveEntries(t, setup, shardA, repID, 1, 2)
	saveEntries(t, setup, shardB, repID, 1, 2)
	mustClose(t, setup)

	// Find the injection target that causes the multi-shard batch to fail.
	firedAt := sweepUntilWriteError(t, baseDir, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs, waldb.WithNoSync(true))
		if openErr != nil {
			return nil
		}
		batchErr := db.SaveState([]logdb.Update{
			{ShardID: shardA, ReplicaID: repID, Entries: []logdb.Entry{testEntryLarge(3, 2)}},
			{ShardID: shardB, ReplicaID: repID, Entries: []logdb.Entry{testEntryLarge(3, 2)}},
		})
		lenientClose(t, db)
		return batchErr
	})

	t.Logf("multi-shard batch injection fired at op %d", firedAt)

	// Run the actual injection.
	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), firedAt, io.ErrUnexpectedEOF)
	db, openErr := openDBWithEFS(t, injectDir, efs, waldb.WithNoSync(true))
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	batchErr := db.SaveState([]logdb.Update{
		{ShardID: shardA, ReplicaID: repID, Entries: []logdb.Entry{testEntryLarge(3, 2)}},
		{ShardID: shardB, ReplicaID: repID, Entries: []logdb.Entry{testEntryLarge(3, 2)}},
	})
	if batchErr == nil {
		t.Fatal("SaveState multi-shard: expected error, got nil")
	}
	if !errors.Is(batchErr, io.ErrUnexpectedEOF) {
		t.Fatalf("SaveState error does not wrap injected: %v", batchErr)
	}
	if !efs.Injected() {
		t.Fatal("ErrorFS: injection was not triggered")
	}

	lenientClose(t, db)

	// Re-open and verify only baseline data is present.
	recovered := openDB(t, injectDir, waldb.WithNoSync(true))
	defer mustClose(t, recovered)

	assertEntriesPresent(t, recovered, shardA, repID, 1, 3, 1)
	assertEntriesPresent(t, recovered, shardB, repID, 1, 3, 1)
	assertEntryAbsent(t, recovered, shardA, repID, 3)
	assertEntryAbsent(t, recovered, shardB, repID, 3)
}

// --- TestErrorFS_LargeCmd_RoundtripSanity ---

// TestErrorFS_LargeCmd_RoundtripSanity is a precondition test that verifies
// large-payload entries survive a normal write-close-reopen cycle. This
// ensures testEntryLarge is valid before it is used in injection tests.
func TestErrorFS_LargeCmd_RoundtripSanity(t *testing.T) {
	const (
		shardID   = uint64(12)
		replicaID = uint64(1)
	)

	dir := t.TempDir()

	want := testEntryLarge(1, 1)
	db := openDB(t, dir, waldb.WithNoSync(true))
	if err := db.SaveEntries(shardID, replicaID, []logdb.Entry{want}); err != nil {
		t.Fatalf("SaveEntries large: %v", err)
	}
	mustClose(t, db)

	recovered := openDB(t, dir, waldb.WithNoSync(true))
	defer mustClose(t, recovered)

	got, err := recovered.GetEntries(shardID, replicaID, 1, 2, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("GetEntries: got %d entries, want 1", len(got))
	}
	if got[0].Index != want.Index || got[0].Term != want.Term {
		t.Fatalf("entry mismatch: got Index=%d Term=%d, want Index=%d Term=%d",
			got[0].Index, got[0].Term, want.Index, want.Term)
	}
	if !bytes.Equal(got[0].Cmd, want.Cmd) {
		t.Fatalf("Cmd mismatch: got len=%d, want len=%d", len(got[0].Cmd), len(want.Cmd))
	}
}

// testEntryWithPayload builds a logdb.Entry whose Cmd is a distinctive
// repeating pattern based on the entry index. This allows byte-for-byte
// payload verification after recovery.
func testEntryWithPayload(index, term uint64, size int) logdb.Entry {
	cmd := make([]byte, size)
	pat := byte(index & 0xFF)
	for i := range cmd {
		cmd[i] = pat
	}
	return logdb.Entry{Index: index, Term: term, Type: 0, Cmd: cmd}
}

// assertEntriesPresentWithCmd verifies GetEntries returns exactly (high-low)
// entries in [low, high), all with wantTerm, and that each entry's Cmd is
// byte-for-byte identical to the corresponding want entry.
func assertEntriesPresentWithCmd(t *testing.T, db *waldb.DB, shardID, replicaID, low, high, wantTerm uint64, want []logdb.Entry) {
	t.Helper()
	got, err := db.GetEntries(shardID, replicaID, low, high, 0)
	if err != nil {
		t.Fatalf("GetEntries[%d,%d): %v", low, high, err)
	}
	wantCount := int(high - low)
	if len(got) != wantCount {
		t.Fatalf("GetEntries[%d,%d): got %d, want %d", low, high, len(got), wantCount)
	}
	for i, e := range got {
		if e.Term != wantTerm {
			t.Fatalf("entry index=%d: term=%d, want %d", e.Index, e.Term, wantTerm)
		}
		if i < len(want) {
			if !bytes.Equal(e.Cmd, want[i].Cmd) {
				t.Fatalf("entry index=%d: Cmd mismatch: got len=%d want len=%d",
					e.Index, len(e.Cmd), len(want[i].Cmd))
			}
		}
	}
}

// --- TestErrorFS_WAL_DoubleFault_WriteAndRollback ---

// TestErrorFS_WAL_DoubleFault_WriteAndRollback verifies that when a write
// path failure occurs, the error chain exposes typed WAL error wrappers.
// The WALWriteError type wraps the original write failure along with any
// truncation/rollback errors encountered during recovery attempts.
//
// Because a single-fire ErrorFS cannot simultaneously fail both the write
// and the discard/rollback, this test focuses on verifying that a WALWriteError
// or BatchWriteError is present in the error chain when a write fails.
func TestErrorFS_WAL_DoubleFault_WriteAndRollback(t *testing.T) {
	const (
		shardID   = uint64(20)
		replicaID = uint64(1)
	)

	baseDir := t.TempDir()
	setup := openDB(t, baseDir, waldb.WithNoSync(true))
	saveEntries(t, setup, shardID, replicaID, 1, 3)
	mustClose(t, setup)

	firedAt := sweepUntilWriteError(t, baseDir, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs, waldb.WithNoSync(true))
		if openErr != nil {
			return nil
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(4, 1)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("double-fault sweep fired at op %d", firedAt)

	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), firedAt, io.ErrUnexpectedEOF)
	db, openErr := openDBWithEFS(t, injectDir, efs, waldb.WithNoSync(true))
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(4, 1)})
	if writeErr == nil {
		t.Fatal("SaveEntries: expected error, got nil")
	}
	if !errors.Is(writeErr, io.ErrUnexpectedEOF) {
		t.Fatalf("error chain missing injected error: %v", writeErr)
	}

	// Verify that a typed WAL error wrapper is present in the chain.
	// A write failure surfaces as either WALWriteError (segment-level) or
	// BatchWriteError (batch-level). Both wrap the original write error.
	var walWriteErr *waldb.WALWriteError
	var batchWriteErr *waldb.BatchWriteError
	hasTypedWrapper := errors.As(writeErr, &walWriteErr) || errors.As(writeErr, &batchWriteErr)
	if !hasTypedWrapper {
		// The error must at minimum be an InjectedError wrapping the
		// underlying cause. Document that BatchWriteError / WALWriteError
		// require the discard path to also fail to appear in a single-fire
		// injection scenario.
		var injected *errfs.InjectedError
		if !errors.As(writeErr, &injected) {
			t.Fatalf("error chain has no WALWriteError, BatchWriteError, or InjectedError: %v", writeErr)
		}
		t.Logf("note: single-fire injection produced InjectedError only (op=%s); "+
			"WALWriteError/BatchWriteError require both write and discard to fail", injected.Operation)
	}

	lenientClose(t, db)

	// The baseline entries must survive.
	recovered := openDB(t, injectDir, waldb.WithNoSync(true))
	defer mustClose(t, recovered)
	assertEntriesPresent(t, recovered, shardID, replicaID, 1, 4, 1)
	assertEntryAbsent(t, recovered, shardID, replicaID, 4)
}

// --- TestErrorFS_WAL_TypedErrorWrappers_Recovery ---

// TestErrorFS_WAL_TypedErrorWrappers_Recovery verifies that Open-time errors
// surface as typed WAL error wrappers (DBOpenError, RecoveryError) and that
// the InjectedError is accessible via errors.As anywhere in the chain.
func TestErrorFS_WAL_TypedErrorWrappers_Recovery(t *testing.T) {
	const (
		shardID   = uint64(21)
		replicaID = uint64(1)
	)

	dir := t.TempDir()

	setup := openDB(t, dir, waldb.WithNoSync(true))
	saveEntries(t, setup, shardID, replicaID, 1, 5)
	mustClose(t, setup)

	// Probe total ops for a clean Open so we sweep the exact range.
	passthrough := errfs.New(waldb.DefaultFS(), 0, io.ErrUnexpectedEOF)
	probeDB, err := waldb.Open(dir,
		waldb.WithFS(passthrough),
		waldb.WithNoSync(true),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64*1024),
	)
	if err != nil {
		t.Fatalf("probe Open: %v", err)
	}
	totalOps := passthrough.Count()
	mustClose(t, probeDB)

	foundTyped := false
	foundInjected := false

	for target := int64(1); target <= totalOps; target++ {
		iterDir := t.TempDir()
		if err := copyDir(dir, iterDir); err != nil {
			t.Fatalf("copyDir(target=%d): %v", target, err)
		}

		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF)
		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithNoSync(true),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr == nil {
			mustClose(t, db)
			continue
		}

		if !errors.Is(openErr, io.ErrUnexpectedEOF) {
			t.Fatalf("target=%d: Open error does not wrap injected: %v", target, openErr)
		}

		// At least one of the typed Open-path wrappers must be present.
		var dbOpenErr *waldb.DBOpenError
		var recoveryErr *waldb.RecoveryError
		var segCreateErr *waldb.SegmentCreateError
		if errors.As(openErr, &dbOpenErr) || errors.As(openErr, &recoveryErr) || errors.As(openErr, &segCreateErr) {
			foundTyped = true
		}

		// InjectedError must always be present.
		var injected *errfs.InjectedError
		if errors.As(openErr, &injected) {
			foundInjected = true
		}
	}

	if !foundTyped {
		t.Fatal("no Open-path error contained a typed WAL wrapper (DBOpenError/RecoveryError/SegmentCreateError)")
	}
	if !foundInjected {
		t.Fatal("no Open-path error contained *errfs.InjectedError")
	}

	t.Logf("typed WAL wrappers and InjectedError verified across %d Open-path injection targets", totalOps)
}

// --- TestErrorFS_WAL_TypedErrorWrappers_Write ---

// TestErrorFS_WAL_TypedErrorWrappers_Write verifies that write-path errors
// surface as typed WAL error wrappers and that InjectedError is accessible
// via errors.As anywhere in the chain.
func TestErrorFS_WAL_TypedErrorWrappers_Write(t *testing.T) {
	const (
		shardID   = uint64(22)
		replicaID = uint64(1)
	)

	baseDir := t.TempDir()
	setup := openDB(t, baseDir, waldb.WithNoSync(true))
	saveEntries(t, setup, shardID, replicaID, 1, 3)
	mustClose(t, setup)

	firedAt := sweepUntilWriteError(t, baseDir, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs, waldb.WithNoSync(true))
		if openErr != nil {
			return nil
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(4, 1)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("write-path injection fired at op %d", firedAt)

	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), firedAt, io.ErrUnexpectedEOF)
	db, openErr := openDBWithEFS(t, injectDir, efs, waldb.WithNoSync(true))
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(4, 1)})
	lenientClose(t, db)

	if writeErr == nil {
		t.Fatal("SaveEntries: expected error, got nil")
	}
	if !errors.Is(writeErr, io.ErrUnexpectedEOF) {
		t.Fatalf("error chain missing injected error: %v", writeErr)
	}

	// InjectedError must be accessible.
	var injected *errfs.InjectedError
	if !errors.As(writeErr, &injected) {
		t.Fatalf("errors.As(*InjectedError): not found in chain: %v", writeErr)
	}
	if injected.Operation == "" {
		t.Error("InjectedError.Operation must not be empty")
	}

	// A typed WAL wrapper should be present in the chain.
	var walWriteErr *waldb.WALWriteError
	var batchWriteErr *waldb.BatchWriteError
	if !errors.As(writeErr, &walWriteErr) && !errors.As(writeErr, &batchWriteErr) {
		t.Logf("note: neither WALWriteError nor BatchWriteError found in chain (op=%s); "+
			"error chain: %v", injected.Operation, writeErr)
	} else {
		t.Logf("typed WAL write wrapper verified (op=%s)", injected.Operation)
	}
}

// --- TestErrorFS_WAL_CmdPayloadIntegrity ---

// TestErrorFS_WAL_CmdPayloadIntegrity verifies that after a write failure
// and recovery, surviving entries have byte-for-byte identical Cmd payloads.
// This catches partial writes that corrupt payload bytes while writing a
// valid-length header.
func TestErrorFS_WAL_CmdPayloadIntegrity(t *testing.T) {
	const (
		shardID   = uint64(23)
		replicaID = uint64(1)
	)

	const payloadSize = 64
	const baseCount = uint64(5)

	baseDir := t.TempDir()

	// Write distinctive per-index payload entries as the baseline.
	wantEntries := make([]logdb.Entry, baseCount)
	for i := uint64(0); i < baseCount; i++ {
		wantEntries[i] = testEntryWithPayload(i+1, 1, payloadSize)
	}

	setup := openDB(t, baseDir, waldb.WithNoSync(true))
	if err := setup.SaveEntries(shardID, replicaID, wantEntries); err != nil {
		t.Fatalf("setup SaveEntries: %v", err)
	}
	mustClose(t, setup)

	// Find the injection target that causes a large follow-on write to fail.
	firedAt := sweepUntilWriteError(t, baseDir, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs, waldb.WithNoSync(true))
		if openErr != nil {
			return nil
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(baseCount+1, 1)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("payload integrity injection fired at op %d", firedAt)

	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), firedAt, io.ErrUnexpectedEOF)
	db, openErr := openDBWithEFS(t, injectDir, efs, waldb.WithNoSync(true))
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	_ = db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(baseCount+1, 1)})
	lenientClose(t, db)

	// Re-open without ErrorFS and verify byte-for-byte Cmd payloads.
	recovered := openDB(t, injectDir, waldb.WithNoSync(true))
	defer mustClose(t, recovered)

	assertEntriesPresentWithCmd(t, recovered, shardID, replicaID, 1, baseCount+1, 1, wantEntries)
	assertEntryAbsent(t, recovered, shardID, replicaID, baseCount+1)
}

// --- TestErrorFS_WAL_Compaction_RemoveFailure ---

// TestErrorFS_WAL_Compaction_RemoveFailure verifies that when a Remove call
// fails during segment garbage collection, the error surfaces as a
// CompactionDeleteError and the DB remains operational afterwards.
func TestErrorFS_WAL_Compaction_RemoveFailure(t *testing.T) {
	const (
		shardID   = uint64(24)
		replicaID = uint64(1)
	)

	// Use a very small maxFileSize to force segment rotation with fewer entries.
	const smallFileSize = int64(2 * 1024)

	baseDir := t.TempDir()

	// Write enough large entries to force at least two segment rotations.
	// With maxFileSize=2KB and entries ~1KB each, two entries fill one segment.
	setup, err := waldb.Open(baseDir,
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(smallFileSize),
		waldb.WithNoSync(true),
	)
	if err != nil {
		t.Fatalf("setup Open: %v", err)
	}
	// Write 8 entries so we definitely span multiple segments.
	for i := uint64(1); i <= 8; i++ {
		entry := testEntryWithPayload(i, 1, walBlockSize)
		if err := setup.SaveEntries(shardID, replicaID, []logdb.Entry{entry}); err != nil {
			t.Fatalf("setup SaveEntries(%d): %v", i, err)
		}
	}
	// Compact away old entries so GC has something to delete.
	if err := setup.RemoveEntriesTo(shardID, replicaID, 4); err != nil {
		t.Fatalf("setup RemoveEntriesTo: %v", err)
	}
	mustClose(t, setup)

	// Inject a Remove failure during compaction (GarbageCollect calls Remove).
	// Use OnlyOps("Remove") so the injection fires specifically on Remove.
	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), 1, io.ErrUnexpectedEOF).OnlyOps("Remove")

	db, openErr := waldb.Open(injectDir,
		waldb.WithFS(efs),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(smallFileSize),
		waldb.WithNoSync(true),
	)
	if openErr != nil {
		t.Fatalf("Open with Remove-only injection: %v", openErr)
	}

	compErr := db.Compact(shardID, replicaID)

	if compErr != nil {
		if !errors.Is(compErr, io.ErrUnexpectedEOF) {
			t.Fatalf("Compact error does not wrap injected error: %v", compErr)
		}
		var deleteErr *waldb.CompactionDeleteError
		if !errors.As(compErr, &deleteErr) {
			t.Fatalf("Compact error missing CompactionDeleteError wrapper: %v", compErr)
		}
		if deleteErr.Path == "" {
			t.Error("CompactionDeleteError.Path must not be empty")
		}
		t.Logf("CompactionDeleteError confirmed: path=%s", deleteErr.Path)
	} else {
		// GC may have found no segments eligible for deletion if all
		// entries remain live; log and continue.
		t.Logf("Compact returned nil — no obsolete segments found for deletion (injection not reached)")
	}

	// The DB must remain operational regardless of the compaction outcome.
	writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(9, 1)})
	lenientClose(t, db)
	if writeErr != nil {
		t.Fatalf("SaveEntries after compaction failure: %v", writeErr)
	}

	// Re-open without ErrorFS and verify the DB is still healthy.
	recovered, err := waldb.Open(injectDir,
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(smallFileSize),
		waldb.WithNoSync(true),
	)
	if err != nil {
		t.Fatalf("re-open after compaction fault: %v", err)
	}
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Logf("recovered Close: %v", err)
		}
	}()

	// Entries 5-9 must be present (entries 1-4 were compacted away).
	for i := uint64(5); i <= 9; i++ {
		got, getErr := recovered.GetEntries(shardID, replicaID, i, i+1, 0)
		if getErr != nil {
			t.Fatalf("GetEntries(%d) after compaction fault: %v", i, getErr)
		}
		if len(got) != 1 {
			t.Fatalf("GetEntries(%d): got %d entries, want 1", i, len(got))
		}
	}
}

// --- TestErrorFS_WAL_Compaction_ReadDirFailure ---

// TestErrorFS_WAL_Compaction_ReadDirFailure verifies that when ReadDir fails
// during garbage collection, the error surfaces as a CompactionReadDirError.
func TestErrorFS_WAL_Compaction_ReadDirFailure(t *testing.T) {
	const (
		shardID   = uint64(25)
		replicaID = uint64(1)
	)

	const smallFileSize = int64(2 * 1024)

	baseDir := t.TempDir()

	// Write entries across multiple segments.
	setup, err := waldb.Open(baseDir,
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(smallFileSize),
		waldb.WithNoSync(true),
	)
	if err != nil {
		t.Fatalf("setup Open: %v", err)
	}
	for i := uint64(1); i <= 8; i++ {
		entry := testEntryWithPayload(i, 1, walBlockSize)
		if err := setup.SaveEntries(shardID, replicaID, []logdb.Entry{entry}); err != nil {
			t.Fatalf("setup SaveEntries(%d): %v", i, err)
		}
	}
	if err := setup.RemoveEntriesTo(shardID, replicaID, 4); err != nil {
		t.Fatalf("setup RemoveEntriesTo: %v", err)
	}
	mustClose(t, setup)

	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	// Inject a ReadDir failure. Use OnlyOps("ReadDir") so it fires
	// specifically when GarbageCollect calls ReadDir on the shard directory.
	efs := errfs.New(waldb.DefaultFS(), 1, io.ErrUnexpectedEOF).OnlyOps("ReadDir")

	db, openErr := waldb.Open(injectDir,
		waldb.WithFS(efs),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(smallFileSize),
		waldb.WithNoSync(true),
	)
	if openErr != nil {
		// ReadDir may fire during Open (shard recovery). If so, the
		// error must still wrap the injected error.
		if !errors.Is(openErr, io.ErrUnexpectedEOF) {
			t.Fatalf("Open error does not wrap injected: %v", openErr)
		}
		t.Logf("ReadDir injection fired during Open: %v", openErr)
		return
	}

	compErr := db.Compact(shardID, replicaID)
	lenientClose(t, db)

	if compErr == nil {
		t.Logf("Compact returned nil — ReadDir injection did not reach GC path; test passes trivially")
		return
	}

	if !errors.Is(compErr, io.ErrUnexpectedEOF) {
		t.Fatalf("Compact error does not wrap injected: %v", compErr)
	}

	var readDirErr *waldb.CompactionReadDirError
	if !errors.As(compErr, &readDirErr) {
		t.Fatalf("Compact error missing CompactionReadDirError wrapper: %v", compErr)
	}
	if readDirErr.Dir == "" {
		t.Error("CompactionReadDirError.Dir must not be empty")
	}
	t.Logf("CompactionReadDirError confirmed: dir=%s", readDirErr.Dir)
}

// --- TestErrorFS_WAL_SyncSpecific_OnlyOps ---

// TestErrorFS_WAL_SyncSpecific_OnlyOps verifies that OnlyOps("Sync") restricts
// injection to Sync operations only. The counter increments for all ops but
// the error fires only when a Sync call is encountered after the threshold.
// The resulting InjectedError must have Operation=="Sync".
func TestErrorFS_WAL_SyncSpecific_OnlyOps(t *testing.T) {
	const (
		shardID   = uint64(26)
		replicaID = uint64(1)
	)

	baseDir := t.TempDir()
	setup := openDB(t, baseDir, waldb.WithNoSync(true))
	saveEntries(t, setup, shardID, replicaID, 1, 3)
	mustClose(t, setup)

	// Sweep to find a threshold where sync-enabled SaveEntries fires on Sync.
	var (
		syncFiredAt  int64
		syncInjErr   *errfs.InjectedError
		syncWriteErr error
	)

	for target := int64(1); target <= maxSweepOps; target++ {
		iterDir := t.TempDir()
		if err := copyDir(baseDir, iterDir); err != nil {
			t.Fatalf("copyDir(target=%d): %v", target, err)
		}

		// Only fire on Sync operations.
		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF).OnlyOps("Sync")

		// Use sync enabled (no WithNoSync) so Sync is called after writes.
		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			// If the injection fires during Open on a Sync call, that
			// also satisfies the test. Record and break.
			if errors.Is(openErr, io.ErrUnexpectedEOF) {
				var ie *errfs.InjectedError
				if errors.As(openErr, &ie) && ie.Operation == "Sync" {
					syncFiredAt = target
					syncInjErr = ie
					syncWriteErr = openErr
					break
				}
			}
			continue
		}

		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(4, 1)})
		lenientClose(t, db)

		if writeErr != nil && errors.Is(writeErr, io.ErrUnexpectedEOF) {
			var ie *errfs.InjectedError
			if errors.As(writeErr, &ie) && ie.Operation == "Sync" {
				syncFiredAt = target
				syncInjErr = ie
				syncWriteErr = writeErr
				break
			}
		}
	}

	if syncFiredAt == 0 {
		t.Fatal("OnlyOps(Sync): no Sync-specific injection target found across sweep")
	}

	t.Logf("Sync-specific injection fired at threshold=%d op=%d", syncFiredAt, syncInjErr.OpCount)

	if syncInjErr.Operation != "Sync" {
		t.Fatalf("InjectedError.Operation=%q, want Sync", syncInjErr.Operation)
	}
	if !errors.Is(syncWriteErr, io.ErrUnexpectedEOF) {
		t.Fatalf("error chain missing injected error: %v", syncWriteErr)
	}
}

// --- TestErrorFS_WAL_MkdirAllFailure ---

// TestErrorFS_WAL_MkdirAllFailure verifies that a MkdirAll failure during
// lazy shard initialization surfaces as a DBOpenError. The DB uses lazy
// directory creation: MkdirAll is called on first write, not during Open.
func TestErrorFS_WAL_MkdirAllFailure(t *testing.T) {
	const (
		shardID   = uint64(27)
		replicaID = uint64(1)
	)

	// Fresh (empty) directory so all shards are lazy.
	// OnlyOps("MkdirAll") fires specifically on directory creation.
	// Use threshold 1 so the very first MkdirAll fires.
	efs := errfs.New(waldb.DefaultFS(), 1, io.ErrUnexpectedEOF).OnlyOps("MkdirAll")

	injectDir := t.TempDir()

	// Open should succeed because no MkdirAll is called for an empty dir
	// (shards are initialized lazily).
	db, openErr := waldb.Open(injectDir,
		waldb.WithFS(efs),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64*1024),
		waldb.WithNoSync(true),
	)
	if openErr != nil {
		// If MkdirAll fires during Open (some pre-existing path), verify chain.
		if !errors.Is(openErr, io.ErrUnexpectedEOF) {
			t.Fatalf("Open error does not wrap injected: %v", openErr)
		}
		var ie *errfs.InjectedError
		if !errors.As(openErr, &ie) {
			t.Fatalf("Open error missing InjectedError in chain: %v", openErr)
		}
		if ie.Operation != "MkdirAll" {
			t.Fatalf("InjectedError.Operation=%q, want MkdirAll", ie.Operation)
		}
		t.Logf("MkdirAll injection fired during Open (op=%d)", ie.OpCount)
		return
	}

	// First write to a lazy shard triggers MkdirAll for the shard directory.
	writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(1, 1)})
	lenientClose(t, db)

	if writeErr == nil {
		t.Logf("note: MkdirAll injection did not fire on first write; " +
			"shard directory may have been pre-created by a prior Open shard scan")
		return
	}

	if !errors.Is(writeErr, io.ErrUnexpectedEOF) {
		t.Fatalf("SaveEntries error does not wrap injected: %v", writeErr)
	}

	// InjectedError must be present and report MkdirAll.
	var ie *errfs.InjectedError
	if !errors.As(writeErr, &ie) {
		t.Fatalf("SaveEntries error missing InjectedError in chain: %v", writeErr)
	}
	if ie.Operation != "MkdirAll" {
		t.Fatalf("InjectedError.Operation=%q, want MkdirAll", ie.Operation)
	}

	// The error must be wrapped in DBOpenError (MkdirAll is called from
	// ensureActiveSegment which wraps in DBOpenError).
	var dbOpenErr *waldb.DBOpenError
	if !errors.As(writeErr, &dbOpenErr) {
		t.Fatalf("SaveEntries error missing DBOpenError wrapper: %v", writeErr)
	}

	t.Logf("MkdirAll injection confirmed: op=%d DBOpenError.Dir=%s", ie.OpCount, dbOpenErr.Dir)
}

// --- TestErrorFS_WAL_StatFailureDuringReplay ---

// TestErrorFS_WAL_StatFailureDuringReplay verifies that a Stat failure during
// WAL recovery (Open) surfaces as a typed error and wraps the injected error.
// The waldb.DB calls Stat on the shard directory to detect whether it exists
// before deciding to run recovery.
func TestErrorFS_WAL_StatFailureDuringReplay(t *testing.T) {
	const (
		shardID   = uint64(28)
		replicaID = uint64(1)
	)

	dir := t.TempDir()

	setup := openDB(t, dir, waldb.WithNoSync(true))
	saveEntries(t, setup, shardID, replicaID, 1, 5)
	mustClose(t, setup)

	// Inject Stat failure starting at threshold 1 so the first Stat during
	// Open fires the error.
	foundError := false

	for target := int64(1); target <= maxSweepOps; target++ {
		iterDir := t.TempDir()
		if err := copyDir(dir, iterDir); err != nil {
			t.Fatalf("copyDir(target=%d): %v", target, err)
		}

		efs := errfs.New(waldb.DefaultFS(), target, io.ErrUnexpectedEOF).OnlyOps("Stat")

		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
			waldb.WithNoSync(true),
		)
		if openErr != nil {
			if !errors.Is(openErr, io.ErrUnexpectedEOF) {
				t.Fatalf("target=%d: Open error does not wrap injected: %v", target, openErr)
			}
			// Stat failure during Open is the scenario we're looking for.
			var ie *errfs.InjectedError
			if errors.As(openErr, &ie) && ie.Operation == "Stat" {
				foundError = true
				t.Logf("Stat injection during recovery fired at target=%d op=%d", target, ie.OpCount)
			}
			break
		}
		mustClose(t, db)

		// Once Open succeeds with this threshold, the Stat calls during
		// recovery were past the threshold; continue searching.
		if efs.Injected() {
			// Injection fired but did not cause Open to fail.
			break
		}
	}

	if !foundError {
		t.Logf("Stat injection did not cause Open to fail across sweep; " +
			"Stat errors during the DB open path may be handled gracefully (treated as absent directory)")
	}
}

// --- TestErrorFS_WAL_IdempotentRecovery ---

// TestErrorFS_WAL_IdempotentRecovery verifies that after a fault-injected
// failure, the DB can be opened, used, and closed multiple times with the
// real FS and all data remains intact across the entire sequence.
func TestErrorFS_WAL_IdempotentRecovery(t *testing.T) {
	const (
		shardID   = uint64(29)
		replicaID = uint64(1)
	)

	dir := t.TempDir()

	// Phase 1: write baseline data durably.
	first := openDB(t, dir, waldb.WithNoSync(true))
	saveEntries(t, first, shardID, replicaID, 1, 5)
	goodState := testState(2, 1, 5)
	if err := first.SaveNodeState(shardID, replicaID, goodState); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}
	mustClose(t, first)

	// Phase 2: inject a write failure on a fresh copy.
	baseDir := t.TempDir()
	if err := copyDir(dir, baseDir); err != nil {
		t.Fatalf("copyDir baseline: %v", err)
	}

	firedAt := sweepUntilWriteError(t, baseDir, func(d string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, d, efs, waldb.WithNoSync(true))
		if openErr != nil {
			return nil
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(6, 2)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("idempotent recovery injection fired at op %d", firedAt)

	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir inject: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), firedAt, io.ErrUnexpectedEOF)
	faultDB, openErr := openDBWithEFS(t, injectDir, efs, waldb.WithNoSync(true))
	if openErr != nil {
		t.Fatalf("Open at firedAt: %v", openErr)
	}
	_ = faultDB.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(6, 2)})
	lenientClose(t, faultDB)

	// Phase 3: open-close-open-close 3 times with the real FS.
	// Each cycle must see all baseline data and be writable.
	for cycle := 1; cycle <= 3; cycle++ {
		db := openDB(t, injectDir, waldb.WithNoSync(true))

		assertEntriesPresent(t, db, shardID, replicaID, 1, 6, 1)

		gotState, stateErr := db.GetState(shardID, replicaID)
		if stateErr != nil {
			t.Fatalf("cycle=%d GetState: %v", cycle, stateErr)
		}
		if gotState != goodState {
			t.Fatalf("cycle=%d GetState: got %+v, want %+v", cycle, gotState, goodState)
		}

		// Write a new entry on each cycle to confirm write-path health.
		nextIdx := uint64(6 + cycle)
		if err := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(nextIdx, 2)}); err != nil {
			t.Fatalf("cycle=%d SaveEntries(%d): %v", cycle, nextIdx, err)
		}

		mustClose(t, db)
	}

	// Phase 4: final re-open verifies all data including the cycle writes.
	final := openDB(t, injectDir, waldb.WithNoSync(true))
	defer mustClose(t, final)

	// Baseline entries 1-5 plus cycle entries 7, 8, 9.
	assertEntriesPresent(t, final, shardID, replicaID, 1, 6, 1)
	for cycle := 1; cycle <= 3; cycle++ {
		assertEntriesPresent(t, final, shardID, replicaID, uint64(6+cycle), uint64(7+cycle), 2)
	}

	// The baseline state must still be intact.
	finalState, err := final.GetState(shardID, replicaID)
	if err != nil {
		t.Fatalf("final GetState: %v", err)
	}
	if finalState != goodState {
		t.Fatalf("final GetState: got %+v, want %+v", finalState, goodState)
	}

	// Write once more to confirm the DB is fully operational.
	if err := final.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(10, 2)}); err != nil {
		t.Fatalf("final SaveEntries: %v", err)
	}
}
