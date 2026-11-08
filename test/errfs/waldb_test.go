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
//   - sweepUntilOpError finds the first injectAt that causes a fault on one of
//     the operations the test NAMES, and returns the operation it fired on so
//     the test can assert it. An unconstrained sweep would settle on whichever
//     operation happened to fail first and prove nothing about the path under
//     test.
//
// The tests use the quicraft_errfs build tag so they are excluded from the
// default test run and can be targeted explicitly with:
//
//	go test -tags quicraft_errfs ./test/errfs/...
package errfs_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/test/errfs"
)

// errInjectedIO is the failure every injection in this suite reports.
//
// It is deliberately NOT io.ErrUnexpectedEOF, which this suite injected until
// three of its tests started failing on it. A short read is a legitimate,
// non-error outcome for several WAL read paths: readWALFormat reads the
// geometry marker with io.ReadFull and treats an EOF-family error as "these
// bytes are not a marker", i.e. absent, because a zero-length marker is the
// residue of a stamp that never delivered its content and must be re-stamped
// rather than refused. Injecting an error that unwraps to io.ErrUnexpectedEOF
// therefore hands production a value it is entitled to read as DATA, and the
// injection is silently absorbed — the three tests saw ErrWALFormatMissing
// instead of the fault they armed.
//
// A real filesystem reports EIO or ENOSPC here, and no waldb path treats those
// as content. errInjectedIO models that: an error value with no meaning to any
// production classifier, so an assertion that it reached the caller is an
// assertion about error propagation and nothing else.
var errInjectedIO = errors.New("errfs_test: injected I/O failure")

// walBlockSize is the WAL block size used throughout these tests.
const walBlockSize = 1024

// largeCmdSize is the Cmd payload for entries that must overflow the block
// buffer during segment.Write. A Cmd of this size causes padAndFlushBlock to
// be called, triggering File.Write inline during SaveEntries — independent of
// whether sync is enabled. With blockSize=1024 and recordHeaderSize=7, a Cmd
// of 1088 bytes spans at least two blocks.
const largeCmdSize = walBlockSize + 64

// compactionEntryPayload is the Cmd size the compaction fault tests give each
// entry they write into a smallFileSize (2 KiB) segment.
//
// A WAL record must fit a segment WHOLE, and the largest payload waldb accepts
// is (segmentBlocks-1) * (blockSize-recordHeaderSize) — one block is reserved
// for the one a record may have to pad past to start. With a 2 KiB segment and
// 1 KiB blocks that ceiling is 1013 bytes, and the record spends a further 77
// on the payload-type byte and the per-entry header. A Cmd of walBlockSize
// therefore no longer fits in ANY 2 KiB segment and SaveEntries rejects it
// permanently with ErrRecordTooLarge, which is what these two tests were
// failing on in setup: the ceiling moved when the record header grew and the
// batch splitter started pre-checking it, and the fixture did not follow.
//
// 768 bytes sits under the ceiling with margin and still costs 856 framed
// bytes, so two entries fill a segment and the eight these tests write span
// four of them — enough for compaction to have segments to delete.
const compactionEntryPayload = 768

// maxSweepOps is the maximum number of injection targets to try in a sweep
// before declaring failure. It is intentionally generous to handle machines
// with many open-phase ops.
//
// It has to cover the PRODUCTION operation stream. Every phase here runs with
// fsync enabled, so the counter also advances through the segment flushes and directory metadata
// syncs that WithNoSync skipped entirely — and under WithNoSync no injection
// target could ever land on a flush at all, which made the whole fsync surface
// unreachable from a suite whose job is to fault every filesystem call the WAL
// makes.
const maxSweepOps = 900

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

// walWriteOps are the operations a WAL record write actually issues. Naming
// them is what turns a sweep into a test of the write path rather than a test
// that SOMETHING under the filesystem can fail.
var walWriteOps = []string{errfs.OpWriteAt, errfs.OpWrite}

// walSyncOps is the flush an acknowledgement rests on.
var walSyncOps = []string{errfs.OpSync}

// sweepUntilOpError tries injectAt values 1, 2, … until writeFn returns an
// error wrapping errInjectedIO, with the fault CONSTRAINED to ops. It returns
// the injectAt that fired and the operation it fired on. Each attempt runs
// against a fresh copy of srcDir so the operation counter is reproducible.
//
// The constraint is the whole point, and it replaces an unconstrained sweep
// that was self-calibrating in the worst way: it walked the counter until
// ANYTHING failed, so a test named for a failing write settled just as happily
// on an OpenFile, a PreAllocate or a Stat, and then asserted only that some
// injected error had come back. Such a test passes for any implementation that
// can fail at all — including one in which the write path it names no longer
// exists. Pinning the sweep to the operations the test is about, and returning
// the one that fired so the caller can assert it, is what makes the result a
// statement about that path.
func sweepUntilOpError(
	t *testing.T,
	srcDir string,
	ops []string,
	writeFn func(dir string, efs *errfs.ErrorFS) error,
) (int64, string) {
	t.Helper()
	for target := int64(1); target <= maxSweepOps; target++ {
		iterDir := t.TempDir()
		if err := copyDir(srcDir, iterDir); err != nil {
			t.Fatalf("copyDir(target=%d): %v", target, err)
		}
		err := writeFn(iterDir, injectOnOps(target, ops))
		if err == nil || !errors.Is(err, errInjectedIO) {
			continue
		}
		var ie *errfs.InjectedError
		if !errors.As(err, &ie) {
			t.Fatalf("sweepUntilOpError: the error wraps errInjectedIO but carries "+
				"no *errfs.InjectedError, so the operation that failed cannot be "+
				"identified: %v", err)
		}
		if !slices.Contains(ops, ie.Operation) {
			t.Fatalf("sweepUntilOpError: the fault fired on %q, which is not one of "+
				"%v; the OnlyOps filter did not hold", ie.Operation, ops)
		}
		return target, ie.Operation
	}
	t.Fatalf("sweepUntilOpError: no %v failure surfaced across %d injection "+
		"attempts; the path under test never issues one of those operations",
		ops, maxSweepOps)
	return 0, ""
}

// injectOnOps builds an ErrorFS that fires at target, on ops only.
//
// The injection phase MUST carry the same filter the sweep used. Without it the
// counter lands on whatever operation happens to sit at that ordinal, which is
// not necessarily the one the sweep found — OnlyOps defers the fault to the
// next matching operation, so the two ordinals do not correspond.
func injectOnOps(target int64, ops []string) *errfs.ErrorFS {
	return errfs.New(waldb.DefaultFS(), target, errInjectedIO).OnlyOps(ops...)
}

// assertInjectedOn asserts that err carries an InjectedError raised on one of
// ops, so a test cannot pass on a fault from a path it is not about.
func assertInjectedOn(t *testing.T, err error, ops []string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an injected %v failure, got nil", ops)
	}
	var ie *errfs.InjectedError
	if !errors.As(err, &ie) {
		t.Fatalf("error chain carries no *errfs.InjectedError: %v", err)
	}
	if !slices.Contains(ops, ie.Operation) {
		t.Fatalf("the fault fired on %q, not on one of %v: this test's subject is "+
			"that path, and a failure from anywhere else says nothing about it",
			ie.Operation, ops)
	}
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
	setup := openDB(t, baseDir)
	saveEntries(t, setup, shardID, replicaID, 1, 3)
	mustClose(t, setup)

	// Find the injection target that causes SaveEntries to fail.
	firedAt, firedOp := sweepUntilOpError(t, baseDir, walWriteOps, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs)
		if openErr != nil {
			return nil // injection fired during Open; skip
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(4, 1)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("write injection fired at op %d on %s", firedAt, firedOp)

	// Run the actual injection against a fresh copy of the baseline.
	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := injectOnOps(firedAt, walWriteOps)
	db, openErr := openDBWithEFS(t, injectDir, efs)
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(4, 1)})
	if writeErr == nil {
		t.Fatal("SaveEntries: expected error, got nil")
	}
	if !errors.Is(writeErr, errInjectedIO) {
		t.Fatalf("SaveEntries error chain does not contain injected error: %v", writeErr)
	}
	assertInjectedOn(t, writeErr, walWriteOps)
	if !efs.Injected() {
		t.Fatal("ErrorFS: injection was not triggered")
	}

	lenientClose(t, db)

	// Re-open injectDir WITHOUT ErrorFS and verify data integrity.
	recovered := openDB(t, injectDir)
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

// TestErrorFS_WAL_SyncFailure verifies that when the fdatasync an
// acknowledgement rests on fails, the error propagates to the caller and
// previously-committed entries survive recovery.
//
// The injection is constrained to File.Sync. Unconstrained, the sweep would
// settle on whatever operation failed first — routinely an OpenFile or a
// WriteAt — so a test named for a sync failure would never have to reach one.
// Every phase runs with fsync enabled, as the whole suite does: under
// WithNoSync segment.Sync returns before touching the file, and there is no
// fsync left to fail.
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
	firedAt, firedOp := sweepUntilOpError(t, baseDir, walSyncOps, func(dir string, efs *errfs.ErrorFS) error {
		// WithNoSync NOT set: sync is enabled, so an fsync actually happens.
		db, openErr := openDBWithEFS(t, dir, efs)
		if openErr != nil {
			return nil
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(4, 1)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("sync injection fired at op %d on %s", firedAt, firedOp)

	// Inject at the discovered target and verify the error surfaces.
	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := injectOnOps(firedAt, walSyncOps)
	db, openErr := openDBWithEFS(t, injectDir, efs)
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	syncErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(4, 1)})
	if syncErr == nil {
		t.Fatal("SaveEntries: expected error, got nil")
	}
	if !errors.Is(syncErr, errInjectedIO) {
		t.Fatalf("SaveEntries error does not wrap injected error: %v", syncErr)
	}
	assertInjectedOn(t, syncErr, walSyncOps)

	lenientClose(t, db)

	// Re-open without ErrorFS — must recover cleanly.
	recovered := openDB(t, injectDir)
	defer mustClose(t, recovered)

	// The 3 baseline entries must survive.
	assertEntriesPresent(t, recovered, shardID, replicaID, 1, 4, 1)
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
	setup := openDB(t, baseDir)
	goodState := testState(2, 1, 3)
	if err := setup.SaveNodeState(shardID, replicaID, goodState); err != nil {
		t.Fatalf("setup SaveNodeState: %v", err)
	}
	saveEntries(t, setup, shardID, replicaID, 1, 2)
	mustClose(t, setup)

	// Find the injection target for the SaveState write path.
	firedAt, firedOp := sweepUntilOpError(t, baseDir, walWriteOps, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs)
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

	t.Logf("SaveState write injection fired at op %d on %s", firedAt, firedOp)

	// Execute injection and verify the error.
	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := injectOnOps(firedAt, walWriteOps)
	db, openErr := openDBWithEFS(t, injectDir, efs)
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
	if !errors.Is(writeErr, errInjectedIO) {
		t.Fatalf("SaveState error chain does not contain injected error: %v", writeErr)
	}
	assertInjectedOn(t, writeErr, walWriteOps)
	if !efs.Injected() {
		t.Fatal("ErrorFS: injection was not triggered")
	}

	lenientClose(t, db)

	// Re-open without ErrorFS and verify baseline data is intact.
	recovered := openDB(t, injectDir)
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
	first := openDB(t, dir)
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
		passthrough := errfs.New(waldb.DefaultFS(), 0, errInjectedIO)
		probeDB, err := waldb.Open(dir,
			waldb.WithFS(passthrough),
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

		efs := errfs.New(waldb.DefaultFS(), target, errInjectedIO)
		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			continue
		}
		_ = db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(6, 2)})
		lenientClose(t, db)

		// After each injection, verify the original dir is still recoverable.
		check := openDB(t, iterDir)
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
	recovered := openDB(t, dir)
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
	passthrough := errfs.New(waldb.DefaultFS(), 0, errInjectedIO)
	probeDB, openErr := waldb.Open(baseDir,
		waldb.WithFS(passthrough),
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
		efs := errfs.New(waldb.DefaultFS(), target, errInjectedIO)

		db, openErr := waldb.Open(injectDir,
			waldb.WithFS(efs),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			if !errors.Is(openErr, errInjectedIO) {
				t.Fatalf("target=%d: Open error does not wrap injected: %v", target, openErr)
			}
			foundError = true
			continue
		}

		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntry(1, 1)})
		lenientClose(t, db)

		if writeErr != nil {
			if !errors.Is(writeErr, errInjectedIO) {
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
		efs := errfs.New(waldb.DefaultFS(), target, errInjectedIO)
		db, openErr := waldb.Open(dir,
			waldb.WithFS(efs),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			continue
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(1, 1)})
		lenientClose(t, db)

		if writeErr != nil && errors.Is(writeErr, errInjectedIO) {
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
	firstFiredEFS.Reset(0, errInjectedIO)

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
		firstFiredEFS.Reset(target, errInjectedIO)

		db2, openErr2 := waldb.Open(dir2,
			waldb.WithFS(firstFiredEFS),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr2 != nil {
			// Injection fired during Open (non-critical-path check OK too).
			if errors.Is(openErr2, errInjectedIO) {
				secondFired = true
				break
			}
			continue
		}

		secondErr := db2.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(1, 1)})
		lenientClose(t, db2)

		if secondErr != nil && errors.Is(secondErr, errInjectedIO) {
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
	setup := openDB(t, baseDir)
	saveEntries(t, setup, shardA, repID, 1, 2)
	saveEntries(t, setup, shardB, repID, 1, 2)
	mustClose(t, setup)

	// Find the injection target that causes the multi-shard batch to fail.
	firedAt, firedOp := sweepUntilOpError(t, baseDir, walWriteOps, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs)
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

	t.Logf("multi-shard batch injection fired at op %d on %s", firedAt, firedOp)

	// Run the actual injection.
	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := injectOnOps(firedAt, walWriteOps)
	db, openErr := openDBWithEFS(t, injectDir, efs)
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
	if !errors.Is(batchErr, errInjectedIO) {
		t.Fatalf("SaveState error does not wrap injected: %v", batchErr)
	}
	assertInjectedOn(t, batchErr, walWriteOps)
	if !efs.Injected() {
		t.Fatal("ErrorFS: injection was not triggered")
	}

	lenientClose(t, db)

	// Re-open and verify only baseline data is present.
	recovered := openDB(t, injectDir)
	defer mustClose(t, recovered)

	assertEntriesPresent(t, recovered, shardA, repID, 1, 3, 1)
	assertEntriesPresent(t, recovered, shardB, repID, 1, 3, 1)
	assertEntryAbsent(t, recovered, shardA, repID, 3)
	assertEntryAbsent(t, recovered, shardB, repID, 3)
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

	setup := openDB(t, dir)
	saveEntries(t, setup, shardID, replicaID, 1, 5)
	mustClose(t, setup)

	// Probe total ops for a clean Open so we sweep the exact range.
	passthrough := errfs.New(waldb.DefaultFS(), 0, errInjectedIO)
	probeDB, err := waldb.Open(dir,
		waldb.WithFS(passthrough),
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

		efs := errfs.New(waldb.DefaultFS(), target, errInjectedIO)
		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr == nil {
			mustClose(t, db)
			continue
		}

		if !errors.Is(openErr, errInjectedIO) {
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

	setup := openDB(t, baseDir)
	if err := setup.SaveEntries(shardID, replicaID, wantEntries); err != nil {
		t.Fatalf("setup SaveEntries: %v", err)
	}
	mustClose(t, setup)

	// Find the injection target that causes a large follow-on write to fail.
	firedAt, firedOp := sweepUntilOpError(t, baseDir, walWriteOps, func(dir string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, dir, efs)
		if openErr != nil {
			return nil
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(baseCount+1, 1)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("payload integrity injection fired at op %d on %s", firedAt, firedOp)

	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir: %v", err)
	}

	efs := injectOnOps(firedAt, walWriteOps)
	db, openErr := openDBWithEFS(t, injectDir, efs)
	if openErr != nil {
		t.Fatalf("Open at fired injectAt: %v", openErr)
	}

	assertInjectedOn(t,
		db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(baseCount+1, 1)}),
		walWriteOps)
	lenientClose(t, db)

	// Re-open without ErrorFS and verify byte-for-byte Cmd payloads.
	recovered := openDB(t, injectDir)
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
	// With maxFileSize=2KB and compactionEntryPayload-sized entries, two
	// entries fill one segment.
	setup, err := waldb.Open(baseDir,
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(smallFileSize),
	)
	if err != nil {
		t.Fatalf("setup Open: %v", err)
	}
	// Write 8 entries so we definitely span multiple segments.
	for i := uint64(1); i <= 8; i++ {
		entry := testEntryWithPayload(i, 1, compactionEntryPayload)
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

	// Open with the injection DISARMED and arm it afterwards. Armed from op 1
	// the fault lands on whichever Remove the open path happens to make first,
	// which is not the call under test and leaves the assertion below
	// unreached; Reset after Open puts the next Remove — GarbageCollect's —
	// first in line.
	efs := errfs.New(waldb.DefaultFS(), 0, errInjectedIO).OnlyOps(errfs.OpRemove)

	db, openErr := waldb.Open(injectDir,
		waldb.WithFS(efs),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(smallFileSize),
	)
	if openErr != nil {
		t.Fatalf("Open with Remove-only injection: %v", openErr)
	}

	efs.Reset(1, errInjectedIO)

	compErr := db.Compact(shardID, replicaID)

	// The compaction floor was advanced to 4 before this DB was opened, so the
	// segments holding entries 1-3 are dead and GarbageCollect must unlink
	// them. A nil error here means no Remove was attempted at all, which is a
	// failure of the test's premise, not a tolerable outcome.
	if compErr == nil {
		t.Fatalf("Compact returned nil with a Remove fault armed: GC unlinked no segment, so the delete-error path was never exercised (fires=%d)", efs.Fires())
	}
	if !errors.Is(compErr, errInjectedIO) {
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
	)
	if err != nil {
		t.Fatalf("setup Open: %v", err)
	}
	for i := uint64(1); i <= 8; i++ {
		entry := testEntryWithPayload(i, 1, compactionEntryPayload)
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

	// Open with the injection DISARMED and arm it afterwards. Open reads every
	// shard directory, so a fault armed from op 1 fires there and the test
	// returns before Compact — "passing" without ever reaching the garbage
	// collector it names.
	efs := errfs.New(waldb.DefaultFS(), 0, errInjectedIO).OnlyOps(errfs.OpReadDir)

	db, openErr := waldb.Open(injectDir,
		waldb.WithFS(efs),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(smallFileSize),
	)
	if openErr != nil {
		t.Fatalf("Open with the ReadDir fault disarmed: %v", openErr)
	}

	efs.Reset(1, errInjectedIO)

	compErr := db.Compact(shardID, replicaID)
	lenientClose(t, db)

	// GarbageCollect lists the shard directory before it deletes anything, so
	// the armed fault must reach it. A nil error means it did not, and the
	// wrapper assertions below would never run.
	if compErr == nil {
		t.Fatalf("Compact returned nil with a ReadDir fault armed: GC never listed a shard directory (fires=%d)", efs.Fires())
	}

	if !errors.Is(compErr, errInjectedIO) {
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
	efs := errfs.New(waldb.DefaultFS(), 1, errInjectedIO).OnlyOps("MkdirAll")

	injectDir := t.TempDir()

	// Open should succeed because no MkdirAll is called for an empty dir
	// (shards are initialized lazily).
	db, openErr := waldb.Open(injectDir,
		waldb.WithFS(efs),
		waldb.WithBlockSize(walBlockSize),
		waldb.WithMaxFileSize(64*1024),
	)
	if openErr != nil {
		// If MkdirAll fires during Open (some pre-existing path), verify chain.
		if !errors.Is(openErr, errInjectedIO) {
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

	if !errors.Is(writeErr, errInjectedIO) {
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

	setup := openDB(t, dir)
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

		// OpStat is the stat of a PATH, which is the call this test is about;
		// the fstat of an open handle is OpFileStat and is not armed here.
		efs := errfs.New(waldb.DefaultFS(), target, errInjectedIO).OnlyOps(errfs.OpStat)

		db, openErr := waldb.Open(iterDir,
			waldb.WithFS(efs),
			waldb.WithBlockSize(walBlockSize),
			waldb.WithMaxFileSize(64*1024),
		)
		if openErr != nil {
			if !errors.Is(openErr, errInjectedIO) {
				t.Fatalf("target=%d: Open error does not wrap injected: %v", target, openErr)
			}
			// Stat failure during Open is the scenario we're looking for.
			var ie *errfs.InjectedError
			if errors.As(openErr, &ie) && ie.Operation == errfs.OpStat {
				foundError = true
				t.Logf("Stat injection during recovery fired at target=%d op=%d", target, ie.OpCount)
			}
			break
		}
		mustClose(t, db)

		// Open survived this target. That is NOT a reason to stop: the open
		// path stats several different things — the WAL root, the format
		// marker, each shard directory — and only some of them are load-bearing.
		// Breaking the moment an injection fired without failing the open
		// would give up at whichever tolerated stat came first and never reach
		// the ones this test is about — and with fsync enabled, which changes
		// which stat the counter lands on first, the sweep would quit at target
		// 1 having proved nothing.
		//
		// Not firing at all is the real stop condition: it means the open path
		// has no stats left above this target.
		if !efs.Injected() {
			break
		}
	}

	// Open stats the WAL root and each shard directory to decide whether it
	// exists, and mistaking an unreadable directory for an absent one is how a
	// populated WAL gets stamped with the wrong format. The failure must
	// therefore reach the caller from at least one target in the sweep; a
	// sweep that finds none means those stats are being swallowed, which is
	// the defect this test exists to catch and not an acceptable outcome to
	// log and pass over.
	if !foundError {
		t.Fatalf("no Stat injection target in %d attempts made Open fail: path-stat errors "+
			"during the open path are being read as an absent directory", maxSweepOps)
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
	first := openDB(t, dir)
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

	firedAt, firedOp := sweepUntilOpError(t, baseDir, walWriteOps, func(d string, efs *errfs.ErrorFS) error {
		db, openErr := openDBWithEFS(t, d, efs)
		if openErr != nil {
			return nil
		}
		writeErr := db.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(6, 2)})
		lenientClose(t, db)
		return writeErr
	})

	t.Logf("idempotent recovery injection fired at op %d on %s", firedAt, firedOp)

	injectDir := t.TempDir()
	if err := copyDir(baseDir, injectDir); err != nil {
		t.Fatalf("copyDir inject: %v", err)
	}

	efs := injectOnOps(firedAt, walWriteOps)
	faultDB, openErr := openDBWithEFS(t, injectDir, efs)
	if openErr != nil {
		t.Fatalf("Open at firedAt: %v", openErr)
	}
	assertInjectedOn(t,
		faultDB.SaveEntries(shardID, replicaID, []logdb.Entry{testEntryLarge(6, 2)}),
		walWriteOps)
	lenientClose(t, faultDB)

	// Phase 3: open-close-open-close 3 times with the real FS.
	// Each cycle must see all baseline data and be writable.
	for cycle := 1; cycle <= 3; cycle++ {
		db := openDB(t, injectDir)

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
	final := openDB(t, injectDir)
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
