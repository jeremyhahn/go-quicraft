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

// This file is an EXTERNAL test package so it can import test/errfs, which
// itself imports waldb. Everything here therefore goes through waldb's public
// surface, which is also what a migration tool has.
package waldb_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
	"github.com/jeremyhahn/go-quicraft/test/errfs"
)

// migrationBlockSize is deliberately not waldb's default (32 KiB): the whole
// point of these tests is a WAL whose geometry the caller chose.
const migrationBlockSize = 4096

// seedWAL opens a WAL with the given options, writes one node's bootstrap,
// hard state and a run of entries, and closes it cleanly.
func seedWAL(t *testing.T, dir string, entryCount int, opts ...waldb.Option) []logdb.Entry {
	t.Helper()

	db, err := waldb.Open(dir, opts...)
	if err != nil {
		t.Fatalf("Open %s: %v", dir, err)
	}
	if err := db.SaveBootstrap(1, 1, logdb.Bootstrap{
		Addresses: map[uint64]string{1: "127.0.0.1:1"}, Join: false, Type: 7,
	}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}
	entries := make([]logdb.Entry, 0, entryCount)
	for i := 0; i < entryCount; i++ {
		entries = append(entries, logdb.Entry{
			Index: uint64(i + 1), Term: 1,
			Cmd: []byte(fmt.Sprintf("migrated-payload-%d", i)),
		})
	}
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State:   logdb.State{Term: 1, Vote: 1, Commit: uint64(entryCount)},
		Entries: entries,
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close %s: %v", dir, err)
	}
	return entries
}

// TestReencrypt_NonDefaultBlockSizeIsMigratable is the regression test for a
// WAL that could not be migrated AT ALL. Reencrypt opened both databases with
// no option seam, so both got the DEFAULT block size: a directory written with
// WithBlockSize was refused by Open's WALFORMAT geometry check before a single
// record was read, and a rekey — the one operation the encryption design
// depends on being available — was impossible for that deployment.
func TestReencrypt_NonDefaultBlockSizeIsMigratable(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	dstDir := filepath.Join(root, "dst")

	want := seedWAL(t, srcDir, 5, waldb.WithBlockSize(migrationBlockSize))

	count, err := waldb.Reencrypt(srcDir, nil, dstDir, nil, nil,
		waldb.WithBlockSize(migrationBlockSize))
	if err != nil {
		t.Fatalf("Reencrypt of a %d-byte-block WAL: %v", migrationBlockSize, err)
	}
	if count != 1 {
		t.Fatalf("Reencrypt copied %d nodes, want 1", count)
	}

	// Opening the destination with the DEFAULT geometry must be refused: that
	// refusal is the proof that the destination recorded the source's block
	// size rather than the default.
	defaultDB, err := waldb.Open(dstDir)
	if err == nil {
		_ = defaultDB.Close()
		t.Fatal("the destination opened with the default block size: it was " +
			"written with the default geometry, not the source's")
	}
	var geoErr *waldb.WALGeometryMismatchError
	if !errors.As(err, &geoErr) {
		t.Fatalf("Open error = %v, want a *WALGeometryMismatchError", err)
	}
	if geoErr.Recorded != migrationBlockSize {
		t.Fatalf("destination records block size %d, want %d",
			geoErr.Recorded, migrationBlockSize)
	}

	// The destination must be a geometry-faithful copy, not merely readable:
	// the caller swaps it in and reopens it with the node's own configuration.
	dst, err := waldb.Open(dstDir, waldb.WithBlockSize(migrationBlockSize))
	if err != nil {
		t.Fatalf("Open destination with the source's geometry: %v", err)
	}
	defer func() { _ = dst.Close() }()

	got, _, err := dst.IterateEntries(nil, 0, 1, 1, 1, uint64(len(want))+1, 0)
	if err != nil {
		t.Fatalf("IterateEntries on the destination: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("destination holds %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Index != want[i].Index || string(got[i].Cmd) != string(want[i].Cmd) {
			t.Fatalf("entry %d = {%d, %q}, want {%d, %q}",
				i, got[i].Index, got[i].Cmd, want[i].Index, want[i].Cmd)
		}
	}
	state, _, err := dst.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState on the destination: %v", err)
	}
	if state.Commit != uint64(len(want)) {
		t.Fatalf("destination commit index = %d, want %d", state.Commit, len(want))
	}
}

// TestReencrypt_MismatchedBlockSizeFailsClosed pins the deliberate boundary of
// the option seam: Reencrypt threads the caller's geometry, it does not detect
// it. A caller naming a block size the source does not have is a configuration
// error and must be reported as one, naming both values, rather than silently
// overridden with whatever the directory happens to record.
func TestReencrypt_MismatchedBlockSizeFailsClosed(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	dstDir := filepath.Join(root, "dst")

	seedWAL(t, srcDir, 3, waldb.WithBlockSize(migrationBlockSize))

	_, err := waldb.Reencrypt(srcDir, nil, dstDir, nil, nil,
		waldb.WithBlockSize(2*migrationBlockSize))
	if err == nil {
		t.Fatal("Reencrypt accepted a block size the source does not record")
	}
	var reErr *waldb.ReencryptError
	if !errors.As(err, &reErr) {
		t.Fatalf("error = %T (%v), want a *ReencryptError", err, err)
	}
	if reErr.Op != "open-source" {
		t.Errorf("ReencryptError.Op = %q, want %q", reErr.Op, "open-source")
	}
	var geoErr *waldb.WALGeometryMismatchError
	if !errors.As(err, &geoErr) {
		t.Fatalf("error = %v, want it to wrap a *WALGeometryMismatchError", err)
	}
}

// newTestNoneBarrier returns an initialized barrier running the none strategy.
// It performs no encryption, which is all these tests need: what matters is
// that a barrier is INVOLVED, since that is what makes Reencrypt fail closed
// on a snapshot it cannot re-encrypt.
func newTestNoneBarrier(t *testing.T) *seal.Barrier {
	t.Helper()
	b := seal.MustNewBarrier(seal.DefaultBarrierConfig(1))
	if err := b.Initialize(context.Background(), seal.NewNoneStrategy(), seal.Credentials{}); err != nil {
		t.Fatalf("none barrier Initialize: %v", err)
	}
	return b
}

// errCloseDest is the injected fault standing in for a disk that fails while
// the destination's segments are being completed.
var errCloseDest = errors.New("injected: destination truncate failed")

// TestReencrypt_CloseDestErrorIsReported proves the destination's Close error
// is reported rather than swallowed. DB.Close flushes, truncates and closes
// every shard's active segment, so a discarded error here returns (count, nil)
// — "the migration succeeded" — for a rekey whose newly written WAL was never
// completed on disk, and the caller's very next act is to swap that directory
// into place.
//
// It is constructible only because Reencrypt takes options: the fault is
// injected through WithFS. The first File.Truncate of the run belongs to the
// destination's first shard segment as DB.Close completes it — the copy itself
// only writes and syncs — so the migration succeeds in full and fails at
// exactly the step under test.
func TestReencrypt_CloseDestErrorIsReported(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	dstDir := filepath.Join(root, "dst")

	// Seed through the plain filesystem so the injection counter starts clean.
	seedWAL(t, srcDir, 4)

	efs := errfs.New(waldb.DefaultFS(), 1, errCloseDest).OnlyOps(errfs.OpTruncate)

	count, err := waldb.Reencrypt(srcDir, nil, dstDir, nil, nil, waldb.WithFS(efs))
	if !efs.Injected() {
		t.Fatal("the fault never fired: the test proves nothing about the close path")
	}
	if err == nil {
		t.Fatal("Reencrypt reported success for a destination whose Close failed")
	}
	var reErr *waldb.ReencryptError
	if !errors.As(err, &reErr) {
		t.Fatalf("error = %T (%v), want a *ReencryptError", err, err)
	}
	if reErr.Op != "close-dest" {
		t.Fatalf("ReencryptError.Op = %q, want %q", reErr.Op, "close-dest")
	}
	if !errors.Is(err, errCloseDest) {
		t.Errorf("error = %v, want it to wrap the injected fault", err)
	}
	// The copy itself completed; only completing the destination on disk did
	// not. The count must still report what was copied.
	if count != 1 {
		t.Errorf("count = %d, want 1: the node was copied before Close failed", count)
	}
}

// TestReencrypt_CloseDestErrorDoesNotMaskMigrationError pins the ordering of
// the two failures. A migration that fails on its own terms must keep
// reporting its own cause; the destination Close error may only fill in when
// there is nothing else to report.
func TestReencrypt_CloseDestErrorDoesNotMaskMigrationError(t *testing.T) {
	root := t.TempDir()
	srcDir := filepath.Join(root, "src")
	dstDir := filepath.Join(root, "dst")

	seedWAL(t, srcDir, 4)

	// A source-side barrier with no snapshot hook fails the migration itself
	// (fail-closed on snapshot re-encryption), while the destination Close
	// fault is armed at the same time.
	db, err := waldb.Open(srcDir)
	if err != nil {
		t.Fatalf("Open src: %v", err)
	}
	if err := db.SaveSnapshot(1, 1, logdb.Snapshot{
		Index: 2, Term: 1,
		Membership: logdb.Membership{Addresses: map[uint64]string{1: "127.0.0.1:1"}},
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close src: %v", err)
	}

	efs := errfs.New(waldb.DefaultFS(), 1, errCloseDest).OnlyOps(errfs.OpTruncate)
	_, err = waldb.Reencrypt(srcDir, nil, dstDir, newTestNoneBarrier(t), nil,
		waldb.WithFS(efs))
	if err == nil {
		t.Fatal("Reencrypt succeeded with a snapshot it cannot re-encrypt")
	}
	if !errors.Is(err, waldb.ErrSnapshotReencryptRequired) {
		t.Fatalf("error = %v, want the migration's own cause "+
			"(ErrSnapshotReencryptRequired), not the destination Close error", err)
	}
}
