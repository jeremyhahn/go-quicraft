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
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// newNoneBarrier returns an initialized none-strategy barrier (passthrough,
// zero-epoch) — the on-disk format of a Raft host that was started without
// encryption.
func newNoneBarrier(t *testing.T) *seal.Barrier {
	t.Helper()
	b := seal.NewBarrier(testBarrierConfig(1))
	if err := b.Initialize(context.Background(), seal.NewNoneStrategy(), seal.Credentials{}); err != nil {
		t.Fatalf("none barrier Initialize: %v", err)
	}
	return b
}

// writeSampleState populates a node's bootstrap, snapshot, hard state, and a
// run of log entries.
func writeSampleState(t *testing.T, db *DB, shardID, replicaID uint64, n int) ([]logdb.Entry, logdb.State) {
	t.Helper()
	if err := db.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Addresses: map[uint64]string{replicaID: "127.0.0.1:1"}, Join: false, Type: 7,
	}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}
	if err := db.SaveSnapshot(shardID, replicaID, logdb.Snapshot{
		Index: 2, Term: 1,
		Membership: logdb.Membership{Addresses: map[uint64]string{replicaID: "127.0.0.1:1"}},
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	entries := make([]logdb.Entry, 0, n)
	for i := 0; i < n; i++ {
		entries = append(entries, logdb.Entry{
			Index: uint64(i + 1), Term: 1, Type: 0,
			Cmd: []byte(fmt.Sprintf("command-payload-%d", i)),
		})
	}
	state := logdb.State{Term: 1, Vote: replicaID, Commit: uint64(n)}
	if err := db.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID, State: state, Entries: entries,
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	return entries, state
}

// TestReencrypt_NoneToKeyed is the core proof: a none-encrypted WAL is
// re-encrypted into a keyed WAL with every Raft record preserved, and the
// result is genuinely encrypted (the none barrier can no longer read it).
func TestReencrypt_NoneToKeyed(t *testing.T) {
	const shardID, replicaID = uint64(1), uint64(1)
	srcDir := t.TempDir() + "/wal"
	dstDir := t.TempDir() + "/wal.keyed"

	noneBarrier := newNoneBarrier(t)
	keyedBarrier := newTestBarrier(t)

	// 1. Write a none-encrypted WAL.
	src := openWALAt(t, srcDir, noneBarrier)
	wantEntries, wantState := writeSampleState(t, src, shardID, replicaID, 25)
	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	// 2. Re-encrypt none -> keyed. A barrier is involved and the node has a
	// snapshot, so a SnapshotReencryptFunc is required; it records the
	// destination at-rest epoch (the sample state writes snapshot metadata only,
	// so the hook simply reports the dst barrier's epoch).
	n, err := Reencrypt(srcDir, noneBarrier, dstDir, keyedBarrier,
		func(_, _ uint64, _ logdb.Snapshot) (uint64, error) { return keyedBarrier.CurrentEpoch(), nil })
	if err != nil {
		t.Fatalf("Reencrypt: %v", err)
	}
	if n != 1 {
		t.Fatalf("re-encrypted %d nodes, want 1", n)
	}

	// 3. Read back through the keyed barrier — every record must match.
	dst := openWALAt(t, dstDir, keyedBarrier)
	defer func() { _ = dst.Close() }()

	gotState, gotMembership, serr := dst.ReadState(shardID, replicaID)
	if serr != nil {
		t.Fatalf("ReadState: %v", serr)
	}
	if gotState != wantState {
		t.Fatalf("state = %+v, want %+v", gotState, wantState)
	}
	if gotMembership.Addresses[replicaID] != "127.0.0.1:1" {
		t.Fatalf("membership not preserved: %+v", gotMembership)
	}
	bs, ok, _ := dst.GetBootstrap(shardID, replicaID)
	if !ok || bs.Type != 7 {
		t.Fatalf("bootstrap not preserved: %+v ok=%v", bs, ok)
	}
	first, length, _ := dst.EntryRange(shardID, replicaID)
	got, _, eerr := dst.IterateEntries(nil, 0, shardID, replicaID, first, first+length, 1<<30)
	if eerr != nil {
		t.Fatalf("IterateEntries: %v", eerr)
	}
	if len(got) != len(wantEntries) {
		t.Fatalf("got %d entries, want %d", len(got), len(wantEntries))
	}
	for i := range got {
		if got[i].Index != wantEntries[i].Index || string(got[i].Cmd) != string(wantEntries[i].Cmd) {
			t.Fatalf("entry %d = %+v, want %+v", i, got[i], wantEntries[i])
		}
	}

	// 4. The WAL is genuinely encrypted: opening it through the none barrier
	// must fail — the none decrypter rejects the non-zero keyed epoch written
	// by the real strategy. (Open replays segments, decrypting each record.)
	if probe, perr := Open(dstDir, WithBarrier(noneBarrier)); perr == nil {
		_ = probe.Close()
		t.Fatal("none barrier opened the keyed WAL — data was NOT encrypted")
	}
}

// TestReencrypt_KeyedToKeyed proves barrier rotation (key A -> key B) also
// round-trips, which is the same primitive used for a future rekey.
func TestReencrypt_KeyedToKeyed(t *testing.T) {
	const shardID, replicaID = uint64(3), uint64(1)
	srcDir := t.TempDir() + "/wal"
	dstDir := t.TempDir() + "/wal2"
	bA := newTestBarrierWithPassphrase(t, "key-A")
	bB := newTestBarrierWithPassphrase(t, "key-B")

	src := openWALAt(t, srcDir, bA)
	wantEntries, _ := writeSampleState(t, src, shardID, replicaID, 5)
	_ = src.Close()

	if _, err := Reencrypt(srcDir, bA, dstDir, bB,
		func(_, _ uint64, _ logdb.Snapshot) (uint64, error) { return bB.CurrentEpoch(), nil }); err != nil {
		t.Fatalf("Reencrypt: %v", err)
	}

	dst := openWALAt(t, dstDir, bB)
	defer func() { _ = dst.Close() }()
	first, length, _ := dst.EntryRange(shardID, replicaID)
	got, _, _ := dst.IterateEntries(nil, 0, shardID, replicaID, first, first+length, 1<<30)
	if len(got) != len(wantEntries) {
		t.Fatalf("got %d entries, want %d", len(got), len(wantEntries))
	}
}

// TestReencrypt_NoSnapshot proves a node that has never snapshotted (a young
// shard whose entire log fits in the live range) re-encrypts successfully:
// GetSnapshot's ErrNoSnapshot is tolerated, not treated as a failure. This is
// the common case for a freshly-used tenant Host being migrated none -> keyed.
func TestReencrypt_NoSnapshot(t *testing.T) {
	const shardID, replicaID = uint64(9), uint64(1)
	srcDir := t.TempDir() + "/wal"
	dstDir := t.TempDir() + "/wal.keyed"

	noneBarrier := newNoneBarrier(t)
	keyedBarrier := newTestBarrier(t)

	// Write bootstrap + state + entries but NO snapshot.
	src := openWALAt(t, srcDir, noneBarrier)
	if err := src.SaveBootstrap(shardID, replicaID, logdb.Bootstrap{
		Addresses: map[uint64]string{replicaID: "127.0.0.1:1"}, Join: false, Type: 7,
	}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}
	wantEntries := make([]logdb.Entry, 0, 8)
	for i := 0; i < 8; i++ {
		wantEntries = append(wantEntries, logdb.Entry{
			Index: uint64(i + 1), Term: 1, Type: 0,
			Cmd: []byte(fmt.Sprintf("nosnap-%d", i)),
		})
	}
	if err := src.SaveState([]logdb.Update{{
		ShardID: shardID, ReplicaID: replicaID,
		State:   logdb.State{Term: 1, Vote: replicaID, Commit: 8},
		Entries: wantEntries,
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	// Sanity: no snapshot is saved for this node.
	if _, err := src.GetSnapshot(shardID, replicaID); !errors.Is(err, logdb.ErrNoSnapshot) {
		t.Fatalf("expected ErrNoSnapshot, got %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	// This node has NO snapshot, so no SnapshotReencryptFunc is needed even
	// though a barrier is involved (snapFn may be nil).
	n, err := Reencrypt(srcDir, noneBarrier, dstDir, keyedBarrier, nil)
	if err != nil {
		t.Fatalf("Reencrypt (no snapshot): %v", err)
	}
	if n != 1 {
		t.Fatalf("re-encrypted %d nodes, want 1", n)
	}

	dst := openWALAt(t, dstDir, keyedBarrier)
	defer func() { _ = dst.Close() }()
	first, length, _ := dst.EntryRange(shardID, replicaID)
	got, _, eerr := dst.IterateEntries(nil, 0, shardID, replicaID, first, first+length, 1<<30)
	if eerr != nil {
		t.Fatalf("IterateEntries: %v", eerr)
	}
	if len(got) != len(wantEntries) {
		t.Fatalf("got %d entries, want %d", len(got), len(wantEntries))
	}
}

// TestReencryptError_Type covers the typed error.
func TestReencryptError_Type(t *testing.T) {
	base := errors.New("x")
	e := &ReencryptError{Op: "open-source", Err: base}
	if !errors.Is(e, base) {
		t.Fatal("Unwrap")
	}
}

// openWALAt opens a waldb at dir with the given barrier (helper).
func openWALAt(t *testing.T, dir string, barrier WALBarrier) *DB {
	t.Helper()
	db, err := Open(dir, WithBarrier(barrier))
	if err != nil {
		t.Fatalf("Open %s: %v", dir, err)
	}
	return db
}

// TestReencrypt_SnapshotFailClosedWithoutHook is a CX2 regression test: a
// barrier-involved migration of a node that HAS a snapshot must fail closed when
// no SnapshotReencryptFunc is provided, rather than silently copy snapshot
// metadata pointing at a source-sealed data file the destination cannot decrypt.
func TestReencrypt_SnapshotFailClosedWithoutHook(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	noneBarrier := newNoneBarrier(t)
	keyedBarrier := newTestBarrier(t)

	src := openWALAt(t, srcDir, noneBarrier)
	writeSampleState(t, src, 1, 1, 5) // includes a snapshot record
	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	if _, err := Reencrypt(srcDir, noneBarrier, dstDir, keyedBarrier, nil); !errors.Is(err, ErrSnapshotReencryptRequired) {
		t.Fatalf("Reencrypt(snapshot, nil hook) = %v, want ErrSnapshotReencryptRequired", err)
	}
}

// TestReencrypt_SnapshotHookRecordsDstEpoch verifies the hook is invoked for a
// node with a snapshot and its returned destination epoch is recorded in the
// migrated snapshot metadata (so the destination decrypts the re-encrypted file
// at the right epoch).
func TestReencrypt_SnapshotHookRecordsDstEpoch(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	noneBarrier := newNoneBarrier(t)
	keyedBarrier := newTestBarrier(t)

	src := openWALAt(t, srcDir, noneBarrier)
	writeSampleState(t, src, 3, 1, 5)
	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	const wantEpoch = uint64(42)
	called := 0
	if _, err := Reencrypt(srcDir, noneBarrier, dstDir, keyedBarrier,
		func(shard, replica uint64, _ logdb.Snapshot) (uint64, error) {
			called++
			if shard != 3 || replica != 1 {
				t.Errorf("hook got (%d,%d), want (3,1)", shard, replica)
			}
			return wantEpoch, nil
		}); err != nil {
		t.Fatalf("Reencrypt: %v", err)
	}
	if called != 1 {
		t.Fatalf("snapshot hook called %d times, want 1", called)
	}

	dst := openWALAt(t, dstDir, keyedBarrier)
	defer dst.Close()
	snap, err := dst.GetSnapshot(3, 1)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap.Epoch != wantEpoch {
		t.Fatalf("migrated snapshot epoch = %d, want %d (hook's dst epoch)", snap.Epoch, wantEpoch)
	}
}

// TestReencrypt_StreamsLargeLogInBatches is the CX4 regression test: a live log
// spanning multiple batches must migrate via per-batch streaming (no single
// oversized destination record / OOM), and the destination must hold the full,
// in-order entry range.
func TestReencrypt_StreamsLargeLogInBatches(t *testing.T) {
	// Lower the batch threshold so a few hundred small entries span many
	// batches, exercising the streaming path without writing 64 MiB.
	saved := reencryptBatchBytes
	reencryptBatchBytes = 4096
	defer func() { reencryptBatchBytes = saved }()

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	noneBarrier := newNoneBarrier(t)

	src := openWALAt(t, srcDir, noneBarrier)
	if err := src.SaveBootstrap(1, 1, logdb.Bootstrap{Addresses: map[uint64]string{1: "127.0.0.1:1"}}); err != nil {
		t.Fatalf("SaveBootstrap: %v", err)
	}
	const total = 300
	entries := make([]logdb.Entry, total)
	for i := range entries {
		entries[i] = logdb.Entry{Index: uint64(i + 1), Term: 1, Cmd: []byte("entry-payload-deadbeefcafef00d")}
	}
	if err := src.SaveEntries(1, 1, entries); err != nil {
		t.Fatalf("SaveEntries: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	n, err := Reencrypt(srcDir, noneBarrier, dstDir, noneBarrier, nil)
	if err != nil {
		t.Fatalf("Reencrypt: %v", err)
	}
	if n != 1 {
		t.Fatalf("migrated %d nodes, want 1", n)
	}

	dst := openWALAt(t, dstDir, noneBarrier)
	defer dst.Close()
	got, err := dst.GetEntries(1, 1, 1, total+1, 0)
	if err != nil {
		t.Fatalf("GetEntries: %v", err)
	}
	if len(got) != total {
		t.Fatalf("migrated %d entries, want %d (streaming dropped entries)", len(got), total)
	}
	for i, e := range got {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry %d has index %d, want %d (out-of-order or gap)", i, e.Index, i+1)
		}
	}
}
