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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestRetiredRegistry_IsSixteenBytesPerReplica verifies the registry is the
// compact record it exists to be — a 16-byte header, 16 bytes per retired
// replica and a 4-byte checksum — and that it round-trips, sorted, through a
// reopen.
func TestRetiredRegistry_IsSixteenBytesPerReplica(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	retire := []nodeKey{{ShardID: 9, ReplicaID: 2}, {ShardID: 1, ReplicaID: 7}, {ShardID: 1, ReplicaID: 3}}
	for _, k := range retire {
		if err := db.RemoveNodeData(k.ShardID, k.ReplicaID); err != nil {
			t.Fatalf("RemoveNodeData(%d, %d): %v", k.ShardID, k.ReplicaID, err)
		}
	}
	// Retiring a replica twice changes nothing.
	if err := db.RemoveNodeData(1, 7); err != nil {
		t.Fatalf("RemoveNodeData again: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, retiredFilename))
	if err != nil {
		t.Fatalf("read registry: %v", err)
	}
	if want := retiredHeaderSize + len(retire)*retiredEntrySize + retiredTrailerSize; len(raw) != want {
		t.Fatalf("registry is %d bytes for %d retirements, want %d", len(raw), len(retire), want)
	}
	keys, err := parseRetiredRegistry(raw)
	if err != nil {
		t.Fatalf("parseRetiredRegistry: %v", err)
	}
	want := []nodeKey{{ShardID: 1, ReplicaID: 3}, {ShardID: 1, ReplicaID: 7}, {ShardID: 9, ReplicaID: 2}}
	if len(keys) != len(want) {
		t.Fatalf("registry holds %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("registry holds %v, want %v (sorted)", keys, want)
		}
	}

	db, err = Open(dir, WithNoSync(true))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	for _, k := range retire {
		if removed, err := db.NodeRemoved(k.ShardID, k.ReplicaID); err != nil || !removed {
			t.Fatalf("NodeRemoved(%d, %d) after reopen = (%v, %v), want (true, nil)", k.ShardID, k.ReplicaID, removed, err)
		}
	}
}

// TestOpen_RefusesAMalformedRetiredRegistry verifies a registry that does not
// parse fails the open: opening without it would hand every retired replica ID
// back.
func TestOpen_RefusesAMalformedRetiredRegistry(t *testing.T) {
	valid := marshalRetiredRegistry([]nodeKey{{ShardID: 1, ReplicaID: 1}, {ShardID: 2, ReplicaID: 1}})
	flip := func(off int) []byte {
		b := append([]byte(nil), valid...)
		b[off] ^= 0x01
		return b
	}
	cases := map[string][]byte{
		"empty":        {},
		"bad_magic":    flip(0),
		"bad_count":    flip(8),
		"bad_entry":    flip(retiredHeaderSize),
		"bad_checksum": flip(len(valid) - 1),
		"truncated":    valid[:len(valid)-1],
		"trailing":     append(append([]byte(nil), valid...), 0),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, WithNoSync(true))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, retiredFilename), content, 0o600); err != nil {
				t.Fatalf("write registry: %v", err)
			}
			if _, err := Open(dir, WithNoSync(true)); !errors.Is(err, ErrRetiredRegistryMalformed) {
				t.Fatalf("Open over a %s registry = %v, want ErrRetiredRegistryMalformed", name, err)
			}
		})
	}
}

// TestRemoveNodeData_FailedRegistryWriteChangesNothing verifies a retirement
// that cannot be published leaves the node exactly as it was: its data is
// readable, it is not retired, it can still be written, and no temporary file
// is left in the WAL root.
func TestRemoveNodeData_FailedRegistryWriteChangesNothing(t *testing.T) {
	dir := t.TempDir()
	ffs := newMarkerRecordingFS()
	db, err := Open(dir, WithFS(ffs), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 4, Vote: 1}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}

	ffs.mu.Lock()
	ffs.failWriteOn = retiredTempFilename
	ffs.mu.Unlock()
	if err := db.RemoveNodeData(1, 1); err == nil {
		t.Fatal("RemoveNodeData succeeded although its registry could not be written")
	}
	ffs.mu.Lock()
	ffs.failWriteOn = ""
	ffs.mu.Unlock()

	if removed, _ := db.NodeRemoved(1, 1); removed {
		t.Fatal("a failed RemoveNodeData retired the node")
	}
	if st, err := db.GetState(1, 1); err != nil || st.Term != 4 {
		t.Fatalf("GetState after a failed RemoveNodeData = (%+v, %v), want the node intact", st, err)
	}
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 5, Vote: 1}); err != nil {
		t.Fatalf("SaveNodeState after a failed RemoveNodeData: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, retiredTempFilename)); !os.IsNotExist(err) {
		t.Fatalf("a failed retirement left %s behind (stat err %v)", retiredTempFilename, err)
	}
}

// TestRemoveNodeData_RegistryDirSyncFailurePoisons verifies a retirement
// whose registry was renamed into place but whose directory fsync failed
// fail-stops the DB: whether the retirement survives a crash is unknown, the
// same uncertainty a failed WAL fsync leaves, and only a restart settles it.
func TestRemoveNodeData_RegistryDirSyncFailurePoisons(t *testing.T) {
	dir := t.TempDir()
	ffs := &dirSyncFailFS{FS: defaultFS()}
	db, err := Open(dir, WithFS(ffs), WithBlockSize(1024), WithMaxFileSize(64*1024))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	if err := db.SaveNodeState(1, 1, logdb.State{Term: 4, Vote: 1}); err != nil {
		t.Fatalf("SaveNodeState: %v", err)
	}

	injected := errors.New("test: WAL root fsync failed")
	ffs.mu.Lock()
	ffs.failDir = dir
	ffs.syncErr = injected
	ffs.mu.Unlock()
	if err := db.RemoveNodeData(1, 1); !errors.Is(err, injected) {
		t.Fatalf("RemoveNodeData = %v, want the directory fsync failure", err)
	}
	if err := db.SaveNodeState(2, 1, logdb.State{Term: 1}); !errors.Is(err, ErrDBPoisoned) {
		t.Fatalf("a write after an unproven retirement = %v, want ErrDBPoisoned", err)
	}
}

// TestOpen_FinishesARemovalThatStoppedAfterItsRetirement verifies the open
// that follows an interrupted removal finishes it: the retirement was
// published, but the process stopped before the node's records were dropped
// and its segments collected (or that collection failed). Replay finds the
// records, purgeRetired drops them, and the segment holding nothing else is
// reclaimed there — not left until some later compaction happens to run.
func TestOpen_FinishesARemovalThatStoppedAfterItsRetirement(t *testing.T) {
	dir := t.TempDir()
	open := func() *DB {
		db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	const nodeA, nodeB = uint64(1), uint64(17) // same WAL shard
	db := open()
	if err := db.SaveEntries(nodeA, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries A: %v", err)
	}
	rotateShard(t, db, nodeA)
	if err := db.SaveEntries(nodeB, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries B: %v", err)
	}
	// Publish the retirement only, as a removal interrupted right after it
	// would have left things.
	if _, err := db.retired.retire(db.opts.fs, db.dir, []nodeKey{{ShardID: nodeA, ReplicaID: 1}}, false, nil); err != nil {
		t.Fatalf("retire: %v", err)
	}
	seg0 := segmentPath(db, nodeA, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(seg0); err != nil {
		t.Fatalf("fixture: segment 0 missing before reopen: %v", err)
	}

	db = open()
	defer db.Close()
	if _, err := os.Stat(seg0); !os.IsNotExist(err) {
		t.Fatalf("segment 0, holding only the retired node, survived the open (stat err %v)", err)
	}
	if got, err := db.GetEntries(nodeA, 1, 1, 6, 0); err != nil || got != nil {
		t.Fatalf("retired node's entries after open = (%d, %v), want none", len(got), err)
	}
	if first, n, err := db.EntryRange(nodeB, 1); err != nil || first != 1 || n != 3 {
		t.Fatalf("surviving node's range = (%d, %d, %v), want (1, 3, nil)", first, n, err)
	}
}

// TestRetire_VisibleOnlyAfterTheDrop verifies a retirement is reported only
// once its node's records are gone: apply — where RemoveNodeData drops them —
// runs before the key joins the set, so NodeRemoved can never be true while a
// reader still sees the node.
func TestRetire_VisibleOnlyAfterTheDrop(t *testing.T) {
	dir := t.TempDir()
	r, err := loadRetiredRegistry(defaultFS(), dir)
	if err != nil {
		t.Fatalf("loadRetiredRegistry: %v", err)
	}
	key := nodeKey{ShardID: 1, ReplicaID: 1}
	ran := false
	if _, err := r.retire(defaultFS(), dir, []nodeKey{key}, true, func() {
		ran = true
		if r.contains(key) {
			t.Error("retirement visible before the node's records were dropped")
		}
	}); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if !ran || !r.contains(key) {
		t.Fatalf("apply ran = %v, retired = %v; want both", ran, r.contains(key))
	}
}

// TestReencrypt_CarriesTheRegistryInOneWrite verifies the migration carries
// every retirement across in a single registry publication, not one rewrite
// of the whole registry per retired replica.
func TestReencrypt_CarriesTheRegistryInOneWrite(t *testing.T) {
	root := t.TempDir()
	srcDir, dstDir := filepath.Join(root, "src"), filepath.Join(root, "dst")
	src, err := Open(srcDir, WithNoSync(true))
	if err != nil {
		t.Fatalf("Open src: %v", err)
	}
	for shard := uint64(1); shard <= 5; shard++ {
		if err := src.RemoveNodeData(shard, 1); err != nil {
			t.Fatalf("RemoveNodeData(%d): %v", shard, err)
		}
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close src: %v", err)
	}

	ffs := newMarkerRecordingFS()
	if _, err := Reencrypt(srcDir, nil, dstDir, nil, nil, WithFS(ffs)); err != nil {
		t.Fatalf("Reencrypt: %v", err)
	}
	publications := 0
	for _, op := range ffs.recorded() {
		if op == "rename "+retiredTempFilename+" -> "+retiredFilename {
			publications++
		}
	}
	if publications != 1 {
		t.Fatalf("Reencrypt published the registry %d times for 5 retirements, want once", publications)
	}
	dst, err := Open(dstDir)
	if err != nil {
		t.Fatalf("Open dst: %v", err)
	}
	defer dst.Close()
	for shard := uint64(1); shard <= 5; shard++ {
		if removed, _ := dst.NodeRemoved(shard, 1); !removed {
			t.Fatalf("replica (%d, 1) not retired in the destination", shard)
		}
	}
}

// TestOpen_ReclaimsARepairedFinalSegmentHoldingOnlyRetiredRecords verifies the
// reclaim that follows replay can take the final segment even when recovery
// had to repair a torn tail in it. The repaired segment's clean prefix is
// recovered like any clean segment's, so it is committed, rather than left
// above the commit high-water mark, out of GC's reach, until some later write.
func TestOpen_ReclaimsARepairedFinalSegmentHoldingOnlyRetiredRecords(t *testing.T) {
	dir := t.TempDir()
	open := func() *DB {
		db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	const nodeA, nodeB = uint64(1), uint64(17) // same WAL shard
	db := open()
	if err := db.SaveEntries(nodeB, 1, makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("SaveEntries B: %v", err)
	}
	rotateShard(t, db, nodeA)
	// The final segment: node A alone, which is then retired as an
	// interrupted removal would leave it.
	if err := db.SaveEntries(nodeA, 1, makeEntries(1, 5, 1)); err != nil {
		t.Fatalf("SaveEntries A: %v", err)
	}
	final := segmentPath(db, nodeA, 1)
	if _, err := db.retired.retire(db.opts.fs, db.dir, []nodeKey{{ShardID: nodeA, ReplicaID: 1}}, false, nil); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A torn tail after A's records: bytes of an unsynced write that never
	// completed.
	info, err := os.Stat(final)
	if err != nil {
		t.Fatalf("stat final segment: %v", err)
	}
	f, err := os.OpenFile(final, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open final segment: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04}, info.Size()); err != nil {
		t.Fatalf("tear the tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close final segment: %v", err)
	}

	db = open()
	defer db.Close()
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("the repaired final segment, holding only retired records, survived the open (stat err %v)", err)
	}
	if first, n, err := db.EntryRange(nodeB, 1); err != nil || first != 1 || n != 3 {
		t.Fatalf("surviving node's range = (%d, %d, %v), want (1, 3, nil)", first, n, err)
	}
}

// TestOpen_ReclaimsEachShardOnceHoweverManyRetiredNodesItHolds verifies the
// reclaim that follows replay runs once per affected WAL shard, not once per
// retired node: each reclaim scans the shard directory and fsyncs it, and an
// open after many interrupted removals would otherwise pay one of each per
// retired replica. Three retired nodes on one shard cost what one does.
func TestOpen_ReclaimsEachShardOnceHoweverManyRetiredNodesItHolds(t *testing.T) {
	shardDirSyncs := func(t *testing.T, retire int) int {
		t.Helper()
		dir := t.TempDir()
		db, err := Open(dir, WithBlockSize(1024), WithMaxFileSize(64*1024))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		var keys []nodeKey
		for i := 0; i < retire; i++ {
			shardID := uint64(1 + i*DefaultShardCount) // all on WAL shard 1
			if err := db.SaveEntries(shardID, 1, makeEntries(1, 3, 1)); err != nil {
				t.Fatalf("SaveEntries: %v", err)
			}
			rotateShard(t, db, shardID)
			keys = append(keys, nodeKey{ShardID: shardID, ReplicaID: 1})
		}
		if err := db.SaveEntries(1+uint64(retire)*DefaultShardCount, 1, makeEntries(1, 1, 1)); err != nil {
			t.Fatalf("SaveEntries survivor: %v", err)
		}
		if _, err := db.retired.retire(db.opts.fs, db.dir, keys, false, nil); err != nil {
			t.Fatalf("retire: %v", err)
		}
		shardDir := db.getShard(1).dir
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		sfs := newSyncAccountingFS()
		db, err = Open(dir, WithFS(sfs), WithBlockSize(1024), WithMaxFileSize(64*1024))
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer db.Close()
		for _, k := range keys {
			if got, err := db.GetEntries(k.ShardID, k.ReplicaID, 1, 4, 0); err != nil || got != nil {
				t.Fatalf("retired node %d still readable: (%d, %v)", k.ShardID, len(got), err)
			}
		}
		return sfs.metaSyncCount(shardDir)
	}
	one := shardDirSyncs(t, 1)
	three := shardDirSyncs(t, 3)
	if three != one {
		t.Fatalf("opening with 3 retired nodes on one shard fsynced its directory %d times, with 1 %d: "+
			"the reclaim ran once per node, not once per shard", three, one)
	}
}
