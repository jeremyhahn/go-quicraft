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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// Crash-consistency tests kill a real writer process with SIGKILL mid-write
// and assert the WAL recovers. Unlike the injected-fault suites (see
// gap_hardening_test.go, segment_hardening_test.go) nothing here is
// simulated: the child process dies without unwinding, without closing the
// segment, and without flushing whatever the writer had buffered.
//
// A SIGKILL leaves everything already handed to write(2) in the page cache,
// so the surviving tail is exactly the sequence of block writes the child
// issued: the buffered open block is lost, and a multi-block record can end
// as a RecordFirst chunk whose continuation blocks were never written.
// A process death alone therefore cannot produce a CRC-corrupt block -- that
// needs the medium to lose a partially written block, which
// TestWALCrashTornTailRepairedAcrossRestarts adds on top of the real kill.
const (
	// crashChildEnv guards the child entry point. The parent sets it to "1".
	crashChildEnv = "WALDB_CRASH_CHILD"
	// crashDirEnv names the WAL directory the child writes to.
	crashDirEnv = "WALDB_CRASH_DIR"
	// crashAckEnv names the file the child fsyncs an ack line to after every
	// SaveState that returned nil.
	crashAckEnv = "WALDB_CRASH_ACK"
	// crashSealEnv names the persisted sealed root key. Non-empty selects the
	// encrypted (barrier-backed) WAL path.
	crashSealEnv = "WALDB_CRASH_SEAL"

	// crashChildTest is the child test name re-invoked through os.Args[0].
	crashChildTest = "TestWALCrashChildWriter"

	// crashPassphrase seals the crash test's barrier root key. The sealed
	// blob lives in the test's temp dir and never outlives it.
	crashPassphrase = "waldb-crash-test-passphrase"

	// crashBatchSize is the number of entries each SaveState appends per node.
	crashBatchSize = 4
	// crashTermPeriod is the number of SaveState iterations per Raft term.
	// An fsynced SaveState costs milliseconds, so a kill lands ~10 iterations
	// in; terms must advance well inside that window for the recovered hard
	// state to be a meaningful non-regression check.
	crashTermPeriod = 2

	crashReplicaID = uint64(1)

	// crashBlockSize and crashMaxFileSize are deliberately tiny: entries are
	// larger than one block (so records split into First/Middle/Last chunks
	// across separate write(2) calls) and segments rotate every two or three
	// writes, putting rotation inside the kill window.
	crashBlockSize   = 1024
	crashMaxFileSize = 8 * 1024

	// crashChildDeadline bounds the child if the parent dies before killing it.
	crashChildDeadline = 60 * time.Second
	// crashAckTimeout bounds the wait for the child's durable acks.
	crashAckTimeout = 30 * time.Second
)

// crashShardIDs are written by every SaveState batch, so each batch spans
// three independent WAL shards and the kill can land between two shards'
// durable writes.
var crashShardIDs = [...]uint64{1, 2, 3}

// crashKillDelays are the delays between the child's first durable ack and
// the SIGKILL. Spread across the write pipeline they land in different
// phases: mid-block fill, mid-block flush, mid-rotation and mid-fsync.
var crashKillDelays = []time.Duration{
	0,
	3 * time.Millisecond,
	11 * time.Millisecond,
	29 * time.Millisecond,
	73 * time.Millisecond,
	151 * time.Millisecond,
}

// crashTermForIndex maps a log index to the term the child wrote it under.
// The mapping is a pure function of the index, so the parent can regenerate
// every entry the child could possibly have written and reject anything else
// as torn or garbage.
func crashTermForIndex(index uint64) uint64 {
	return 1 + ((index-1)/crashBatchSize)/crashTermPeriod
}

// crashVoteForTerm maps a term to the vote persisted with it. A recovered
// (Term, Vote) pair that violates this mapping is a franken-state: the hard
// state record is written as one CRC-protected record and can never be
// recovered half-applied.
func crashVoteForTerm(term uint64) uint64 {
	return term%7 + 1
}

// crashEntry regenerates the entry the child wrote at the given index.
func crashEntry(index uint64) logdb.Entry {
	cmd := make([]byte, 64+int((index*37)%900))
	for i := range cmd {
		cmd[i] = byte(index + uint64(i))
	}
	return logdb.Entry{
		Index:       index,
		Term:        crashTermForIndex(index),
		Type:        index % 2,
		Key:         index * 11,
		ClientID:    index%5 + 1,
		SeriesID:    index,
		RespondedTo: index / 2,
		Cmd:         cmd,
	}
}

// crashOpenDB opens the WAL with the settings shared by the child and the
// parent. Syncing is enabled: a crash test on an unsynced WAL proves nothing.
func crashOpenDB(dir string, barrier WALBarrier) (*DB, error) {
	opts := []Option{
		WithBlockSize(crashBlockSize),
		WithMaxFileSize(crashMaxFileSize),
	}
	if barrier != nil {
		opts = append(opts, WithBarrier(barrier))
	}
	return Open(dir, opts...)
}

// crashWriteSealed persists a sealed root key so the process that reopens the
// encrypted WAL can re-derive the same DEKs.
func crashWriteSealed(path string, sealed *seal.SealedRootKey) error {
	data, err := json.Marshal(sealed)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// crashBarrier initializes the barrier at sealPath on first call and restores
// it on every later call. The config carries a StatePersister because barriers
// fail closed with seal.ErrNoStatePersister without one — and because RESTORING
// one rotates: the spent AEAD invocation budget of the loaded epoch cannot be
// known across a restart, so the barrier moves to a fresh epoch rather than
// reuse a counter it cannot bound.
//
// That makes every barrier built from this blob a point-in-time view. Two live
// barriers over one blob go stale against each other the moment either restores
// or rotates, so a caller that holds one across a child process's lifetime must
// re-load it afterwards rather than reuse the object.
func crashBarrier(sealPath string) (*seal.Barrier, error) {
	_ = seal.AllowWeakKDFParams()
	strategy, err := seal.NewSoftwareStrategyWithParams(
		[]byte(crashPassphrase), 1, 64*1024, 1,
	)
	if err != nil {
		return nil, err
	}
	cfg := testBarrierConfig(1)
	cfg.StatePersister = func(s *seal.SealedRootKey) error {
		return crashWriteSealed(sealPath, s)
	}
	b := seal.MustNewBarrier(cfg)

	data, readErr := os.ReadFile(sealPath)
	switch {
	case readErr == nil:
		var sealed seal.SealedRootKey
		if err := json.Unmarshal(data, &sealed); err != nil {
			return nil, err
		}
		if err := b.SetSealedRootKey(&sealed); err != nil {
			return nil, err
		}
		if err := b.Unseal(context.Background(), strategy, seal.Credentials{}); err != nil {
			return nil, err
		}
	case errors.Is(readErr, fs.ErrNotExist):
		if err := b.Initialize(context.Background(), strategy, seal.Credentials{}); err != nil {
			return nil, err
		}
		if err := crashWriteSealed(sealPath, b.SealedRootKeyData()); err != nil {
			return nil, err
		}
	default:
		return nil, readErr
	}
	return b, nil
}

// TestWALCrashChildWriter is the child half of the crash tests. It is inert
// unless the parent selected it through crashChildEnv.
func TestWALCrashChildWriter(t *testing.T) {
	if os.Getenv(crashChildEnv) != "1" {
		t.Skip("child process entry point; driven by TestWALCrashConsistency")
	}
	crashChildMain()
	os.Exit(0)
}

// crashChildMain writes entries until the parent kills the process. It never
// returns cleanly under test: the parent's SIGKILL arrives first. Any error
// exits non-zero so the parent can distinguish a bug from a kill.
func crashChildMain() {
	fail := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "crash child: "+format+"\n", args...)
		os.Exit(1)
	}

	dir := os.Getenv(crashDirEnv)
	ackPath := os.Getenv(crashAckEnv)
	sealPath := os.Getenv(crashSealEnv)
	if dir == "" || ackPath == "" {
		fail("missing %s or %s", crashDirEnv, crashAckEnv)
	}

	var barrier WALBarrier
	if sealPath != "" {
		b, err := crashBarrier(sealPath)
		if err != nil {
			fail("barrier: %v", err)
		}
		barrier = b
	}

	db, err := crashOpenDB(dir, barrier)
	if err != nil {
		fail("open: %v", err)
	}

	ack, err := os.OpenFile(ackPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		fail("open ack: %v", err)
	}

	updates := make([]logdb.Update, len(crashShardIDs))
	deadline := time.Now().Add(crashChildDeadline)
	for iter := uint64(0); time.Now().Before(deadline); iter++ {
		first := iter*crashBatchSize + 1
		last := first + crashBatchSize - 1
		term := crashTermForIndex(first)

		entries := make([]logdb.Entry, 0, crashBatchSize)
		for idx := first; idx <= last; idx++ {
			entries = append(entries, crashEntry(idx))
		}
		for i, shardID := range crashShardIDs {
			updates[i] = logdb.Update{
				ShardID:   shardID,
				ReplicaID: crashReplicaID,
				State: logdb.State{
					Term:   term,
					Vote:   crashVoteForTerm(term),
					Commit: last,
				},
				Entries: entries,
			}
		}

		if err := db.SaveState(updates); err != nil {
			fail("SaveState at index %d: %v", last, err)
		}

		// The ack is only written after SaveState reports durability, and is
		// itself fsynced, so every line the parent reads is a promise the WAL
		// made and must keep.
		line := fmt.Sprintf("%d %d %d\n", last, term, crashVoteForTerm(term))
		if _, err := ack.WriteString(line); err != nil {
			fail("ack write: %v", err)
		}
		if err := ack.Sync(); err != nil {
			fail("ack sync: %v", err)
		}
	}

	// Reaching the deadline means the parent never killed us; the run proved
	// nothing about crash recovery.
	fail("deadline reached without being killed")
}

// crashAck is one durability promise the child observed.
type crashAck struct {
	index uint64
	term  uint64
	vote  uint64
}

// readCrashAcks parses the complete lines of the ack file. A trailing partial
// line (the child died mid-write) is ignored.
func readCrashAcks(path string) ([]crashAck, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var acks []crashAck
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		var parsed [3]uint64
		ok := true
		for i, f := range fields {
			v, perr := strconv.ParseUint(f, 10, 64)
			if perr != nil {
				ok = false
				break
			}
			parsed[i] = v
		}
		if !ok {
			continue
		}
		acks = append(acks, crashAck{index: parsed[0], term: parsed[1], vote: parsed[2]})
	}
	return acks, nil
}

// waitForAcks blocks until the ack file holds at least n complete lines, so
// the kill window starts from a known amount of durable data rather than from
// process startup, whose cost varies with machine load.
func waitForAcks(t *testing.T, path string, n int) {
	t.Helper()
	deadline := time.Now().Add(crashAckTimeout)
	for time.Now().Before(deadline) {
		acks, err := readCrashAcks(path)
		if err == nil && len(acks) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("child produced fewer than %d durable acks within %v", n, crashAckTimeout)
}

// crashVerifyNode asserts the recovery invariants for one node and returns
// its highest recovered index.
//
//	(1) every acked index is present and readable;
//	(2) the recovered range is contiguous and byte-identical to what the
//	    child wrote -- no torn or garbage entry is served;
//	(3) the hard state never regresses below the last acked term.
func crashVerifyNode(t *testing.T, db *DB, shardID uint64, ack crashAck) uint64 {
	t.Helper()

	first, span, err := db.EntryRange(shardID, crashReplicaID)
	if err != nil {
		t.Fatalf("shard %d: EntryRange: %v", shardID, err)
	}
	if first != 1 {
		t.Fatalf("shard %d: first index %d, want 1 (prefix loss)", shardID, first)
	}
	last := first + span - 1
	if last < ack.index {
		t.Fatalf("shard %d: recovered through index %d but %d was acked durable",
			shardID, last, ack.index)
	}

	entries, _, err := db.IterateEntries(nil, 0, shardID, crashReplicaID, first, last+1, 0)
	if err != nil {
		t.Fatalf("shard %d: IterateEntries [%d,%d]: %v", shardID, first, last, err)
	}
	if uint64(len(entries)) != span {
		t.Fatalf("shard %d: read %d entries for span %d", shardID, len(entries), span)
	}
	for i, got := range entries {
		want := crashEntry(first + uint64(i))
		if got.Index != want.Index || got.Term != want.Term || got.Type != want.Type ||
			got.Key != want.Key || got.ClientID != want.ClientID ||
			got.SeriesID != want.SeriesID || got.RespondedTo != want.RespondedTo ||
			!bytes.Equal(got.Cmd, want.Cmd) {
			t.Fatalf("shard %d: entry at position %d is not what was written:\n got %+v (cmd %d bytes)\nwant %+v (cmd %d bytes)",
				shardID, i, redactCmd(got), len(got.Cmd), redactCmd(want), len(want.Cmd))
		}
	}

	state, _, err := db.ReadState(shardID, crashReplicaID)
	if err != nil {
		t.Fatalf("shard %d: ReadState: %v", shardID, err)
	}
	if state.Term < ack.term {
		t.Fatalf("shard %d: term regressed to %d after crash, %d was acked durable",
			shardID, state.Term, ack.term)
	}
	if state.Vote != crashVoteForTerm(state.Term) {
		t.Fatalf("shard %d: term %d recovered with vote %d, want %d (franken hard state)",
			shardID, state.Term, state.Vote, crashVoteForTerm(state.Term))
	}
	if state.Commit < ack.index {
		t.Fatalf("shard %d: commit regressed to %d, %d was acked durable",
			shardID, state.Commit, ack.index)
	}
	return last
}

// redactCmd drops the Cmd payload so failure output stays readable.
func redactCmd(e logdb.Entry) logdb.Entry {
	e.Cmd = nil
	return e
}

// TestWALCrashConsistency SIGKILLs a real WAL writer at several points in the
// write pipeline and asserts the reopened WAL keeps every promise the killed
// process made.
//
// PROCESS death, not power loss, and the difference is the whole limitation:
// SIGKILL destroys the process and leaves the page cache untouched, so every
// byte the child handed to write(2) is still readable afterwards whether or
// not it was ever fsynced. This test therefore cannot detect a deleted flush —
// stub osFile.Sync to return nil and all of it still passes. It tests the
// property its name states, crash CONSISTENCY: that what survives is a valid
// prefix and that recovery neither loses nor invents records around the tear.
// Durability across a real power loss is asserted elsewhere, by
// TestWALCrashDurability_AckedWritesSurviveModelledPowerLoss (which models the
// medium) and TestOSFileSyncIssuesRealSyscall (which proves the flush is a
// syscall).
func TestWALCrashConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns writer subprocesses; skipped under -short")
	}
	if runtime.GOOS == "windows" {
		t.Skip("kill semantics differ on windows")
	}
	if _, err := os.Stat(os.Args[0]); err != nil {
		t.Skipf("test binary %q not executable for re-invocation: %v", os.Args[0], err)
	}

	for _, delay := range crashKillDelays {
		t.Run(fmt.Sprintf("plain_kill_after_%s", delay), func(t *testing.T) {
			runCrashIteration(t, delay, false)
		})
	}
	// A crash must not leave committed data undecryptable.
	t.Run("encrypted_kill_after_47ms", func(t *testing.T) {
		runCrashIteration(t, 47*time.Millisecond, true)
	})
}

// crashShardDir returns the WAL sub-directory backing a Raft shard.
func crashShardDir(walDir string, shardID uint64) string {
	return filepath.Join(walDir, shardDirName(int(shardID%DefaultShardCount)))
}

// crashSegmentIDs returns the segment IDs present in a shard directory,
// highest first.
func crashSegmentIDs(t *testing.T, shardDir string) []uint64 {
	t.Helper()
	dirEntries, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("read shard dir %s: %v", shardDir, err)
	}
	var ids []uint64
	for _, de := range dirEntries {
		if segID, ok := parseSegmentFilename(de.Name()); ok {
			ids = append(ids, segID)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	return ids
}

// crashTearPoint returns the start offset of the last block in a segment that
// is preceded by at least one complete record, or -1 when the segment has
// none. Tearing at a block start makes the expected clean end exactly that
// offset (decodeRecord fails at chunk offset zero) and guarantees the clean
// prefix is non-empty.
func crashTearPoint(t *testing.T, data []byte) int64 {
	t.Helper()
	best := int64(-1)
	completed := 0
	for blockStart := 0; blockStart < len(data); blockStart += crashBlockSize {
		end := blockStart + crashBlockSize
		if end > len(data) {
			end = len(data)
		}
		block := data[blockStart:end]
		// A zero-headed block is the segment's pre-allocated tail.
		if len(block) < recordHeaderSize+1 || block[0] == 0 {
			break
		}
		if completed > 0 {
			best = int64(blockStart)
		}
		for pos := 0; pos+recordHeaderSize < len(block) && block[pos] != 0; {
			_, recType, _, n, err := decodeRecord(block[pos:])
			if err != nil {
				t.Fatalf("segment already corrupt at offset %d before tearing: %v",
					blockStart+pos, err)
			}
			if recType == RecordFull || recType == RecordLast {
				completed++
			}
			pos += n
		}
	}
	return best
}

// crashTearSegment damages one block of the WAL the killed writer left behind,
// reproducing what a power loss adds on top of the process death: a partially
// persisted block whose record no longer passes CRC.
//
// A torn tail is only ever valid in the FINAL segment: recovery fails-stop on
// corruption in any lower segment (a lower segment was fully fdatasynced before
// a higher one was created, so its corruption is bit rot, not a torn tail).
// The kill can leave a higher, incompletely written active segment with no
// complete record to tear; those higher segments are deleted here so the torn
// segment becomes the genuine final one — the only place prefix-truncation
// recovery is valid. Returns the torn segment's path and the offset recovery
// must truncate it back to.
func crashTearSegment(t *testing.T, shardDir string) (string, int64) {
	t.Helper()
	segIDs := crashSegmentIDs(t, shardDir) // highest first
	for i, segID := range segIDs {
		path := filepath.Join(shardDir, segmentFilename(segID))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read segment %s: %v", path, err)
		}
		tearAt := crashTearPoint(t, data)
		if tearAt < 0 {
			continue
		}
		// Delete every higher segment so the torn one is genuinely final.
		for _, higher := range segIDs[:i] {
			if err := os.Remove(filepath.Join(shardDir, segmentFilename(higher))); err != nil {
				t.Fatalf("remove higher orphan segment %d: %v", higher, err)
			}
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatalf("open segment for tearing: %v", err)
		}
		// Destroy the block from the tear point to its end, not a single byte
		// inside the first record. A torn tail is a block the medium did not
		// finish taking, so NOTHING below the tear survives; flipping one byte
		// and leaving the records after it intact models interior rot instead —
		// bytes that reached the medium and passed CRC — which recovery must
		// refuse rather than truncate (ErrMidSegmentCorruption). That case has
		// its own test; this one must stay a genuine torn tail.
		//
		// tearAt is a block start and crashTearPoint returns the LAST
		// data-bearing block, so this fills exactly the final live block. 0xff
		// is deterministic and cannot decode (0xff is not a valid record type)
		// and is not zero, so it is read as corruption rather than as the
		// pre-allocated zero tail or a write hole.
		tearEnd := tearAt + int64(crashBlockSize)
		if tearEnd > int64(len(data)) {
			tearEnd = int64(len(data))
		}
		torn := bytes.Repeat([]byte{0xff}, int(tearEnd-tearAt))
		if _, err := f.WriteAt(torn, tearAt); err != nil {
			t.Fatalf("tear write: %v", err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("tear sync: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("tear close: %v", err)
		}
		return path, tearAt
	}
	t.Fatalf("no segment in %s holds a complete record to tear after", shardDir)
	return "", 0
}

// TestWALCrashTornTailRepairedAcrossRestarts kills a real writer and then
// tears the final block it wrote, the way an unlucky power loss would. The
// first reopen must replay the clean prefix AND repair the segment on disk;
// if the corrupt tail survives, the next restart re-detects it and discards
// every segment written since -- silently deleting acknowledged data on every
// boot. That recurrence, not the initial truncation, is the regression.
func TestWALCrashTornTailRepairedAcrossRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a writer subprocess; skipped under -short")
	}
	if runtime.GOOS == "windows" {
		t.Skip("kill semantics differ on windows")
	}
	if _, err := os.Stat(os.Args[0]); err != nil {
		t.Skipf("test binary %q not executable for re-invocation: %v", os.Args[0], err)
	}

	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	ackPath := filepath.Join(root, "ack")

	// Three durable batches guarantee the WAL holds a block that follows a
	// complete record, which is what crashTearSegment needs to tear.
	runCrashChildProcess(t, walDir, ackPath, "", 3, 120*time.Millisecond)

	tornShard := crashShardIDs[0]
	shardDir := crashShardDir(walDir, tornShard)
	segPath, cleanEnd := crashTearSegment(t, shardDir)

	// Segments above the torn one are unreachable after the repair and must be
	// deleted; an intact orphan replays its stale hard state on the next
	// restart. Recovery's replacement active segment gets an ID above every
	// observed one, so it can never be mistaken for a surviving orphan.
	tornID, _ := parseSegmentFilename(filepath.Base(segPath))
	orphans := make(map[uint64]bool)
	for _, segID := range crashSegmentIDs(t, shardDir) {
		if segID > tornID {
			orphans[segID] = true
		}
	}

	db, err := crashOpenDB(walDir, nil)
	if err != nil {
		t.Fatalf("reopen after torn tail: %v", err)
	}
	if size := statSize(t, segPath); size != cleanEnd {
		t.Fatalf("torn segment is %d bytes after recovery, want %d (corrupt tail not repaired)",
			size, cleanEnd)
	}
	for _, segID := range crashSegmentIDs(t, shardDir) {
		if orphans[segID] {
			t.Fatalf("orphan segment %d survived the truncation recovery", segID)
		}
	}

	// The torn shard keeps a prefix; the untouched shards keep everything.
	survived := make(map[uint64]uint64, len(crashShardIDs))
	for _, shardID := range crashShardIDs {
		first, span, err := db.EntryRange(shardID, crashReplicaID)
		if err != nil {
			t.Fatalf("shard %d: EntryRange after torn tail: %v", shardID, err)
		}
		if span == 0 {
			t.Fatalf("shard %d: no entries survived the torn tail", shardID)
		}
		if first != 1 {
			t.Fatalf("shard %d: first index %d, want 1 (prefix loss)", shardID, first)
		}
		last := first + span - 1
		entries, _, err := db.IterateEntries(nil, 0, shardID, crashReplicaID, first, last+1, 0)
		if err != nil {
			t.Fatalf("shard %d: IterateEntries after torn tail: %v", shardID, err)
		}
		for i, got := range entries {
			want := crashEntry(first + uint64(i))
			if got.Index != want.Index || got.Term != want.Term || !bytes.Equal(got.Cmd, want.Cmd) {
				t.Fatalf("shard %d: torn entry served at position %d: got %+v, want %+v",
					shardID, i, redactCmd(got), redactCmd(want))
			}
		}
		survived[shardID] = last
	}

	// Append past the repair, restart, and require the new records to survive.
	const resumeTerm = uint64(500)
	newLasts := make(map[uint64]uint64, len(crashShardIDs))
	for _, shardID := range crashShardIDs {
		start := survived[shardID] + 1
		end := start + crashBatchSize - 1
		entries := make([]logdb.Entry, 0, crashBatchSize)
		for idx := start; idx <= end; idx++ {
			e := crashEntry(idx)
			e.Term = resumeTerm
			entries = append(entries, e)
		}
		update := logdb.Update{
			ShardID:   shardID,
			ReplicaID: crashReplicaID,
			State:     logdb.State{Term: resumeTerm, Vote: crashVoteForTerm(resumeTerm), Commit: end},
			Entries:   entries,
		}
		if err := db.SaveState([]logdb.Update{update}); err != nil {
			t.Fatalf("shard %d: SaveState after repair: %v", shardID, err)
		}
		newLasts[shardID] = end
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after repair writes: %v", err)
	}

	db2, err := crashOpenDB(walDir, nil)
	if err != nil {
		t.Fatalf("restart after repair: %v", err)
	}
	defer db2.Close()
	for _, shardID := range crashShardIDs {
		first, span, err := db2.EntryRange(shardID, crashReplicaID)
		if err != nil {
			t.Fatalf("shard %d: EntryRange after restart: %v", shardID, err)
		}
		if first != 1 || first+span-1 != newLasts[shardID] {
			t.Fatalf("shard %d: restart holds [%d,%d], want [1,%d] (repair did not stick: the corrupt tail was re-detected and newer segments were discarded)",
				shardID, first, first+span-1, newLasts[shardID])
		}
		state, _, err := db2.ReadState(shardID, crashReplicaID)
		if err != nil {
			t.Fatalf("shard %d: ReadState after restart: %v", shardID, err)
		}
		if state.Term != resumeTerm || state.Vote != crashVoteForTerm(resumeTerm) {
			t.Fatalf("shard %d: hard state (%d,%d) after restart, want (%d,%d)",
				shardID, state.Term, state.Vote, resumeTerm, crashVoteForTerm(resumeTerm))
		}
	}
}

// runCrashChildProcess spawns the writer child, SIGKILLs it delay after its
// first durable ack, and returns the last promise the child made. The kill is
// unconditional: no graceful stop, no Close, no flush of whatever the writer
// had buffered.
func runCrashChildProcess(t *testing.T, walDir, ackPath, sealPath string, minAcks int, delay time.Duration) crashAck {
	t.Helper()

	env := append(os.Environ(),
		crashChildEnv+"=1",
		crashDirEnv+"="+walDir,
		crashAckEnv+"="+ackPath,
		crashSealEnv+"="+sealPath,
	)
	var out bytes.Buffer
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+crashChildTest+"$")
	cmd.Env = env
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	waitForAcks(t, ackPath, minAcks)
	time.Sleep(delay)
	_ = cmd.Process.Kill()
	waitErr := cmd.Wait()
	if code := cmd.ProcessState.ExitCode(); code != -1 {
		t.Fatalf("child exited with code %d instead of dying by signal: %v\n%s",
			code, waitErr, out.String())
	}
	if strings.Contains(out.String(), "panic:") {
		t.Fatalf("child panicked:\n%s", out.String())
	}

	acks, err := readCrashAcks(ackPath)
	if err != nil {
		t.Fatalf("read acks: %v", err)
	}
	if len(acks) == 0 {
		t.Fatal("no durable ack recorded")
	}
	ack := acks[len(acks)-1]
	if ack.index < crashBatchSize {
		t.Fatalf("last ack index %d below one batch", ack.index)
	}
	return ack
}

// runCrashIteration runs one crash cycle: spawn, kill, reopen, verify,
// write more, reopen again, verify again.
func runCrashIteration(t *testing.T, delay time.Duration, encrypted bool) {
	t.Helper()

	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	ackPath := filepath.Join(root, "ack")

	var barrier WALBarrier
	sealPath := ""
	if encrypted {
		// The parent initializes the barrier so the child only ever restores
		// it; a child killed mid-initialization would otherwise leave a
		// half-written sealed key that says nothing about WAL recovery.
		sealPath = filepath.Join(root, "sealed.json")
		b, err := crashBarrier(sealPath)
		if err != nil {
			t.Fatalf("crashBarrier: %v", err)
		}
		barrier = b
	}

	ack := runCrashChildProcess(t, walDir, ackPath, sealPath, 1, delay)

	if encrypted {
		// Re-load the barrier from the sealed state the CHILD left behind.
		// The parent stands in for a RESTARTING process here, and a restarting
		// process re-reads that state — holding the pre-child barrier object
		// instead assumes no other process ever moved the epoch on.
		//
		// That assumption is now false, and was always fragile: restoring a
		// barrier rotates, because the spent AEAD invocation budget of the
		// loaded epoch is unknowable across a restart and reusing it would
		// silently erode the 2^-32 nonce-collision bound. So the child writes
		// its records under an epoch the parent's stale object has never held a
		// DEK for, and every record fails to decrypt. Reverting seal to before
		// the rotation reproduces the identical failure the moment anything
		// calls Rotate — including the host's own rotation threshold — so this
		// is the harness modelling a restart incorrectly, not a WAL defect.
		b, berr := crashBarrier(sealPath)
		if berr != nil {
			t.Fatalf("crashBarrier (post-child reload): %v", berr)
		}
		barrier = b
	}

	// Invariant 4a: a crash mid-write must not produce an unopenable WAL.
	db, err := crashOpenDB(walDir, barrier)
	if err != nil {
		t.Fatalf("reopen after SIGKILL (last ack %d): %v", ack.index, err)
	}

	lasts := make(map[uint64]uint64, len(crashShardIDs))
	maxTerm := ack.term
	for _, shardID := range crashShardIDs {
		lasts[shardID] = crashVerifyNode(t, db, shardID, ack)
		state, _, serr := db.ReadState(shardID, crashReplicaID)
		if serr != nil {
			t.Fatalf("shard %d: ReadState: %v", shardID, serr)
		}
		if state.Term > maxTerm {
			maxTerm = state.Term
		}
	}

	// Invariant 4b: append past the repaired tail, restart, and require both
	// the new records and the killed run's records to survive. If recovery
	// left the corrupt tail unrepaired, this second reopen re-detects it and
	// discards every segment written since -- the regression this guards.
	resumeTerm := maxTerm + 1
	newLasts := make(map[uint64]uint64, len(crashShardIDs))
	for _, shardID := range crashShardIDs {
		start := lasts[shardID] + 1
		end := start + 2*crashBatchSize - 1
		entries := make([]logdb.Entry, 0, end-start+1)
		for idx := start; idx <= end; idx++ {
			e := crashEntry(idx)
			e.Term = resumeTerm
			entries = append(entries, e)
		}
		update := logdb.Update{
			ShardID:   shardID,
			ReplicaID: crashReplicaID,
			State: logdb.State{
				Term:   resumeTerm,
				Vote:   crashVoteForTerm(resumeTerm),
				Commit: end,
			},
			Entries: entries,
		}
		if err := db.SaveState([]logdb.Update{update}); err != nil {
			t.Fatalf("shard %d: post-recovery SaveState: %v", shardID, err)
		}
		newLasts[shardID] = end
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after post-recovery writes: %v", err)
	}

	db2, err := crashOpenDB(walDir, barrier)
	if err != nil {
		t.Fatalf("second reopen: %v", err)
	}
	defer db2.Close()

	for _, shardID := range crashShardIDs {
		first, span, err := db2.EntryRange(shardID, crashReplicaID)
		if err != nil {
			t.Fatalf("shard %d: EntryRange after second reopen: %v", shardID, err)
		}
		if first != 1 || first+span-1 != newLasts[shardID] {
			t.Fatalf("shard %d: second reopen holds [%d,%d], want [1,%d] (data written after recovery was discarded)",
				shardID, first, first+span-1, newLasts[shardID])
		}
		entries, _, err := db2.IterateEntries(nil, 0, shardID, crashReplicaID, 1, newLasts[shardID]+1, 0)
		if err != nil {
			t.Fatalf("shard %d: IterateEntries after second reopen: %v", shardID, err)
		}
		if uint64(len(entries)) != newLasts[shardID] {
			t.Fatalf("shard %d: read %d entries, want %d", shardID, len(entries), newLasts[shardID])
		}
		for i, got := range entries {
			idx := uint64(i) + 1
			want := crashEntry(idx)
			if idx > lasts[shardID] {
				want.Term = resumeTerm
			}
			if got.Index != want.Index || got.Term != want.Term ||
				!bytes.Equal(got.Cmd, want.Cmd) {
				t.Fatalf("shard %d: entry %d corrupted across the second reopen:\n got %+v\nwant %+v",
					shardID, idx, redactCmd(got), redactCmd(want))
			}
		}
		state, _, err := db2.ReadState(shardID, crashReplicaID)
		if err != nil {
			t.Fatalf("shard %d: ReadState after second reopen: %v", shardID, err)
		}
		if state.Term != resumeTerm || state.Vote != crashVoteForTerm(resumeTerm) {
			t.Fatalf("shard %d: hard state (%d,%d) after second reopen, want (%d,%d)",
				shardID, state.Term, state.Vote, resumeTerm, crashVoteForTerm(resumeTerm))
		}
		if state.Commit != newLasts[shardID] {
			t.Fatalf("shard %d: commit %d after second reopen, want %d",
				shardID, state.Commit, newLasts[shardID])
		}
	}
}
