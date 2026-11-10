package waldb

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestSaveState_BatchLargerThanOneRecord verifies that a batch whose framed
// size exceeds a single WAL record is split across records instead of failing
// permanently.
//
// If one Update always became exactly one record, it would have to fit inside
// one segment. The usable payload is strictly less than the segment size (a
// record spends recordHeaderSize in every block it spans), and the WAL adds a
// 84-byte header per entry that raft's batch cap does not account for. So raft
// could hand the WAL a batch it would reject with ErrRecordTooLarge -- a
// PERMANENT error, deliberately not retried or rotated -- and the node could
// never persist it. A lagging follower catching up on many small entries is
// the realistic trigger.
//
// The split records must also replay: the format tolerates several entry
// records per Update, which is what makes chunking safe rather than a format
// change. So the batch is read back twice, from the live store and again
// after a reopen.
func TestSaveState_BatchLargerThanOneRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	// A small segment keeps the test fast; the ratio is what matters, not the
	// absolute size.
	const blockSize = 4096
	const maxFile = 256 * 1024

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(maxFile))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Build a batch several times larger than one record can hold, using many
	// small entries so the per-entry header overhead dominates.
	const entryCount = 2000
	const cmdLen = 256
	entries := make([]logdb.Entry, entryCount)
	for i := range entries {
		cmd := make([]byte, cmdLen)
		for j := range cmd {
			cmd[j] = byte(i)
		}
		entries[i] = logdb.Entry{
			Index: uint64(i + 1),
			Term:  1,
			Cmd:   cmd,
		}
	}

	framed := 1 + entryCount*(entryRecordHeaderSize+cmdLen)
	usable := (maxFile/blockSize - 1) * (blockSize - recordHeaderSize)
	if framed <= usable {
		t.Fatalf("test is not exercising the split: framed batch %d bytes fits "+
			"one record of %d bytes", framed, usable)
	}

	if err := db.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 1, Vote: 1, Commit: entryCount},
		Entries:   entries,
	}}); err != nil {
		t.Fatalf("SaveState failed for a batch raft can legitimately produce: %v", err)
	}

	// Every entry must be readable back, in order, with its payload intact,
	// and the hard state written after the entries must have survived with
	// them.
	verify := func(t *testing.T, db *DB, when string) {
		t.Helper()
		got, _, err := db.IterateEntries(nil, 0, 1, 1, 1, entryCount+1, 1<<30)
		if err != nil {
			t.Fatalf("%s: IterateEntries failed: %v", when, err)
		}
		if len(got) != entryCount {
			t.Fatalf("%s: read back %d entries, want %d (records split across "+
				"one Update must all be kept)", when, len(got), entryCount)
		}
		for i := range got {
			if got[i].Index != uint64(i+1) {
				t.Fatalf("%s: entry %d has index %d, want %d", when, i, got[i].Index, i+1)
			}
			if len(got[i].Cmd) != cmdLen || got[i].Cmd[0] != byte(i) {
				t.Fatalf("%s: entry %d payload corrupted", when, i)
			}
		}
		state, _, err := db.ReadState(1, 1)
		if err != nil {
			t.Fatalf("%s: ReadState failed: %v", when, err)
		}
		if state.Commit != entryCount {
			t.Errorf("%s: state.Commit = %d, want %d", when, state.Commit, entryCount)
		}
	}
	verify(t, db, "before reopen")

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	reopened, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(maxFile))
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	verify(t, reopened, "after reopen")
}

// TestSaveState_SingleEntryLargerThanRecordStillFails verifies the permanent
// error is preserved for a genuinely unpersistable entry: chunking must not
// turn a hard failure into an infinite split loop.
func TestSaveState_SingleEntryLargerThanRecordStillFails(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	const blockSize = 4096
	const maxFile = 64 * 1024

	db, err := Open(dir, WithBlockSize(blockSize), WithMaxFileSize(maxFile), WithNoSync(true))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	usable := (maxFile/blockSize - 1) * (blockSize - recordHeaderSize)
	err = db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State:   logdb.State{Term: 1, Commit: 1},
		Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: make([]byte, usable+1)}},
	}})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("error = %v, want ErrRecordTooLarge for a lone entry that "+
			"cannot fit any record", err)
	}
}
