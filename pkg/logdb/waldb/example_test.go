package waldb_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
)

// ExampleOpen opens a WAL with a non-default segment size, saves one update,
// and reads the hard state back after a reopen. It is the open snippet of
// docs/log-storage/user.md ("Size it").
func ExampleOpen() {
	walDir, err := os.MkdirTemp("", "waldb-example-open-")
	if err != nil {
		fmt.Println("MkdirTemp:", err)
		return
	}
	defer os.RemoveAll(walDir)

	store, err := waldb.Open(walDir, waldb.WithMaxFileSize(256<<20))
	if err != nil {
		fmt.Println("Open:", err)
		return
	}
	err = store.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 2, Vote: 1, Commit: 3},
		Entries: []logdb.Entry{
			{Index: 1, Term: 2, Cmd: []byte("a")},
			{Index: 2, Term: 2, Cmd: []byte("b")},
			{Index: 3, Term: 2, Cmd: []byte("c")},
		},
	}})
	if err != nil {
		fmt.Println("SaveState:", err)
		return
	}
	if err := store.Close(); err != nil {
		fmt.Println("Close:", err)
		return
	}

	store, err = waldb.Open(walDir, waldb.WithMaxFileSize(256<<20))
	if err != nil {
		fmt.Println("reopen:", err)
		return
	}
	defer store.Close()
	state, _, err := store.ReadState(1, 1)
	if err != nil {
		fmt.Println("ReadState:", err)
		return
	}
	fmt.Printf("term=%d vote=%d commit=%d\n", state.Term, state.Vote, state.Commit)
	// Output:
	// term=2 vote=1 commit=3
}

// ExampleRepair damages the second block of a written segment, shows Open
// refusing it with a *waldb.SegmentCorruptionError, repairs it with the
// offset the error named, and reopens on the records before the damage. It
// is the repair snippet of docs/log-storage/user.md ("Recover a damaged
// segment").
func ExampleRepair() {
	const (
		blockSize = 1024
		shardID   = uint64(1)
	)
	walDir, err := os.MkdirTemp("", "waldb-example-repair-")
	if err != nil {
		fmt.Println("MkdirTemp:", err)
		return
	}
	defer os.RemoveAll(walDir)
	opts := []waldb.Option{waldb.WithBlockSize(blockSize), waldb.WithMaxFileSize(64 << 10)}

	store, err := waldb.Open(walDir, opts...)
	if err != nil {
		fmt.Println("Open:", err)
		return
	}
	for i := uint64(1); i <= 15; i++ {
		e := logdb.Entry{Index: i, Term: 1, Cmd: bytes.Repeat([]byte{byte(i)}, 200)}
		if err := store.SaveEntries(shardID, 1, []logdb.Entry{e}); err != nil {
			fmt.Println("SaveEntries:", err)
			return
		}
	}
	if err := store.Close(); err != nil {
		fmt.Println("Close:", err)
		return
	}

	// Flip one byte in the second block of shard 1's first segment.
	segPath := filepath.Join(walDir, "shard-1", "000000.log")
	f, err := os.OpenFile(segPath, os.O_RDWR, 0o600)
	if err != nil {
		fmt.Println("OpenFile:", err)
		return
	}
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, blockSize+100); err != nil {
		fmt.Println("ReadAt:", err)
		return
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, blockSize+100); err != nil {
		fmt.Println("WriteAt:", err)
		return
	}
	if err := f.Close(); err != nil {
		fmt.Println("close segment:", err)
		return
	}

	_, openErr := waldb.Open(walDir, opts...)
	var corrupt *waldb.SegmentCorruptionError
	if !errors.As(openErr, &corrupt) {
		fmt.Println("Open error is not a *SegmentCorruptionError:", openErr)
		return
	}
	fmt.Printf("open refused: shard index %d, segment %d, clean end %d, mid-segment %t\n",
		corrupt.ShardIndex, corrupt.SegmentID, corrupt.CleanEnd,
		errors.Is(openErr, waldb.ErrMidSegmentCorruption))

	res, err := waldb.Repair(walDir, waldb.RepairTarget{
		ShardIndex: corrupt.ShardIndex,
		SegmentID:  corrupt.SegmentID,
		TruncateAt: corrupt.CleanEnd,
	}, waldb.WithBlockSize(blockSize))
	if err != nil {
		fmt.Println("Repair:", err)
		return
	}
	fmt.Printf("repaired: kept %d records, discarded bytes %t, deleted %d segments\n",
		res.RecordsKept, res.BytesDiscarded > 0, len(res.SegmentsDeleted))

	store, err = waldb.Open(walDir, opts...)
	if err != nil {
		fmt.Println("reopen:", err)
		return
	}
	defer store.Close()
	entries, err := store.GetEntries(shardID, 1, 1, 16, 1<<20)
	if err != nil {
		fmt.Println("GetEntries:", err)
		return
	}
	intact := true
	for i, e := range entries {
		if e.Index != uint64(i+1) || !bytes.Equal(e.Cmd, bytes.Repeat([]byte{byte(e.Index)}, 200)) {
			intact = false
		}
	}
	fmt.Printf("reopened: %d entries before the damage, intact %t\n", len(entries), intact)
	// Output:
	// open refused: shard index 1, segment 0, clean end 1024, mid-segment true
	// repaired: kept 3 records, discarded bytes true, deleted 0 segments
	// reopened: 3 entries before the damage, intact true
}
