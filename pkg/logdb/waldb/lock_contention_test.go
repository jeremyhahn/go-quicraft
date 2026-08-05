package waldb

import (
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// slowSyncFS wraps a filesystem and blocks inside Sync until released, so a
// test can hold a SaveState inside its fsync and observe what else can run.
type slowSyncFS struct {
	FS
	inSync  chan struct{}
	release chan struct{}
	armed   atomic.Bool
	once    sync.Once
}

func (s *slowSyncFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	f, err := s.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &slowSyncFile{File: f, owner: s}, nil
}

type slowSyncFile struct {
	File
	owner *slowSyncFS
}

func (f *slowSyncFile) Sync() error {
	if f.owner.armed.Load() {
		f.owner.once.Do(func() { close(f.owner.inSync) })
		<-f.owner.release
	}
	return f.File.Sync()
}

// TestReadsDoNotBlockOnFsync proves reads are served while a concurrent
// SaveState is stalled inside its fsync.
//
// walShard.mu guards the segment file and is held by SaveState across
// write -> fsync -> commit, which is load-bearing: releasing it between the
// durable write and a rollback previously let a concurrent same-shard batch
// commit past this batch's checkpoint, and the rollback then truncated the
// segment and erased acknowledged records. But readers used to take that SAME
// mutex, so every read for every raft group on the shard waited out the fsync.
// The in-memory maps readers consult are now guarded separately by dataMu,
// which SaveState takes only for its brief Phase 3 commit.
func TestReadsDoNotBlockOnFsync(t *testing.T) {
	slow := &slowSyncFS{
		FS:      osFS{},
		inSync:  make(chan struct{}),
		release: make(chan struct{}),
	}

	db, err := Open(filepath.Join(t.TempDir(), "wal"), WithFS(slow))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Seed a node so there is something to read back.
	if err := db.SaveState([]logdb.Update{{
		ShardID: 1, ReplicaID: 1,
		State:   logdb.State{Term: 1, Vote: 1, Commit: 1},
		Entries: []logdb.Entry{{Index: 1, Term: 1, Cmd: []byte("seed")}},
	}}); err != nil {
		t.Fatalf("seed SaveState failed: %v", err)
	}

	// Arm the stall and start a batch that will park inside fsync.
	slow.armed.Store(true)
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- db.SaveState([]logdb.Update{{
			ShardID: 1, ReplicaID: 1,
			State:   logdb.State{Term: 2, Vote: 1, Commit: 2},
			Entries: []logdb.Entry{{Index: 2, Term: 2, Cmd: []byte("blocked")}},
		}})
	}()

	select {
	case <-slow.inSync:
	case <-time.After(10 * time.Second):
		t.Fatal("writer never reached fsync")
	}

	// The writer is now inside fsync holding shard.mu. Every reader must
	// still complete. Before the lock split these calls blocked until the
	// fsync returned.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		if _, _, err := db.ReadState(1, 1); err != nil {
			t.Errorf("ReadState during fsync: %v", err)
		}
		if _, _, err := db.EntryRange(1, 1); err != nil {
			t.Errorf("EntryRange during fsync: %v", err)
		}
		if _, _, err := db.IterateEntries(nil, 0, 1, 1, 1, 2, 1<<20); err != nil {
			t.Errorf("IterateEntries during fsync: %v", err)
		}
		// ListNodeInfo walks every shard; it used to take all 16 mutexes and
		// could serialize behind up to 16 in-flight fsyncs.
		if _, err := db.ListNodeInfo(); err != nil {
			t.Errorf("ListNodeInfo during fsync: %v", err)
		}
	}()

	select {
	case <-readDone:
	case <-time.After(10 * time.Second):
		close(slow.release)
		t.Fatal("reads blocked on the writer's fsync — the shard lock is still " +
			"shared between the segment write path and the in-memory read path")
	}

	// The commit must NOT be visible yet: the writer has not passed Phase 3.
	if state, _, err := db.ReadState(1, 1); err == nil && state.Commit != 1 {
		t.Errorf("state.Commit = %d during an unfinished fsync, want 1 — reads "+
			"must not observe a batch before it is durable", state.Commit)
	}

	close(slow.release)
	if err := <-writeDone; err != nil {
		t.Fatalf("blocked SaveState failed: %v", err)
	}

	// And it must be visible once the batch commits.
	state, _, err := db.ReadState(1, 1)
	if err != nil {
		t.Fatalf("ReadState after commit: %v", err)
	}
	if state.Commit != 2 {
		t.Errorf("state.Commit = %d after commit, want 2", state.Commit)
	}
}

// TestConcurrentReadersAndWriters exercises the split locks under -race with
// several raft groups deliberately hashed onto one WAL shard, mixing every
// mutator against continuous readers. A reader must never observe a
// half-applied batch: ErrLogGapDetected is the signature of one.
func TestConcurrentReadersAndWriters(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "wal"), WithNoSync(true))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Shard IDs congruent mod DefaultShardCount share one WAL shard, which is
	// the contention this test is about.
	groups := []uint64{1, 1 + DefaultShardCount, 1 + 2*DefaultShardCount}

	const rounds = 60
	var writers sync.WaitGroup
	var readers sync.WaitGroup
	stop := make(chan struct{})

	for _, g := range groups {
		writers.Add(1)
		go func(shardID uint64) {
			defer writers.Done()
			for i := 1; i <= rounds; i++ {
				idx := uint64(i)
				if err := db.SaveState([]logdb.Update{{
					ShardID: shardID, ReplicaID: 1,
					State:   logdb.State{Term: 1, Vote: 1, Commit: idx},
					Entries: []logdb.Entry{{Index: idx, Term: 1, Cmd: []byte("x")}},
				}}); err != nil {
					t.Errorf("SaveState(shard %d, index %d): %v", shardID, idx, err)
					return
				}
				if i%20 == 0 {
					if err := db.SaveSnapshot(shardID, 1, logdb.Snapshot{
						Index: idx, Term: 1,
					}); err != nil {
						t.Errorf("SaveSnapshot(shard %d): %v", shardID, err)
						return
					}
				}
			}
		}(g)
	}

	// Readers hammer every accessor for the duration.
	for r := 0; r < 6; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, g := range groups {
					_, _, _ = db.ReadState(g, 1)
					first, _, err := db.EntryRange(g, 1)
					if err != nil {
						continue
					}
					if _, _, err := db.IterateEntries(nil, 0, g, 1, first, first+5, 1<<20); err == logdb.ErrLogGapDetected {
						t.Errorf("reader saw a half-applied batch on shard %d "+
							"(ErrLogGapDetected)", g)
						return
					}
					_, _ = db.GetSnapshot(g, 1)
				}
				_, _ = db.ListNodeInfo()
			}
		}()
	}

	writers.Wait()
	close(stop)
	readers.Wait()

	// Final state must be exactly what the last write committed.
	for _, g := range groups {
		state, _, err := db.ReadState(g, 1)
		if err != nil {
			t.Errorf("final ReadState(%d): %v", g, err)
			continue
		}
		if state.Commit != rounds {
			t.Errorf("shard %d final Commit = %d, want %d", g, state.Commit, rounds)
		}
	}
}
