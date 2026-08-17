package waldb

import (
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

func TestProbeRollbackDurability(t *testing.T) {
	const (
		root      = "/mnt/probe-rb"
		blockSize = 1024
		maxFile   = 64 * 1024
		replicaID = 7
		shardA    = uint64(1)
		shardB    = uint64(2)
	)
	dir := filepath.Join(root, "wal")
	cfs := newCrashFS(root)
	open := func() *DB {
		db, err := Open(dir, WithFS(cfs), WithBlockSize(blockSize), WithMaxFileSize(maxFile))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}
	entry := func(shardID, index uint64, size int) logdb.Entry {
		cmd := make([]byte, size)
		for i := range cmd {
			cmd[i] = byte(shardID*13 + index + uint64(i))
		}
		return logdb.Entry{Index: index, Term: 1, Cmd: cmd}
	}
	db := open()
	if err := db.SaveState([]logdb.Update{
		{ShardID: shardA, ReplicaID: replicaID, State: logdb.State{Term: 1, Vote: 1, Commit: 1}, Entries: []logdb.Entry{entry(shardA, 1, 2000)}},
		{ShardID: shardB, ReplicaID: replicaID, State: logdb.State{Term: 1, Vote: 1, Commit: 1}, Entries: []logdb.Entry{entry(shardB, 1, 2000)}},
	}); err != nil {
		t.Fatalf("acked: %v", err)
	}
	segA := onlySegmentPath(t, cfs, filepath.Join(dir, shardDirName(int(shardA%DefaultShardCount))))
	fired := 0
	cfs.setHook(func(op crashOp, path string) {
		if op == crashOpWriteAtDone && path == segA {
			fired++
			cfs.Writeback(segA)
		}
	})
	err := db.SaveState([]logdb.Update{
		{ShardID: shardA, ReplicaID: replicaID, State: logdb.State{Term: 2, Vote: 2, Commit: 2}, Entries: []logdb.Entry{entry(shardA, 2, 4000)}},
		{ShardID: shardB, ReplicaID: replicaID, Entries: []logdb.Entry{entry(shardB, 2, 2*maxFile)}},
	})
	cfs.setHook(nil)
	t.Logf("batch err=%v  writeback fires=%d", err, fired)

	cfs.mu.Lock()
	n := cfs.nodes[filepath.Clean(segA)]
	t.Logf("segA live=%d durable=%d", len(n.live), len(n.durable))
	cfs.mu.Unlock()

	cfs.Crash()
	cfs.mu.Lock()
	n = cfs.nodes[filepath.Clean(segA)]
	t.Logf("after crash: segA live=%d durable=%d", len(n.live), len(n.durable))
	cfs.mu.Unlock()

	re := open()
	defer re.Close()
	f, sp, e := re.EntryRange(shardA, replicaID)
	t.Logf("EntryRange(shardA) = (%d,%d,%v)", f, sp, e)
	st, _, se := re.ReadState(shardA, replicaID)
	t.Logf("ReadState(shardA) = %+v  err=%v", st, se)
}
