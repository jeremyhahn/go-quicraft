package quicraft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// compactCountingLogDB wraps a LogDB and counts Compact calls per
// (shardID, replicaID), so a test can see that a request reached the disk
// compaction rather than infer it from markers another path also moves.
type compactCountingLogDB struct {
	logdb.LogDB
	mu     sync.Mutex
	counts map[[2]uint64]int
}

func newCompactCountingLogDB(inner logdb.LogDB) *compactCountingLogDB {
	return &compactCountingLogDB{LogDB: inner, counts: make(map[[2]uint64]int)}
}

func (c *compactCountingLogDB) Compact(shardID, replicaID uint64) error {
	c.mu.Lock()
	c.counts[[2]uint64{shardID, replicaID}]++
	c.mu.Unlock()
	return c.LogDB.Compact(shardID, replicaID)
}

func (c *compactCountingLogDB) compactCalls(shardID, replicaID uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[[2]uint64{shardID, replicaID}]
}

// compactionTestShardConfig keeps only a short tail behind each snapshot, so
// a snapshot over a few hundred entries trims the log well above index 1.
func compactionTestShardConfig() config.Config {
	scfg := testShardConfig(1, 1)
	scfg.CompactionOverhead = 10
	return scfg
}

// proposePuts commits n KV puts keyed from start.
func proposePuts(ctx context.Context, t *testing.T, h *Host, start, n int) {
	t.Helper()
	for i := start; i < start+n; i++ {
		cmd, err := kv.EncodePut([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("value-%04d", i)))
		if err != nil {
			t.Fatalf("EncodePut(%d): %v", i, err)
		}
		if _, err := h.SyncPropose(ctx, 1, cmd); err != nil {
			t.Fatalf("SyncPropose(%d): %v", i, err)
		}
	}
}

// TestRequestCompaction_AfterSnapshotTrimsLogAndRestartRecovers drives the
// operator sequence snapshot -> RequestCompaction on a disk LogDB, checks the
// log really was trimmed (LogRange moves up, QueryRaftLog below it reports
// the entries compacted), then reopens the Host on the same directories and
// checks the state machine recovers to the same hash: the snapshot plus the
// retained tail must still rebuild every committed write.
func TestRequestCompaction_AfterSnapshotTrimsLogAndRestartRecovers(t *testing.T) {
	cfg := testHostConfig(t)
	members := map[uint64]string{1: cfg.RaftAddress}

	// The test opens the WAL itself, as NewHost does by default, so it can
	// count the Compact calls that reach it. The Host closes it in Close.
	wal, err := waldb.Open(cfg.WALDir)
	if err != nil {
		t.Fatalf("waldb.Open: %v", err)
	}
	counted := newCompactCountingLogDB(wal)
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption(), WithLogDB(counted))
	if err != nil {
		_ = wal.Close()
		t.Fatalf("NewHost: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = h.Close()
		}
	}()
	if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), compactionTestShardConfig()); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	before, err := h.LogRange(1)
	if err != nil {
		t.Fatalf("LogRange before: %v", err)
	}
	start := before.FirstIndex

	proposePuts(ctx, t, h, 0, 200)
	snapshotIndex, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	// SyncRequestSnapshot already compacts the log before it returns, so the
	// checks below would pass without RequestCompaction. Its own effect is
	// the one disk compaction it asks for.
	compactsBefore := counted.compactCalls(1, 1)
	if err := h.RequestCompaction(1, 1); err != nil {
		t.Fatalf("RequestCompaction: %v", err)
	}
	if got := counted.compactCalls(1, 1); got != compactsBefore+1 {
		t.Fatalf("LogDB.Compact(1, 1) calls after RequestCompaction = %d, want %d: "+
			"the request must reach the disk compaction", got, compactsBefore+1)
	}

	lr, err := h.LogRange(1)
	if err != nil {
		t.Fatalf("LogRange after compaction: %v", err)
	}
	if lr.FirstIndex <= start || lr.FirstIndex > snapshotIndex {
		t.Fatalf("LogRange after compaction = %+v, want FirstIndex in (%d, %d]: "+
			"the snapshot must trim the log, and never past its own index", lr, start, snapshotIndex)
	}
	if lr.LastIndex < snapshotIndex {
		t.Fatalf("LogRange after compaction = %+v, want LastIndex >= snapshot index %d", lr, snapshotIndex)
	}

	if _, err := h.QueryRaftLog(ctx, 1, start, lr.FirstIndex, 1<<20); !errors.Is(err, logdb.ErrCompacted) {
		t.Fatalf("QueryRaftLog [%d, %d) below the first index = %v, want logdb.ErrCompacted", start, lr.FirstIndex, err)
	}
	if ents, err := h.QueryRaftLog(ctx, 1, lr.FirstIndex, lr.LastIndex+1, 1<<30); err != nil {
		t.Fatalf("QueryRaftLog of the retained range [%d, %d]: %v", lr.FirstIndex, lr.LastIndex, err)
	} else if uint64(len(ents)) != lr.LastIndex-lr.FirstIndex+1 {
		t.Fatalf("QueryRaftLog of the retained range returned %d entries, want %d",
			len(ents), lr.LastIndex-lr.FirstIndex+1)
	}

	// A tail past the snapshot, so the restart must replay log entries on
	// top of the recovered snapshot rather than load the snapshot alone.
	proposePuts(ctx, t, h, 200, 20)

	wantHash, wantApplied, ok, err := h.StateMachineHash(1)
	if err != nil || !ok {
		t.Fatalf("StateMachineHash before close = ok %v, err %v", ok, err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true

	reopenedWAL, err := waldb.Open(cfg.WALDir)
	if err != nil {
		t.Fatalf("waldb.Open (reopen): %v", err)
	}
	reopened, err := NewHost(cfg, WithoutTransport(), WithoutEncryption(),
		WithLogDB(newCompactCountingLogDB(reopenedWAL)))
	if err != nil {
		_ = reopenedWAL.Close()
		t.Fatalf("NewHost (reopen): %v", err)
	}
	defer reopened.Close()
	if err := reopened.StartShard(members, false, kv.NewMemoryCreateFunc(), compactionTestShardConfig()); err != nil {
		t.Fatalf("StartShard (reopen): %v", err)
	}
	waitForLeader(t, reopened, 1)

	deadline := time.Now().Add(30 * time.Second)
	for {
		gotHash, gotApplied, ok, err := reopened.StateMachineHash(1)
		if err != nil {
			t.Fatalf("StateMachineHash after reopen: %v", err)
		}
		if ok && gotApplied >= wantApplied {
			if gotHash != wantHash {
				t.Fatalf("StateMachineHash after reopen = %#x at applied %d, want %#x (applied %d before close)",
					gotHash, gotApplied, wantHash, wantApplied)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reopened replica applied %d (ok %v), never reached %d", gotApplied, ok, wantApplied)
		}
		time.Sleep(5 * time.Millisecond)
	}

	reopenedRange, err := reopened.LogRange(1)
	if err != nil {
		t.Fatalf("LogRange after reopen: %v", err)
	}
	if reopenedRange.FirstIndex <= start {
		t.Fatalf("LogRange after reopen = %+v, want FirstIndex above %d: the compaction must survive the restart",
			reopenedRange, start)
	}
}
