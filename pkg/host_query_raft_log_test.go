package quicraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

const (
	queryRaftLogEntries = 40
	queryRaftLogCmdSize = 1024
)

// startQueryRaftLogHost starts a single-voter transportless Host on a real
// WAL, proposes queryRaftLogEntries commands of exactly queryRaftLogCmdSize
// bytes, and returns the Host, the index of the first proposed entry and the
// proposed commands in index order.
func startQueryRaftLogHost(t *testing.T) (*Host, uint64, [][]byte) {
	t.Helper()
	cfg := testHostConfig(t)
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	members := map[uint64]string{1: cfg.RaftAddress}
	// CompactionOverhead 10 so a snapshot trims the log in the compaction test.
	if err := h.StartShard(members, false, kv.NewMemoryCreateFunc(), compactionTestShardConfig()); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	before, err := h.LogRange(1)
	if err != nil {
		t.Fatalf("LogRange before: %v", err)
	}
	empty, err := kv.EncodePut([]byte("key-0000"), nil)
	if err != nil {
		t.Fatalf("EncodePut: %v", err)
	}
	valueLen := queryRaftLogCmdSize - len(empty)
	cmds := make([][]byte, 0, queryRaftLogEntries)
	for i := 0; i < queryRaftLogEntries; i++ {
		value := bytes.Repeat([]byte{byte('a' + i%26)}, valueLen)
		cmd, err := kv.EncodePut([]byte(fmt.Sprintf("key-%04d", i)), value)
		if err != nil {
			t.Fatalf("EncodePut(%d): %v", i, err)
		}
		if len(cmd) != queryRaftLogCmdSize {
			t.Fatalf("command %d is %d bytes, want %d", i, len(cmd), queryRaftLogCmdSize)
		}
		if _, err := h.SyncPropose(ctx, 1, cmd); err != nil {
			t.Fatalf("SyncPropose(%d): %v", i, err)
		}
		cmds = append(cmds, cmd)
	}
	after, err := h.LogRange(1)
	if err != nil {
		t.Fatalf("LogRange after: %v", err)
	}
	first := before.LastIndex + 1
	if after.LastIndex != first+queryRaftLogEntries-1 {
		t.Fatalf("LogRange after %d proposals = %+v, want LastIndex %d",
			queryRaftLogEntries, after, first+queryRaftLogEntries-1)
	}
	return h, first, cmds
}

// checkQueryRaftLogPrefix asserts entries are a non-empty run of consecutive
// indexes starting at first whose commands match cmds.
func checkQueryRaftLogPrefix(t *testing.T, entries []proto.Entry, first uint64, cmds [][]byte) {
	t.Helper()
	if len(entries) == 0 {
		t.Fatal("QueryRaftLog returned no entries, want at least one")
	}
	if len(entries) > len(cmds) {
		t.Fatalf("QueryRaftLog returned %d entries, want at most %d", len(entries), len(cmds))
	}
	for i := range entries {
		if want := first + uint64(i); entries[i].Index != want {
			t.Fatalf("entry %d has index %d, want %d: the result must be a contiguous prefix", i, entries[i].Index, want)
		}
		if !bytes.Equal(entries[i].Cmd, cmds[i]) {
			t.Fatalf("entry at index %d carries a %d-byte Cmd that is not the proposed command", entries[i].Index, len(entries[i].Cmd))
		}
	}
}

func TestQueryRaftLogMaxSize_ReturnsContiguousPrefixWithinBudget(t *testing.T) {
	h, first, cmds := startQueryRaftLogHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const maxSize = 4 * 1024
	entries, err := h.QueryRaftLog(ctx, 1, first, first+queryRaftLogEntries, maxSize)
	if err != nil {
		t.Fatalf("QueryRaftLog: %v", err)
	}
	checkQueryRaftLogPrefix(t, entries, first, cmds)
	if len(entries) >= queryRaftLogEntries {
		t.Fatalf("QueryRaftLog with maxSize %d returned all %d entries of %d bytes: the size limit was ignored",
			maxSize, len(entries), queryRaftLogCmdSize)
	}
	var total int
	for i := range entries {
		total += len(entries[i].Cmd)
	}
	if total > maxSize && len(entries) > 1 {
		t.Fatalf("QueryRaftLog returned %d entries totalling %d Cmd bytes, want at most %d (or a single entry)",
			len(entries), total, maxSize)
	}

	// A budget smaller than one entry still returns that one entry.
	one, err := h.QueryRaftLog(ctx, 1, first, first+queryRaftLogEntries, 1)
	if err != nil {
		t.Fatalf("QueryRaftLog maxSize 1: %v", err)
	}
	checkQueryRaftLogPrefix(t, one, first, cmds)
	if len(one) != 1 {
		t.Fatalf("QueryRaftLog maxSize 1 returned %d entries, want exactly 1", len(one))
	}
}

func TestQueryRaftLogMaxSize_LargeBudgetReturnsWholeHalfOpenRange(t *testing.T) {
	h, first, cmds := startQueryRaftLogHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entries, err := h.QueryRaftLog(ctx, 1, first, first+queryRaftLogEntries, 1<<30)
	if err != nil {
		t.Fatalf("QueryRaftLog: %v", err)
	}
	checkQueryRaftLogPrefix(t, entries, first, cmds)
	if len(entries) != queryRaftLogEntries {
		t.Fatalf("QueryRaftLog returned %d entries, want all %d", len(entries), queryRaftLogEntries)
	}

	// lastIndex is exclusive: a sub-range stops one short of it.
	lo, hi := first+5, first+15
	sub, err := h.QueryRaftLog(ctx, 1, lo, hi, 1<<30)
	if err != nil {
		t.Fatalf("QueryRaftLog [%d, %d): %v", lo, hi, err)
	}
	checkQueryRaftLogPrefix(t, sub, lo, cmds[5:15])
	if uint64(len(sub)) != hi-lo {
		t.Fatalf("QueryRaftLog [%d, %d) returned %d entries, want %d", lo, hi, len(sub), hi-lo)
	}
	for i := range sub {
		if sub[i].Index >= hi {
			t.Fatalf("QueryRaftLog [%d, %d) returned index %d: lastIndex must be excluded", lo, hi, sub[i].Index)
		}
	}
}

func TestQueryRaftLogMaxSize_ResultIsOwnedCopy(t *testing.T) {
	h, first, cmds := startQueryRaftLogHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entries, err := h.QueryRaftLog(ctx, 1, first, first+queryRaftLogEntries, 1<<30)
	if err != nil {
		t.Fatalf("QueryRaftLog: %v", err)
	}
	checkQueryRaftLogPrefix(t, entries, first, cmds)
	for i := range entries {
		for j := range entries[i].Cmd {
			entries[i].Cmd[j] ^= 0xff
		}
	}

	again, err := h.QueryRaftLog(ctx, 1, first, first+queryRaftLogEntries, 1<<30)
	if err != nil {
		t.Fatalf("second QueryRaftLog: %v", err)
	}
	checkQueryRaftLogPrefix(t, again, first, cmds)
	if len(again) != queryRaftLogEntries {
		t.Fatalf("second QueryRaftLog returned %d entries, want %d", len(again), queryRaftLogEntries)
	}
}

func TestQueryRaftLogMaxSize_RangeEdges(t *testing.T) {
	h, first, _ := startQueryRaftLogHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	last := first + queryRaftLogEntries - 1

	// lastIndex one past the last entry is the whole tail.
	if ents, err := h.QueryRaftLog(ctx, 1, first, last+1, 1<<30); err != nil {
		t.Fatalf("QueryRaftLog [%d, %d): %v", first, last+1, err)
	} else if len(ents) != queryRaftLogEntries {
		t.Fatalf("QueryRaftLog [%d, %d) returned %d entries, want %d", first, last+1, len(ents), queryRaftLogEntries)
	}

	// lastIndex further past the end is not clamped: it is a LogDB error.
	if ents, err := h.QueryRaftLog(ctx, 1, first, last+10, 1<<30); !errors.Is(err, logdb.ErrUnavailable) {
		t.Fatalf("QueryRaftLog [%d, %d) past the log end = %d entries, err %v, want logdb.ErrUnavailable",
			first, last+10, len(ents), err)
	}

	// An empty range and a zero budget are ErrInvalidRange.
	if ents, err := h.QueryRaftLog(ctx, 1, first, first, 1<<30); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("QueryRaftLog [%d, %d) = %d entries, err %v, want ErrInvalidRange", first, first, len(ents), err)
	}
	if ents, err := h.QueryRaftLog(ctx, 1, first, last+1, 0); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("QueryRaftLog maxSize 0 = %d entries, err %v, want ErrInvalidRange", len(ents), err)
	}
}

func TestQueryRaftLogMaxSize_BelowCompactionPointIsCompacted(t *testing.T) {
	h, first, _ := startQueryRaftLogHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	snapshotIndex, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{})
	if err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	lr, err := h.LogRange(1)
	if err != nil {
		t.Fatalf("LogRange: %v", err)
	}
	if lr.FirstIndex <= first || lr.FirstIndex > snapshotIndex {
		t.Fatalf("LogRange after snapshot = %+v, want FirstIndex in (%d, %d]: the snapshot must trim the log",
			lr, first, snapshotIndex)
	}

	if ents, err := h.QueryRaftLog(ctx, 1, first, lr.FirstIndex, 1<<30); !errors.Is(err, logdb.ErrCompacted) {
		t.Fatalf("QueryRaftLog [%d, %d) below the first index = %d entries, err %v, want logdb.ErrCompacted",
			first, lr.FirstIndex, len(ents), err)
	}
	// A range straddling the compaction point is compacted too.
	if _, err := h.QueryRaftLog(ctx, 1, lr.FirstIndex-1, lr.LastIndex+1, 1<<30); !errors.Is(err, logdb.ErrCompacted) {
		t.Fatalf("QueryRaftLog from %d (below first index %d) = %v, want logdb.ErrCompacted", lr.FirstIndex-1, lr.FirstIndex, err)
	}
	// The retained range still reads.
	if ents, err := h.QueryRaftLog(ctx, 1, lr.FirstIndex, lr.LastIndex+1, 1<<30); err != nil {
		t.Fatalf("QueryRaftLog of the retained range: %v", err)
	} else if uint64(len(ents)) != lr.LastIndex-lr.FirstIndex+1 {
		t.Fatalf("QueryRaftLog of the retained range returned %d entries, want %d", len(ents), lr.LastIndex-lr.FirstIndex+1)
	}
}
