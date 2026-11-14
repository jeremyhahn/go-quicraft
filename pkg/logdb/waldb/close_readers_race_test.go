package waldb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// closeReadersRaceShards are the shards the race test saves data on. Each
// holds one replica, closeReadersRaceReplica.
var closeReadersRaceShards = []uint64{1, 2, 3, 4}

const (
	closeReadersRaceReplica   = uint64(1)
	closeReadersRaceEntries   = uint64(10)
	closeReadersRaceCompacted = uint64(2)
)

// closeReadersRaceReader is one read path under test. It returns nil when the
// call returned exactly what was saved for shardID, the call's own error when
// it failed, and a mismatch error when it returned anything else.
type closeReadersRaceReader struct {
	name string
	read func(ctx context.Context, shardID uint64) error
}

func closeReadersRaceState(shardID uint64) logdb.State {
	return logdb.State{Term: shardID + 1, Vote: closeReadersRaceReplica, Commit: closeReadersRaceEntries}
}

func closeReadersRaceSnapshot(shardID uint64) logdb.Snapshot {
	return logdb.Snapshot{
		Index:      closeReadersRaceEntries,
		Term:       shardID,
		Membership: logdb.Membership{ConfigChangeID: closeReadersRaceEntries},
	}
}

func closeReadersRaceBootstrap(shardID uint64) logdb.Bootstrap {
	return logdb.Bootstrap{
		Addresses: map[uint64]string{closeReadersRaceReplica: fmt.Sprintf("replica-%d", shardID)},
		Type:      shardID,
	}
}

// closeReadersRaceOpen opens a DB and saves entries, a hard state, a snapshot
// and a bootstrap on every shard, then compacts each log to
// closeReadersRaceCompacted so CompactedTo and RemoveEntriesTo have a floor.
func closeReadersRaceOpen(ctx context.Context, t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), WithNoSync(true))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, s := range closeReadersRaceShards {
		if err := ctx.Err(); err != nil {
			t.Fatalf("setup: %v", err)
		}
		if err := db.SaveEntries(s, closeReadersRaceReplica, makeEntries(1, closeReadersRaceEntries, s)); err != nil {
			t.Fatalf("SaveEntries(%d): %v", s, err)
		}
		if err := db.SaveNodeState(s, closeReadersRaceReplica, closeReadersRaceState(s)); err != nil {
			t.Fatalf("SaveNodeState(%d): %v", s, err)
		}
		if err := db.SaveSnapshot(s, closeReadersRaceReplica, closeReadersRaceSnapshot(s)); err != nil {
			t.Fatalf("SaveSnapshot(%d): %v", s, err)
		}
		if err := db.SaveBootstrap(s, closeReadersRaceReplica, closeReadersRaceBootstrap(s)); err != nil {
			t.Fatalf("SaveBootstrap(%d): %v", s, err)
		}
		if err := db.RemoveEntriesTo(s, closeReadersRaceReplica, closeReadersRaceCompacted); err != nil {
			t.Fatalf("RemoveEntriesTo(%d): %v", s, err)
		}
	}
	return db
}

// closeReadersRaceCheckEntries reports whether got is exactly the saved log
// above the compaction floor for shardID.
func closeReadersRaceCheckEntries(ctx context.Context, shardID uint64, got []logdb.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	want := makeEntries(closeReadersRaceCompacted+1, closeReadersRaceEntries-closeReadersRaceCompacted, shardID)
	if len(got) != len(want) {
		return fmt.Errorf("shard %d: %d entries, want %d", shardID, len(got), len(want))
	}
	for i := range want {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term || !bytes.Equal(got[i].Cmd, want[i].Cmd) {
			return fmt.Errorf("shard %d: entry %d = %+v, want %+v", shardID, i, got[i], want[i])
		}
	}
	return nil
}

// closeReadersRaceReaders returns one reader per DB read path, each checking
// its result against what closeReadersRaceOpen saved.
func closeReadersRaceReaders(ctx context.Context, db *DB) []closeReadersRaceReader {
	_ = ctx
	return []closeReadersRaceReader{
		{"IterateEntries", func(ctx context.Context, s uint64) error {
			got, _, err := db.IterateEntries(nil, 0, s, closeReadersRaceReplica,
				closeReadersRaceCompacted+1, closeReadersRaceEntries+1, 0)
			if err != nil {
				return err
			}
			return closeReadersRaceCheckEntries(ctx, s, got)
		}},
		{"GetEntries", func(ctx context.Context, s uint64) error {
			got, err := db.GetEntries(s, closeReadersRaceReplica,
				closeReadersRaceCompacted+1, closeReadersRaceEntries+1, 0)
			if err != nil {
				return err
			}
			return closeReadersRaceCheckEntries(ctx, s, got)
		}},
		{"ReadState", func(ctx context.Context, s uint64) error {
			st, m, err := db.ReadState(s, closeReadersRaceReplica)
			if err != nil {
				return err
			}
			if st != closeReadersRaceState(s) || m.ConfigChangeID != closeReadersRaceSnapshot(s).Membership.ConfigChangeID {
				return fmt.Errorf("shard %d: state %+v membership %+v", s, st, m)
			}
			return ctx.Err()
		}},
		{"EntryRange", func(ctx context.Context, s uint64) error {
			first, length, err := db.EntryRange(s, closeReadersRaceReplica)
			if err != nil {
				return err
			}
			if first != closeReadersRaceCompacted+1 || length != closeReadersRaceEntries-closeReadersRaceCompacted {
				return fmt.Errorf("shard %d: range (%d, %d)", s, first, length)
			}
			return ctx.Err()
		}},
		{"RemoveEntriesTo", func(ctx context.Context, s uint64) error {
			// At the existing floor: a no-op that must not move it.
			if err := db.RemoveEntriesTo(s, closeReadersRaceReplica, closeReadersRaceCompacted); err != nil {
				return err
			}
			return ctx.Err()
		}},
		{"GetSnapshot", func(ctx context.Context, s uint64) error {
			ss, err := db.GetSnapshot(s, closeReadersRaceReplica)
			if err != nil {
				return err
			}
			want := closeReadersRaceSnapshot(s)
			if ss.Index != want.Index || ss.Term != want.Term ||
				ss.Membership.ConfigChangeID != want.Membership.ConfigChangeID {
				return fmt.Errorf("shard %d: snapshot %+v, want %+v", s, ss, want)
			}
			return ctx.Err()
		}},
		{"NodeRemoved", func(ctx context.Context, s uint64) error {
			removed, err := db.NodeRemoved(s, closeReadersRaceReplica)
			if err != nil {
				return err
			}
			if removed {
				return fmt.Errorf("shard %d: NodeRemoved = true for a live node", s)
			}
			return ctx.Err()
		}},
		{"ListNodeInfo", func(ctx context.Context, _ uint64) error {
			nodes, err := db.ListNodeInfo()
			if err != nil {
				return err
			}
			sort.Slice(nodes, func(i, j int) bool { return nodes[i].ShardID < nodes[j].ShardID })
			want := make([]logdb.NodeInfo, 0, len(closeReadersRaceShards))
			for _, s := range closeReadersRaceShards {
				want = append(want, logdb.NodeInfo{ShardID: s, ReplicaID: closeReadersRaceReplica})
			}
			if !reflect.DeepEqual(nodes, want) {
				return fmt.Errorf("nodes %+v, want %+v", nodes, want)
			}
			return ctx.Err()
		}},
		{"GetBootstrap", func(ctx context.Context, s uint64) error {
			bs, ok, err := db.GetBootstrap(s, closeReadersRaceReplica)
			if err != nil {
				return err
			}
			want := closeReadersRaceBootstrap(s)
			if !ok || bs.Type != want.Type || bs.Join != want.Join || !reflect.DeepEqual(bs.Addresses, want.Addresses) {
				return fmt.Errorf("shard %d: bootstrap %+v (found %v), want %+v", s, bs, ok, want)
			}
			return ctx.Err()
		}},
		{"CompactedTo", func(ctx context.Context, s uint64) error {
			floor, err := db.CompactedTo(s, closeReadersRaceReplica)
			if err != nil {
				return err
			}
			if floor != closeReadersRaceCompacted {
				return fmt.Errorf("shard %d: CompactedTo = %d, want %d", s, floor, closeReadersRaceCompacted)
			}
			return ctx.Err()
		}},
	}
}

// closeReadersRaceCall runs one read and turns a panic into an error, so a
// reader that panics on a closed DB fails the test instead of the binary.
func closeReadersRaceCall(ctx context.Context, r closeReadersRaceReader, shardID uint64) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%s panicked: %v", r.name, p)
		}
	}()
	return r.read(ctx, shardID)
}

// closeReadersRaceWait waits for wg, failing the test if ctx ends first.
func closeReadersRaceWait(ctx context.Context, t *testing.T, wg *sync.WaitGroup, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("%s did not return before the deadline: %v", what, ctx.Err())
	}
}

// TestCloseReadersRace races one goroutine per read path against Close.
// Every call returns what was saved or logdb.ErrClosed, nothing panics, every
// goroutine returns, a second Close returns nil, and every read after Close
// returns logdb.ErrClosed.
func TestCloseReadersRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := closeReadersRaceOpen(ctx, t)
	readers := closeReadersRaceReaders(ctx, db)

	// started counts readers that have completed one full, successful pass
	// over the shards, so Close races readers already in their loop.
	var wg, started sync.WaitGroup
	for _, r := range readers {
		wg.Add(1)
		started.Add(1)
		go func(r closeReadersRaceReader) {
			defer wg.Done()
			for pass := 0; ; pass++ {
				for _, s := range closeReadersRaceShards {
					err := closeReadersRaceCall(ctx, r, s)
					if err == nil {
						continue
					}
					if pass == 0 {
						started.Done()
					}
					if !errors.Is(err, logdb.ErrClosed) {
						t.Errorf("%s shard %d: %v", r.name, s, err)
					}
					return
				}
				if pass == 0 {
					started.Done()
				}
			}
		}(r)
	}

	closeReadersRaceWait(ctx, t, &started, "first reader pass")
	if err := db.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	closeReadersRaceWait(ctx, t, &wg, "readers")

	if err := db.Close(); err != nil {
		t.Errorf("second Close: %v, want nil", err)
	}
	for _, r := range readers {
		for _, s := range closeReadersRaceShards {
			if err := closeReadersRaceCall(ctx, r, s); !errors.Is(err, logdb.ErrClosed) {
				t.Errorf("%s shard %d after Close: %v, want logdb.ErrClosed", r.name, s, err)
			}
		}
	}
}
