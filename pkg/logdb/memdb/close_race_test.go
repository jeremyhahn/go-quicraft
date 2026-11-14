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

package memdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

const (
	closeRaceShards    = 8
	closeRaceReplica   = 1
	closeRacePreloaded = 10 // entries 1-10 are stored before the race starts
	closeRaceFloor     = 5  // RemoveEntriesTo compacts to this index, below every read
	closeRaceRounds    = 40
)

// closeRaceOK reports whether err is one a caller racing Close may see: nil
// before the close takes effect, ErrClosed after it, nothing else.
func closeRaceOK(err error) bool {
	return err == nil || errors.Is(err, logdb.ErrClosed)
}

// closeRaceCatch turns a panic in a racing goroutine into a test failure.
func closeRaceCatch(ctx context.Context, t *testing.T, who string) {
	t.Helper()
	if r := recover(); r != nil {
		t.Errorf("%s panicked racing Close: %v (ctx err %v)", who, r, ctx.Err())
	}
}

// closeRacePreload stores entries 1-10, a hard state and a snapshot on every
// shard, so a reader that finds no entries, no state or no snapshot during the
// race has observed the released map rather than a node it never wrote.
func closeRacePreload(ctx context.Context, t *testing.T, db *MemDB) {
	t.Helper()
	for s := uint64(1); s <= closeRaceShards; s++ {
		ents := make([]logdb.Entry, closeRacePreloaded)
		for i := range ents {
			ents[i] = logdb.Entry{Index: uint64(i + 1), Term: 1}
		}
		if err := db.SaveState([]logdb.Update{{
			ShardID: s, ReplicaID: closeRaceReplica,
			State:   logdb.State{Term: 1, Vote: 1, Commit: closeRacePreloaded},
			Entries: ents,
		}}); err != nil {
			t.Fatalf("preload SaveState shard %d: %v (ctx err %v)", s, err, ctx.Err())
		}
		if err := db.SaveSnapshot(s, closeRaceReplica, logdb.Snapshot{Index: 1, Term: 1}); err != nil {
			t.Fatalf("preload SaveSnapshot shard %d: %v", s, err)
		}
	}
}

// closeRaceWriter appends two entries plus a state per SaveState, saves an
// ever newer snapshot and compacts to the fixed floor, until it sees
// ErrClosed. last holds the last index of the newest batch SaveState
// reported saved.
func closeRaceWriter(ctx context.Context, t *testing.T, db *MemDB, shard uint64, last *atomic.Uint64) {
	who := fmt.Sprintf("writer shard %d", shard)
	defer closeRaceCatch(ctx, t, who)
	next := uint64(closeRacePreloaded + 1)
	snap := uint64(1)
	for ctx.Err() == nil {
		err := db.SaveState([]logdb.Update{{
			ShardID: shard, ReplicaID: closeRaceReplica,
			State:   logdb.State{Term: 1, Vote: 1, Commit: next + 1},
			Entries: []logdb.Entry{{Index: next, Term: 1}, {Index: next + 1, Term: 1}},
		}})
		if errors.Is(err, logdb.ErrClosed) {
			return
		}
		if err != nil {
			t.Errorf("%s: SaveState = %v, want nil or ErrClosed", who, err)
			return
		}
		last.Store(next + 1)
		next += 2

		snap++
		if err := db.SaveSnapshot(shard, closeRaceReplica, logdb.Snapshot{Index: snap, Term: 1}); !closeRaceOK(err) {
			t.Errorf("%s: SaveSnapshot = %v, want nil or ErrClosed", who, err)
			return
		} else if err != nil {
			return
		}
		if err := db.RemoveEntriesTo(shard, closeRaceReplica, closeRaceFloor); !closeRaceOK(err) {
			t.Errorf("%s: RemoveEntriesTo = %v, want nil or ErrClosed", who, err)
			return
		} else if err != nil {
			return
		}
	}
	t.Errorf("%s: never saw ErrClosed: %v", who, ctx.Err())
}

// closeRaceReader reads the shard through every read method until one
// returns ErrClosed. Each read starts after loading last, so a nil read must
// reflect at least the batch SaveState had already reported saved. hits
// counts the EntryRange reads that checked that.
func closeRaceReader(ctx context.Context, t *testing.T, db *MemDB, shard uint64, last *atomic.Uint64, hits *atomic.Int64) {
	who := fmt.Sprintf("reader shard %d", shard)
	defer closeRaceCatch(ctx, t, who)
	for ctx.Err() == nil {
		saved := last.Load()

		first, n, err := db.EntryRange(shard, closeRaceReplica)
		if errors.Is(err, logdb.ErrClosed) {
			return
		}
		if err != nil {
			t.Errorf("%s: EntryRange = %v, want nil or ErrClosed", who, err)
			return
		}
		if n == 0 || first+n-1 < saved {
			t.Errorf("%s: EntryRange = (%d, %d) after a batch ending at %d was saved: the batch is not whole",
				who, first, n, saved)
			return
		}
		hits.Add(1)

		ents, _, err := db.IterateEntries(nil, 0, shard, closeRaceReplica, saved, saved+1, 0)
		if errors.Is(err, logdb.ErrClosed) {
			return
		}
		if err != nil {
			t.Errorf("%s: IterateEntries [%d, %d) = %v, want nil or ErrClosed", who, saved, saved+1, err)
			return
		}
		if len(ents) != 1 || ents[0].Index != saved {
			t.Errorf("%s: IterateEntries [%d, %d) returned %d entries", who, saved, saved+1, len(ents))
			return
		}

		if _, _, err := db.ReadState(shard, closeRaceReplica); errors.Is(err, logdb.ErrClosed) {
			return
		} else if err != nil {
			t.Errorf("%s: ReadState = %v, want nil or ErrClosed", who, err)
			return
		}

		if _, err := db.GetSnapshot(shard, closeRaceReplica); errors.Is(err, logdb.ErrClosed) {
			return
		} else if err != nil {
			t.Errorf("%s: GetSnapshot = %v, want nil or ErrClosed", who, err)
			return
		}

		infos, err := db.ListNodeInfo()
		if errors.Is(err, logdb.ErrClosed) {
			return
		}
		if err != nil {
			t.Errorf("%s: ListNodeInfo = %v, want nil or ErrClosed", who, err)
			return
		}
		if len(infos) != closeRaceShards {
			t.Errorf("%s: ListNodeInfo listed %d nodes, want %d: it observed the released map",
				who, len(infos), closeRaceShards)
			return
		}
	}
	t.Errorf("%s: never saw ErrClosed: %v", who, ctx.Err())
}

// closeRaceAllClosed checks that every method raced above returns ErrClosed.
func closeRaceAllClosed(ctx context.Context, t *testing.T, db *MemDB) {
	t.Helper()
	const s, r = 1, closeRaceReplica
	check := func(name string, err error) {
		if !errors.Is(err, logdb.ErrClosed) {
			t.Errorf("%s after Close = %v, want ErrClosed (ctx err %v)", name, err, ctx.Err())
		}
	}
	check("SaveState", db.SaveState([]logdb.Update{{
		ShardID: s, ReplicaID: r,
		State:   logdb.State{Term: 2},
		Entries: []logdb.Entry{{Index: 1 << 20, Term: 2}},
	}}))
	_, _, err := db.IterateEntries(nil, 0, s, r, closeRaceFloor+1, closeRaceFloor+2, 0)
	check("IterateEntries", err)
	_, _, err = db.ReadState(s, r)
	check("ReadState", err)
	_, _, err = db.EntryRange(s, r)
	check("EntryRange", err)
	check("SaveSnapshot", db.SaveSnapshot(s, r, logdb.Snapshot{Index: 1 << 20, Term: 2}))
	_, err = db.GetSnapshot(s, r)
	check("GetSnapshot", err)
	check("RemoveEntriesTo", db.RemoveEntriesTo(s, r, closeRaceFloor+1))
	_, err = db.ListNodeInfo()
	check("ListNodeInfo", err)
}

// closeRaceRound runs one race: a writer and a reader per shard (16
// goroutines) against two concurrent Close calls.
func closeRaceRound(ctx context.Context, t *testing.T) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	db := New()
	closeRacePreload(ctx, t, db)

	var (
		workers sync.WaitGroup
		closers sync.WaitGroup
		hits    atomic.Int64
	)
	start := make(chan struct{})
	for s := uint64(1); s <= closeRaceShards; s++ {
		last := new(atomic.Uint64)
		last.Store(closeRacePreloaded)
		workers.Add(2)
		go func() {
			defer workers.Done()
			<-start
			closeRaceWriter(ctx, t, db, s, last)
		}()
		go func() {
			defer workers.Done()
			<-start
			closeRaceReader(ctx, t, db, s, last, &hits)
		}()
	}
	closeErrs := make([]error, 2)
	for i := range closeErrs {
		closers.Add(1)
		go func() {
			defer closers.Done()
			defer closeRaceCatch(ctx, t, fmt.Sprintf("closer %d", i))
			<-start
			time.Sleep(200 * time.Microsecond)
			closeErrs[i] = db.Close()
		}()
	}
	close(start)
	closers.Wait()
	for i, err := range closeErrs {
		if err != nil {
			t.Errorf("Close %d = %v, want nil", i, err)
		}
	}
	workers.Wait()
	closeRaceAllClosed(ctx, t, db)
	return hits.Load()
}

// TestCloseRaceReadersAndWritersSeeNilOrErrClosed races readers and writers on
// eight shards against two Close calls: every error is nil or ErrClosed, no
// goroutine panics, a SaveState reported saved is visible whole to a later
// read, and once both Close calls return every method returns ErrClosed.
func TestCloseRaceReadersAndWritersSeeNilOrErrClosed(t *testing.T) {
	ctx := context.Background()
	var hits int64
	for round := 0; round < closeRaceRounds && !t.Failed(); round++ {
		hits += closeRaceRound(ctx, t)
	}
	if !t.Failed() && hits == 0 {
		t.Fatal("no reader completed an EntryRange before Close: the whole-batch check never ran")
	}
}

// TestCloseRaceEveryMethodAfterCloseReturnsErrClosed checks the post-close
// contract on its own, after two sequential Close calls on a loaded store.
func TestCloseRaceEveryMethodAfterCloseReturnsErrClosed(t *testing.T) {
	ctx := context.Background()
	db := New()
	closeRacePreload(ctx, t, db)
	for i := 0; i < 2; i++ {
		if err := db.Close(); err != nil {
			t.Fatalf("Close %d = %v, want nil", i, err)
		}
	}
	closeRaceAllClosed(ctx, t, db)
}
