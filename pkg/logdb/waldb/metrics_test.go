// Copyright 2025 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package waldb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// metricsCounts is a snapshot of the calls a countingMetrics sink received.
type metricsCounts struct {
	write      int
	fsync      int
	compaction int
	byteCalls  int
	bytes      int64
}

// countingMetrics is a LogDBMetrics sink that counts calls per method and
// sums the bytes reported. It is safe for concurrent use: SaveState fsyncs
// several WAL shards in parallel.
type countingMetrics struct {
	mu sync.Mutex
	c  metricsCounts
}

func (m *countingMetrics) ObserveWriteLatency(time.Duration) {
	m.mu.Lock()
	m.c.write++
	m.mu.Unlock()
}

func (m *countingMetrics) ObserveFsyncLatency(time.Duration) {
	m.mu.Lock()
	m.c.fsync++
	m.mu.Unlock()
}

func (m *countingMetrics) ObserveCompactionLatency(time.Duration) {
	m.mu.Lock()
	m.c.compaction++
	m.mu.Unlock()
}

func (m *countingMetrics) IncWriteBytes(n int64) {
	m.mu.Lock()
	m.c.byteCalls++
	m.c.bytes += n
	m.mu.Unlock()
}

func (m *countingMetrics) snapshot() metricsCounts {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.c
}

// delta returns the calls received since base.
func (c metricsCounts) delta(base metricsCounts) metricsCounts {
	return metricsCounts{
		write:      c.write - base.write,
		fsync:      c.fsync - base.fsync,
		compaction: c.compaction - base.compaction,
		byteCalls:  c.byteCalls - base.byteCalls,
		bytes:      c.bytes - base.bytes,
	}
}

// openWALMetricsDB opens a DB in a fresh directory with the options the
// metrics tests share; a nil sink opens it without WithMetrics. The DB is
// closed when the test ends.
func openWALMetricsDB(ctx context.Context, t *testing.T, sink LogDBMetrics, noSync bool) *DB {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	opts := []Option{WithNoSync(noSync), WithBlockSize(1024), WithMaxFileSize(64 * 1024)}
	if sink != nil {
		opts = append(opts, WithMetrics(sink))
	}
	db, err := Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// walMetricsUpdate returns one Update for shard 1 replica 1: State
// {Term: 1, Commit: 3} and entries 1..3, each with a 100-byte Cmd.
func walMetricsUpdate(ctx context.Context, t *testing.T) []logdb.Update {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	entries := make([]logdb.Entry, 0, 3)
	for i := uint64(1); i <= 3; i++ {
		cmd := make([]byte, 100)
		for j := range cmd {
			cmd[j] = byte(i)
		}
		entries = append(entries, logdb.Entry{Index: i, Term: 1, Cmd: cmd})
	}
	return []logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 1, Commit: 3},
		Entries:   entries,
	}}
}

// TestWALMetrics_SyncedSaveStateAndCompact checks (a) one synced SaveState
// reports exactly one write latency, at least one fsync latency and at least
// the payload's bytes, and (d) one Compact reports exactly one compaction
// latency.
func TestWALMetrics_SyncedSaveStateAndCompact(t *testing.T) {
	ctx := context.Background()
	sink := &countingMetrics{}
	db := openWALMetricsDB(ctx, t, sink, false)

	base := sink.snapshot()
	if err := db.SaveState(walMetricsUpdate(ctx, t)); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	got := sink.snapshot().delta(base)
	if got.write != 1 {
		t.Errorf("ObserveWriteLatency calls = %d, want 1", got.write)
	}
	if got.fsync < 1 {
		t.Errorf("ObserveFsyncLatency calls = %d, want >= 1", got.fsync)
	}
	if got.bytes < 300 {
		t.Errorf("IncWriteBytes sum = %d, want >= 300", got.bytes)
	}
	if got.compaction != 0 {
		t.Errorf("ObserveCompactionLatency calls = %d after SaveState, want 0", got.compaction)
	}

	base = sink.snapshot()
	if err := db.Compact(1, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	got = sink.snapshot().delta(base)
	if got.compaction != 1 {
		t.Errorf("ObserveCompactionLatency calls = %d, want 1", got.compaction)
	}
	if got.write != 0 {
		t.Errorf("ObserveWriteLatency calls = %d after Compact, want 0", got.write)
	}
}

// TestWALMetrics_EmptyBatchReportsNothing checks (b): SaveState of a nil or
// empty batch returns nil and calls no sink method.
func TestWALMetrics_EmptyBatchReportsNothing(t *testing.T) {
	ctx := context.Background()
	sink := &countingMetrics{}
	db := openWALMetricsDB(ctx, t, sink, false)

	base := sink.snapshot()
	if err := db.SaveState(nil); err != nil {
		t.Fatalf("SaveState(nil): %v", err)
	}
	if err := db.SaveState([]logdb.Update{}); err != nil {
		t.Fatalf("SaveState(empty): %v", err)
	}
	if got := sink.snapshot().delta(base); got != (metricsCounts{}) {
		t.Errorf("empty batches changed counters: %+v", got)
	}
}

// TestWALMetrics_NoSyncSkipsFsync checks (c): under WithNoSync(true) a
// SaveState reports one write latency and its bytes but no fsync latency.
func TestWALMetrics_NoSyncSkipsFsync(t *testing.T) {
	ctx := context.Background()
	sink := &countingMetrics{}
	db := openWALMetricsDB(ctx, t, sink, true)

	base := sink.snapshot()
	if err := db.SaveState(walMetricsUpdate(ctx, t)); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	got := sink.snapshot().delta(base)
	if got.write != 1 {
		t.Errorf("ObserveWriteLatency calls = %d, want 1", got.write)
	}
	if got.bytes <= 0 {
		t.Errorf("IncWriteBytes sum = %d, want > 0", got.bytes)
	}
	if got.fsync != 0 {
		t.Errorf("ObserveFsyncLatency calls = %d, want 0 under noSync", got.fsync)
	}
}

// TestWALMetrics_ClosedDBReportsNothing checks (e): after Close, SaveState
// returns logdb.ErrClosed and calls no sink method.
func TestWALMetrics_ClosedDBReportsNothing(t *testing.T) {
	ctx := context.Background()
	sink := &countingMetrics{}
	db := openWALMetricsDB(ctx, t, sink, false)
	if err := db.SaveState(walMetricsUpdate(ctx, t)); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	base := sink.snapshot()
	if err := db.SaveState(walMetricsUpdate(ctx, t)); !errors.Is(err, logdb.ErrClosed) {
		t.Fatalf("SaveState after Close = %v, want logdb.ErrClosed", err)
	}
	if got := sink.snapshot().delta(base); got != (metricsCounts{}) {
		t.Errorf("SaveState after Close changed counters: %+v", got)
	}
}

// TestWALMetrics_NilSinkSkipped checks (f): a DB opened without WithMetrics
// accepts the same SaveState and Compact; the nil sink is skipped.
func TestWALMetrics_NilSinkSkipped(t *testing.T) {
	ctx := context.Background()
	for _, noSync := range []bool{false, true} {
		db := openWALMetricsDB(ctx, t, nil, noSync)
		if err := db.SaveState(walMetricsUpdate(ctx, t)); err != nil {
			t.Fatalf("noSync=%v SaveState: %v", noSync, err)
		}
		if err := db.Compact(1, 1); err != nil {
			t.Fatalf("noSync=%v Compact: %v", noSync, err)
		}
	}
}
