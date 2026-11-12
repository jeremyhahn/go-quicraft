package quicraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// diskWarnStateMachine keeps the value of every small key and only counts
// the filler entries, so its snapshot stays small while the WAL grows.
type diskWarnStateMachine struct {
	mu     sync.Mutex
	Values map[string]string
	Filled uint64
}

func (m *diskWarnStateMachine) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, e := range entries {
		key, value, ok := bytes.Cut(e.Cmd, []byte{0})
		if !ok {
			m.Filled++
		} else {
			m.Values[string(key)] = string(value)
		}
		results[i] = sm.Result{Value: e.Index}
	}
	return nil
}

func (m *diskWarnStateMachine) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("diskWarnStateMachine: query is not a string")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Values[key], nil
}

func (m *diskWarnStateMachine) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.NewEncoder(w).Encode(m)
}

func (m *diskWarnStateMachine) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return json.NewDecoder(r).Decode(m)
}

func (m *diskWarnStateMachine) Close(_ context.Context) error { return nil }

var _ sm.StateMachine = (*diskWarnStateMachine)(nil)

func diskWarnCreateFunc(_, _ uint64) interface{} {
	return &diskWarnStateMachine{Values: make(map[string]string)}
}

// diskEvents records the disk callbacks a Host fires.
type diskEvents struct {
	mu       sync.Mutex
	warnings []config.DiskInfo
	fulls    []config.DiskInfo
}

func (d *diskEvents) counts() (warnings, fulls int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.warnings), len(d.fulls)
}

// proposeFillUntil proposes filler entries of size bytes until done reports
// true or limit proposals were made. It returns the first proposal error, if
// any; reaching the limit fails the test.
func proposeFillUntil(ctx context.Context, t *testing.T, h *Host, shardID uint64, size, limit int, done func() bool) error {
	t.Helper()
	fill := bytes.Repeat([]byte{'f'}, size)
	for i := 0; i < limit; i++ {
		if done() {
			return nil
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := h.SyncPropose(pctx, shardID, fill)
		cancel()
		if err != nil {
			return err
		}
	}
	if done() {
		return nil
	}
	t.Fatalf("condition not reached after %d proposals of %d bytes", limit, size)
	return nil
}

// TestDiskWarning_FiresAndRecoversAfterCompaction drives a real Host's WAL
// past 85% of MaxWALDiskSize and then to its full threshold, and checks that
// a snapshot with compaction frees the space and proposals are accepted
// again. A WAL segment is preallocated to 64 MiB, so the WAL directory grows
// in 64 MiB steps: one segment (open) at start, two after the first rotation
// (inside the warning band of the 140 MiB limit: 119 MiB to 133 MiB), three
// after the second (past the full threshold).
func TestDiskWarning_FiresAndRecoversAfterCompaction(t *testing.T) {
	const (
		shardID, replicaID uint64 = 981, 1
		limit                     = 140 << 20
		fillSize                  = 512 << 10
		maxFills                  = 800
	)
	ctx := context.Background()

	ev := &diskEvents{}
	cfg := testHostConfig(t)
	cfg.NoSyncWAL = true
	cfg.MaxWALDiskSize = limit
	cfg.EventListener = &config.EventListener{
		OnLogDBDiskWarning: func(info config.DiskInfo) {
			ev.mu.Lock()
			ev.warnings = append(ev.warnings, info)
			ev.mu.Unlock()
		},
		OnLogDBDiskFull: func(info config.DiskInfo) {
			ev.mu.Lock()
			ev.fulls = append(ev.fulls, info)
			ev.mu.Unlock()
		},
	}
	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })

	members := map[uint64]string{replicaID: "127.0.0.1:5000"}
	if err := h.StartShard(members, false, diskWarnCreateFunc, testShardConfig(shardID, replicaID)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	if w, f := ev.counts(); w != 0 || f != 0 {
		t.Fatalf("a fresh Host fired %d warnings and %d full events", w, f)
	}

	// (a) Past 85%: exactly one warning, carrying the WAL directory, the
	// usage, the limit and the shard; not yet full.
	if err := proposeFillUntil(ctx, t, h, shardID, fillSize, maxFills, func() bool {
		w, _ := ev.counts()
		return w > 0
	}); err != nil {
		t.Fatalf("propose before the warning: %v", err)
	}
	ev.mu.Lock()
	warnings, fulls := slices.Clone(ev.warnings), len(ev.fulls)
	ev.mu.Unlock()
	if len(warnings) != 1 {
		t.Fatalf("OnLogDBDiskWarning fired %d times on one crossing, want 1", len(warnings))
	}
	if fulls != 0 {
		t.Fatalf("OnLogDBDiskFull fired %d times inside the warning band", fulls)
	}
	w := warnings[0]
	if w.WALDir != cfg.WALDir {
		t.Errorf("DiskInfo.WALDir = %q, want the Host's WAL directory %q", w.WALDir, cfg.WALDir)
	}
	if w.MaxBytes != limit {
		t.Errorf("DiskInfo.MaxBytes = %d, want the MaxWALDiskSize %d", w.MaxBytes, uint64(limit))
	}
	if w.UsedBytes < uint64(float64(w.MaxBytes)*0.85) {
		t.Errorf("DiskInfo.UsedBytes = %d, below 85%% of MaxBytes %d", w.UsedBytes, w.MaxBytes)
	}
	if w.UsedBytes >= uint64(float64(w.MaxBytes)*0.95) {
		t.Errorf("DiskInfo.UsedBytes = %d fired as a warning at or past the full threshold of %d",
			w.UsedBytes, w.MaxBytes)
	}
	if !slices.Contains(w.Shards, shardID) {
		t.Errorf("DiskInfo.Shards = %v, want it to contain shard %d", w.Shards, shardID)
	}
	if !h.HealthCheck().DiskOK {
		t.Fatal("HealthCheck reports DiskOK false inside the warning band")
	}

	// (b) On to the full threshold.
	perr := proposeFillUntil(ctx, t, h, shardID, fillSize, maxFills, func() bool {
		_, f := ev.counts()
		return f > 0
	})
	if _, f := ev.counts(); f == 0 {
		t.Fatalf("OnLogDBDiskFull never fired (last proposal error: %v)", perr)
	}
	if h.HealthCheck().DiskOK {
		t.Fatal("HealthCheck reports DiskOK true at the full threshold")
	}
	if w, _ := ev.counts(); w != 1 {
		t.Fatalf("OnLogDBDiskWarning fired %d times without usage falling below 85%%, want 1", w)
	}

	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := h.SyncRequestSnapshot(sctx, shardID, SnapshotOption{
		OverrideCompactionOverhead: true,
		CompactionOverhead:         0,
	}); err != nil {
		t.Fatalf("SyncRequestSnapshot with compaction: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !h.HealthCheck().DiskOK {
		if time.Now().After(deadline) {
			t.Fatal("HealthCheck still reports DiskOK false 10s after the snapshot compacted the log")
		}
		time.Sleep(10 * time.Millisecond)
	}

	pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
	defer pcancel()
	if _, err := h.SyncPropose(pctx, shardID, []byte("after-recovery\x00value-981")); err != nil {
		t.Fatalf("SyncPropose after recovery: %v", err)
	}
	got, err := h.SyncRead(pctx, shardID, "after-recovery")
	if err != nil {
		t.Fatalf("SyncRead after recovery: %v", err)
	}
	if got != "value-981" {
		t.Fatalf("SyncRead after recovery = %v, want value-981", got)
	}
}
