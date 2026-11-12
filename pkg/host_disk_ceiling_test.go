//go:build linux

package quicraft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// diskCeilingSM records every applied command so a restart can prove that
// the proposals acknowledged before the disk ceiling tripped were kept.
type diskCeilingSM struct {
	mu      sync.Mutex
	applied map[string]bool
}

func (s *diskCeilingSM) Update(_ context.Context, entries []sm.Entry, results []sm.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applied == nil {
		s.applied = make(map[string]bool)
	}
	for i := range entries {
		s.applied[string(entries[i].Cmd)] = true
		results[i] = sm.Result{Value: entries[i].Index}
	}
	return nil
}

// Lookup reports whether the command named by query (a string) was applied.
func (s *diskCeilingSM) Lookup(_ context.Context, query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		return nil, fmt.Errorf("diskCeilingSM: query %T, want string", query)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied[key], nil
}

func (s *diskCeilingSM) SaveSnapshot(_ context.Context, w io.Writer, _ <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.NewEncoder(w).Encode(s.applied)
}

func (s *diskCeilingSM) RecoverFromSnapshot(_ context.Context, r io.Reader, _ <-chan struct{}) error {
	applied := make(map[string]bool)
	if err := json.NewDecoder(r).Decode(&applied); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = applied
	return nil
}

func (s *diskCeilingSM) Close(_ context.Context) error { return nil }

// TestDiskCeiling_RefusesProposalsFiresOnceAndKeepsAcknowledgedEntries drives
// the documented MaxWALDiskSize contract on a real WAL: the statfs sample
// trips the ceiling, proposals are refused with ErrDiskFull, OnLogDBDiskFull
// fires once, the shard is not failed, HealthCheck reports DiskOK false, and
// a restart without the ceiling keeps every acknowledged proposal.
func TestDiskCeiling_RefusesProposalsFiresOnceAndKeepsAcknowledgedEntries(t *testing.T) {
	const (
		shardID  = 1
		maxBytes = uint64(1 << 20)
		maxCalls = 500
	)

	var (
		mu          sync.Mutex
		fullEvents  []config.DiskInfo
		failedCount int
	)
	listener := &config.EventListener{
		OnLogDBDiskFull: func(info config.DiskInfo) {
			mu.Lock()
			defer mu.Unlock()
			fullEvents = append(fullEvents, info)
		},
		OnShardFailed: func(config.ShardFailedInfo) {
			mu.Lock()
			defer mu.Unlock()
			failedCount++
		},
	}

	cfg := testHostConfig(t)
	// Any real filesystem already uses more than 1 MiB, so the first statfs
	// sample (every DefaultCheckEvery WAL writes) is past the 95% threshold.
	cfg.MaxWALDiskSize = maxBytes
	cfg.EventListener = listener

	h, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			h.Close()
		}
	}()

	members := map[uint64]string{1: cfg.RaftAddress}
	createSM := func(_, _ uint64) interface{} { return &diskCeilingSM{} }
	if err := h.StartShard(members, false, createSM, testShardConfig(shardID, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitForLeader(t, h, shardID)

	// (a) Propose until the ceiling refuses a proposal.
	var acked []string
	var refusal error
	for i := 0; i < maxCalls && refusal == nil; i++ {
		cmd := fmt.Sprintf("disk-ceiling-%04d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := h.SyncPropose(ctx, shardID, []byte(cmd))
		cancel()
		switch {
		case err == nil:
			acked = append(acked, cmd)
		case errors.Is(err, ErrDiskFull):
			refusal = err
		default:
			// The proposal whose WAL write tripped the monitor may fail
			// with another error; only acknowledged proposals are checked.
			t.Logf("proposal %d: %v", i, err)
		}
	}
	if refusal == nil {
		t.Fatalf("(a) %d SyncPropose calls never returned ErrDiskFull "+
			"(%d acknowledged)", maxCalls, len(acked))
	}
	if len(acked) == 0 {
		t.Fatal("(a) no proposal was acknowledged before the refusal; " +
			"the restart check would prove nothing")
	}

	// A further proposal is refused as well: the shard stays refused.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = h.SyncPropose(ctx, shardID, []byte("after-refusal"))
	cancel()
	if !errors.Is(err, ErrDiskFull) {
		t.Errorf("(a) proposal after the refusal: err = %v, want ErrDiskFull", err)
	}

	// (b) OnLogDBDiskFull fired exactly once with the documented DiskInfo.
	mu.Lock()
	events := slices.Clone(fullEvents)
	failed := failedCount
	mu.Unlock()
	if len(events) != 1 {
		t.Errorf("(b) OnLogDBDiskFull fired %d times, want 1", len(events))
	}
	if len(events) > 0 {
		info := events[0]
		if info.WALDir != cfg.WALDir {
			t.Errorf("(b) DiskInfo.WALDir = %q, want %q", info.WALDir, cfg.WALDir)
		}
		if info.MaxBytes != maxBytes {
			t.Errorf("(b) DiskInfo.MaxBytes = %d, want %d", info.MaxBytes, maxBytes)
		}
		if info.UsedBytes < maxBytes*95/100 {
			t.Errorf("(b) DiskInfo.UsedBytes = %d, below the 95%% threshold of %d",
				info.UsedBytes, maxBytes)
		}
		if !slices.Contains(info.Shards, shardID) {
			t.Errorf("(b) DiskInfo.Shards = %v, want it to contain %d", info.Shards, shardID)
		}
	}

	// (c) The ceiling refuses proposals; it does not fail the shard.
	if failed != 0 {
		t.Errorf("(c) OnShardFailed fired %d times, want 0", failed)
	}

	// (d) HealthCheck reports the full disk.
	if hs := h.HealthCheck(); hs.DiskOK {
		t.Error("(d) HealthCheck DiskOK = true after the ceiling tripped, want false")
	}

	// (e) Restart on the same WALDir without the ceiling.
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true

	cfg.MaxWALDiskSize = 0
	cfg.EventListener = nil
	h2, err := NewHost(cfg, WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("(e) NewHost on restart: %v", err)
	}
	defer h2.Close()
	if err := h2.StartShard(members, false, createSM, testShardConfig(shardID, 1)); err != nil {
		t.Fatalf("(e) StartShard on restart: %v", err)
	}
	waitForLeader(t, h2, shardID)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	_, err = h2.SyncPropose(ctx, shardID, []byte("after-restart"))
	cancel()
	if err != nil {
		t.Fatalf("(e) proposal after restart without the ceiling: %v", err)
	}

	for _, cmd := range acked {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		v, err := h2.SyncRead(ctx, shardID, cmd)
		cancel()
		if err != nil {
			t.Fatalf("(e) SyncRead(%q): %v", cmd, err)
		}
		if ok, _ := v.(bool); !ok {
			t.Errorf("(e) acknowledged proposal %q is not applied after restart", cmd)
		}
	}
}
