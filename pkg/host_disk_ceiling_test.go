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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb/waldb"
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
	// Any real filesystem already uses more than 1 MiB, so the proposal
	// gate's first statfs sample (every proposalDiskCheckEvery proposals) is
	// past its 90% threshold, and it refuses proposals before the WAL's own
	// sample (every waldb.DefaultCheckEvery writes) reaches 95%.
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
		if info.UsedBytes < maxBytes*90/100 {
			t.Errorf("(b) DiskInfo.UsedBytes = %d, below the 90%% proposal threshold of %d",
				info.UsedBytes, maxBytes)
		}
		if !slices.Contains(info.Shards, shardID) {
			t.Errorf("(b) DiskInfo.Shards = %v, want it to contain %d", info.Shards, shardID)
		}
	}

	// (c) The ceiling refuses proposals; it does not fail the shard.
	// OnShardFailed runs through the engine's listener dispatcher, so the
	// count may lag the failure; the engine's latch is checked directly,
	// and the count again after Close has drained the dispatcher.
	if n := h.engine.GetNode(shardID); n == nil || n.IsFailed() {
		t.Errorf("(c) shard %d failure latch set after the refusal (node present: %v)",
			shardID, n != nil)
	}
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

	// (c) again: Close drained the listener dispatcher, so a queued
	// OnShardFailed has run by now.
	mu.Lock()
	failed = failedCount
	mu.Unlock()
	if failed != 0 {
		t.Errorf("(c) OnShardFailed fired %d times by Close, want 0", failed)
	}

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

// TestDiskGate_ConcurrentRecoveryClearsRefusalAndHealth pins that once the
// proposal gate has tripped and usage drops, proposals racing the recovery
// cannot leave the Host refusing: a refusal sampled before the recovery must
// not be republished after it. Each round trips the gate, starts concurrent
// proposalDiskFull callers, drops usage while they run, and checks that the
// Host then admits proposals and HealthCheck reports DiskOK true.
func TestDiskGate_ConcurrentRecoveryClearsRefusalAndHealth(t *testing.T) {
	const (
		maxBytes = uint64(1000)
		rounds   = 20
		callers  = 16
		calls    = 50
	)
	var usage atomic.Uint64
	var fired atomic.Int64
	cfg := config.HostConfig{
		WALDir:         t.TempDir(),
		MaxWALDiskSize: maxBytes,
		EventListener: &config.EventListener{
			OnLogDBDiskFull: func(config.DiskInfo) { fired.Add(1) },
		},
	}
	var flag atomic.Bool
	gateOpts, _ := hostDiskMonitorOptions(cfg, &flag, nil)
	gate := waldb.NewDiskMonitor(cfg.WALDir, maxBytes, append(gateOpts,
		waldb.WithDiskUsageFunc(func(string) (uint64, error) { return usage.Load(), nil }),
	)...)
	h := &Host{diskFull: &flag, diskGate: gate}

	for r := 0; r < rounds; r++ {
		usage.Store(maxBytes * 95 / 100)
		tripped := false
		for i := 0; i <= proposalDiskCheckEvery && !tripped; i++ {
			tripped = h.proposalDiskFull()
		}
		if !tripped {
			t.Fatalf("round %d: the gate never refused at 95%% usage", r)
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		for c := 0; c < callers; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < calls; i++ {
					h.proposalDiskFull()
				}
			}()
		}
		close(start)
		usage.Store(maxBytes / 10)
		wg.Wait()

		if h.proposalDiskFull() {
			t.Fatalf("round %d: proposals still refused after usage dropped", r)
		}
		if hs := h.HealthCheck(); !hs.DiskOK {
			t.Fatalf("round %d: HealthCheck DiskOK = false after usage dropped", r)
		}
	}
	if got := fired.Load(); got != rounds {
		t.Errorf("OnLogDBDiskFull fired %d times over %d trips, want %d", got, rounds, rounds)
	}
}

// TestDiskGate_StaleWALFullSampleDoesNotRefuseAfterRecovery pins finding 1 of
// the #911 round 2 review: the proposal gate and the WAL monitor publish into
// one flag. A WAL sample at 96% that pauses before it publishes must not set
// the flag after a newer proposal-gate sample below 90% has cleared it.
func TestDiskGate_StaleWALFullSampleDoesNotRefuseAfterRecovery(t *testing.T) {
	const maxBytes = uint64(1000)
	cfg := config.HostConfig{WALDir: t.TempDir(), MaxWALDiskSize: maxBytes}
	var flag atomic.Bool
	gateOpts, walOpts := hostDiskMonitorOptions(cfg, &flag, nil)

	var gateUsage atomic.Uint64
	gate := waldb.NewDiskMonitor(cfg.WALDir, maxBytes, append(gateOpts,
		waldb.WithDiskUsageFunc(func(string) (uint64, error) { return gateUsage.Load(), nil }),
	)...)
	sampled := make(chan struct{})
	release := make(chan struct{})
	wal := waldb.NewDiskMonitor(cfg.WALDir, maxBytes, append(walOpts,
		waldb.WithDiskUsageFunc(func(string) (uint64, error) {
			close(sampled)
			<-release
			return maxBytes * 96 / 100, nil
		}),
	)...)

	gateUsage.Store(maxBytes * 92 / 100)
	if err := gate.ForceCheck(); !errors.Is(err, waldb.ErrDiskFull) {
		t.Fatalf("gate at 92%%: got %v, want ErrDiskFull", err)
	}
	if !flag.Load() {
		t.Fatal("flag not set with the gate full")
	}

	walDone := make(chan error, 1)
	go func() { walDone <- wal.ForceCheck() }()
	select {
	case <-sampled:
	case <-time.After(5 * time.Second):
		t.Fatal("the WAL monitor never sampled")
	}

	gateUsage.Store(maxBytes * 50 / 100)
	if err := gate.ForceCheck(); err != nil {
		close(release)
		t.Fatalf("gate at 50%%: got %v, want nil", err)
	}
	close(release)
	if err := <-walDone; !errors.Is(err, waldb.ErrDiskFull) {
		t.Fatalf("WAL at 96%%: got %v, want ErrDiskFull", err)
	}
	if flag.Load() {
		t.Error("a WAL full sample older than the gate's recovery set the flag")
	}
	if err := gate.ForceCheck(); err != nil {
		t.Fatalf("gate at 50%%: got %v, want nil", err)
	}
	if flag.Load() {
		t.Error("a newer low gate sample did not clear the flag")
	}
}

// TestDiskGate_NewerHealthyWALSampleClearsStaleGateFull checks that a WAL
// sample below the proposal threshold, newer than the gate's full sample,
// clears the flag and HealthCheck's DiskOK without another proposal.
func TestDiskGate_NewerHealthyWALSampleClearsStaleGateFull(t *testing.T) {
	const maxBytes = uint64(1000)
	cfg := config.HostConfig{WALDir: t.TempDir(), MaxWALDiskSize: maxBytes}
	var flag atomic.Bool
	gateOpts, walOpts := hostDiskMonitorOptions(cfg, &flag, nil)

	var usage atomic.Uint64
	usageFn := waldb.WithDiskUsageFunc(func(string) (uint64, error) { return usage.Load(), nil })
	gate := waldb.NewDiskMonitor(cfg.WALDir, maxBytes, append(gateOpts, usageFn)...)
	wal := waldb.NewDiskMonitor(cfg.WALDir, maxBytes, append(walOpts, usageFn)...)
	h := &Host{diskFull: &flag, diskGate: gate}

	usage.Store(maxBytes * 92 / 100)
	if err := gate.ForceCheck(); !errors.Is(err, waldb.ErrDiskFull) {
		t.Fatalf("gate at 92%%: got %v, want ErrDiskFull", err)
	}
	if h.HealthCheck().DiskOK {
		t.Fatal("DiskOK with the gate full")
	}

	// A WAL sample between the two thresholds proves nothing about recovery.
	if err := wal.ForceCheck(); err != nil {
		t.Fatalf("WAL at 92%%: got %v, want nil", err)
	}
	if !flag.Load() || h.HealthCheck().DiskOK {
		t.Error("a WAL sample above the proposal threshold cleared the gate's full state")
	}

	usage.Store(maxBytes * 50 / 100)
	if err := wal.ForceCheck(); err != nil {
		t.Fatalf("WAL at 50%%: got %v, want nil", err)
	}
	if flag.Load() {
		t.Error("a newer healthy WAL sample did not clear the flag")
	}
	if !h.HealthCheck().DiskOK {
		t.Error("DiskOK false after a newer healthy WAL sample")
	}

	// The gate's own full state recovers on the next proposal's sample.
	if h.proposalDiskFull() {
		t.Error("proposal refused after recovery")
	}
}
