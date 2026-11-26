package quicraft

import (
	"context"
	"encoding/json"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// healthStatusJSONKeys are the documented JSON field names of HealthStatus
// (pkg/info.go). "shards" is omitempty and is checked separately.
var healthStatusJSONKeys = []string{
	"status", "barrier_sealed", "disk_ok", "shard_count", "diagnostics_dropped",
}

// shardInfoJSONKeys are the documented JSON field names of ShardInfo that are
// always present. "replicas" is omitempty and may also appear.
var shardInfoJSONKeys = []string{
	"shard_id", "replica_id", "leader_id", "term", "is_leader", "is_observer",
	"is_witness", "state_machine_type", "state_machine_type_name", "pending",
	"nodes", "health", "last_applied", "config_change_index", "paused",
}

// wantShard is the expected entry for one shard in HealthStatus.Shards.
// A nil wantShard pointer in the table means the shard must be absent.
type wantShard struct {
	health   *ShardHealth // nil: not asserted
	paused   bool
	isLeader *bool // nil: not asserted
}

func healthPtr(h ShardHealth) *ShardHealth { return &h }
func boolPtr(b bool) *bool                 { return &b }

// TestHealthCheck_ReportsEveryCallerReachableState drives one in-process Host
// through every state a caller can put a shard or the host in and checks
// HealthCheck against the documented values: Status is "unhealthy" when no
// shard is loaded and "ok" otherwise unless a shard has ShardError
// (docs/multi-group-host/api.md, Host.HealthCheck); ShardInfo.Paused follows
// PauseShard/ResumeShard; a stopped shard leaves Shards.
func TestHealthCheck_ReportsEveryCallerReachableState(t *testing.T) {
	h := newPauseTestHost(t, 1, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	steps := []struct {
		name       string
		act        func(t *testing.T)
		wantStatus string
		wantShards map[uint64]*wantShard
	}{
		{
			name:       "started and led",
			act:        func(t *testing.T) {},
			wantStatus: "ok",
			wantShards: map[uint64]*wantShard{
				1: {health: healthPtr(ShardHealthy), isLeader: boolPtr(true)},
				2: {health: healthPtr(ShardHealthy), isLeader: boolPtr(true)},
			},
		},
		{
			name: "paused",
			act: func(t *testing.T) {
				if err := h.PauseShard(ctx, 1); err != nil {
					t.Fatalf("PauseShard: %v", err)
				}
			},
			// Only ShardError degrades the aggregate, so a paused shard
			// leaves the host "ok". The paused replica neither campaigns nor
			// heartbeats and discards every inbound message: it leads
			// nothing and knows no leader, so its entry must not claim
			// leadership (another replica may be leading the shard).
			wantStatus: "ok",
			wantShards: map[uint64]*wantShard{
				1: {paused: true, health: healthPtr(ShardNoLeader), isLeader: boolPtr(false)},
				2: {health: healthPtr(ShardHealthy), isLeader: boolPtr(true)},
			},
		},
		{
			name: "resumed",
			act: func(t *testing.T) {
				if err := h.ResumeShard(ctx, 1); err != nil {
					t.Fatalf("ResumeShard: %v", err)
				}
				// The replica rejoins as a follower and elects itself again.
				// A proposal that completes proves it leads, not just that a
				// stale leader is still published.
				for {
					_, err := h.SyncPropose(ctx, 1, []byte("cmd"))
					if err == nil {
						break
					}
					if ctx.Err() != nil {
						t.Fatalf("SyncPropose after resume: %v", err)
					}
					time.Sleep(5 * time.Millisecond)
				}
				waitForLeader(t, h, 1)
			},
			wantStatus: "ok",
			wantShards: map[uint64]*wantShard{
				1: {health: healthPtr(ShardHealthy), isLeader: boolPtr(true)},
				2: {health: healthPtr(ShardHealthy), isLeader: boolPtr(true)},
			},
		},
		{
			name: "stopped",
			act: func(t *testing.T) {
				if err := h.StopShard(1); err != nil {
					t.Fatalf("StopShard: %v", err)
				}
			},
			wantStatus: "ok",
			wantShards: map[uint64]*wantShard{
				1: nil,
				2: {health: healthPtr(ShardHealthy), isLeader: boolPtr(true)},
			},
		},
		{
			name: "host draining",
			act: func(t *testing.T) {
				if err := h.Drain(); err != nil {
					t.Fatalf("Drain: %v", err)
				}
			},
			// Drain refuses new work but unloads nothing: the shard is
			// still loaded and not in ShardError, so the host is "ok".
			wantStatus: "ok",
			wantShards: map[uint64]*wantShard{
				1: nil,
				2: {health: healthPtr(ShardHealthy), isLeader: boolPtr(true)},
			},
		},
		{
			name: "host closed",
			act: func(t *testing.T) {
				if err := h.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
			},
			// Close stops every shard: no shard is loaded.
			wantStatus: "unhealthy",
			wantShards: map[uint64]*wantShard{1: nil, 2: nil},
		},
	}

	for _, st := range steps {
		ok := t.Run(st.name, func(t *testing.T) {
			st.act(t)
			status := h.HealthCheck()
			if status.Status != st.wantStatus {
				t.Errorf("Status = %q, want %q", status.Status, st.wantStatus)
			}
			if status.BarrierSealed {
				t.Error("BarrierSealed = true, want false (no barrier)")
			}
			if !status.DiskOK {
				t.Error("DiskOK = false, want true")
			}

			got := make(map[uint64]ShardInfo, len(status.Shards))
			for _, s := range status.Shards {
				got[s.ShardID] = s
			}
			wantCount := 0
			for id, w := range st.wantShards {
				s, present := got[id]
				if w == nil {
					if present {
						t.Errorf("shard %d present in Shards, want absent", id)
					}
					continue
				}
				wantCount++
				if !present {
					t.Errorf("shard %d absent from Shards", id)
					continue
				}
				if s.Paused != w.paused {
					t.Errorf("shard %d Paused = %v, want %v", id, s.Paused, w.paused)
				}
				if w.health != nil && s.Health != *w.health {
					t.Errorf("shard %d Health = %v, want %v", id, s.Health, *w.health)
				}
				if w.isLeader != nil && s.IsLeader != *w.isLeader {
					t.Errorf("shard %d IsLeader = %v, want %v", id, s.IsLeader, *w.isLeader)
				}
				if w.isLeader != nil && *w.isLeader && s.LeaderID != s.ReplicaID {
					t.Errorf("shard %d LeaderID = %d, want own replica %d", id, s.LeaderID, s.ReplicaID)
				}
				if w.health != nil && *w.health == ShardNoLeader && s.LeaderID != 0 {
					t.Errorf("shard %d LeaderID = %d with no leader known, want 0", id, s.LeaderID)
				}
			}
			if status.ShardCount != wantCount || len(status.Shards) != wantCount {
				t.Errorf("ShardCount = %d, len(Shards) = %d, want %d",
					status.ShardCount, len(status.Shards), wantCount)
			}

			checkHealthStatusJSON(t, status)
		})
		if !ok {
			// Later states build on this one.
			t.FailNow()
		}
	}
}

// notReadyShardsJSON decodes the not_ready_shards list from the JSON
// encoding of status, the field an operator's probe reads.
func notReadyShardsJSON(t *testing.T, status *HealthStatus) []uint64 {
	t.Helper()
	b, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("json.Marshal(HealthStatus): %v", err)
	}
	var got struct {
		NotReadyShards []uint64 `json:"not_ready_shards"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	return got.NotReadyShards
}

// TestHealthCheck_NotReadyWhenLeaderlessPastElectionTimeout pins issue
// #1175: a started shard that has had no known leader for longer than its
// election timeout cannot commit a write, so the host is not ready, and
// the status names the shard. A shard inside its election window is left
// alone so a normal election does not flap health.
func TestHealthCheck_NotReadyWhenLeaderlessPastElectionTimeout(t *testing.T) {
	unreachable := map[uint64]string{
		1: "127.0.0.1:5000",
		2: "127.0.0.1:5001",
		3: "127.0.0.1:5002",
	}
	newHost := func(t *testing.T) *Host {
		t.Helper()
		h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost: %v", err)
		}
		t.Cleanup(func() { h.Close() })
		return h
	}

	t.Run("unreachable peers past the election timeout", func(t *testing.T) {
		h := newHost(t)
		cfg := testShardConfig(7, 1)
		timeout := time.Duration(cfg.ElectionRTT*testHostConfig(t).RTTMillisecond) * time.Millisecond
		start := time.Now()
		if err := h.StartShard(unreachable, false, testCreateFunc, cfg); err != nil {
			t.Fatalf("StartShard: %v", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		var status *HealthStatus
		for {
			status = h.HealthCheck()
			if status.Status == "not-ready" || time.Now().After(deadline) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if status.Status != "not-ready" {
			t.Fatalf("Status = %q with no quorum reachable, want %q", status.Status, "not-ready")
		}
		if since := time.Since(start); since < timeout {
			t.Errorf("not-ready after %v, before the election timeout %v", since, timeout)
		}
		if len(status.Shards) != 1 || status.Shards[0].LeaderID != 0 {
			t.Fatalf("Shards = %+v, want one leaderless shard", status.Shards)
		}
		if got := notReadyShardsJSON(t, status); len(got) != 1 || got[0] != 7 {
			t.Errorf("not_ready_shards = %v, want [7]", got)
		}
	})

	t.Run("healthy single-member shard", func(t *testing.T) {
		h := newPauseTestHost(t, 3)
		cfg := testShardConfig(3, 1)
		// Well past the election timeout: a led shard stays ok.
		time.Sleep(5 * time.Duration(cfg.ElectionRTT*testHostConfig(t).RTTMillisecond) * time.Millisecond)
		status := h.HealthCheck()
		if status.Status != "ok" {
			t.Errorf("Status = %q, want %q", status.Status, "ok")
		}
		if got := notReadyShardsJSON(t, status); len(got) != 0 {
			t.Errorf("not_ready_shards = %v, want none", got)
		}
		checkHealthStatusJSON(t, status)
	})

	t.Run("leaderless within the election timeout", func(t *testing.T) {
		h := newHost(t)
		cfg := testShardConfig(9, 1)
		// A 10 minute election timeout: the shard is still inside its first
		// election window for the whole test.
		cfg.ElectionRTT = 600_000
		if err := h.StartShard(unreachable, false, testCreateFunc, cfg); err != nil {
			t.Fatalf("StartShard: %v", err)
		}
		status := h.HealthCheck()
		if status.Status != "ok" {
			t.Errorf("Status = %q inside the election timeout, want %q", status.Status, "ok")
		}
		if len(status.Shards) != 1 || status.Shards[0].Health != ShardNoLeader {
			t.Fatalf("Shards = %+v, want one shard with no leader", status.Shards)
		}
		if got := notReadyShardsJSON(t, status); len(got) != 0 {
			t.Errorf("not_ready_shards = %v, want none", got)
		}
		checkHealthStatusJSON(t, status)
	})
}

// TestHealthCheck_LeaderlessGraceCoversRandomizedElection pins the window
// to the randomized election timeout: raft campaigns after a timeout drawn
// from [ElectionRTT, 2*ElectionRTT) ticks and the campaign then has one
// more election timeout to win, so a shard leaderless for less than
// 3 * ElectionRTT * RTTMillisecond may still be in a normal first election
// and must not count as not-ready.
func TestHealthCheck_LeaderlessGraceCoversRandomizedElection(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	cfg := testShardConfig(11, 1)
	cfg.ElectionRTT = 10
	ns := &nodeState{shardID: 11, replicaID: 1, cfg: cfg}
	nominal := time.Duration(cfg.ElectionRTT*h.cfg.RTTMillisecond) * time.Millisecond
	now := h.leaderlessNow()

	// Past the nominal timeout, and past the largest randomized one, but
	// inside the campaign that follows it.
	for _, tenths := range []int64{11, 19, 29} {
		age := time.Duration(int64(nominal) * tenths / 10)
		ns.leaderlessSince.Store(now - int64(age))
		if h.leaderlessPastElectionTimeout(ns, now) {
			t.Errorf("leaderless for %v (< 3 * %v) reported past the election window", age, nominal)
		}
	}
	// Past the randomized timeout plus a full election round.
	age := nominal * 31 / 10
	ns.leaderlessSince.Store(now - int64(age))
	if !h.leaderlessPastElectionTimeout(ns, now) {
		t.Errorf("leaderless for %v (> 3 * %v) not reported past the election window", age, nominal)
	}
	// A known leader is never past the window.
	ns.leaderlessSince.Store(0)
	if h.leaderlessPastElectionTimeout(ns, now) {
		t.Error("shard with a known leader reported past the election window")
	}
}

// TestHealthCheck_HealthyStartupNeverNotReady observes health on every
// step of a healthy single-member shard's startup, from StartShard until
// its leader is published: it never reports not-ready inside the
// election window, and reports ok once the leader is known.
//
// The window is wall-clock (docs/observability/api.md: 3 * ElectionRTT *
// RTTMillisecond from StartShard) while the election runs on logical
// ticks from a time.Ticker, which drops ticks whenever the step worker
// overruns the tick interval. Under -race on a loaded runner a 1ms tick
// overran often enough that the 50-100 tick election outlasted the 150ms
// window (CI run 44581832844: not-ready 0.16s after StartShard). That
// shard really was leaderless past its window, so not-ready was the
// contract's answer. The test therefore uses a 10ms tick, against which
// the step worker's per-tick cost is small, and fails only on a not-ready
// report before the window has elapsed since StartShard was called.
func TestHealthCheck_HealthyStartupNeverNotReady(t *testing.T) {
	for round := 0; round < 5; round++ {
		hc := testHostConfig(t)
		hc.RTTMillisecond = 10
		h, err := NewHost(hc, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost: %v", err)
		}
		// 100ms nominal timeout, 300ms window: the election's fixed costs
		// (log persist, leader publication, tick scheduling) stay small
		// against it.
		cfg := testShardConfig(5, 1)
		cfg.ElectionRTT = 10
		window := 3 * time.Duration(cfg.ElectionRTT*hc.RTTMillisecond) * time.Millisecond
		start := time.Now()
		if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, cfg); err != nil {
			h.Close()
			t.Fatalf("StartShard: %v", err)
		}
		deadline := start.Add(10 * time.Second)
		for {
			status := h.HealthCheck()
			if status.Status == "not-ready" {
				// Read the clock after the report: the window opened
				// after start, so elapsed overstates the shard's age and
				// a report it calls early is early.
				if elapsed := time.Since(start); elapsed <= window {
					h.Close()
					t.Fatalf("round %d: healthy startup reported not-ready %v after StartShard, inside its %v window: not_ready_shards = %v",
						round, elapsed, window, notReadyShardsJSON(t, status))
				}
			}
			if len(status.Shards) == 1 && status.Shards[0].LeaderID != 0 {
				if status.Status != "ok" {
					h.Close()
					t.Fatalf("round %d: leader %d known, Status = %q, want ok",
						round, status.Shards[0].LeaderID, status.Status)
				}
				break
			}
			if time.Now().After(deadline) {
				h.Close()
				t.Fatalf("round %d: no leader elected", round)
			}
			time.Sleep(100 * time.Microsecond)
		}
		h.Close()
	}
}

// TestHealthCheck_ResumeRestartsElectionWindow pins that time spent paused
// does not count against a resumed shard: ResumeShard gives the peer a
// fresh election timeout, so a shard that was leaderless before a pause
// longer than its window is not reported not-ready the moment it resumes.
func TestHealthCheck_ResumeRestartsElectionWindow(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := testShardConfig(13, 1)
	// A 500ms nominal timeout (1.5s window) leaves the check after resume
	// far inside the fresh window, even under the race detector.
	cfg.ElectionRTT = 500
	unreachable := map[uint64]string{1: "127.0.0.1:5000", 2: "127.0.0.1:5001", 3: "127.0.0.1:5002"}
	if err := h.StartShard(unreachable, false, testCreateFunc, cfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	waitNotReady := func(what string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for h.HealthCheck().Status != "not-ready" {
			if time.Now().After(deadline) {
				t.Fatalf("%s: shard 13 never reported not-ready", what)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitNotReady("before pause")

	if err := h.PauseShard(ctx, 13); err != nil {
		t.Fatalf("PauseShard: %v", err)
	}
	// Paused for longer than the whole election window.
	time.Sleep(3*time.Duration(cfg.ElectionRTT*h.cfg.RTTMillisecond)*time.Millisecond + 200*time.Millisecond)
	if err := h.ResumeShard(ctx, 13); err != nil {
		t.Fatalf("ResumeShard: %v", err)
	}
	status := h.HealthCheck()
	if status.Status != "ok" {
		t.Errorf("Status right after resume = %q, want %q (fresh election window)", status.Status, "ok")
	}
	if got := notReadyShardsJSON(t, status); len(got) != 0 {
		t.Errorf("not_ready_shards right after resume = %v, want none", got)
	}
	// Still no quorum: once the fresh window passes it is not-ready again.
	waitNotReady("after resume")
}

// resumeProbeClock is a steppedClock that, once armed, runs probe on the
// first reading taken while the shard is unpaused, then disarms. A reading
// is how ResumeShard stamps the fresh window, so the probe lands exactly
// in the interleaving a concurrent health probe can hit during a resume.
type resumeProbeClock struct {
	steppedClock
	ns    atomic.Pointer[nodeState]
	armed atomic.Bool
	probe func()
}

func (c *resumeProbeClock) monoNanos() int64 {
	if ns := c.ns.Load(); ns != nil && !ns.paused.Load() && c.armed.CompareAndSwap(true, false) {
		c.probe()
	}
	return c.steppedClock.monoNanos()
}

// TestHealthCheck_ResumeNeverExposesExpiredWindow checks ResumeShard installs a fresh window before publishing the shard as unpaused.
func TestHealthCheck_ResumeNeverExposesExpiredWindow(t *testing.T) {
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	clk := &resumeProbeClock{}
	clk.mono.Store(int64(time.Hour))
	h.clock = clk
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := testShardConfig(23, 1)
	cfg.ElectionRTT = 10
	unreachable := map[uint64]string{1: "127.0.0.1:5000", 2: "127.0.0.1:5001", 3: "127.0.0.1:5002"}
	if err := h.StartShard(unreachable, false, testCreateFunc, cfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	v, ok := h.nodes.Load(uint64(23))
	if !ok {
		t.Fatal("shard 23 not registered")
	}
	ns := v.(*nodeState)
	grace := h.leaderlessGrace(ns)
	clk.advance(2 * grace)
	if got := h.HealthCheck().Status; got != "not-ready" {
		t.Fatalf("leaderless for %v (window %v): Status = %q, want not-ready", 2*grace, grace, got)
	}
	if err := h.PauseShard(ctx, 23); err != nil {
		t.Fatalf("PauseShard: %v", err)
	}
	clk.advance(2 * grace)

	var during atomic.Pointer[HealthStatus]
	clk.probe = func() {
		// A separate goroutine, as a readiness probe is: ResumeShard holds
		// the pause gate, which HealthCheck does not take.
		done := make(chan struct{})
		go func() {
			defer close(done)
			during.Store(h.HealthCheck())
		}()
		<-done
	}
	clk.ns.Store(ns)
	clk.armed.Store(true)
	if err := h.ResumeShard(ctx, 23); err != nil {
		t.Fatalf("ResumeShard: %v", err)
	}
	clk.armed.Store(false)
	if s := during.Load(); s != nil && s.Status != "ok" {
		t.Errorf("HealthCheck during ResumeShard: Status = %q (not_ready_shards %v), want ok: the pre-pause window leaked",
			s.Status, notReadyShardsJSON(t, s))
	}
	if got := h.HealthCheck().Status; got != "ok" {
		t.Errorf("Status right after resume = %q, want ok (fresh election window)", got)
	}
}

// TestHealthCheck_UnrepresentableGraceNeverNotReady checks the window saturates when tripling the election timeout overflows time.Duration.
func TestHealthCheck_UnrepresentableGraceNeverNotReady(t *testing.T) {
	hc := testHostConfig(t)
	hc.RTTMillisecond = 60000
	h, err := NewHost(hc, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	clk := &steppedClock{}
	clk.mono.Store(int64(time.Hour))
	h.clock = clk

	cfg := testShardConfig(24, 1)
	cfg.ElectionRTT = 100000000 // nominal 6e18 ns fits; 3x does not
	unreachable := map[uint64]string{1: "127.0.0.1:5000", 2: "127.0.0.1:5001", 3: "127.0.0.1:5002"}
	if err := h.StartShard(unreachable, false, testCreateFunc, cfg); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	v, ok := h.nodes.Load(uint64(24))
	if !ok {
		t.Fatal("shard 24 not registered")
	}
	ns := v.(*nodeState)
	if grace := h.leaderlessGrace(ns); grace <= 0 {
		t.Errorf("leaderlessGrace = %v, want a positive (saturated) window", grace)
	}
	if got := h.HealthCheck().Status; got != "ok" {
		t.Errorf("freshly started leaderless shard: Status = %q, want ok", got)
	}
	// Past the nominal timeout but inside any representable window.
	clk.advance(time.Duration(cfg.ElectionRTT*hc.RTTMillisecond)*time.Millisecond + time.Second)
	if got := h.HealthCheck().Status; got != "ok" {
		t.Errorf("leaderless just past the nominal timeout: Status = %q, want ok", got)
	}
}

// steppedClock is a leaderlessClock whose wall clock a test can step
// without moving its monotonic clock, as NTP or an operator does.
type steppedClock struct{ wall, mono atomic.Int64 }

func (c *steppedClock) wallNanos() int64 { return c.wall.Load() }
func (c *steppedClock) monoNanos() int64 { return c.mono.Load() }

// advance lets d of real time pass: both clocks move.
func (c *steppedClock) advance(d time.Duration) {
	c.wall.Add(int64(d))
	c.mono.Add(int64(d))
}

// stepWall corrects the wall clock by d: the monotonic clock stays.
func (c *steppedClock) stepWall(d time.Duration) { c.wall.Add(int64(d)) }

// clockedShard is a leaderless shard registered on a host whose leaderless
// windows run on a steppedClock.
type clockedShard struct {
	h     *Host
	ns    *nodeState
	clk   *steppedClock
	grace time.Duration
}

func newClockedShard(t *testing.T, shardID uint64) *clockedShard {
	t.Helper()
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	clk := &steppedClock{}
	clk.wall.Store(time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC).UnixNano())
	clk.mono.Store(int64(time.Hour))
	h.clock = clk
	cfg := testShardConfig(shardID, 1)
	cfg.ElectionRTT = 10
	ns := &nodeState{shardID: shardID, replicaID: 1, cfg: cfg}
	// As StartShard does: a starting shard opens its election window.
	ns.leaderlessSince.Store(h.leaderlessNow())
	h.nodes.Store(shardID, ns)
	// Runs before Close (cleanups are LIFO): Close never sees the bare state.
	t.Cleanup(func() { h.nodes.Delete(shardID) })
	return &clockedShard{h: h, ns: ns, clk: clk, grace: h.leaderlessGrace(ns)}
}

// notReady reports whether HealthCheck reports the host not-ready with
// the shard listed.
func (s *clockedShard) notReady(t *testing.T) bool {
	t.Helper()
	status := s.h.HealthCheck()
	got := notReadyShardsJSON(t, status)
	listed := len(got) == 1 && got[0] == s.ns.shardID
	if listed != (status.Status == "not-ready") {
		t.Fatalf("Status = %q with not_ready_shards %v", status.Status, got)
	}
	return listed
}

// TestHealthCheck_LeaderlessWindowIgnoresWallClockSteps checks the window uses the monotonic clock.
func TestHealthCheck_LeaderlessWindowIgnoresWallClockSteps(t *testing.T) {
	t.Run("backward step keeps an expired window expired", func(t *testing.T) {
		s := newClockedShard(t, 21)
		s.clk.advance(2 * s.grace)
		if !s.notReady(t) {
			t.Fatalf("leaderless for %v (window %v): want not-ready", 2*s.grace, s.grace)
		}
		s.clk.stepWall(-time.Hour)
		if !s.notReady(t) {
			t.Error("wall clock stepped back an hour: host reported ready with no leader")
		}
	})

	t.Run("forward step does not expire a fresh window", func(t *testing.T) {
		s := newClockedShard(t, 22)
		s.clk.stepWall(time.Hour)
		if s.notReady(t) {
			t.Error("wall clock stepped forward an hour: not-ready inside the election window")
		}
		s.clk.advance(s.grace / 2)
		if s.notReady(t) {
			t.Errorf("leaderless for %v (window %v): want ready", s.grace/2, s.grace)
		}
		s.clk.advance(s.grace)
		if !s.notReady(t) {
			t.Errorf("leaderless for %v (window %v): want not-ready", s.grace*3/2, s.grace)
		}
	})
}

// TestHealthCheck_LeaderLossAfterElectionOpensFreshWindow checks a later leader loss opens a fresh window that a new leader clears.
func TestHealthCheck_LeaderLossAfterElectionOpensFreshWindow(t *testing.T) {
	s := newClockedShard(t, 23)
	s.clk.advance(s.grace / 2)
	s.h.updateNodeLeader(23, 1, 1)
	if s.notReady(t) {
		t.Fatal("shard with a leader reported not-ready")
	}
	// Far past the startup window, then the leader is lost.
	s.clk.advance(2 * s.grace)
	s.h.updateNodeLeader(23, 0, 2)
	if s.notReady(t) {
		t.Fatal("leader lost just now: not-ready inside the fresh election window")
	}
	half := s.grace / 2
	s.clk.advance(half)
	if s.notReady(t) {
		t.Fatalf("leaderless for %v (window %v): want ready", half, s.grace)
	}
	// Another leaderless update (a new term with no winner) keeps the
	// window at the loss time.
	s.h.updateNodeLeader(23, 0, 3)
	s.clk.advance(s.grace - half + time.Millisecond)
	if !s.notReady(t) {
		t.Fatalf("leaderless for %v (window %v) across a repeated update: want not-ready",
			s.grace+time.Millisecond, s.grace)
	}
	s.h.updateNodeLeader(23, 1, 3)
	if s.notReady(t) {
		t.Error("new leader published: still not-ready")
	}
	if since := s.ns.leaderlessSince.Load(); since != 0 {
		t.Errorf("leaderlessSince = %d with a known leader, want 0", since)
	}
}

// checkHealthStatusJSON asserts status marshals with exactly the documented
// field names and that the encoded values agree with the struct.
func checkHealthStatusJSON(t *testing.T, status *HealthStatus) {
	t.Helper()
	b, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("json.Marshal(HealthStatus): %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(b, &top); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	want := append([]string(nil), healthStatusJSONKeys...)
	if len(status.Shards) > 0 {
		want = append(want, "shards")
	}
	if len(status.NotReadyShards) > 0 {
		want = append(want, "not_ready_shards")
	}
	assertKeys(t, "HealthStatus", top, want, nil)

	var gotStatus string
	if err := json.Unmarshal(top["status"], &gotStatus); err != nil || gotStatus != status.Status {
		t.Errorf("JSON status = %q (err %v), want %q", gotStatus, err, status.Status)
	}
	var gotCount int
	if err := json.Unmarshal(top["shard_count"], &gotCount); err != nil || gotCount != status.ShardCount {
		t.Errorf("JSON shard_count = %d (err %v), want %d", gotCount, err, status.ShardCount)
	}
	if len(status.Shards) == 0 {
		return
	}

	var shards []map[string]json.RawMessage
	if err := json.Unmarshal(top["shards"], &shards); err != nil {
		t.Fatalf("json.Unmarshal(shards): %v", err)
	}
	if len(shards) != len(status.Shards) {
		t.Fatalf("JSON shards length = %d, want %d", len(shards), len(status.Shards))
	}
	for i, s := range shards {
		assertKeys(t, "ShardInfo", s, shardInfoJSONKeys, []string{"replicas"})
		var health string
		if err := json.Unmarshal(s["health"], &health); err != nil || health != status.Shards[i].Health.String() {
			t.Errorf("JSON shards[%d].health = %q (err %v), want %q",
				i, health, err, status.Shards[i].Health.String())
		}
		var paused bool
		if err := json.Unmarshal(s["paused"], &paused); err != nil || paused != status.Shards[i].Paused {
			t.Errorf("JSON shards[%d].paused = %v (err %v), want %v", i, paused, err, status.Shards[i].Paused)
		}
	}

	// The encoding round-trips.
	var back HealthStatus
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("json.Unmarshal(HealthStatus): %v", err)
	}
	if back.Status != status.Status || back.ShardCount != status.ShardCount || len(back.Shards) != len(status.Shards) {
		t.Errorf("round trip = %+v, want %+v", back, *status)
	}
}

// assertKeys checks obj has every key in want, no key outside want and
// optional.
func assertKeys(t *testing.T, what string, obj map[string]json.RawMessage, want, optional []string) {
	t.Helper()
	allowed := make(map[string]bool, len(want)+len(optional))
	for _, k := range want {
		allowed[k] = true
		if _, ok := obj[k]; !ok {
			t.Errorf("%s JSON lacks field %q", what, k)
		}
	}
	for _, k := range optional {
		allowed[k] = true
	}
	var extra []string
	for k := range obj {
		if !allowed[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Errorf("%s JSON has undocumented fields %v", what, extra)
	}
}
