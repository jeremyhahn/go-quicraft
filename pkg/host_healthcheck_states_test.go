package quicraft

import (
	"context"
	"encoding/json"
	"sort"
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
// from [ElectionRTT, 2*ElectionRTT) ticks, so a shard leaderless for less
// than 2 * ElectionRTT * RTTMillisecond may not have started its first
// election and must not count as not-ready.
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
	now := time.Now().UnixNano()

	// Past the nominal timeout but inside the randomized one.
	ns.leaderlessSince.Store(now - int64(nominal*19/10))
	if h.leaderlessPastElectionTimeout(ns, now) {
		t.Errorf("leaderless for %v (< 2 * %v) reported past the election window", nominal*19/10, nominal)
	}
	// Past the largest randomized timeout.
	ns.leaderlessSince.Store(now - int64(nominal*21/10))
	if !h.leaderlessPastElectionTimeout(ns, now) {
		t.Errorf("leaderless for %v (> 2 * %v) not reported past the election window", nominal*21/10, nominal)
	}
	// A known leader is never past the window.
	ns.leaderlessSince.Store(0)
	if h.leaderlessPastElectionTimeout(ns, now) {
		t.Error("shard with a known leader reported past the election window")
	}
}

// TestHealthCheck_HealthyStartupNeverNotReady observes health on every
// step of a healthy single-member shard's startup, from StartShard until
// its leader is published: it never reports not-ready on the way.
func TestHealthCheck_HealthyStartupNeverNotReady(t *testing.T) {
	for round := 0; round < 5; round++ {
		h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost: %v", err)
		}
		if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, testShardConfig(5, 1)); err != nil {
			h.Close()
			t.Fatalf("StartShard: %v", err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			status := h.HealthCheck()
			if status.Status == "not-ready" {
				h.Close()
				t.Fatalf("round %d: healthy startup reported not-ready: not_ready_shards = %v",
					round, notReadyShardsJSON(t, status))
			}
			if len(status.Shards) == 1 && status.Shards[0].LeaderID != 0 {
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
	// A 1s nominal timeout (2s window) leaves the check after resume far
	// inside the fresh window, even under the race detector.
	cfg.ElectionRTT = 1000
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
	time.Sleep(2*time.Duration(cfg.ElectionRTT*h.cfg.RTTMillisecond)*time.Millisecond + 200*time.Millisecond)
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
