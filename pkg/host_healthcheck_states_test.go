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
