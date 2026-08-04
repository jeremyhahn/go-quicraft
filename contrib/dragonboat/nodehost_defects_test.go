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

package dragonboat

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	dbClient "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/client"
	dbConfig "github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// testNodeHostConfig returns the standard test NodeHostConfig rooted at dir.
func testNodeHostConfig(dir string) dbConfig.NodeHostConfig {
	return dbConfig.NodeHostConfig{
		DeploymentID: 1,
		WALDir:       filepath.Join(dir, "wal"),
		NodeHostDir:  filepath.Join(dir, "data"),
		// A real port: dragonboat's address validator rejects port 0,
		// and initial-member targets are validated at Start*Replica.
		RaftAddress:    "localhost:63001",
		ListenAddress:  "localhost:0",
		RTTMillisecond: 1,
	}
}

// newTestNodeHostWithConfig creates a test NodeHost from an explicit
// configuration, backed by an in-memory LogDB and no transport.
func newTestNodeHostWithConfig(t *testing.T, nhConfig dbConfig.NodeHostConfig) *NodeHost {
	t.Helper()
	nh, err := NewNodeHostWithOptions(nhConfig,
		quicraft.WithMemoryLogDB(),
		quicraft.WithoutTransport(),
	)
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions: %v", err)
	}
	t.Cleanup(func() { nh.Close() })
	return nh
}

// ---------------------------------------------------------------------------
// C-M1: CommittedC must be nil when NotifyCommit is disabled on the host
// (dragonboat callers detect commit-notification availability by nil).
// ---------------------------------------------------------------------------

func TestRequestState_CommittedC_NilWhenNotifyCommitDisabled(t *testing.T) {
	nh := newTestNodeHost(t) // NotifyCommit defaults to false
	startTestShard(t, nh, 1, 1)

	session := nh.GetNoOPSession(1)
	rs, err := nh.Propose(session, []byte("cmd"), 5*time.Second)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	defer rs.Release()

	if rs.CommittedC() != nil {
		t.Error("CommittedC() must be nil when NotifyCommit is disabled on the host")
	}

	select {
	case <-rs.ResultC():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for propose result")
	}
}

func TestRequestState_CommittedC_DeliversWhenNotifyCommitEnabled(t *testing.T) {
	cfg := testNodeHostConfig(t.TempDir())
	cfg.NotifyCommit = true
	nh := newTestNodeHostWithConfig(t, cfg)
	startTestShard(t, nh, 1, 1)

	session := nh.GetNoOPSession(1)
	rs, err := nh.Propose(session, []byte("cmd"), 5*time.Second)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	defer rs.Release()

	committedC := rs.CommittedC()
	if committedC == nil {
		t.Fatal("CommittedC() must be non-nil when NotifyCommit is enabled")
	}

	select {
	case r := <-committedC:
		if !r.Committed() {
			t.Errorf("committed notification code = %v, want Committed()", r.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for committed notification")
	}

	select {
	case r := <-rs.ResultC():
		if !r.Completed() {
			t.Errorf("apply result code = %v, want Completed()", r.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for apply result")
	}
}

// ---------------------------------------------------------------------------
// C-M2: Release must wait for the bridge goroutines to exit before
// releasing (and thereby recycling) the inner pooled RequestState. A
// stale bridge selecting on the recycled channel could otherwise steal a
// successor request's result.
// ---------------------------------------------------------------------------

func TestRequestState_Release_WaitsForBridgeGoroutines(t *testing.T) {
	pool := quicraft.NewRequestStatePool()

	baseline := runtime.NumGoroutine()

	const n = 100
	states := make([]*RequestState, n)
	for i := range states {
		inner := pool.Get().(*quicraft.RequestState)
		states[i] = WrapRequestState(inner, false)
	}
	for _, rs := range states {
		rs.Release()
	}

	// Release waits for its bridges, so the goroutine count is back at
	// the baseline the moment the loop finishes; no grace sleep needed.
	after := runtime.NumGoroutine()
	if after > baseline+5 {
		t.Errorf("goroutines after Release = %d, baseline = %d: Release returned before bridge goroutines exited",
			after, baseline)
	}
}

func TestRequestState_ReleaseWithoutRead_DoesNotStealSuccessorResult(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)
	session := nh.GetNoOPSession(1)

	// Releasing a RequestState without reading its result recycles the
	// pooled inner object. The next proposal receives the recycled
	// object; its result must reach the new wrapper, never the released
	// one. Iterate to give a stale bridge (the pre-fix behavior) many
	// chances to steal.
	for i := 0; i < 200; i++ {
		abandoned, err := nh.Propose(session, []byte("abandoned"), 5*time.Second)
		if err != nil {
			t.Fatalf("iteration %d: Propose(abandoned): %v", i, err)
		}
		abandoned.Release()

		rs, err := nh.Propose(session, []byte("watched"), 5*time.Second)
		if err != nil {
			t.Fatalf("iteration %d: Propose(watched): %v", i, err)
		}
		select {
		case r := <-rs.ResultC():
			if !r.Completed() {
				t.Fatalf("iteration %d: result code = %v, want Completed()", i, r.code)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: result never delivered: stolen by a released request's bridge", i)
		}
		rs.Release()
	}
}

// ---------------------------------------------------------------------------
// C-M3: the RemoveData guard must survive a NodeHost restart. Dragonboat
// persists a marker; a restarted host must keep rejecting StartReplica
// with ErrReplicaRemoved.
// ---------------------------------------------------------------------------

func TestNodeHost_RemoveData_GuardSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	nhConfig := testNodeHostConfig(dir)

	nh1, err := NewNodeHostWithOptions(nhConfig,
		quicraft.WithMemoryLogDB(),
		quicraft.WithoutTransport(),
	)
	if err != nil {
		t.Fatalf("NewNodeHostWithOptions: %v", err)
	}
	startTestShard(t, nh1, 1, 1)

	if err := nh1.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	if err := nh1.RemoveData(1, 1); err != nil {
		t.Fatalf("RemoveData: %v", err)
	}

	// The durable marker must exist on disk.
	marker := nh1.removedMarkerPath(1, 1)
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("removal marker not persisted at %s: %v", marker, err)
	}

	// Same process: restart must be rejected.
	cfg := dbConfig.Config{
		ShardID: 1, ReplicaID: 1,
		ElectionRTT: 5, HeartbeatRTT: 1,
	}
	members := map[uint64]Target{1: nh1.RaftAddress()}
	if err := nh1.StartReplica(members, false, testSMFactory(), cfg); !errors.Is(err, ErrReplicaRemoved) {
		t.Fatalf("StartReplica after RemoveData = %v, want ErrReplicaRemoved", err)
	}
	nh1.Close()

	// New NodeHost over the same NodeHostDir: the guard must persist.
	nh2 := newTestNodeHostWithConfig(t, nhConfig)
	members = map[uint64]Target{1: nh2.RaftAddress()}
	err = nh2.StartReplica(members, false, testSMFactory(), cfg)
	if !errors.Is(err, ErrReplicaRemoved) {
		t.Fatalf("StartReplica after restart = %v, want ErrReplicaRemoved", err)
	}
	if errors.Is(err, ErrReplicaNotFound) {
		t.Error("removal guard must report ErrReplicaRemoved, not ErrReplicaNotFound")
	}

	// A different replica of the same shard is unaffected by the marker.
	if nh2.isRemoved(1, 2) {
		t.Error("marker for replica 1 must not cover replica 2")
	}
}

func TestNodeHost_SyncRemoveData_PersistsMarker(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 3, 1)
	if err := nh.StopShard(3); err != nil {
		t.Fatalf("StopShard: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := nh.SyncRemoveData(ctx, 3, 1); err != nil {
		t.Fatalf("SyncRemoveData: %v", err)
	}
	if _, err := os.Stat(nh.removedMarkerPath(3, 1)); err != nil {
		t.Fatalf("removal marker not persisted: %v", err)
	}
}

func TestParseRemovedMarkerName(t *testing.T) {
	tests := []struct {
		name      string
		shardID   uint64
		replicaID uint64
		ok        bool
	}{
		{"shard-1-replica-2", 1, 2, true},
		{"shard-18446744073709551615-replica-7", 18446744073709551615, 7, true},
		{"shard-1-replica-", 0, 0, false},
		{"shard--replica-2", 0, 0, false},
		{"shard-1", 0, 0, false},
		{"replica-2", 0, 0, false},
		{"garbage", 0, 0, false},
		{"shard-x-replica-2", 0, 0, false},
		{"shard-1-replica-y", 0, 0, false},
	}
	for _, tt := range tests {
		shardID, replicaID, ok := parseRemovedMarkerName(tt.name)
		if ok != tt.ok || shardID != tt.shardID || replicaID != tt.replicaID {
			t.Errorf("parseRemovedMarkerName(%q) = (%d, %d, %v), want (%d, %d, %v)",
				tt.name, shardID, replicaID, ok, tt.shardID, tt.replicaID, tt.ok)
		}
	}
}

// ---------------------------------------------------------------------------
// C-M4: the register path returns without blocking on consensus and does
// not corrupt the session's series tracking when the caller follows the
// documented dragonboat pattern (register → wait → PrepareForPropose).
// ---------------------------------------------------------------------------

func TestNodeHost_ProposeSession_RegisterAsyncPattern(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	session := dbClient.NewSession(1, nil)
	session.PrepareForRegister()

	rs, err := nh.ProposeSession(session, 5*time.Second)
	if err != nil {
		t.Fatalf("ProposeSession(register): %v", err)
	}
	defer rs.Release()

	select {
	case r := <-rs.ResultC():
		if !r.Completed() {
			t.Fatalf("register result code = %v, want Completed()", r.code)
		}
		if r.GetResult().Value != session.ClientID() {
			t.Fatalf("register result value = %d, want client ID %d",
				r.GetResult().Value, session.ClientID())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for register result")
	}

	// Documented dragonboat pattern: the caller advances the session
	// after a successful registration. This must be an idempotent no-op
	// (never a series regression) even though quicraft's RegisterSession
	// prepared the session internally.
	session.PrepareForPropose()
	if session.SeriesID() != quicraft.SeriesIDFirstProposal {
		t.Fatalf("SeriesID after PrepareForPropose = %d, want %d",
			session.SeriesID(), quicraft.SeriesIDFirstProposal)
	}

	// Two distinct tracked proposals must both be applied: a series
	// regression would make the second look like a retransmission of the
	// first and silently return the cached result.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res1, err := nh.SyncPropose(ctx, session, []byte("a"))
	if err != nil {
		t.Fatalf("SyncPropose #1: %v", err)
	}
	if res1.Value != 1 { // testStateMachine returns len(cmd)
		t.Fatalf("first proposal Value = %d, want 1", res1.Value)
	}
	session.ProposalCompleted()

	res2, err := nh.SyncPropose(ctx, session, []byte("bb"))
	if err != nil {
		t.Fatalf("SyncPropose #2: %v", err)
	}
	if res2.Value != 2 {
		t.Fatalf("second proposal Value = %d, want 2 (got a deduplicated cached result: series regressed)", res2.Value)
	}
	session.ProposalCompleted()
}

// ---------------------------------------------------------------------------
// C-M5: a live in-use session passed to ProposeSession must be rejected
// with ErrInvalidSession, never silently unregistered.
// ---------------------------------------------------------------------------

func TestNodeHost_ProposeSession_LiveSessionRejected(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := nh.SyncGetSession(ctx, 1)
	if err != nil {
		t.Fatalf("SyncGetSession: %v", err)
	}

	// The registered session's series is in the normal proposal range:
	// it is NOT valid for a session operation.
	rs, err := nh.ProposeSession(session, 5*time.Second)
	if !errors.Is(err, ErrInvalidSession) {
		if rs != nil {
			rs.Release()
		}
		t.Fatalf("ProposeSession(live session) = %v, want ErrInvalidSession", err)
	}

	// The session must remain intact and usable for proposals.
	if _, err := nh.SyncPropose(ctx, session, []byte("still-alive")); err != nil {
		t.Fatalf("SyncPropose after rejected ProposeSession: %v (session was destroyed)", err)
	}
	session.ProposalCompleted()
}

func TestNodeHost_ProposeSession_NoOPSessionRejected(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	session := nh.GetNoOPSession(1)
	_, err := nh.ProposeSession(session, time.Second)
	if !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("ProposeSession(noop session) = %v, want ErrInvalidSession", err)
	}
}

// ---------------------------------------------------------------------------
// C-L1: a node user must reject sessions belonging to a different shard.
// ---------------------------------------------------------------------------

func TestNodeUser_Propose_ForeignSessionRejected(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	user, err := nh.GetNodeUser(1)
	if err != nil {
		t.Fatalf("GetNodeUser: %v", err)
	}

	foreign := nh.GetNoOPSession(2)
	rs, err := user.Propose(foreign, []byte("cmd"), time.Second)
	if !errors.Is(err, ErrInvalidSession) {
		if rs != nil {
			rs.Release()
		}
		t.Fatalf("Propose with foreign session = %v, want ErrInvalidSession", err)
	}
}

// ---------------------------------------------------------------------------
// C-L3: Start*Replica must validate the shard config and reject
// join=true with non-empty initialMembers (dragonboat's
// ErrInvalidShardSettings).
// ---------------------------------------------------------------------------

func TestNodeHost_StartReplica_JoinWithInitialMembersRejected(t *testing.T) {
	nh := newTestNodeHost(t)
	cfg := dbConfig.Config{
		ShardID: 1, ReplicaID: 1,
		ElectionRTT: 5, HeartbeatRTT: 1,
	}
	members := map[uint64]Target{1: nh.RaftAddress()}

	if err := nh.StartReplica(members, true, testSMFactory(), cfg); !errors.Is(err, ErrInvalidShardSettings) {
		t.Errorf("StartReplica(join+members) = %v, want ErrInvalidShardSettings", err)
	}
	if err := nh.StartConcurrentReplica(members, true, nil, cfg); !errors.Is(err, ErrInvalidShardSettings) {
		t.Errorf("StartConcurrentReplica(join+members) = %v, want ErrInvalidShardSettings", err)
	}
	if err := nh.StartOnDiskReplica(members, true, nil, cfg); !errors.Is(err, ErrInvalidShardSettings) {
		t.Errorf("StartOnDiskReplica(join+members) = %v, want ErrInvalidShardSettings", err)
	}
}

func TestNodeHost_StartReplica_InvalidConfigRejected(t *testing.T) {
	nh := newTestNodeHost(t)
	// ElectionRTT must be > 2 * HeartbeatRTT; this violates it.
	cfg := dbConfig.Config{
		ShardID: 1, ReplicaID: 1,
		ElectionRTT: 2, HeartbeatRTT: 1,
	}
	members := map[uint64]Target{1: nh.RaftAddress()}

	err := nh.StartReplica(members, false, testSMFactory(), cfg)
	if !errors.Is(err, ErrInvalidShardSettings) {
		t.Fatalf("StartReplica(invalid config) = %v, want ErrInvalidShardSettings", err)
	}
	var vErr *dbConfig.ValidationError
	if !errors.As(err, &vErr) {
		t.Errorf("error should preserve the underlying *config.ValidationError, got %T", err)
	}
}

// ---------------------------------------------------------------------------
// C-L4: ErrReplicaRemoved must be distinct from quicraft's "replica not
// found" so an unrelated lookup miss is never reported as a permanent
// removal.
// ---------------------------------------------------------------------------

func TestErrReplicaRemoved_DistinctFromReplicaNotFound(t *testing.T) {
	if errors.Is(ErrReplicaRemoved, quicraft.ErrReplicaNotFound) {
		t.Error("ErrReplicaRemoved must not alias quicraft.ErrReplicaNotFound")
	}
	if errors.Is(ErrReplicaNotFound, ErrReplicaRemoved) {
		t.Error("ErrReplicaNotFound must not match ErrReplicaRemoved")
	}
	if got := translateError(quicraft.ErrReplicaNotFound); !errors.Is(got, ErrReplicaNotFound) {
		t.Errorf("translateError(quicraft.ErrReplicaNotFound) = %v, want ErrReplicaNotFound", got)
	}
	if got := translateError(quicraft.ErrReplicaNotFound); errors.Is(got, ErrReplicaRemoved) {
		t.Error("translateError(quicraft.ErrReplicaNotFound) must not report ErrReplicaRemoved")
	}
}

// ---------------------------------------------------------------------------
// H1: NewNodeHost must validate the NodeHostConfig
// ---------------------------------------------------------------------------

func TestNewNodeHost_RejectsInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name   string
		mutate func(*dbConfig.NodeHostConfig)
	}{
		{"EmptyNodeHostDir", func(c *dbConfig.NodeHostConfig) { c.NodeHostDir = "" }},
		{"EmptyWALDir", func(c *dbConfig.NodeHostConfig) { c.WALDir = "" }},
		{"EmptyRaftAddress", func(c *dbConfig.NodeHostConfig) { c.RaftAddress = "" }},
		{"ZeroRTT", func(c *dbConfig.NodeHostConfig) { c.RTTMillisecond = 0 }},
		{"Gossip", func(c *dbConfig.NodeHostConfig) {
			c.Gossip = dbConfig.GossipConfig{BindAddress: "localhost:9000"}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testNodeHostConfig(dir)
			tt.mutate(&cfg)

			nh, err := NewNodeHost(cfg)
			if err == nil {
				nh.Close()
				t.Fatal("NewNodeHost accepted an invalid config")
			}
			var ce *NodeHostConstructionError
			if !errors.As(err, &ce) {
				t.Fatalf("expected *NodeHostConstructionError, got %T: %v", err, err)
			}

			nh, err = NewNodeHostWithOptions(cfg,
				quicraft.WithMemoryLogDB(), quicraft.WithoutTransport())
			if err == nil {
				nh.Close()
				t.Fatal("NewNodeHostWithOptions accepted an invalid config")
			}
			if !errors.As(err, &ce) {
				t.Fatalf("expected *NodeHostConstructionError, got %T: %v", err, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// H4: quicraft's not-leader error must never reach the caller as
// ErrNotLeader, an error dragonboat does not have and IsTempError used to
// reject.
// ---------------------------------------------------------------------------

func TestNodeHost_NotLeaderSurfacesAsShardNotReady(t *testing.T) {
	nh := newTestNodeHost(t)

	// A shard that is joining has no leader, so every proposal path fails
	// fast with quicraft's not-leader error.
	cfg := dbConfig.Config{
		ShardID:      7,
		ReplicaID:    2,
		ElectionRTT:  10,
		HeartbeatRTT: 1,
	}
	if err := nh.StartReplica(nil, true, testSMFactory(), cfg); err != nil {
		t.Fatalf("StartReplica(join): %v", err)
	}

	session := nh.GetNoOPSession(7)
	_, err := nh.Propose(session, []byte("cmd"), time.Second)
	if err == nil {
		t.Skip("proposal accepted; leaderless path not exercised")
	}
	if errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose returned ErrNotLeader, an error dragonboat never returns: %v", err)
	}
	if !errors.Is(err, ErrShardNotReady) {
		t.Fatalf("Propose = %v, want ErrShardNotReady", err)
	}
	if !IsTempError(err) {
		t.Error("a leaderless proposal failure must be a temporary error")
	}
}

// ---------------------------------------------------------------------------
// H5: configChangeIndex is ignored unless OrderedConfigChange is true
// ---------------------------------------------------------------------------

func TestNodeHost_ConfigChangeIndexIgnoredWhenUnordered(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	// The shard was started with OrderedConfigChange false (the dragonboat
	// default), so a stale cached index must not be forwarded.
	if got := nh.effectiveConfigChangeIndex(1, 99); got != 0 {
		t.Errorf("effectiveConfigChangeIndex = %d, want 0 when unordered", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A stale index must not produce dragonboat's non-existent
	// ErrStaleConfigChange.
	err := nh.SyncRequestAddNonVoting(ctx, 1, 2, "localhost:63002", 99)
	if errors.Is(err, ErrStaleConfigChange) {
		t.Fatalf("stale index rejected despite OrderedConfigChange=false: %v", err)
	}
}

func TestNodeHost_ConfigChangeIndexHonoredWhenOrdered(t *testing.T) {
	nh := newTestNodeHost(t)

	members := map[uint64]Target{1: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:             2,
		ReplicaID:           1,
		ElectionRTT:         5,
		HeartbeatRTT:        1,
		CheckQuorum:         true,
		OrderedConfigChange: true,
	}
	if err := nh.StartReplica(members, false, testSMFactory(), cfg); err != nil {
		t.Fatalf("StartReplica: %v", err)
	}
	if got := nh.effectiveConfigChangeIndex(2, 99); got != 99 {
		t.Errorf("effectiveConfigChangeIndex = %d, want 99 when ordered", got)
	}
	if !nh.orderedConfigChange(2) {
		t.Error("orderedConfigChange must report the retained shard config")
	}
}

// ---------------------------------------------------------------------------
// M: QueryRaftLog out-of-range resolves with RequestOutOfRange
// ---------------------------------------------------------------------------

func TestNodeHost_QueryRaftLog_OutOfRange(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	rs, err := nh.QueryRaftLog(1, 1_000_000, 1_000_010, 1024)
	if err != nil {
		t.Fatalf("QueryRaftLog must resolve out-of-range through the RequestState, got %v", err)
	}
	defer rs.Release()

	select {
	case r := <-rs.ResultC():
		if !r.RequestOutOfRange() {
			t.Fatalf("result code = %v, want RequestOutOfRange", r.code)
		}
		entries, lr := r.RaftLogs()
		if entries != nil {
			t.Errorf("expected nil entries, got %d", len(entries))
		}
		if lr.LastIndex == 0 {
			t.Error("expected the available LogRange so a paginating caller can re-anchor")
		}
	case <-time.After(time.Second):
		t.Fatal("pre-resolved RequestState must never block")
	}
}

// ---------------------------------------------------------------------------
// L: Sync* methods reject an already-expired deadline
// ---------------------------------------------------------------------------

func TestNodeHost_SyncMethodsRejectExpiredDeadline(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	session := nh.GetNoOPSession(1)
	if _, err := nh.SyncPropose(expired, session, []byte("x")); !errors.Is(err, ErrInvalidDeadline) {
		t.Errorf("SyncPropose = %v, want ErrInvalidDeadline", err)
	}
	if _, err := nh.SyncRead(expired, 1, nil); !errors.Is(err, ErrInvalidDeadline) {
		t.Errorf("SyncRead = %v, want ErrInvalidDeadline", err)
	}
	if _, err := nh.SyncGetShardMembership(expired, 1); !errors.Is(err, ErrInvalidDeadline) {
		t.Errorf("SyncGetShardMembership = %v, want ErrInvalidDeadline", err)
	}
	if err := nh.SyncRequestDeleteReplica(expired, 1, 2, 0); !errors.Is(err, ErrInvalidDeadline) {
		t.Errorf("SyncRequestDeleteReplica = %v, want ErrInvalidDeadline", err)
	}
	if _, err := nh.SyncRequestSnapshot(expired, 1, DefaultSnapshotOption); !errors.Is(err, ErrInvalidDeadline) {
		t.Errorf("SyncRequestSnapshot = %v, want ErrInvalidDeadline", err)
	}

	noDeadline := context.Background()
	if _, err := nh.SyncPropose(noDeadline, session, []byte("x")); !errors.Is(err, ErrDeadlineNotSet) {
		t.Errorf("SyncPropose(no deadline) = %v, want ErrDeadlineNotSet", err)
	}
}

// ---------------------------------------------------------------------------
// L: a timeout shorter than one RTT is rejected with ErrTimeoutTooSmall
// ---------------------------------------------------------------------------

func TestNodeHost_AsyncMethodsRejectTinyTimeout(t *testing.T) {
	cfg := testNodeHostConfig(t.TempDir())
	cfg.RTTMillisecond = 100
	nh := newTestNodeHostWithConfig(t, cfg)
	startTestShard(t, nh, 1, 1)

	session := nh.GetNoOPSession(1)
	if _, err := nh.Propose(session, []byte("x"), time.Millisecond); !errors.Is(err, ErrTimeoutTooSmall) {
		t.Errorf("Propose = %v, want ErrTimeoutTooSmall", err)
	}
	if _, err := nh.ReadIndex(1, time.Millisecond); !errors.Is(err, ErrTimeoutTooSmall) {
		t.Errorf("ReadIndex = %v, want ErrTimeoutTooSmall", err)
	}
}

// ---------------------------------------------------------------------------
// L: Start*Replica validates initial member targets
// ---------------------------------------------------------------------------

func TestNodeHost_StartReplica_RejectsInvalidTarget(t *testing.T) {
	nh := newTestNodeHost(t)
	cfg := dbConfig.Config{
		ShardID:      5,
		ReplicaID:    1,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
	}
	members := map[uint64]Target{1: "localhost:0"}
	if err := nh.StartReplica(members, false, testSMFactory(), cfg); !errors.Is(err, ErrInvalidTarget) {
		t.Errorf("StartReplica = %v, want ErrInvalidTarget", err)
	}
}

// ---------------------------------------------------------------------------
// L: GetLogReader reports ErrLogDBNotCreatedOrClosed for an unknown shard
// ---------------------------------------------------------------------------

func TestNodeHost_GetLogReader_UnknownShard(t *testing.T) {
	nh := newTestNodeHost(t)
	if _, err := nh.GetLogReader(4242); !errors.Is(err, ErrLogDBNotCreatedOrClosed) {
		t.Errorf("GetLogReader = %v, want ErrLogDBNotCreatedOrClosed", err)
	}
}

// ---------------------------------------------------------------------------
// L: RequestLeaderTransfer to an invalid target reports ErrInvalidTarget
// ---------------------------------------------------------------------------

func TestNodeHost_RequestLeaderTransfer_InvalidTarget(t *testing.T) {
	nh := newTestNodeHost(t)
	startTestShard(t, nh, 1, 1)

	if err := nh.RequestLeaderTransfer(1, 0); !errors.Is(err, ErrInvalidTarget) {
		t.Errorf("RequestLeaderTransfer(0) = %v, want ErrInvalidTarget", err)
	}
}

// ---------------------------------------------------------------------------
// M: ShardInfo.Pending carries dragonboat's meaning, not a count
// ---------------------------------------------------------------------------

func TestShardInfo_PendingIsNothingAppliedYet(t *testing.T) {
	si := fromQuicraftShardInfo(quicraft.ShardInfo{
		ShardID:     1,
		LastApplied: 0,
		Pending:     4,
	})
	if !si.Pending {
		t.Error("Pending must be true while nothing has been applied")
	}
	if si.PendingProposals != 4 {
		t.Errorf("PendingProposals = %d, want 4", si.PendingProposals)
	}

	si = fromQuicraftShardInfo(quicraft.ShardInfo{
		ShardID:     1,
		LastApplied: 12,
		Pending:     0,
	})
	if si.Pending {
		t.Error("Pending must be false once entries have been applied")
	}
}

// ---------------------------------------------------------------------------
// H2: ResultC must aggregate committed+applied when NotifyCommit is on
// ---------------------------------------------------------------------------

func TestRequestState_ResultC_AggregatesCommittedThenApplied(t *testing.T) {
	cfg := testNodeHostConfig(t.TempDir())
	cfg.NotifyCommit = true
	nh := newTestNodeHostWithConfig(t, cfg)
	startTestShard(t, nh, 1, 1)

	session := nh.GetNoOPSession(1)
	rs, err := nh.Propose(session, []byte("cmd"), 5*time.Second)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	defer rs.Release()

	// The canonical dragonboat NotifyCommit consumer loop: the first
	// receive reports the commit, the second the apply result. Returning
	// only the applied channel would park the second receive forever.
	resultC := rs.ResultC()
	if resultC != rs.ResultC() {
		t.Fatal("ResultC must return the same aggregate channel on every call")
	}

	select {
	case first := <-resultC:
		if first.code != RequestCommitted {
			t.Fatalf("first value = %v, want RequestCommitted", first.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the commit notification")
	}

	select {
	case second := <-resultC:
		if !second.Completed() {
			t.Fatalf("second value = %v, want Completed", second.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the apply result")
	}
}

func TestRequestState_ResultC_IsAppliedChannelWhenNotifyCommitOff(t *testing.T) {
	nh := newTestNodeHost(t) // NotifyCommit defaults to false
	startTestShard(t, nh, 1, 1)

	session := nh.GetNoOPSession(1)
	rs, err := nh.Propose(session, []byte("cmd"), 5*time.Second)
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	defer rs.Release()

	if rs.ResultC() != rs.AppliedC() {
		t.Error("ResultC must be the applied channel when NotifyCommit is off")
	}

	select {
	case r := <-rs.ResultC():
		if !r.Completed() {
			t.Errorf("result = %v, want Completed", r.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the apply result")
	}
}

// TestRequestState_CommitBridgeIsLazy verifies no commit-notification
// goroutine is spawned until the caller asks for one. Dragonboat spawns
// its aggregate goroutine only when ResultC is called with NotifyCommit
// enabled; the adapter must not create a permanently parked goroutine per
// request.
func TestRequestState_CommitBridgeIsLazy(t *testing.T) {
	pool := quicraft.NewRequestStatePool()
	inner := pool.Get().(*quicraft.RequestState)
	rs := WrapRequestState(inner, true)
	defer rs.Release()

	rs.mu.Lock()
	created := rs.committedC != nil || rs.aggrC != nil
	rs.mu.Unlock()
	if created {
		t.Fatal("commit bridge must not be created before ResultC/CommittedC is called")
	}

	if rs.CommittedC() == nil {
		t.Fatal("CommittedC must be non-nil once requested")
	}
	rs.mu.Lock()
	created = rs.committedC != nil
	rs.mu.Unlock()
	if !created {
		t.Error("CommittedC must create the bridge on first use")
	}
}

// ---------------------------------------------------------------------------
// L: on-disk state machines must reject tracked sessions
// ---------------------------------------------------------------------------

func TestNodeHost_OnDiskReplicaRejectsTrackedSession(t *testing.T) {
	nh := newTestNodeHost(t)

	members := map[uint64]Target{1: nh.RaftAddress()}
	cfg := dbConfig.Config{
		ShardID:      3,
		ReplicaID:    1,
		ElectionRTT:  5,
		HeartbeatRTT: 1,
		CheckQuorum:  true,
		// Also covers the WaitReady fix: dragonboat's startShard blocks
		// until the replica is initialized, so a proposal issued right
		// after the call must not fail as not ready.
		WaitReady: true,
	}
	if err := nh.StartOnDiskReplica(members, false, testOnDiskSMFactory(), cfg); err != nil {
		t.Fatalf("StartOnDiskReplica: %v", err)
	}
	if _, _, valid, err := nh.GetLeaderID(3); err != nil || !valid {
		t.Fatalf("WaitReady returned before a leader was known: valid=%v err=%v", valid, err)
	}

	tracked := dbClient.NewSession(3, nil)
	if _, err := nh.Propose(tracked, []byte("cmd"), time.Second); !errors.Is(err, ErrRejected) {
		t.Errorf("Propose(tracked session on on-disk SM) = %v, want ErrRejected", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := nh.SyncPropose(ctx, tracked, []byte("cmd")); !errors.Is(err, ErrRejected) {
		t.Errorf("SyncPropose(tracked session on on-disk SM) = %v, want ErrRejected", err)
	}

	tracked.Inner().PrepareForRegister()
	if _, err := nh.ProposeSession(tracked, time.Second); !errors.Is(err, ErrRejected) {
		t.Errorf("ProposeSession(tracked session on on-disk SM) = %v, want ErrRejected", err)
	}
}
