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

package quicraft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
)

// TestStartShard_SeedsSessionPolicyIntoMembership pins that the shard's session
// policy — the three limits the deterministic apply path reads — reaches
// REPLICATED state at bootstrap, from the founder's Config and from nowhere
// else.
//
// The seed is the whole mechanism: the apply path reads the committed value, so
// a bootstrap that failed to write it would leave every replica applying on its
// own configuration, which is the divergence being closed. The assertion is
// therefore on the committed membership, not on any node-local field.
func TestStartShard_SeedsSessionPolicyIntoMembership(t *testing.T) {
	h := newTestHost(t)

	scfg := testShardConfig(1, 1)
	scfg.SessionExpiryEntries = 5000
	scfg.MaxTotalSessions = 3
	scfg.MaxResponsesPerSession = 2
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	m, err := h.ShardMembership(1)
	if err != nil {
		t.Fatalf("ShardMembership: %v", err)
	}
	if m.SessionExpiryEntries != 5000 || m.MaxTotalSessions != 3 ||
		m.MaxResponsesPerSession != 2 {
		t.Fatalf("committed session policy = {expiry:%d total:%d perClient:%d}, "+
			"want {5000 3 2}. StartShard must seed the shard's replicated "+
			"membership from this founder's Config; a shard that commits no "+
			"policy leaves every replica expiring sessions on its own "+
			"configuration",
			m.SessionExpiryEntries, m.MaxTotalSessions, m.MaxResponsesPerSession)
	}

	// The apply path must be evaluating the same limits: the response cache
	// bound lives inside the session manager the state machine applies through.
	if got := testLoadNode(h, 1).rsm.Sessions().MaxResponses(); got != 2 {
		t.Errorf("session manager MaxResponses = %d, want 2", got)
	}
}

// TestStartShard_RefusesConfigThatDisagreesWithSessionPolicy is the end-to-end
// half, and the one that pins the check.
//
// A shard is bootstrapped with a session policy, stopped, and restarted with a
// DIFFERENT local configuration — the disagreement an edited config file or a
// binary with new defaults produces. A replica applying its own limits would
// admit registrations its peers refuse and the two state machines would part
// company with every Raft invariant intact; one silently adopting the
// committed limits would run on values it was not configured with. StartShard
// refuses it, naming the setting and both values, and a restart with the
// shard's values then starts and enforces them.
func TestStartShard_RefusesConfigThatDisagreesWithSessionPolicy(t *testing.T) {
	h := newTestHost(t)

	scfg := testShardConfig(1, 1)
	scfg.SessionExpiryEntries = 5000
	scfg.MaxTotalSessions = 3
	scfg.MaxResponsesPerSession = 2
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard failed: %v", err)
	}
	waitForLeader(t, h, 1)

	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}

	// Each setting on its own is enough to refuse the restart.
	for _, tc := range []struct {
		setting string
		mutate  func(*config.Config)
		want    string
	}{
		{"SessionExpiryEntries", func(c *config.Config) { c.SessionExpiryEntries = 100000 },
			"SessionExpiryEntries is 100000 in this node's Config, 5000"},
		{"MaxTotalSessions", func(c *config.Config) { c.MaxTotalSessions = 50 },
			"MaxTotalSessions is 50 in this node's Config, 3"},
		{"MaxResponsesPerSession", func(c *config.Config) { c.MaxResponsesPerSession = 40 },
			"MaxResponsesPerSession is 40 in this node's Config, 2"},
		{"MaxSessionsPerOwner", func(c *config.Config) { c.MaxSessionsPerOwner = scfg.MaxSessionsPerOwner + 1 },
			"MaxSessionsPerOwner is"},
	} {
		restarted := scfg
		tc.mutate(&restarted)
		err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
			testCreateFunc, restarted)
		if !errors.Is(err, ErrSessionPolicyMismatch) {
			t.Fatalf("StartShard with a disagreeing %s = %v, want ErrSessionPolicyMismatch",
				tc.setting, err)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("StartShard error %q does not contain %q", err, tc.want)
		}
	}

	// The shard's own values start it.
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard after restart with the committed values failed: %v", err)
	}
	waitForLeader(t, h, 1)

	// The committed policy is unchanged by the restart: a rejoining replica
	// is held to the shard's configuration, it does not re-seed it.
	m, err := h.ShardMembership(1)
	if err != nil {
		t.Fatalf("ShardMembership after restart: %v", err)
	}
	if m.SessionExpiryEntries != 5000 || m.MaxTotalSessions != 3 ||
		m.MaxResponsesPerSession != 2 {
		t.Fatalf("committed session policy after restart = {expiry:%d total:%d "+
			"perClient:%d}, want the bootstrap {5000 3 2}",
			m.SessionExpiryEntries, m.MaxTotalSessions, m.MaxResponsesPerSession)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 1; i <= 3; i++ {
		if _, err := h.NewSession(ctx, 1); err != nil {
			t.Fatalf("NewSession #%d failed: %v (the committed MaxTotalSessions "+
				"of 3 must admit it)", i, err)
		}
	}
	_, err = h.NewSession(ctx, 1)
	var sessErr *SessionError
	if !errors.As(err, &sessErr) || sessErr.Op != "register" {
		t.Fatalf("NewSession #4 error = %v, want a *SessionError from register; "+
			"the shard committed MaxTotalSessions=3", err)
	}
}

// recordSnapshotWithSessionPolicy records a snapshot at index whose
// membership carries the given session policy, the state a replica whose log
// was compacted past the index-1 EntryInitialMembership restarts from. A zero
// policy is a shard bootstrapped before the policy was recorded.
func recordSnapshotWithSessionPolicy(t *testing.T, h *Host, index uint64, p logdb.Membership) {
	t.Helper()
	snapDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(snapDir, "snapshot.dat"),
		[]byte{0, 0, 0, 0}, 0o600); err != nil {
		t.Fatalf("writing the snapshot artifact: %v", err)
	}
	stampTestSnapshotMeta(t, snapDir, index, 1)
	p.ConfigChangeID = 1
	p.Addresses = map[uint64]string{1: "127.0.0.1:5000"}
	if err := h.logdb.SaveSnapshot(1, 1, logdb.Snapshot{
		Index:      index,
		Term:       1,
		Membership: p,
		Filepath:   snapDir,
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := h.logdb.SaveState([]logdb.Update{{
		ShardID:   1,
		ReplicaID: 1,
		State:     logdb.State{Term: 1, Vote: 1, Commit: index},
	}}); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
}

// TestStartShard_SessionPolicySurvivesSnapshotRecovery pins that the check
// holds for a replica that relearns the policy from the snapshot-metadata
// record rather than from the log: the disagreeing Config is refused and the
// shard's values start the shard on the committed limits.
func TestStartShard_SessionPolicySurvivesSnapshotRecovery(t *testing.T) {
	h := newTestHost(t)
	scfg := testShardConfig(1, 1)
	scfg.SessionExpiryEntries = 6000
	scfg.MaxTotalSessions = 7
	scfg.MaxResponsesPerSession = 5
	recordSnapshotWithSessionPolicy(t, h, 40, logdb.Membership{
		SessionExpiryEntries:   6000,
		MaxTotalSessions:       7,
		MaxResponsesPerSession: 5,
		MaxSessionsPerOwner:    scfg.MaxSessionsPerOwner,
	})

	disagreeing := scfg
	disagreeing.MaxTotalSessions = 8
	err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, disagreeing)
	if !errors.Is(err, ErrSessionPolicyMismatch) {
		t.Fatalf("StartShard with MaxTotalSessions=8 on a snapshot that recorded 7 = %v, "+
			"want ErrSessionPolicyMismatch", err)
	}

	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard with the recorded values: %v", err)
	}
	defer func() { _ = h.StopShard(1) }()
	waitForLeader(t, h, 1)
	if got := testLoadNode(h, 1).rsm.Sessions().MaxResponses(); got != 5 {
		t.Fatalf("session manager MaxResponses = %d, want the recorded 5", got)
	}
}

// TestStartShard_NoRecordedSessionPolicyKeepsLocalConfig pins the documented
// behaviour for a shard bootstrapped before the session policy was recorded:
// there is nothing to compare against, so any Config starts and the replica
// applies its own limits.
func TestStartShard_NoRecordedSessionPolicyKeepsLocalConfig(t *testing.T) {
	h := newTestHost(t)
	recordSnapshotWithSessionPolicy(t, h, 40, logdb.Membership{})

	scfg := testShardConfig(1, 1)
	scfg.MaxTotalSessions = 9
	scfg.MaxResponsesPerSession = 6
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, scfg); err != nil {
		t.Fatalf("StartShard on a shard with no recorded session policy: %v", err)
	}
	defer func() { _ = h.StopShard(1) }()
	waitForLeader(t, h, 1)
	if got := testLoadNode(h, 1).rsm.Sessions().MaxResponses(); got != 6 {
		t.Fatalf("session manager MaxResponses = %d, want the local 6", got)
	}
}
