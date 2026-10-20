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
	"testing"
	"time"
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

// TestStartShard_ReplicatedSessionPolicyOverridesLocalConfig is the end-to-end
// half, and the one that pins the actual fix.
//
// A shard is bootstrapped with a session policy, stopped, and restarted with a
// DIFFERENT local configuration — the disagreement an edited config file or a
// binary with new defaults produces. The restarted replica must apply the
// COMMITTED limits, because its peers are applying those: a replica that
// admitted registrations its peers refuse would answer subsequent entries of
// that session normally while they answer ResultSessionExpired, and the two
// state machines would part company with every Raft invariant intact.
//
// The observable is deliberately end-to-end rather than a field read: the
// fourth NewSession is refused because the SHARD committed a cap of 3, while
// this node is configured with 50.
func TestStartShard_ReplicatedSessionPolicyOverridesLocalConfig(t *testing.T) {
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

	// Restart with limits that disagree with what the shard committed, in the
	// direction that matters: every local limit is LOOSER, so a replica reading
	// its own configuration would admit what its peers refuse.
	restarted := testShardConfig(1, 1)
	restarted.SessionExpiryEntries = 100000
	restarted.MaxTotalSessions = 50
	restarted.MaxResponsesPerSession = 40
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false,
		testCreateFunc, restarted); err != nil {
		t.Fatalf("StartShard after restart failed: %v", err)
	}
	waitForLeader(t, h, 1)

	// The committed policy is unchanged by the restart: a rejoining replica
	// adopts the shard's configuration, it does not re-seed it.
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
		t.Fatalf("NewSession #4 error = %v, want a *SessionError from register. "+
			"The shard committed MaxTotalSessions=3; this node is configured with "+
			"50, and a replica that applies its own limit admits sessions its "+
			"peers refuse", err)
	}
}
