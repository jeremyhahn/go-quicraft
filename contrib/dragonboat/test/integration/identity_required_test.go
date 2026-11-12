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

//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// identityProbeAddr is the address every identity-less add names. Nothing
// listens there; the refusal must come before any message is sent to it.
func identityProbeAddr(replicaID uint64) dragonboat.Target {
	return fmt.Sprintf("127.10.0.1:%d", 59900+replicaID)
}

// issueOnLeader issues one config change on the replica leading the shard at
// the moment of each issue, and re-issues it only while the answer is a
// not-leader refusal: such an answer says nothing about the identity check
// under test. It returns the first answer that is not a not-leader refusal.
func issueOnLeader(ctx context.Context, t *testing.T, cluster *dbTestCluster, desc string,
	issue func(ctx context.Context, nh *dragonboat.NodeHost) error,
) error {
	t.Helper()
	var answer error
	waitForCondition(t, failoverTimeout, desc+" reaches a leader", func() bool {
		attemptCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
		answer = issue(attemptCtx, cluster.waitForLeaderHost(1, failoverTimeout))
		return !isNotLeaderRejection(answer)
	})
	return answer
}

// requireIdentityRefusal fails the test unless err is the refusal of an add
// that carries no node identity.
func requireIdentityRefusal(ctx context.Context, t *testing.T, desc string, err error) {
	t.Helper()
	if ctx.Err() != nil {
		t.Fatalf("%s: context ended before the answer was checked: %v", desc, ctx.Err())
	}
	if err == nil {
		t.Fatalf("%s was accepted; a NodeHost with a transport must refuse an add "+
			"that carries no node identity", desc)
	}
	if !errors.Is(err, quicraft.ErrMemberIdentityRequired) {
		t.Fatalf("%s was refused with %v, want ErrMemberIdentityRequired", desc, err)
	}
	t.Logf("%s refused: %v", desc, err)
}

// requireAsyncIdentityRefusal checks the answer of an async identity-less
// add: either the call returns ErrMemberIdentityRequired at once with a nil
// RequestState, or the RequestState it returns resolves to a result that is
// not Completed.
func requireAsyncIdentityRefusal(ctx context.Context, t *testing.T, desc string, rs *dragonboat.RequestState, err error) {
	t.Helper()
	if err != nil {
		if rs != nil {
			rs.Release()
			t.Fatalf("%s returned both an error (%v) and a RequestState", desc, err)
		}
		requireIdentityRefusal(ctx, t, desc, err)
		return
	}
	if rs == nil {
		t.Fatalf("%s returned neither an error nor a RequestState", desc)
	}
	defer rs.Release()
	select {
	case result := <-rs.ResultC():
		if result.Completed() {
			t.Fatalf("%s completed; a NodeHost with a transport must refuse an add "+
				"that carries no node identity", desc)
		}
		t.Logf("%s resolved without completing: %v", desc, result)
	case <-time.After(defaultTimeout):
		t.Fatalf("%s: RequestState did not resolve within %v", desc, defaultTimeout)
	case <-ctx.Done():
		t.Fatalf("%s: context ended before the RequestState resolved: %v", desc, ctx.Err())
	}
}

// TestIntegration_DB_IdentityRequired verifies that a dragonboat NodeHost
// with a transport refuses every add call that commits no node identity
// (SyncRequestAddReplica, SyncRequestAddNonVoting, SyncRequestAddWitness
// and the async RequestAdd* forms) with quicraft.ErrMemberIdentityRequired,
// that the refusals leave the membership and its ConfigChangeID unchanged,
// and that they leave no pending config change behind: the WithIdentity add
// of the same non-voting ID lands afterwards.
func TestIntegration_DB_IdentityRequired(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*membershipTimeout)
	defer cancel()

	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	trackers := cluster.startShard(1)
	leaderIdx := cluster.waitForClusterLeader(1, leaderElectionTimeout)
	t.Logf("leader elected at node[%d]", leaderIdx)

	// Warmup entry required after election (PhD 6.4).
	proposeAndWait(t, cluster, trackers, 1, "warmup", "ok")
	waitForReplication(t, cluster, trackers, 1, "warmup", "ok", defaultTimeout)

	before := cluster.membershipFromLeader(t, 1, membershipTimeout)
	ccID := before.ConfigChangeID
	t.Logf("membership before the probes: ConfigChangeID=%d", ccID)

	const (
		syncReplicaID   uint64 = 21
		syncNonVotingID uint64 = 22
		syncWitnessID   uint64 = 23
		asyncReplicaID  uint64 = 24
		asyncNonVoting  uint64 = 25
		asyncWitnessID  uint64 = 26
	)
	probeIDs := []uint64{
		syncReplicaID, syncNonVotingID, syncWitnessID,
		asyncReplicaID, asyncNonVoting, asyncWitnessID,
	}

	// (a) The three Sync add calls.
	syncProbes := []struct {
		desc  string
		issue func(ctx context.Context, nh *dragonboat.NodeHost) error
	}{
		{"SyncRequestAddReplica", func(ctx context.Context, nh *dragonboat.NodeHost) error {
			//quicraft:direct-config-change the refusal of an add with no node identity is the assertion; an idempotent helper would retry it
			return nh.SyncRequestAddReplica(ctx, 1, syncReplicaID, identityProbeAddr(syncReplicaID), ccID)
		}},
		{"SyncRequestAddNonVoting", func(ctx context.Context, nh *dragonboat.NodeHost) error {
			//quicraft:direct-config-change the refusal of an add with no node identity is the assertion; an idempotent helper would retry it
			return nh.SyncRequestAddNonVoting(ctx, 1, syncNonVotingID, identityProbeAddr(syncNonVotingID), ccID)
		}},
		{"SyncRequestAddWitness", func(ctx context.Context, nh *dragonboat.NodeHost) error {
			//quicraft:direct-config-change the refusal of an add with no node identity is the assertion; an idempotent helper would retry it
			return nh.SyncRequestAddWitness(ctx, 1, syncWitnessID, identityProbeAddr(syncWitnessID), ccID)
		}},
	}
	for _, p := range syncProbes {
		err := issueOnLeader(ctx, t, cluster, p.desc, p.issue)
		requireIdentityRefusal(ctx, t, p.desc, err)
	}

	// (b) The async add calls. The RequestState, if any, is carried out of
	// the issue closure so that a not-leader answer can be re-issued.
	asyncProbes := []struct {
		desc  string
		issue func(nh *dragonboat.NodeHost) (*dragonboat.RequestState, error)
	}{
		{"RequestAddReplica", func(nh *dragonboat.NodeHost) (*dragonboat.RequestState, error) {
			//quicraft:direct-config-change the refusal of an add with no node identity is the assertion; an idempotent helper would retry it
			return nh.RequestAddReplica(1, asyncReplicaID, identityProbeAddr(asyncReplicaID), ccID, defaultTimeout)
		}},
		{"RequestAddNonVoting", func(nh *dragonboat.NodeHost) (*dragonboat.RequestState, error) {
			//quicraft:direct-config-change the refusal of an add with no node identity is the assertion; an idempotent helper would retry it
			return nh.RequestAddNonVoting(1, asyncNonVoting, identityProbeAddr(asyncNonVoting), ccID, defaultTimeout)
		}},
		{"RequestAddWitness", func(nh *dragonboat.NodeHost) (*dragonboat.RequestState, error) {
			//quicraft:direct-config-change the refusal of an add with no node identity is the assertion; an idempotent helper would retry it
			return nh.RequestAddWitness(1, asyncWitnessID, identityProbeAddr(asyncWitnessID), ccID, defaultTimeout)
		}},
	}
	for _, p := range asyncProbes {
		var rs *dragonboat.RequestState
		err := issueOnLeader(ctx, t, cluster, p.desc, func(_ context.Context, nh *dragonboat.NodeHost) error {
			var issueErr error
			rs, issueErr = p.issue(nh)
			if isNotLeaderRejection(issueErr) && rs != nil {
				rs.Release()
				rs = nil
			}
			return issueErr
		})
		requireAsyncIdentityRefusal(ctx, t, p.desc, rs, err)
	}

	// (c) Nothing the probes named entered the membership, and no config
	// change committed.
	after := cluster.membershipFromLeader(t, 1, membershipTimeout)
	if after.ConfigChangeID != ccID {
		t.Fatalf("ConfigChangeID moved from %d to %d across refused adds; "+
			"a refused add must commit nothing", ccID, after.ConfigChangeID)
	}
	for _, id := range probeIDs {
		if _, ok := after.Nodes[id]; ok {
			t.Fatalf("replica %d is in Nodes after a refused add: %v", id, after.Nodes)
		}
		if _, ok := after.NonVotings[id]; ok {
			t.Fatalf("replica %d is in NonVotings after a refused add: %v", id, after.NonVotings)
		}
		if _, ok := after.Witnesses[id]; ok {
			t.Fatalf("replica %d is in Witnesses after a refused add: %v", id, after.Witnesses)
		}
	}
	t.Logf("membership unchanged after the refused adds: ConfigChangeID=%d", after.ConfigChangeID)

	// (d) The refusals left no pending config change: the WithIdentity add
	// of the same non-voting ID lands against the same ConfigChangeID.
	cluster.addNonVotingToLeader(t, 1, syncNonVotingID, identityProbeAddr(syncNonVotingID),
		dbIdentityFor(syncNonVotingID), ccID, membershipTimeout)
	final := cluster.membershipFromLeader(t, 1, membershipTimeout)
	if _, ok := final.NonVotings[syncNonVotingID]; !ok {
		t.Fatalf("replica %d is not in NonVotings after SyncRequestAddNonVotingWithIdentity: %v",
			syncNonVotingID, final.NonVotings)
	}
	if final.ConfigChangeID == ccID {
		t.Fatalf("ConfigChangeID is still %d after the WithIdentity add committed", ccID)
	}
	t.Logf("SyncRequestAddNonVotingWithIdentity for replica %d landed: ConfigChangeID=%d",
		syncNonVotingID, final.ConfigChangeID)
}
