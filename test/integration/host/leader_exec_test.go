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

package host_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// execShard is the one shard every test in this file starts.
const execShard uint64 = 1

// execRecord is a 3-node cluster's leader-executed request handlers and the
// record of every execution: which replica ran the request, under which term.
type execRecord struct {
	mu   sync.Mutex
	runs map[uint64][]uint64 // term -> replicas that executed under it

	started chan uint64 // replica ID, per execution
}

// serveLeaderExec registers a handler on every host BEFORE the shard starts,
// as an embedder does. A replica with a gate holds its handler until the gate
// closes or the request's context ends.
func serveLeaderExec(t *testing.T, cluster *testCluster, gates map[uint64]chan struct{}) *execRecord {
	t.Helper()
	rec := &execRecord{runs: make(map[uint64][]uint64), started: make(chan uint64, 16)}
	for i, h := range cluster.hosts {
		replicaID := uint64(i + 1)
		gate := gates[replicaID]
		err := h.RegisterLeaderHandler(execShard, func(ctx context.Context, term uint64, req []byte) ([]byte, error) {
			rec.mu.Lock()
			rec.runs[term] = append(rec.runs[term], replicaID)
			rec.mu.Unlock()
			rec.started <- replicaID
			if gate != nil {
				select {
				case <-gate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return fmt.Appendf(nil, "%d@%d:%s", replicaID, term, req), nil
		})
		if err != nil {
			t.Fatalf("RegisterLeaderHandler on node[%d]: %v", i, err)
		}
	}
	return rec
}

// assertOncePerTerm fails if any term executed the request more than once.
func (r *execRecord) assertOncePerTerm(t *testing.T) map[uint64][]uint64 {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for term, replicas := range r.runs {
		if len(replicas) > 1 {
			t.Fatalf("the request executed %d times under term %d (replicas %v)", len(replicas), term, replicas)
		}
	}
	runs := make(map[uint64][]uint64, len(r.runs))
	for term, replicas := range r.runs {
		runs[term] = append([]uint64(nil), replicas...)
	}
	return runs
}

// TestIntegration_LeaderExec_FollowerRequestIsExecutedByTheLeader: a request
// made on a follower crosses the QUIC transport, is executed by the leader
// under its term, and the leader's reply comes back.
func TestIntegration_LeaderExec_FollowerRequestIsExecutedByTheLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()
	rec := serveLeaderExec(t, cluster, nil)
	cluster.startShard(execShard)

	leaderIdx := cluster.waitForClusterLeader(execShard, failoverTimeout)
	followerIdx := cluster.waitForFollower(execShard, failoverTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	reply, err := cluster.hosts[followerIdx].ExecuteOnLeader(ctx, execShard, []byte("renew"))
	if err != nil {
		t.Fatalf("ExecuteOnLeader on follower node[%d]: %v", followerIdx, err)
	}

	runs := rec.assertOncePerTerm(t)
	if len(runs) != 1 {
		t.Fatalf("executions by term = %v, want exactly one", runs)
	}
	for term, replicas := range runs {
		want := fmt.Sprintf("%d@%d:renew", leaderIdx+1, term)
		if replicas[0] != uint64(leaderIdx+1) || string(reply) != want {
			t.Fatalf("executed by replica %d, reply %q; want the leader (replica %d) and %q",
				replicas[0], reply, leaderIdx+1, want)
		}
	}
}

// TestIntegration_LeaderExec_LeaderChangeMidRequestIsRetried: the leader is
// executing a follower's request when leadership moves away from it. The
// follower takes the request to the new leader, which executes it under the
// new term; the deposed leader's late answer is never returned. No term
// executes the request twice.
func TestIntegration_LeaderExec_LeaderChangeMidRequestIsRetried(t *testing.T) {
	cluster := newTestCluster(t, 3)
	defer cluster.close()
	cluster.startShard(execShard)
	oldIdx := cluster.waitForClusterLeader(execShard, failoverTimeout)
	oldID := uint64(oldIdx + 1)

	// Gate only the first leader, so the request is in its hands when
	// leadership moves.
	gate := make(chan struct{})
	gateOnce := sync.OnceFunc(func() { close(gate) })
	defer gateOnce()
	rec := serveLeaderExec(t, cluster, map[uint64]chan struct{}{oldID: gate})

	callerIdx := cluster.waitForFollower(execShard, failoverTimeout)
	targetIdx := 3 - oldIdx - callerIdx // the replica that is neither

	type result struct {
		reply []byte
		err   error
	}
	done := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*failoverTimeout)
	defer cancel()
	go func() {
		reply, err := cluster.hosts[callerIdx].ExecuteOnLeader(ctx, execShard, []byte("renew"))
		done <- result{reply, err}
	}()

	select {
	case id := <-rec.started:
		if id != oldID {
			t.Fatalf("first execution on replica %d, want the leader %d", id, oldID)
		}
	case <-ctx.Done():
		t.Fatal("the leader never started executing the request")
	}

	transferLeadershipTo(t, cluster, execShard, targetIdx, failoverTimeout)

	var res result
	select {
	case res = <-done:
	case <-ctx.Done():
		t.Fatal("ExecuteOnLeader did not return after the leader change")
	}
	if res.err != nil {
		t.Fatalf("ExecuteOnLeader after the leader change: %v", res.err)
	}

	// Release the deposed leader: its answer, whatever it is, must not change
	// the result already returned, and it must not execute again.
	gateOnce()

	runs := rec.assertOncePerTerm(t)
	var oldTerm, newTerm uint64
	for term, replicas := range runs {
		switch replicas[0] {
		case oldID:
			oldTerm = term
		default:
			newTerm = term
			want := fmt.Sprintf("%d@%d:renew", replicas[0], term)
			if string(res.reply) != want {
				t.Fatalf("reply %q, want the new leader's %q", res.reply, want)
			}
		}
	}
	if len(runs) != 2 || oldTerm == 0 || newTerm <= oldTerm {
		t.Fatalf("executions by term = %v: want the old leader once and a new leader once, "+
			"under a later term", runs)
	}
}
