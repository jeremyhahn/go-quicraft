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

package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ---------------------------------------------------------------------------
// A restarted replica must be able to resume replication
// ---------------------------------------------------------------------------

// wedgeRemoteBehindFullWindow reproduces the leader-side state observed when a
// replica restarts underneath a saturated replication pipeline:
//
//	r5[match=8752 next=9083 state=replicate paused=true active=true inflights=256]
//
// The peer is in replicate state, its in-flight window is FULL, and every
// record in that window sits strictly above the peer's match — those messages
// were delivered to the incarnation of the peer that died, so no ack for them
// can ever arrive. Returns the peer's match index.
func wedgeRemoteBehindFullWindow(t *testing.T, r *raft, peer uint64) uint64 {
	t.Helper()

	// One entry per Replicate message, which is what a leader pipelining a
	// steady stream of small proposals actually sends -- and what produced the
	// 256 single-index window records in the failure this reproduces.
	r.maxReplicationPayload = 1

	base := r.log.lastIndex()
	entries := make([]proto.Entry, 0, 2*maxInflightMsgs)
	for i := uint64(1); i <= 2*maxInflightMsgs; i++ {
		entries = append(entries, proto.Entry{Index: base + i, Term: r.term})
	}
	r.log.append(entries)
	persistLog(r)
	r.drainMessages()

	rm := r.getRemote(peer)
	if rm == nil {
		t.Fatalf("fixture: no remote for peer %d", peer)
	}
	rm.state = remoteReplicate
	rm.match = base
	rm.inflights.reset()
	// One batch per index, exactly as a leader that is pipelining a steady
	// proposal stream records them: the window covers base+1 .. base+256 and
	// next sits just past it.
	for i := uint64(1); i <= maxInflightMsgs; i++ {
		if err := rm.inflights.add(base + i); err != nil {
			t.Fatalf("fixture: filling in-flight window: %v", err)
		}
	}
	rm.next = base + maxInflightMsgs + 1
	rm.setActive()

	if !rm.isPaused() {
		t.Fatal("fixture: peer is not paused, the window is not full")
	}
	if rm.next > r.log.lastIndex() {
		t.Fatalf("fixture: next %d is past lastIndex %d, which the pre-existing "+
			"heartbeat recovery already covers -- this test would not exercise "+
			"the stall it is written for", rm.next, r.log.lastIndex())
	}
	return base
}

// replicateTo returns the first Replicate message addressed to the peer.
func replicateTo(msgs []proto.Message, peer uint64) *proto.Message {
	for i := range msgs {
		if msgs[i].Type == proto.Replicate && msgs[i].To == peer {
			return &msgs[i]
		}
	}
	return nil
}

// TestRestartedReplicaResumesReplicationAfterFullWindow is the unit-level
// reproduction of a replication deadlock: a replica that restarts underneath a
// saturated pipeline was never sent another entry, with the leader alive and
// committing.
//
// The in-flight window only drains from ABOVE — freeTo frees records at or
// below an acknowledged index — while every ack a restarted replica can still
// send reports a position BELOW the window. So the window stayed full, a full
// window means isPaused, and sendAppend skipped the peer on every path.
// Nothing in the steady state could break the cycle: the leader never sent
// match+1, so the replica never advanced, so it never acked anything that
// would free a slot.
//
// The escape is the heartbeat response: it proves the peer is alive, so one
// slot is released and one message gets through. The peer rejects it, and the
// rejection drives the normal backtracking that repairs its position.
func TestRestartedReplicaResumesReplicationAfterFullWindow(t *testing.T) {
	const peer = uint64(2)
	r := newTestRaft(1)
	r.becomeCandidate()
	r.becomeLeader()
	r.drainMessages()

	match := wedgeRemoteBehindFullWindow(t, r, peer)
	rm := r.getRemote(peer)

	// The restarted replica reports the position it came back at. This frees
	// nothing (every record in the window is above it) and is NOT itself the
	// recovery -- it is the shape of the deadlock, and it stays true.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     peer,
		Term:     r.term,
		LogIndex: match,
	}); err != nil {
		t.Fatalf("step ReplicateResp: %v", err)
	}
	if got := rm.inflights.Count(); got != maxInflightMsgs {
		t.Fatalf("an ack below the window freed %d slots, want 0: the fixture "+
			"no longer reproduces the stall", maxInflightMsgs-got)
	}
	r.drainMessages()

	// The heartbeat response is the liveness signal that must break the cycle.
	if err := r.step(proto.Message{
		Type: proto.HeartbeatResp,
		From: peer,
		Term: r.term,
	}); err != nil {
		t.Fatalf("step HeartbeatResp: %v", err)
	}
	msgs := r.drainMessages()
	probe := replicateTo(msgs, peer)
	if probe == nil {
		t.Fatalf("a restarted replica was sent nothing after a heartbeat "+
			"response: remote is %s with a full in-flight window (%d), so it "+
			"is paused forever -- the leader can never send it index %d, so it "+
			"can never ack anything that frees a slot. It is permanently "+
			"wedged %d entries behind with the leader alive and committing.",
			rm, rm.inflights.Count(), rm.match+1, r.log.lastIndex()-rm.match)
	}

	// The replica is at match, so it rejects the message and hints its real
	// last index -- exactly what the integration failure's replica does.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     peer,
		Term:     r.term,
		Reject:   true,
		LogIndex: probe.LogIndex,
		Hint:     match,
	}); err != nil {
		t.Fatalf("step rejected ReplicateResp: %v", err)
	}
	if rm.state != remoteProbe {
		t.Fatalf("after the rejection the remote is in %s state, want Probe: "+
			"the in-flight window is not cleared in any other state, so the "+
			"peer would re-wedge immediately", rm.state)
	}
	if rm.next != match+1 {
		t.Fatalf("after the rejection next is %d, want %d (match+1): "+
			"replication does not resume from the replica's real position",
			rm.next, match+1)
	}
	if rm.inflights.Count() != 0 {
		t.Fatalf("the in-flight window still holds %d records after falling "+
			"back to probe", rm.inflights.Count())
	}

	resumed := replicateTo(r.drainMessages(), peer)
	if resumed == nil {
		t.Fatal("no Replicate followed the rejection: replication did not resume")
	}
	if len(resumed.Entries) == 0 || resumed.Entries[0].Index != match+1 {
		t.Fatalf("replication resumed at the wrong index: %v (want first entry "+
			"%d)", resumed.Entries, match+1)
	}
}

// TestHealthyPipelineSurvivesFullInflightWindow is the guard on the recovery
// above. The stall is not distinguishable from healthy pipelining by state
// alone: a peer that is merely BUSY also sits at a full window with every
// record above its match, and its next legitimately runs hundreds of entries
// ahead of match. Any condition that reads "behind" as "broken" — the naive
// match < next-1, for instance — fires on every healthy peer at saturation and
// resets the pipeline continuously, which costs far more throughput than the
// bug it is trying to fix.
//
// So this asserts the recovery costs a healthy peer nothing: in-order acks
// keep it in replicate state, keep the window full and rotating, and never
// rewind next to match+1. Heartbeat responses in between must not change that.
func TestHealthyPipelineSurvivesFullInflightWindow(t *testing.T) {
	const peer = uint64(2)
	r := newTestRaft(1)
	r.becomeCandidate()
	r.becomeLeader()
	r.drainMessages()

	base := wedgeRemoteBehindFullWindow(t, r, peer)
	rm := r.getRemote(peer)
	startNext := rm.next

	// The peer is healthy: it acknowledges the window in order, one batch at a
	// time, with a heartbeat response interleaved to cover the recovery path.
	for i := uint64(1); i <= 32; i++ {
		if err := r.step(proto.Message{
			Type: proto.HeartbeatResp,
			From: peer,
			Term: r.term,
		}); err != nil {
			t.Fatalf("step HeartbeatResp: %v", err)
		}
		if rm.state != remoteReplicate {
			t.Fatalf("a heartbeat response knocked a healthy peer out of "+
				"replicate into %s after %d acks: the pipeline is reset on "+
				"every heartbeat and throughput collapses to one batch per "+
				"round trip", rm.state, i-1)
		}
		if err := r.step(proto.Message{
			Type:     proto.ReplicateResp,
			From:     peer,
			Term:     r.term,
			LogIndex: base + i,
		}); err != nil {
			t.Fatalf("step ReplicateResp: %v", err)
		}
		if rm.state != remoteReplicate {
			t.Fatalf("an in-order ack knocked a healthy peer out of replicate "+
				"into %s after %d acks (match=%d next=%d): a peer that is "+
				"merely pipelining is being treated as stalled",
				rm.state, i, rm.match, rm.next)
		}
		if rm.match != base+i {
			t.Fatalf("match is %d after acking %d, want %d", rm.match, base+i, base+i)
		}
		if rm.next <= rm.match+1 {
			t.Fatalf("next collapsed to %d (match=%d): the leader is no longer "+
				"pipelining, it is waiting for each batch to be acked",
				rm.next, rm.match)
		}
		r.drainMessages()
	}

	if rm.next <= startNext {
		t.Fatalf("next did not advance across 32 acked batches (%d -> %d): "+
			"replication is not making progress", startNext, rm.next)
	}
	if !rm.inflights.full() {
		t.Fatalf("the in-flight window drained to %d of %d while the leader "+
			"still had %d entries to send: the pipeline is running below "+
			"capacity", rm.inflights.Count(), maxInflightMsgs,
			r.log.lastIndex()-rm.match)
	}
}
