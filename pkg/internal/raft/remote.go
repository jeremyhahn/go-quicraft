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
	"fmt"
	"log/slog"
)

// remoteState represents the replication state for a remote peer.
// Each follower tracked by the leader is in exactly one of these states.
type remoteState uint8

const (
	// remoteReplicate indicates the peer is actively receiving appends.
	// The leader sends entries optimistically using a sliding window
	// controlled by the inflights tracker.
	remoteReplicate remoteState = iota

	// remoteProbe indicates the leader is probing to find the peer's
	// correct next index. Only one message is in-flight at a time;
	// the leader waits for a response before sending the next probe.
	remoteProbe

	// remoteSnapshot indicates the leader is sending a snapshot to
	// the peer because it has fallen too far behind (its next index
	// is below the compacted log boundary).
	remoteSnapshot
)

// remoteStateNames provides human-readable names for remote states.
var remoteStateNames = [...]string{
	remoteReplicate: "Replicate",
	remoteProbe:     "Probe",
	remoteSnapshot:  "Snapshot",
}

// String returns the human-readable name of the remote state.
func (s remoteState) String() string {
	if int(s) < len(remoteStateNames) {
		return remoteStateNames[s]
	}
	return fmt.Sprintf("Unknown(%d)", s)
}

// remote tracks the replication progress of a single follower as seen
// by the leader. It implements a state machine with three states that
// govern how the leader sends entries to this peer.
type remote struct {
	// match is the highest log index known to be replicated on this peer.
	match uint64
	// next is the next log index to send to this peer.
	next uint64
	// state is the current replication state for this peer.
	state remoteState
	// active indicates whether this peer has responded recently.
	// Used by the leader's checkQuorum mechanism.
	active bool
	// inflights tracks in-flight append messages for flow control.
	inflights *inflights

	// snapshotIndex is the log index of the snapshot being sent to
	// this peer. Only meaningful when state is remoteSnapshot.
	snapshotIndex uint64

	// snapshotSeq identifies the snapshot transfer in flight; a SnapshotStatus is honored only if it matches.
	snapshotSeq uint64

	// snapshotWaitTicks is the post-transfer ack/retry countdown; zero means the transfer is in flight.
	snapshotWaitTicks int

	// probeSent indicates whether a probe message has been sent and
	// the leader is waiting for a response. Only meaningful when
	// state is remoteProbe.
	probeSent bool
}

// newRemote creates a new remote tracker for a follower. The peer starts
// in the probe state with the given next index and an inflights tracker
// of the given capacity.
func newRemote(next uint64, maxInflight int) *remote {
	return &remote{
		next:      next,
		state:     remoteProbe,
		inflights: newInflights(maxInflight),
	}
}

// String returns a readable form of the remote state, including the snapshot wait phase.
func (r *remote) String() string {
	return fmt.Sprintf("match:%d,next:%d,state:%s,snapshot:%d,snapshotwait:%d",
		r.match, r.next, r.state, r.snapshotIndex, r.snapshotWaitTicks)
}

// becomeReplicate enters the replicate state with next at match+1 and the inflights reset.
func (r *remote) becomeReplicate() {
	r.state = remoteReplicate
	r.next = r.match + 1
	r.snapshotIndex = 0
	r.snapshotSeq = 0
	r.snapshotWaitTicks = 0
	r.probeSent = false
	r.inflights.reset()
}

// becomeProbe enters the probe state with next at match+1, or past the snapshot index from snapshot state.
func (r *remote) becomeProbe() {
	if r.state == remoteSnapshot {
		// When transitioning from snapshot, advance next past the
		// snapshot index if it exceeds match+1, since the peer will
		// have the snapshot data up to snapshotIndex.
		pendingSnapshot := r.snapshotIndex
		r.state = remoteProbe
		r.next = max(r.match+1, pendingSnapshot+1)
	} else {
		r.state = remoteProbe
		r.next = r.match + 1
	}
	r.snapshotIndex = 0
	r.snapshotSeq = 0
	r.snapshotWaitTicks = 0
	r.probeSent = false
	r.inflights.reset()
}

// becomeSnapshot enters the snapshot state for index and seq, arming a snapshotTransferTicks backstop in case no status report arrives.
const snapshotTransferTicks = 60

func (r *remote) becomeSnapshot(snapshotIndex, seq uint64) {
	r.state = remoteSnapshot
	r.snapshotIndex = snapshotIndex
	r.snapshotSeq = seq
	// Arm the backstop countdown rather than leaving the sentinel 0. Expiry
	// runs through tickSnapshotWaits -> becomeProbe, which re-derives
	// next = max(match+1, snapshotIndex+1) and resumes the normal retry path.
	r.snapshotWaitTicks = snapshotTransferTicks
	r.probeSent = false
	r.inflights.reset()
}

// update updates the match and next indices after a successful append
// response. Returns true if the match index was actually advanced.
func (r *remote) update(index uint64) bool {
	updated := false
	if r.match < index {
		r.match = index
		updated = true
	}
	if r.next < index+1 {
		r.next = index + 1
	}
	return updated
}

// optimisticUpdate advances the next index optimistically during
// replication. Unlike update, this does not change the match index
// because the entries have not been acknowledged yet.
func (r *remote) optimisticUpdate(index uint64) {
	r.next = index + 1
}

// respondedTo records a response, moving probe to replicate, or snapshot to probe once match reaches the snapshot index.
func (r *remote) respondedTo() {
	switch r.state {
	case remoteProbe:
		r.becomeReplicate()
	case remoteSnapshot:
		if r.match >= r.snapshotIndex {
			r.becomeProbe()
		}
	default:
		// In replicate state, nothing to do.
	}
}

// declineFrom backtracks next to min(rejected, hintIndex+1) and reports whether next changed.
func (r *remote) declineFrom(rejectedIndex, hintIndex uint64) bool {
	if r.state == remoteReplicate {
		// In replicate state, a rejection means our optimistic next
		// was wrong. Fall back to match+1 which is known safe.
		if rejectedIndex <= r.match {
			// Stale rejection -- the entry was already matched.
			return false
		}
		r.next = r.match + 1
		return true
	}
	// In probe/snapshot state, use the hint to skip backward.
	// The rejection must be for next-1 (the index we actually sent).
	if r.next-1 != rejectedIndex {
		// Stale rejection.
		return false
	}
	// Use the hint: next = min(rejectedIndex, hintIndex+1), but never
	// below 1 since log indices start at 1.
	r.next = max(1, min(rejectedIndex, hintIndex+1))
	r.probeSent = false
	return true
}

// isPaused reports whether sends are paused: a probe in flight, any snapshot state, or a full inflights window.
func (r *remote) isPaused() bool {
	switch r.state {
	case remoteProbe:
		return r.probeSent
	case remoteReplicate:
		return r.inflights.full()
	case remoteSnapshot:
		return true
	default:
		slog.Error("remote: unexpected state, treating as paused", "state", r.state)
		return true
	}
}

// snapshotDone records a completed transfer and arms a waitTicks countdown (at least 1) for the peer's ack before falling back to probe.
func (r *remote) snapshotDone(waitTicks int) {
	r.snapshotWaitTicks = max(1, waitTicks)
}

// snapshotFailed records a failed transfer and arms the countdown as a retry backoff.
func (r *remote) snapshotFailed(waitTicks int) {
	r.snapshotWaitTicks = max(1, waitTicks)
}

// tickSnapshotWait advances the snapshot countdown and reports whether it expired.
func (r *remote) tickSnapshotWait() bool {
	if r.state != remoteSnapshot || r.snapshotWaitTicks == 0 {
		return false
	}
	r.snapshotWaitTicks--
	return r.snapshotWaitTicks == 0
}

// isActive returns whether the peer is considered active (has responded
// recently). Used by the leader's checkQuorum mechanism.
func (r *remote) isActive() bool {
	return r.active
}

// setActive marks the peer as active.
func (r *remote) setActive() {
	r.active = true
}

// setNotActive marks the peer as not active.
func (r *remote) setNotActive() {
	r.active = false
}
