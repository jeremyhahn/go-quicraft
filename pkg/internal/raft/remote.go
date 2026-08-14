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

	// snapshotSeq identifies the specific transfer attempt in flight
	// (raft.nextSnapshotSeq). A SnapshotStatus report is only honored
	// when its sequence matches: the snapshot index alone cannot
	// disambiguate a RE-SENT snapshot at the same index, so a late
	// status from a superseded same-index attempt could otherwise arm
	// the countdown (or force a backoff) for the newer in-flight
	// stream. Only meaningful when state is remoteSnapshot.
	snapshotSeq uint64

	// snapshotWaitTicks is the ack/retry countdown armed after the
	// snapshot transfer reports its outcome; see snapshotDone. Only
	// meaningful when state is remoteSnapshot; zero means the transfer
	// is still in flight (no countdown armed).
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

// String returns a human-readable representation of the remote state.
// snapshotwait distinguishes a snapshot transfer in flight (0) from one
// whose outcome was reported and is awaiting the peer's ack or the
// countdown fallback (>0) — without it, a peer stuck in Snapshot state
// gives an operator no clue which phase it is in.
func (r *remote) String() string {
	return fmt.Sprintf("match:%d,next:%d,state:%s,snapshot:%d,snapshotwait:%d",
		r.match, r.next, r.state, r.snapshotIndex, r.snapshotWaitTicks)
}

// becomeReplicate transitions to the replicate state. The next index
// is set to match+1 and the inflights tracker is reset. This is called
// when the leader has confirmed the peer's log position and can begin
// optimistic replication.
func (r *remote) becomeReplicate() {
	r.state = remoteReplicate
	r.next = r.match + 1
	r.snapshotIndex = 0
	r.snapshotSeq = 0
	r.snapshotWaitTicks = 0
	r.probeSent = false
	r.inflights.reset()
}

// becomeProbe transitions to the probe state. The next index is set
// based on the current state:
//   - From snapshot state: next = max(match+1, snapshotIndex+1)
//   - From other states: next = match+1
//
// The inflights tracker is reset and probeSent is cleared.
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

// becomeSnapshot transitions to the snapshot state, recording the index
// of the snapshot being sent and the sequence identifying this transfer
// attempt (see snapshotSeq). The inflights tracker is reset.
func (r *remote) becomeSnapshot(snapshotIndex, seq uint64) {
	r.state = remoteSnapshot
	r.snapshotIndex = snapshotIndex
	r.snapshotSeq = seq
	r.snapshotWaitTicks = 0
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

// respondedTo records that the peer has responded to a message. In
// probe state, this transitions to replicate since we now know the
// peer's position. In snapshot state, this transitions to probe if
// the match has caught up to or passed the snapshot index.
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

// declineFrom handles a rejected append at the given rejected index.
// The hintIndex is the follower's min(msg.LogIndex, lastIndex), used
// for index-based backtracking: next = min(rejectedIndex, hintIndex+1).
// This gives O(entries) convergence, matching the dragonboat reference
// implementation's decreaseTo(rejected, last) formula.
//
// Returns true if the next index was actually changed (false means
// the rejection was stale and should be ignored).
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

// isPaused returns whether sending to this peer should be paused.
//   - Probe: paused if a probe has already been sent (waiting for response)
//   - Snapshot: always paused — during the transfer AND during the
//     post-outcome ack/retry wait (see snapshotDone/snapshotFailed)
//   - Replicate: paused if the inflights window is full
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

// snapshotDone records that the snapshot TRANSFER completed successfully.
// The peer stays paused in remoteSnapshot: transfer completion only means
// the bytes arrived — the peer's raft layer may still be restoring, and
// probing it before it acknowledges (ReplicateResp with LogIndex >=
// snapshotIndex, via respondedTo) draws a rejection that backtracks next
// into compacted territory and triggers a redundant snapshot re-send.
// Instead, arm a countdown of waitTicks leader ticks as a liveness
// fallback: if the ack never arrives (peer crashed, message lost),
// tickSnapshotWait expires the wait and the peer falls back to probe so
// the normal retry path resumes. This mirrors the dragonboat reference's
// wait state + delayed snapshot ack. This is the canonical description of
// the post-transfer wait; other sites reference it.
//
// waitTicks must be positive: zero collides with the "no countdown armed"
// sentinel and would leave the peer paused forever, so it is clamped to 1
// (immediate expiry on the next tick) rather than silently never expiring.
func (r *remote) snapshotDone(waitTicks int) {
	r.snapshotWaitTicks = max(1, waitTicks)
}

// snapshotFailed records that the snapshot transfer failed. The peer stays
// paused in remoteSnapshot with the countdown armed as a retry backoff:
// probing immediately would draw a rejection that backtracks into compacted
// territory and re-stream the full snapshot with zero pause, so a
// persistently failing transfer (unreachable peer, full receiver disk)
// would saturate the leader's uplink with back-to-back streams. Expiry
// falls back to probe, resuming the retry path. Same clamp rationale as
// snapshotDone.
func (r *remote) snapshotFailed(waitTicks int) {
	r.snapshotWaitTicks = max(1, waitTicks)
}

// tickSnapshotWait advances the snapshot ack/retry countdown by one leader
// tick. Returns true when the countdown expires, meaning the peer never
// acknowledged the transfer outcome and the caller should fall back to
// probe. Returns false when no countdown is armed (transfer still in
// flight) or ticks remain.
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
