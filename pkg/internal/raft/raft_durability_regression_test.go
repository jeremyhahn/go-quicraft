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

// TestTryCommit_LeaderSelfMatchIsDurableIndex verifies that tryCommit
// counts the leader's own match at its durable index (inmem.savedTo),
// never at the tip of its in-memory log. In a 3-node cluster a single
// follower ack plus a non-durable local append is NOT a quorum of
// durable matches: an entry acknowledged by one follower must stay
// uncommitted until the leader's own append reaches stable storage,
// at which point the persistence-edge re-evaluation completes the
// quorum. Counting the in-memory tip would allow a commit backed by
// only one durable copy.
func TestTryCommit_LeaderSelfMatchIsDurableIndex(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	commitNoOp(r)

	// Propose an entry; it is appended in memory but NOT persisted, so
	// the leader's durable index (savedTo) still points at the no-op.
	if err := r.step(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal,
		Entries: []proto.Entry{{Cmd: []byte("durable-quorum")}},
	}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	entryIdx := r.log.lastIndex()
	r.drainMessages()

	if r.log.inmem.savedTo >= entryIdx {
		t.Fatalf("precondition: entry %d must not be durable yet, savedTo=%d",
			entryIdx, r.log.inmem.savedTo)
	}

	// A single follower ack gives one durable copy besides the leader's
	// non-durable append. Quorum is 2 durable matches; committed must
	// not advance.
	if err := r.step(proto.Message{
		Type:     proto.ReplicateResp,
		From:     2,
		Term:     r.term,
		LogIndex: entryIdx,
	}); err != nil {
		t.Fatalf("ReplicateResp: %v", err)
	}
	r.drainMessages()
	if r.log.committed >= entryIdx {
		t.Fatalf("committed must not advance on one follower ack while the "+
			"leader's own append is not durable: committed=%d, entryIdx=%d",
			r.log.committed, entryIdx)
	}

	// Persisting the leader's log is the persistence edge: its durable
	// match now covers entryIdx, completing the quorum with follower 2.
	persistLog(r)
	if r.log.committed < entryIdx {
		t.Fatalf("expected committed >= %d once the leader's append is durable, got %d",
			entryIdx, r.log.committed)
	}
}

// TestHandleSnapshot_MatchingLogAdvancesProcessed verifies that when an
// InstallSnapshot matches the local log at the snapshot index (same
// index and term, Raft Figure 13), handleSnapshot advances BOTH the
// committed and the processed markers to the snapshot index. The state
// machine is restored from the snapshot, so entries at or below the
// snapshot index are already represented in the recovered state; a
// processed marker left behind would re-emit them to the apply worker
// and double-apply them on top of the restored state, which is what
// happens if only committed advances on the matching-log branch.
func TestHandleSnapshot_MatchingLogAdvancesProcessed(t *testing.T) {
	const (
		snapIndex = uint64(10)
		snapTerm  = uint64(2)
	)

	r := newTestRaftPreVoteDisabled(1)
	r.becomeFollower(snapTerm, 2)

	// Build a contiguous log from index 1..12, all at the snapshot term,
	// with nothing committed: the log matches the snapshot at snapIndex
	// and retains the entries beyond it.
	entries := make([]proto.Entry, 12)
	for i := range entries {
		entries[i] = proto.Entry{Index: uint64(i + 1), Term: snapTerm}
	}
	r.log.inmem.merge(entries)

	if r.log.committed >= snapIndex {
		t.Fatalf("precondition: committed must be below snapshot index, committed=%d",
			r.log.committed)
	}

	msg := proto.Message{
		Type: proto.InstallSnapshot, Origin: proto.OriginLocal,
		From: 2,
		Term: snapTerm,
		Snapshot: proto.Snapshot{
			Index: snapIndex,
			Term:  snapTerm,
			Membership: proto.Membership{
				Addresses: map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"},
			},
		},
	}
	if err := r.step(msg); err != nil {
		t.Fatalf("step InstallSnapshot: %v", err)
	}

	if r.log.committed != snapIndex {
		t.Fatalf("expected committed == %d after matching-log snapshot, got %d",
			snapIndex, r.log.committed)
	}
	if r.log.processed != snapIndex {
		t.Fatalf("expected processed == %d after matching-log snapshot "+
			"(covered entries must not be re-emitted to apply), got %d",
			snapIndex, r.log.processed)
	}
}
