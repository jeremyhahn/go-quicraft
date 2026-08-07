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

// proposeTinyEntries appends n zero-payload entries through the leader in one
// proposal. Each is 60 wire bytes, so thousands of them stay far below
// maxReplicationPayload — the byte bound cannot bound their count.
func proposeTinyEntries(t *testing.T, r *raft, n int) {
	t.Helper()
	entries := make([]proto.Entry, n)
	if err := r.step(proto.Message{
		Type: proto.Propose, Origin: proto.OriginLocal,
		From:    r.replicaID,
		Entries: entries,
	}); err != nil {
		t.Fatalf("propose failed: %v", err)
	}
}

// TestSendAppendBoundsEntryCount is the producer half of the transport dial
// storm: maxReplicationPayload (Config.MaxApplyEntrySize) bounds a Replicate
// message in BYTES only, so a follower catching up on small entries drew a
// message with hundreds of thousands of them. Every peer's decoder rejects a
// message above proto.MaxEntriesPerMessage and resets the stream it arrived
// on, so the follower could never be fed the entries it was missing.
func TestSendAppendBoundsEntryCount(t *testing.T) {
	r := newTestRaftPreVoteDisabled(1)
	electLeader(r)
	if r.state != leader {
		t.Fatal("expected leader state after election")
	}

	const extra = 50
	proposeTinyEntries(t, r, proto.MaxEntriesPerMessage+extra)

	// Follower 2 is far behind: it has only the bootstrap entry.
	rm := r.remotes[2]
	rm.becomeReplicate()
	rm.match = 1
	rm.next = 2
	r.drainMessages()

	r.sendAppend(2)

	msgs := collectMessages(r, proto.Replicate)
	if len(msgs) == 0 {
		t.Fatal("expected at least one Replicate message to peer 2")
	}

	// Every message must be individually acceptable to a peer's decoder.
	total := 0
	for i := range msgs {
		if len(msgs[i].Entries) > proto.MaxEntriesPerMessage {
			t.Fatalf("Replicate message %d carries %d entries, above the wire limit of %d: "+
				"every peer rejects it with proto.ErrTooManyMessages and resets the stream",
				i, len(msgs[i].Entries), proto.MaxEntriesPerMessage)
		}
		total += len(msgs[i].Entries)
	}

	// Truncation must not lose entries: pipelining carries the remainder in
	// the following messages of the same cycle, contiguously from next.
	if total < proto.MaxEntriesPerMessage+extra {
		t.Fatalf("expected all %d entries to be sent across the pipelined messages, got %d",
			proto.MaxEntriesPerMessage+extra, total)
	}
	wantIndex := uint64(2)
	for i := range msgs {
		if msgs[i].LogIndex != wantIndex-1 {
			t.Fatalf("message %d has prevLogIndex %d, want %d (gap in the replicated range)",
				i, msgs[i].LogIndex, wantIndex-1)
		}
		for j := range msgs[i].Entries {
			if msgs[i].Entries[j].Index != wantIndex {
				t.Fatalf("message %d entry %d has index %d, want %d",
					i, j, msgs[i].Entries[j].Index, wantIndex)
			}
			wantIndex++
		}
	}
}

// TestSendWitnessAppendBoundsEntryCount asserts the same bound on the witness
// path, which builds its metadata-only entries from the same byte-bounded
// range read.
func TestSendWitnessAppendBoundsEntryCount(t *testing.T) {
	members := map[uint64]string{1: "addr1", 2: "addr2", 3: "addr3"}
	cfg := raftTestCfgPreVote(1, 1, false)
	logdb := newTestLogReaderWithMembership(members)
	logdb.membership.Witnesses = map[uint64]string{4: "addr4"}
	r, err := newRaft(cfg, logdb)
	if err != nil {
		t.Fatalf("newRaft failed: %v", err)
	}
	electLeader(r)
	if r.state != leader {
		t.Fatal("expected leader state after election")
	}
	wit := r.witnesses[4]
	if wit == nil {
		t.Fatal("expected witness 4 in the membership")
	}

	proposeTinyEntries(t, r, proto.MaxEntriesPerMessage+50)

	wit.becomeReplicate()
	wit.match = 1
	wit.next = 2
	r.drainMessages()

	r.sendWitnessAppend(4)

	msgs := collectMessages(r, proto.Replicate)
	if len(msgs) == 0 {
		t.Fatal("expected at least one Replicate message to witness 4")
	}
	for i := range msgs {
		if len(msgs[i].Entries) > proto.MaxEntriesPerMessage {
			t.Fatalf("witness Replicate message %d carries %d entries, above the wire limit of %d",
				i, len(msgs[i].Entries), proto.MaxEntriesPerMessage)
		}
	}
}
