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
	"errors"
	"math"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestHandleSnapshot_RefusesPositionOutsideProtocolRange pins the snapshot
// position bounds on the raft side: an InstallSnapshot is delivered
// process-locally after the artifact is on disk, so it never meets step()'s
// wire term ceiling, and a snapshot at Index = MaxUint64 would otherwise
// restore cleanly and then wrap markerIndex, firstIndex and every remote's
// next to 0. The position is refused before it
// touches the log, and the refusal is the proto sentinel so callers can name it.
func TestHandleSnapshot_RefusesPositionOutsideProtocolRange(t *testing.T) {
	for _, tc := range []struct {
		name string
		ss   proto.Snapshot
		want error
	}{
		{"index at the ceiling", proto.Snapshot{Index: math.MaxUint64, Term: 5}, proto.ErrLogIndexOutOfRange},
		{"index at the bound", proto.Snapshot{Index: proto.MaxLogIndex, Term: 5}, proto.ErrLogIndexOutOfRange},
		{"term at the ceiling", proto.Snapshot{Index: 40, Term: math.MaxUint64}, proto.ErrTermOutOfRange},
		{"term at the bound", proto.Snapshot{Index: 40, Term: proto.MaxTerm}, proto.ErrTermOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := followerWithEstablishedLeader(t)
			committedBefore, lastBefore, leaderBefore := r.log.committed, r.log.lastIndex(), r.leaderID
			tc.ss.Membership = proto.Membership{Addresses: map[uint64]string{1: "a", 2: "b", 3: "c"}}

			err := r.step(proto.Message{
				Type: proto.InstallSnapshot, From: leaderBefore, To: 1, Origin: proto.OriginLocal,
				Snapshot: tc.ss,
			})

			if !errors.Is(err, tc.want) {
				t.Fatalf("step = %v, want %v", err, tc.want)
			}
			if r.log.committed != committedBefore || r.log.lastIndex() != lastBefore {
				t.Fatalf("log moved committed %d->%d lastIndex %d->%d on a refused snapshot",
					committedBefore, r.log.committed, lastBefore, r.log.lastIndex())
			}
			if r.log.firstIndex() == 0 && lastBefore > 0 {
				t.Fatalf("firstIndex wrapped to 0: the refused position became the log's arithmetic base")
			}
		})
	}
}

// TestNewRaft_RefusesPersistedStateAtTheCeiling pins the open-time half of the
// position bounds: a store that holds a snapshot, term or log at the CEILING — where +1
// wraps — fails to open rather than running there. The open bound is higher
// than the admission bound on purpose: state admitted just below
// proto.MaxLogIndex / proto.MaxTerm can be moved by local transitions (the
// leader's no-op, the candidate's increment), and an open check at the
// admission bound would refuse state the replica produced legitimately and
// make a restart fail forever.
func TestNewRaft_RefusesPersistedStateAtTheCeiling(t *testing.T) {
	members := map[uint64]string{1: "a", 2: "b", 3: "c"}
	for _, tc := range []struct {
		name  string
		setup func(lr *testLogReader)
		want  error
	}{
		{"snapshot index", func(lr *testLogReader) {
			lr.snapshot = proto.Snapshot{Index: persistedCeiling, Term: 3}
		}, proto.ErrLogIndexOutOfRange},
		{"snapshot term", func(lr *testLogReader) {
			lr.snapshot = proto.Snapshot{Index: 10, Term: math.MaxUint64}
		}, proto.ErrTermOutOfRange},
		{"hard state term", func(lr *testLogReader) {
			lr.state = proto.State{Term: persistedCeiling, Vote: 1}
		}, proto.ErrTermOutOfRange},
		{"last log index", func(lr *testLogReader) {
			lr.entries = []proto.Entry{{Index: math.MaxUint64, Term: 3}}
		}, proto.ErrLogIndexOutOfRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lr := newTestLogReaderWithMembership(members)
			tc.setup(lr)
			if _, err := newRaft(raftTestCfg(1, 1), lr); !errors.Is(err, tc.want) {
				t.Fatalf("newRaft = %v, want %v", err, tc.want)
			}
		})
	}

	// Everything the local transitions can produce from admitted state opens:
	// a snapshot one below the admission bound, a term the candidate stopped
	// at, a log the leader's no-op pushed onto the bound.
	for _, tc := range []struct {
		name  string
		setup func(lr *testLogReader)
	}{
		{"snapshot one below the admission bound", func(lr *testLogReader) {
			lr.snapshot = proto.Snapshot{Index: proto.MaxLogIndex - 1, Term: proto.MaxTerm - 1}
		}},
		{"term at the admission bound", func(lr *testLogReader) {
			lr.state = proto.State{Term: proto.MaxTerm, Vote: 1}
		}},
		{"log at the admission bound", func(lr *testLogReader) {
			lr.entries = []proto.Entry{{Index: proto.MaxLogIndex, Term: 3}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lr := newTestLogReaderWithMembership(members)
			tc.setup(lr)
			if _, err := newRaft(raftTestCfg(1, 1), lr); err != nil {
				t.Fatalf("newRaft = %v, want nil: this is state the replica itself can produce", err)
			}
		})
	}
}

// TestHandleLeaderPropose_RefusesPastTheAdmissionBound pins that the log stops
// growing at proto.MaxLogIndex: a leader there refuses proposals rather than
// carrying the index toward the ceiling.
func TestHandleLeaderPropose_RefusesPastTheAdmissionBound(t *testing.T) {
	members := map[uint64]string{1: "a", 2: "b", 3: "c"}
	lr := newTestLogReaderWithMembership(members)
	lr.snapshot = proto.Snapshot{Index: proto.MaxLogIndex, Term: 3, Membership: proto.Membership{Addresses: members}}
	lr.state = proto.State{Term: 3, Commit: proto.MaxLogIndex}
	lr.snapshot.Index = proto.MaxLogIndex - 1 // leadership's no-op lands ON the bound
	lr.state.Commit = proto.MaxLogIndex - 1
	r, err := newRaft(raftTestCfg(1, 1), lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.becomeCandidate()
	if err := r.becomeLeader(); err != nil {
		t.Fatalf("becomeLeader: %v", err)
	}
	if got := r.log.lastIndex(); got != proto.MaxLogIndex {
		t.Fatalf("fixture: lastIndex = %d, want the bound", got)
	}
	err = r.step(proto.Message{Type: proto.Propose, Origin: proto.OriginLocal,
		Entries: []proto.Entry{{Type: proto.EntryNormal, Cmd: []byte("x")}}})
	if !errors.Is(err, proto.ErrLogIndexOutOfRange) {
		t.Fatalf("Propose at the admission bound = %v, want ErrLogIndexOutOfRange", err)
	}

	// The whole batch is judged: two entries from one below the bound would
	// straddle it. One entry from two below fits.
	near := newTestLogReaderWithMembership(members)
	near.snapshot = proto.Snapshot{Index: proto.MaxLogIndex - 2, Term: 3, Membership: proto.Membership{Addresses: members}}
	near.state = proto.State{Term: 3, Commit: proto.MaxLogIndex - 2}
	r2, err := newRaft(raftTestCfg(1, 1), near)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r2.becomeCandidate()
	if err := r2.becomeLeader(); err != nil { // no-op lands at MaxLogIndex-1
		t.Fatalf("becomeLeader two below the bound: %v", err)
	}
	two := []proto.Entry{{Type: proto.EntryNormal, Cmd: []byte("a")}, {Type: proto.EntryNormal, Cmd: []byte("b")}}
	if err := r2.step(proto.Message{Type: proto.Propose, Origin: proto.OriginLocal, Entries: two}); !errors.Is(err, proto.ErrLogIndexOutOfRange) {
		t.Fatalf("a two-entry batch from one below the bound = %v, want ErrLogIndexOutOfRange", err)
	}
	if err := r2.step(proto.Message{Type: proto.Propose, Origin: proto.OriginLocal, Entries: two[:1]}); err != nil {
		t.Fatalf("a one-entry batch from one below the bound = %v, want nil", err)
	}
	if got := r2.log.lastIndex(); got != proto.MaxLogIndex {
		t.Fatalf("lastIndex after the fitting batch = %d, want %d", got, proto.MaxLogIndex)
	}

	// Leadership is declined at the bound: the no-op has nowhere to go.
	at := newTestLogReaderWithMembership(members)
	at.snapshot = proto.Snapshot{Index: proto.MaxLogIndex, Term: 3, Membership: proto.Membership{Addresses: members}}
	at.state = proto.State{Term: 3, Commit: proto.MaxLogIndex}
	r3, err := newRaft(raftTestCfg(1, 1), at)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r3.becomeCandidate()
	if err := r3.becomeLeader(); !errors.Is(err, proto.ErrLogIndexOutOfRange) {
		t.Fatalf("becomeLeader at the bound = %v, want ErrLogIndexOutOfRange", err)
	}
}

// TestHandleAppend_RefusesPastTheAdmissionBound pins the follower half of the
// bound: a Replicate whose batch would end above proto.MaxLogIndex is refused
// before tryAppend, whatever the leader says, so no replicated append can
// carry a follower's log past the bound the local paths hold. The bound
// itself may be reached (a leader's no-op lands on it); nothing lands above.
func TestHandleAppend_RefusesPastTheAdmissionBound(t *testing.T) {
	members := map[uint64]string{1: "a", 2: "b", 3: "c"}
	lr := newTestLogReaderWithMembership(members)
	lr.snapshot = proto.Snapshot{Index: proto.MaxLogIndex - 1, Term: 3, Membership: proto.Membership{Addresses: members}}
	lr.state = proto.State{Term: 3, Commit: proto.MaxLogIndex - 1}
	r, err := newRaft(raftTestCfg(1, 1), lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.becomeFollower(3, 2)
	prev := proto.MaxLogIndex - 1

	// A batch from the bound itself: refused.
	past := proto.Message{Type: proto.Replicate, From: 2, To: 1, Term: 3, LogIndex: proto.MaxLogIndex, LogTerm: 3, Commit: prev,
		Entries: []proto.Entry{{Index: proto.MaxLogIndex + 1, Term: 3, Type: proto.EntryNormal}}}
	if err := r.step(past); !errors.Is(err, proto.ErrLogIndexOutOfRange) {
		t.Fatalf("Replicate from the bound = %v, want ErrLogIndexOutOfRange", err)
	}
	// A batch that would straddle it, from below, is refused whole.
	straddle := proto.Message{Type: proto.Replicate, From: 2, To: 1, Term: 3, LogIndex: prev, LogTerm: 3, Commit: prev,
		Entries: []proto.Entry{{Index: proto.MaxLogIndex, Term: 3, Type: proto.EntryNormal}, {Index: proto.MaxLogIndex + 1, Term: 3, Type: proto.EntryNormal}}}
	if err := r.step(straddle); !errors.Is(err, proto.ErrLogIndexOutOfRange) {
		t.Fatalf("Replicate straddling the bound = %v, want ErrLogIndexOutOfRange", err)
	}
	if got := r.log.lastIndex(); got != prev {
		t.Fatalf("lastIndex = %d after the refused appends, want %d", got, prev)
	}
	// One entry landing ON the bound, and an entry-less Replicate, pass the
	// bound check (the former is what a leader's no-op does).
	for _, msg := range []proto.Message{
		{Type: proto.Replicate, From: 2, To: 1, Term: 3, LogIndex: prev, LogTerm: 3, Commit: prev,
			Entries: []proto.Entry{{Index: proto.MaxLogIndex, Term: 3, Type: proto.EntryNormal}}},
		{Type: proto.Replicate, From: 2, To: 1, Term: 3, LogIndex: prev, LogTerm: 3, Commit: prev},
	} {
		if err := r.step(msg); errors.Is(err, proto.ErrLogIndexOutOfRange) {
			t.Fatalf("a Replicate ending at or below the bound was refused: %v", err)
		}
	}
}

// TestHandleAppend_RefusesEntryTermsAboveTheLeaders pins the term half of the
// replicated-append admission: an entry's term never exceeds the term of the
// leader replicating it, and one that does — corrupt or hostile — would
// poison lastTerm on disk. Refused whole before the log is touched.
func TestHandleAppend_RefusesEntryTermsAboveTheLeaders(t *testing.T) {
	members := map[uint64]string{1: "a", 2: "b", 3: "c"}
	lr := newTestLogReaderWithMembership(members)
	lr.state = proto.State{Term: 3}
	r, err := newRaft(raftTestCfg(1, 1), lr)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	r.becomeFollower(3, 2)
	for name, entries := range map[string][]proto.Entry{
		"above the leader's term": {{Index: 1, Term: 3, Type: proto.EntryNormal}, {Index: 2, Term: 4, Type: proto.EntryNormal}},
		"near the ceiling":        {{Index: 1, Term: math.MaxUint64 - 1, Type: proto.EntryNormal}},
		"term zero":               {{Index: 1, Term: 0, Type: proto.EntryNormal}},
	} {
		msg := proto.Message{Type: proto.Replicate, From: 2, To: 1, Term: 3, LogIndex: 0, LogTerm: 0, Commit: 0, Entries: entries}
		if err := r.step(msg); !errors.Is(err, proto.ErrTermOutOfRange) {
			t.Fatalf("%s: step = %v, want ErrTermOutOfRange", name, err)
		}
		if got := r.log.lastIndex(); got != 0 {
			t.Fatalf("%s: lastIndex = %d after the refused append, want 0", name, got)
		}
	}
	// Entries at or below the leader's term are appended.
	ok := proto.Message{Type: proto.Replicate, From: 2, To: 1, Term: 3, LogIndex: 0, LogTerm: 0, Commit: 0,
		Entries: []proto.Entry{{Index: 1, Term: 2, Type: proto.EntryNormal}, {Index: 2, Term: 3, Type: proto.EntryNormal}}}
	if err := r.step(ok); err != nil {
		t.Fatalf("a batch at and below the leader's term = %v, want nil", err)
	}
	if got := r.log.lastIndex(); got != 2 {
		t.Fatalf("lastIndex = %d after a legal append, want 2", got)
	}
}
