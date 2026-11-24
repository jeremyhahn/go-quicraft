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
	"log/slog"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// unreadableLogRaft returns a raft whose OWN last log term cannot be read from
// either the in-memory log or the LogDB — a compaction race, a torn tail, or a
// closed LogDB in production. Every vote it is asked for is denied on that
// basis, no matter what the candidate's log looks like.
func unreadableLogRaft(t *testing.T) *raft {
	t.Helper()
	r := newTestRaft(1)
	r.log = &raftLog{
		logdb: newTestLogReader(nil, proto.State{}),
		inmem: newInMemory(5), // markerIndex 6, so lastIndex() == 5 is unresolvable
	}
	if _, ok := r.log.lastTerm(); ok {
		t.Fatal("test fixture does not actually have an unreadable last term")
	}
	return r
}

// newTestPeerForDiag returns a three-voter Peer for exercising DiagRaftState.
func newTestPeerForDiag(t *testing.T) *Peer {
	t.Helper()
	return newTestPeer(t, raftTestCfg(1, 1), proto.State{}, threeNodeMembership())
}

// TestHandleVoteRequest_UnreadableLogDenialIsReported pins that the fail-closed
// vote denial is reported rather than silent.
//
// isUpToDate must refuse a vote it cannot justify — granting on a fabricated
// term 0 would make every candidate look up to date and break Leader
// Completeness — but the refusal is caused by LOCAL storage and applies to
// every candidate equally. Hit a majority of replicas and the shard cannot
// elect a leader while every node is up, reachable, and reporting nothing at
// all. Its sibling check in startElection warns; without a log, a counter and
// a diagnostic field here, the only observable would be a leaderless shard
// with quorum apparently intact.
func TestHandleVoteRequest_UnreadableLogDenialIsReported(t *testing.T) {
	buf, cleanup := captureSlog(slog.LevelWarn)
	defer cleanup()

	r := unreadableLogRaft(t)

	if err := r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		To:       1,
		Term:     r.term + 1,
		LogIndex: 1000,
		LogTerm:  99,
	}); err != nil {
		t.Fatalf("step(RequestVote) failed: %v", err)
	}

	if r.vote == 2 {
		t.Fatal("vote granted while our own last log term is unreadable")
	}
	if r.unreadableLogVoteDenials == 0 {
		t.Fatal("a vote denied because our own last log term is unreadable was " +
			"not counted: on a majority this condition makes the shard " +
			"unelectable with no other symptom than silence")
	}
	if got := buf.String(); !strings.Contains(got, "local last log term unreadable") {
		t.Fatalf("no warning was logged for a fail-closed vote denial; log was: %q", got)
	}
}

// TestHandleVoteRequest_UnreadableLogDenialWarningIsRateLimited guards the
// report against becoming its own problem: every RequestVote from every
// candidate hits the same condition, and an election storm would otherwise
// emit one line per message.
func TestHandleVoteRequest_UnreadableLogDenialWarningIsRateLimited(t *testing.T) {
	buf, cleanup := captureSlog(slog.LevelWarn)
	defer cleanup()

	r := unreadableLogRaft(t)

	const votes = 50
	for i := range votes {
		if err := r.step(proto.Message{
			Type:     proto.RequestVote,
			From:     2,
			To:       1,
			Term:     r.term + 1,
			LogIndex: uint64(1000 + i),
			LogTerm:  99,
		}); err != nil {
			t.Fatalf("step(RequestVote) failed: %v", err)
		}
	}

	if r.unreadableLogVoteDenials != votes {
		t.Fatalf("counted %d denials, want %d: the counter must record every "+
			"occurrence even though the log lines are sampled",
			r.unreadableLogVoteDenials, votes)
	}
	if lines := strings.Count(buf.String(), "local last log term unreadable"); lines != 1 {
		t.Fatalf("emitted %d warnings for %d denials; the warning must be "+
			"rate-limited", lines, votes)
	}
}

// TestDiagRaftState_ReportsUnreadableLogDenials asserts the condition reaches
// the diagnostic surface an operator actually queries (Host.DiagRaftState),
// not only the log stream.
func TestDiagRaftState_ReportsUnreadableLogDenials(t *testing.T) {
	p := newTestPeerForDiag(t)
	p.r.log = &raftLog{
		logdb: newTestLogReader(nil, proto.State{}),
		inmem: newInMemory(5),
	}

	if _, _, _, _, _, state := p.DiagRaftState(); strings.Contains(state, "unreadable-log-denials") {
		t.Fatalf("state reported unreadable-log denials before any occurred: %q", state)
	}

	if err := p.r.step(proto.Message{
		Type:     proto.RequestVote,
		From:     2,
		To:       1,
		Term:     p.r.term + 1,
		LogIndex: 1000,
		LogTerm:  99,
	}); err != nil {
		t.Fatalf("step(RequestVote) failed: %v", err)
	}

	_, _, _, _, _, state := p.DiagRaftState()
	if !strings.Contains(state, "unreadable-log-denials=1") {
		t.Fatalf("DiagRaftState state = %q, want it to report the fail-closed "+
			"vote denials; a shard that cannot elect because of local storage "+
			"looks identical to one that is merely idle", state)
	}
}

// TestSetSelfMembership_RemovalIsReported covers the other permanently
// unelectable state.
//
// selfRemoved makes startElection return immediately, and nothing this replica
// does can clear it — only a config change that re-adds it, applied from a
// leader it may never hear from. That is a terminal condition, and it is
// EXPECTED for a replica that was just removed but a serious fault for one
// that was not (an empty or truncated membership restored from a damaged
// snapshot). Only an operator can tell those apart, and Debug-level logging
// with no diagnostic field gave them nothing to tell them apart with.
func TestSetSelfMembership_RemovalIsReported(t *testing.T) {
	buf, cleanup := captureSlog(slog.LevelWarn)
	defer cleanup()

	r := newTestRaft(1)
	if r.selfRemoved {
		t.Fatal("fixture starts self-removed")
	}

	r.setSelfMembership(false)

	if !r.selfRemoved {
		t.Fatal("setSelfMembership(false) did not set selfRemoved")
	}
	if got := buf.String(); !strings.Contains(got, "will not campaign") {
		t.Fatalf("entering the permanently unelectable self-removed state was "+
			"not reported above Debug; log was: %q", got)
	}
}

// TestDiagRaftState_ReportsSelfRemoved asserts self-removal reaches the
// diagnostic surface too.
func TestDiagRaftState_ReportsSelfRemoved(t *testing.T) {
	p := newTestPeerForDiag(t)

	if _, _, _, _, _, state := p.DiagRaftState(); strings.Contains(state, "self-removed") {
		t.Fatalf("state reported self-removed on a replica that is a member: %q", state)
	}

	p.r.setSelfMembership(false)

	_, _, _, _, _, state := p.DiagRaftState()
	if !strings.Contains(state, "self-removed") {
		t.Fatalf("DiagRaftState state = %q, want it to report self-removal; "+
			"a replica that will never campaign is otherwise indistinguishable "+
			"from an ordinary follower", state)
	}
}
