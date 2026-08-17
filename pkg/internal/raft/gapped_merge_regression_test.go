package raft

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// The state these tests build is the one every follower passes through between
// a conflicting append and the commit worker persisting that truncation.
//
// tryAppend truncates the log by REPLACING the in-memory suffix (inMemory.merge
// discards everything at or above the conflict index). The LogReader is not
// told about the truncation until the commit worker reaches
// Peer.Commit -> logReader.Append, which is a whole commit/apply/fsync cycle
// later. In that window the LogReader still reports the OLD range and still
// serves the OLD terms for indices the log no longer holds, while
// raftLog.lastIndex() -- which prefers the in-memory tail whenever the buffer
// is non-empty -- correctly reports the truncated end.
//
// Every further message the step worker drains from the inbox is handled inside
// that window, so a delayed or duplicated Replicate carrying a prevLogIndex
// above the truncated tail is an ordinary event, not an exotic one.

// staleTailLog builds a follower whose in-memory log has just been truncated by
// a conflicting append while the LogReader still holds the superseded suffix.
//
// Layout: indices 1..10 at term 1 are persisted, commit index 5. A leader at
// term 3 then appends a conflicting entry at index 6, which replaces 6..10 in
// memory. Afterwards raftLog.lastIndex() == 6 and the LogReader still answers
// for 7..10.
func staleTailLog(t *testing.T) (*raftLog, *logdb.LogReader) {
	t.Helper()

	reader := logdb.NewLogReader(1, 1, nil)
	persisted := make([]proto.Entry, 0, 10)
	for i := uint64(1); i <= 10; i++ {
		persisted = append(persisted, proto.Entry{Index: i, Term: 1})
	}
	if err := reader.Append(persisted); err != nil {
		t.Fatalf("seeding the LogReader failed: %v", err)
	}
	reader.SetState(proto.State{Term: 1, Commit: 5})

	l := newRaftLog(reader)
	if l.committed != 5 {
		t.Fatalf("committed = %d, want 5", l.committed)
	}
	if got := l.lastIndex(); got != 10 {
		t.Fatalf("lastIndex = %d before the conflicting append, want 10", got)
	}

	// The conflicting append: a term-3 leader overwrites index 6.
	lastNew, ok, truncated, err := l.tryAppend(5, 1, []proto.Entry{{Index: 6, Term: 3}})
	if err != nil {
		t.Fatalf("the conflicting append failed: %v", err)
	}
	if !ok || !truncated || lastNew != 6 {
		t.Fatalf("conflicting append: ok=%v truncated=%v lastNew=%d, want true/true/6",
			ok, truncated, lastNew)
	}
	if got := l.lastIndex(); got != 6 {
		t.Fatalf("lastIndex = %d after truncating at 6, want 6: the rest of "+
			"this test depends on the in-memory tail being the log's end", got)
	}

	// The premise: the LogReader has NOT been told about the truncation yet,
	// because nothing has called Peer.Commit. Assert it directly so neither
	// test can pass by the premise quietly disappearing.
	if _, readerLast := reader.IndexRange(); readerLast != 10 {
		t.Fatalf("LogReader last index = %d, want 10: the window this test "+
			"covers is the one where the reader still holds the superseded "+
			"suffix", readerLast)
	}
	if term, err := reader.Term(8); err != nil || term != 1 {
		t.Fatalf("LogReader.Term(8) = (%d, %v), want (1, nil): the reader must "+
			"still be serving the superseded term for this test to mean "+
			"anything", term, err)
	}

	return l, reader
}

// TestRaftLogTermRejectsIndexAboveLastIndex pins the invariant the log itself
// already claims: nothing above raftLog.lastIndex() has a term.
//
// raftLog.term falls back to the LogReader whenever the in-memory buffer cannot
// answer, with no upper bound. Above the in-memory tail the LogReader is not
// authoritative -- it holds a suffix that a conflicting append has already
// removed from the log -- so the fallback resolves a term for an index the log
// does not hold. That makes term() and lastIndex() contradict each other, and
// matchTerm inherits the contradiction: the Log Matching check passes against
// an entry this replica threw away.
func TestRaftLogTermRejectsIndexAboveLastIndex(t *testing.T) {
	l, _ := staleTailLog(t)

	last := l.lastIndex()
	for index := last + 1; index <= 10; index++ {
		term, err := l.term(index)
		if err == nil {
			t.Errorf("term(%d) = %d with lastIndex() = %d: the log resolved a "+
				"term for an index it does not hold, from a LogReader suffix "+
				"the conflicting append already truncated",
				index, term, last)
		}
	}

	// The consequence, stated at the level Raft cares about.
	if l.matchTerm(8, 1) {
		t.Errorf("matchTerm(8, 1) accepted with lastIndex() = %d: a leader's "+
			"prevLogIndex above this replica's last index must be REJECTED so "+
			"handleAppend answers with the shorter-log hint", last)
	}
}

// TestRaftLogTryAppendPastInMemoryTailRejected drives the whole follower path
// with the message shape observed in production: prevLogIndex above the
// truncated in-memory tail, carrying the single entry that follows it.
//
// matchTerm passes on the superseded LogReader term, getConflictIndex reports
// the new entry as the conflict, and tryAppend then asks inMemory.merge to
// place it past the in-memory tail -- a hole in the log. merge asserts against
// exactly that ("gapped merge past in-memory tail"), so on the unfixed code
// this panics on the step worker rather than returning.
//
// The assertion here is on the observed condition -- tryAppend must REJECT --
// not on the absence of a panic; the recover only reports the panic in terms of
// what caused it.
func TestRaftLogTryAppendPastInMemoryTailRejected(t *testing.T) {
	l, _ := staleTailLog(t)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("tryAppend past the in-memory tail panicked (%v): "+
				"matchTerm accepted a prevLogIndex the log no longer holds, so "+
				"getConflictIndex returned an index past the in-memory tail and "+
				"inMemory.merge was asked to open a hole", r)
		}
	}()

	lastNew, ok, truncated, err := l.tryAppend(8, 1, []proto.Entry{{Index: 9, Term: 3}})
	if err != nil {
		t.Fatalf("tryAppend returned %v; a prevLogIndex above lastIndex is an "+
			"ordinary rejection, not an error", err)
	}
	if ok {
		t.Fatalf("tryAppend accepted prevLogIndex=8 (lastNew=%d, truncated=%v) "+
			"while lastIndex() = %d: the replica has just ACKed entries it "+
			"does not hold, and the leader counts that match toward quorum",
			lastNew, truncated, l.lastIndex())
	}

	// The log must be untouched and contiguous.
	if got := l.lastIndex(); got != 6 {
		t.Fatalf("lastIndex = %d after the rejected append, want 6", got)
	}
	if l.inmem.markerIndex != 6 || len(l.inmem.entries) != 1 {
		t.Fatalf("in-memory log is markerIndex=%d len=%d, want 6/1: the "+
			"rejected append must not have moved the buffer",
			l.inmem.markerIndex, len(l.inmem.entries))
	}
}
