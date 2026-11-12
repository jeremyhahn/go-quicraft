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

package quicraft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// startLogReaderViewShard starts a single-replica shard 1 and waits for it
// to elect itself and apply its current-term no-op.
//
// waitForLeader is not used: it syncs the host's cached leader with term 0
// (host_test.go), which would make LeaderIDAndTerm report term 0 for the
// rest of the test and the term assertion below meaningless. This waits for
// the host to learn the leader and its term from the engine instead.
func startLogReaderViewShard(t *testing.T) *Host {
	t.Helper()
	h := newTestHost(t)
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"},
		false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		leaderID, term, ok, err := h.LeaderIDAndTerm(1)
		if err != nil {
			t.Fatalf("LeaderIDAndTerm: %v", err)
		}
		if ok && leaderID == 1 && term != 0 && testLoadNode(h, 1).lastApplied.Load() >= 2 {
			return h
		}
		if time.Now().After(deadline) {
			t.Fatalf("shard 1 never reported leader 1 with a term: leader %d term %d", leaderID, term)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// logReaderViewMatches reports whether the reader agrees with LogRange and
// LeaderIDAndTerm at one instant, and describes any disagreement.
func logReaderViewMatches(h *Host, r ReadonlyLogReader) (bool, string) {
	lr, err := h.LogRange(1)
	if err != nil {
		return false, fmt.Sprintf("LogRange: %v", err)
	}
	_, term, _, err := h.LeaderIDAndTerm(1)
	if err != nil {
		return false, fmt.Sprintf("LeaderIDAndTerm: %v", err)
	}
	first, last := r.GetRange()
	stateLast, stateTerm := r.NodeState()
	if first != lr.FirstIndex || last != lr.LastIndex {
		return false, fmt.Sprintf("GetRange() = (%d, %d), LogRange = (%d, %d)",
			first, last, lr.FirstIndex, lr.LastIndex)
	}
	if stateLast != lr.LastIndex || stateTerm != term {
		return false, fmt.Sprintf("NodeState() = (%d, %d), want (LogRange.LastIndex %d, term %d)",
			stateLast, stateTerm, lr.LastIndex, term)
	}
	return true, ""
}

// requireLogReaderViewMatches waits for the shard to settle (the leader's
// no-op and hard state are persisted asynchronously) and fails with the last
// disagreement if the reader never agrees with LogRange and the term.
func requireLogReaderViewMatches(t *testing.T, h *Host, r ReadonlyLogReader) {
	t.Helper()
	var why string
	deadline := time.Now().Add(5 * time.Second)
	for {
		var ok bool
		if ok, why = logReaderViewMatches(h, r); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("LogReader view disagrees with the Host: %s", why)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestLogReaderViewMatchesLogRangeAndTerm pins (a): GetRange is LogRange and
// NodeState is (LogRange.LastIndex, current term).
func TestLogReaderViewMatchesLogRangeAndTerm(t *testing.T) {
	h := startLogReaderViewShard(t)
	r, err := h.LogReader(1)
	if err != nil {
		t.Fatalf("LogReader: %v", err)
	}
	requireLogReaderViewMatches(t, h, r)

	_, term, _, err := h.LeaderIDAndTerm(1)
	if err != nil {
		t.Fatalf("LeaderIDAndTerm: %v", err)
	}
	if term == 0 {
		t.Fatal("an elected shard reported term 0; the term assertion would be vacuous")
	}
}

// TestLogReaderViewIsLiveAcrossProposals pins (b): a reader obtained before
// proposals is a live view, not a snapshot taken when LogReader was called.
func TestLogReaderViewIsLiveAcrossProposals(t *testing.T) {
	h := startLogReaderViewShard(t)
	r, err := h.LogReader(1)
	if err != nil {
		t.Fatalf("LogReader: %v", err)
	}
	requireLogReaderViewMatches(t, h, r)
	_, lastBefore := r.GetRange()
	stateLastBefore, _ := r.NodeState()

	const proposals = 20
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range proposals {
		if _, err := h.SyncPropose(ctx, 1, []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
	}

	_, lastAfter := r.GetRange()
	if lastAfter < lastBefore+proposals {
		t.Fatalf("GetRange last index %d -> %d after %d proposals; the reader is not live",
			lastBefore, lastAfter, proposals)
	}
	stateLastAfter, _ := r.NodeState()
	if stateLastAfter < stateLastBefore+proposals {
		t.Fatalf("NodeState last index %d -> %d after %d proposals; the reader is not live",
			stateLastBefore, stateLastAfter, proposals)
	}
	requireLogReaderViewMatches(t, h, r)
}

// TestLogReaderViewFollowsCompaction pins (c): after a snapshot with a small
// compaction overhead and RequestCompaction, GetRange's first index is
// LogRange's first index and has moved above 1.
func TestLogReaderViewFollowsCompaction(t *testing.T) {
	h := startLogReaderViewShard(t)
	r, err := h.LogReader(1)
	if err != nil {
		t.Fatalf("LogReader: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range 20 {
		if _, err := h.SyncPropose(ctx, 1, []byte(fmt.Sprintf("c-%d", i))); err != nil {
			t.Fatalf("SyncPropose %d: %v", i, err)
		}
	}
	snapIdx, err := h.SyncRequestSnapshot(ctx, 1, SnapshotOption{
		CompactionOverhead:         2,
		OverrideCompactionOverhead: true,
	})
	if err != nil {
		t.Fatalf("SyncRequestSnapshot: %v", err)
	}
	if err := h.RequestCompaction(1, 1); err != nil {
		t.Fatalf("RequestCompaction: %v", err)
	}

	// Log compaction after the snapshot save runs asynchronously.
	deadline := time.Now().Add(10 * time.Second)
	for {
		first, _ := r.GetRange()
		lr, lrErr := h.LogRange(1)
		if lrErr != nil {
			t.Fatalf("LogRange: %v", lrErr)
		}
		if first > 1 && first == lr.FirstIndex {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after a snapshot at %d and compaction: GetRange first index %d, "+
				"LogRange.FirstIndex %d; want equal and above 1", snapIdx, first, lr.FirstIndex)
		}
		time.Sleep(5 * time.Millisecond)
	}
	requireLogReaderViewMatches(t, h, r)
}

// TestLogReaderViewStoppedShardNotFound pins (d): a stopped shard has no
// reader.
func TestLogReaderViewStoppedShardNotFound(t *testing.T) {
	h := startLogReaderViewShard(t)
	if err := h.StopShard(1); err != nil {
		t.Fatalf("StopShard: %v", err)
	}
	if _, err := h.LogReader(1); !errors.Is(err, ErrShardNotFound) {
		t.Fatalf("LogReader after StopShard returned %v, want ErrShardNotFound", err)
	}
}

// TestLogReaderViewTermAdvancesWithoutAppend pins NodeState to the live Raft
// term when the term advances without a log append: a follower holding a
// term-2 log that accepts a term-3 heartbeat is at term 3, while its log tail
// and the reader's cached hard state still say 2.
func TestLogReaderViewTermAdvancesWithoutAppend(t *testing.T) {
	lr := logdb.NewLogReader(1, 1, nil)
	if err := lr.Append([]proto.Entry{
		{Index: 1, Term: 2},
		{Index: 2, Term: 2},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	lr.SetState(proto.State{Term: 2})
	adapter := &logReaderAdapter{
		reader:      lr,
		currentTerm: func() uint64 { return 3 },
	}
	lastIndex, term := adapter.NodeState()
	if lastIndex != 2 || term != 3 {
		t.Fatalf("NodeState() = (%d, %d), want (2, 3)", lastIndex, term)
	}
}
