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

// Property tests for newRaftLog recovery and the acknowledgement a recovered replica gives a probe.

package raft

import (
	"errors"
	"fmt"
	mathrand "math/rand"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// logRecoveryCase is one recovery scenario: what the LogDB holds, what the
// snapshot record says, and what the HardState claims.
type logRecoveryCase struct {
	// firstIndex is the index of the first recovered entry; entryCount == 0
	// means the entry records are gone entirely.
	firstIndex uint64
	entryCount uint64
	entryTerm  uint64
	// snapshotIndex == 0 means no snapshot record survived.
	snapshotIndex uint64
	snapshotTerm  uint64
	// commit and stateTerm are the recovered HardState.
	commit    uint64
	stateTerm uint64
}

// reachableFromRecovery reports whether the case is a state real recovery can produce; unreachable cases are still run but held to fewer post-conditions.
func (c logRecoveryCase) reachableFromRecovery() bool {
	if c.snapshotIndex == 0 && c.entryCount > 0 && c.firstIndex > 1 {
		return false
	}
	if c.snapshotIndex == ^uint64(0) {
		return false
	}
	return true
}

// outsideProtocolRange reports whether the case describes persisted state
// newRaft refuses to open at all: a snapshot, term or log tail at the
// ceiling (persistedCeiling), where +1 wraps. newRaftLog is still total over
// it -- the log constructor does not take the decision -- but no raft instance
// is ever built on it, so the probe property has nothing to run against.
func (c logRecoveryCase) outsideProtocolRange() bool {
	if c.snapshotIndex >= persistedCeiling || c.snapshotTerm >= persistedCeiling {
		return true
	}
	if c.stateTerm >= persistedCeiling {
		return true
	}
	if c.entryCount > 0 && c.firstIndex+c.entryCount-1 >= persistedCeiling {
		return true
	}
	return false
}

func (c logRecoveryCase) String() string {
	return fmt.Sprintf("entries=[%d,%d)@t%d snapshot=%d@t%d commit=%d term=%d reachable=%t",
		c.firstIndex, c.firstIndex+c.entryCount, c.entryTerm,
		c.snapshotIndex, c.snapshotTerm, c.commit, c.stateTerm,
		c.reachableFromRecovery())
}

// buildRecoveryReader materialises the case as a LogReader.
func buildRecoveryReader(c logRecoveryCase) *testLogReader {
	var entries []proto.Entry
	if c.entryCount > 0 && c.firstIndex > 0 {
		entries = makeEntries(c.firstIndex, c.firstIndex+c.entryCount, c.entryTerm)
	}
	lr := newTestLogReader(entries, proto.State{})
	lr.membership = proto.Membership{Addresses: adversarialMembers()}

	// recoverLogReader's exact ordering (pkg/host.go): ApplySnapshot first --
	// which is what moves the reader's range marker and marker term, so the
	// snapshot record and the marker can never diverge -- then SetState, then
	// SetRange. Going through the same API is what keeps this a model of a
	// recoverable disk rather than of a state no LogReader can be in.
	if c.snapshotIndex > 0 {
		_ = lr.ApplySnapshot(proto.Snapshot{
			ShardID:    adversarialShardID,
			Index:      c.snapshotIndex,
			Term:       c.snapshotTerm,
			Membership: lr.membership,
		})
	}
	lr.SetState(proto.State{Term: c.stateTerm, Commit: c.commit})
	if c.entryCount > 0 && c.firstIndex > 0 {
		_ = lr.SetRange(c.firstIndex, c.entryCount)
	}
	return lr
}

// checkRecoveredLog asserts the post-conditions newRaftLog must establish no
// matter how mangled the recovered records are.
func checkRecoveredLog(tb testing.TB, c logRecoveryCase, l *raftLog) {
	tb.Helper()

	last := l.lastIndex()
	if !c.reachableFromRecovery() {
		// Still exercised for totality (no panic, no wedge), but the frontier
		// post-conditions are not asserted against a state recovery rejects.
		return
	}

	// The replica must never claim a commit index it cannot back with log
	// content. This is the assertion the once-vacuous clamp exists to make
	// true, and the precondition for every acknowledgement it will send.
	if l.committed > last {
		tb.Fatalf("%s: recovered committed=%d exceeds lastIndex=%d", c, l.committed, last)
	}

	// The apply frontier can never start ahead of the commit frontier: the
	// very first commitUpdate would trip the invariant assertion and take the
	// commit worker down.
	if l.processed > l.committed {
		tb.Fatalf("%s: recovered processed=%d exceeds committed=%d", c, l.processed, l.committed)
	}

	// A snapshot index is committed by construction, so committed must be at or above it.
	if c.snapshotIndex > 0 && l.committed < c.snapshotIndex {
		tb.Fatalf("%s: recovered committed=%d below snapshot index %d",
			c, l.committed, c.snapshotIndex)
	}

	// firstIndex/lastIndex must describe a range, not an inverted one that
	// underflows the binary searches in firstIndexOfTerm/lastIndexOfTerm.
	if first := l.firstIndex(); first > last+1 {
		tb.Fatalf("%s: recovered firstIndex=%d beyond lastIndex+1=%d", c, first, last+1)
	}
}

// recoveryProbeTerms are the prevLogTerm values a probe is tried with: the
// no-term sentinel, the recovered entry and snapshot terms, and a term the
// replica has never seen.
func recoveryProbeTerms(c logRecoveryCase) []uint64 {
	return []uint64{0, 1, c.entryTerm, c.snapshotTerm, c.entryTerm + 1, 9999}
}

// checkProbeAcknowledgements asserts the replica never acknowledges a probe for an index it cannot produce.
func checkProbeAcknowledgements(tb testing.TB, c logRecoveryCase, lr *testLogReader, cov *recoveryCoverage) {
	tb.Helper()

	cfg := raftTestCfg(adversarialShardID, adversarialSelfID)
	r, err := newRaft(cfg, lr)
	if c.outsideProtocolRange() {
		// The ceiling case is not tolerated: running there would overflow
		// markerIndex and make the replica ack the ceiling index, so such a
		// store fails to open.
		if !errors.Is(err, proto.ErrLogIndexOutOfRange) && !errors.Is(err, proto.ErrTermOutOfRange) {
			tb.Fatalf("%s: newRaft = %v, want the store refused as outside the protocol range", c, err)
		}
		return
	}
	if err != nil {
		tb.Fatalf("%s: newRaft: %v", c, err)
	}

	// Probe points spanning the whole interesting range: below, at, and above
	// both the commit frontier and the log tail, plus the uint64 ceiling.
	last := r.log.lastIndex()
	committed := r.log.committed
	probes := []uint64{0, 1, committed, committed + 1, last, last + 1, c.snapshotIndex, c.firstIndex}
	if committed > 0 {
		probes = append(probes, committed-1)
	}
	probes = append(probes, ^uint64(0))

	probeTerm := c.stateTerm + 7
	for _, probe := range probes {
		for _, logTerm := range recoveryProbeTerms(c) {
			r.drainMessages()
			_ = r.step(proto.Message{
				Type:     proto.Replicate,
				From:     2,
				To:       r.replicaID,
				ShardID:  r.shardID,
				Term:     probeTerm,
				LogIndex: probe,
				LogTerm:  logTerm,
				Origin:   proto.OriginRemote,
			})
			if !c.reachableFromRecovery() {
				// The probe is still driven -- handleAppend must be total over
				// any recovered state -- but the acknowledgement property is
				// not asserted against a state recovery rejects first. At
				// ss.Index == MaxUint64 the markerIndex overflow makes the
				// replica ack the ceiling index; see reachableFromRecovery.
				continue
			}
			for i := range r.msgs {
				m := &r.msgs[i]
				if m.Type != proto.ReplicateResp || m.Reject || m.LogIndex == 0 {
					continue
				}
				cov.countAck()
				if m.LogIndex > r.log.lastIndex() {
					tb.Fatalf("%s: probe(index=%d term=%d) acknowledged index %d beyond lastIndex %d",
						c, probe, logTerm, m.LogIndex, r.log.lastIndex())
				}
				// logdb.ErrCompacted means the entry was captured by a snapshot and
				// is therefore held; logdb.ErrUnavailable means it is not there at
				// all, which is the phantom acknowledgement.
				if _, err := r.log.term(m.LogIndex); errors.Is(err, logdb.ErrUnavailable) {
					tb.Fatalf("%s: probe(index=%d term=%d) acknowledged index %d the log does not hold",
						c, probe, logTerm, m.LogIndex)
				}
			}
		}
	}
}

// recoveryCoverage counts what the sweep exercised, so an all-unreachable or all-rejected sweep cannot pass silently.
type recoveryCoverage struct {
	cases            int
	strictlyAsserted int
	clamped          int
	acknowledgements int
}

func (c *recoveryCoverage) countAck() {
	if c != nil {
		c.acknowledgements++
	}
}

// runLogRecoveryCase is the body shared by the table test, the sweep and the
// fuzz target.
func runLogRecoveryCase(tb testing.TB, c logRecoveryCase) {
	tb.Helper()
	runLogRecoveryCaseCov(tb, c, nil)
}

// runLogRecoveryCaseCov is runLogRecoveryCase with optional accounting.
func runLogRecoveryCaseCov(tb testing.TB, c logRecoveryCase, cov *recoveryCoverage) {
	tb.Helper()
	l := newRaftLog(buildRecoveryReader(c))
	checkRecoveredLog(tb, c, l)
	if cov != nil {
		cov.cases++
		if c.reachableFromRecovery() {
			cov.strictlyAsserted++
		}
		if l.committed < c.commit {
			cov.clamped++
		}
	}
	checkProbeAcknowledgements(tb, c, buildRecoveryReader(c), cov)
}

// TestLogRecoveryPathologicalCases pins the specific recovery states this
// project has actually hit, by name, so a regression reports which one.
func TestLogRecoveryPathologicalCases(t *testing.T) {
	quietLogs(t)
	cases := []struct {
		name string
		c    logRecoveryCase
	}{
		{
			// Total loss: the state record survived, every entry record and
			// the snapshot record did not. The replica must come up
			// indistinguishable from a fresh one.
			name: "empty-log-nonzero-commit",
			c:    logRecoveryCase{commit: 25, stateTerm: 4},
		},
		{
			// Same, with a commit index at the uint64 ceiling.
			name: "empty-log-max-commit",
			c:    logRecoveryCase{commit: ^uint64(0), stateTerm: 4},
		},
		{
			// Snapshot record above the surviving entry range: the entries
			// are all captured by the snapshot, so the reader's range starts
			// above them and the replica is snapshot-only.
			name: "snapshot-above-entry-range",
			c: logRecoveryCase{
				firstIndex: 1, entryCount: 5, entryTerm: 1,
				snapshotIndex: 20, snapshotTerm: 3, commit: 20, stateTerm: 3,
			},
		},
		{
			// Snapshot below the entry range: an old snapshot record with a
			// live log on top of it. committed must not be dragged down.
			name: "snapshot-below-entry-range",
			c: logRecoveryCase{
				firstIndex: 10, entryCount: 5, entryTerm: 2,
				snapshotIndex: 3, snapshotTerm: 1, commit: 12, stateTerm: 2,
			},
		},
		{
			// The snapshot-only replica: entries all compacted (or lost)
			// while the snapshot record survived. It legitimately holds
			// state through ss.Index and must not be reset to a fresh one.
			name: "snapshot-only-no-entries",
			c: logRecoveryCase{
				snapshotIndex: 30, snapshotTerm: 5, commit: 30, stateTerm: 5,
			},
		},
		{
			// A snapshot-only replica whose HardState.Commit was lost or lags behind the snapshot.
			name: "snapshot-only-lagging-hardstate",
			c: logRecoveryCase{
				snapshotIndex: 30, snapshotTerm: 5, commit: 0, stateTerm: 5,
			},
		},
		{
			// HardState.Commit ahead of the last recovered entry: a torn
			// batch that kept the state record and lost the entries behind
			// it. This is the phantom-match precondition.
			name: "commit-above-last-entry",
			c: logRecoveryCase{
				firstIndex: 1, entryCount: 5, entryTerm: 1,
				commit: 500, stateTerm: 2,
			},
		},
		{
			// Everything zero: the genuinely fresh replica.
			name: "all-zero",
			c:    logRecoveryCase{},
		},
		{
			// Snapshot exactly at the last entry.
			name: "snapshot-at-entry-tail",
			c: logRecoveryCase{
				firstIndex: 1, entryCount: 5, entryTerm: 1,
				snapshotIndex: 5, snapshotTerm: 1, commit: 5, stateTerm: 1,
			},
		},
		{
			// Snapshot index at the uint64 ceiling: unreachable, but newRaftLog must still be total and newRaft refuses it.
			name: "snapshot-index-max",
			c: logRecoveryCase{
				snapshotIndex: ^uint64(0), snapshotTerm: 1,
				commit: ^uint64(0), stateTerm: 1,
			},
		},
		{
			// A log compacted past its commit index with no snapshot record; recoverLogReader rejects it first.
			name: "gapped-start-no-snapshot",
			c: logRecoveryCase{
				firstIndex: 100, entryCount: 3, entryTerm: 7,
				commit: 50, stateTerm: 7,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runLogRecoveryCase(t, tc.c)
		})
	}
}

// logRecoveryFirstIndexes, logRecoveryCounts, logRecoverySnapshots and
// logRecoveryCommits are the coordinate sets the exhaustive sweep is taken
// over. Every combination of zero, below-range, in-range, at-boundary and
// above-range is present in each.
var (
	logRecoveryFirstIndexes = []uint64{0, 1, 5}
	logRecoveryCounts       = []uint64{0, 1, 5}
	logRecoverySnapshots    = []uint64{0, 1, 4, 5, 9, 100}
	logRecoveryCommits      = []uint64{0, 1, 4, 5, 9, 100, ^uint64(0)}
)

// TestLogRecoverySweep is the exhaustive property test over the full
// (entry range, snapshot index, HardState.Commit) product, in both the
// coherent and the torn recovery orderings.
func TestLogRecoverySweep(t *testing.T) {
	quietLogs(t)
	cov := &recoveryCoverage{}
	var n int
	for _, first := range logRecoveryFirstIndexes {
		for _, count := range logRecoveryCounts {
			for _, snap := range logRecoverySnapshots {
				for _, commit := range logRecoveryCommits {
					runLogRecoveryCaseCov(t, logRecoveryCase{
						firstIndex:    first,
						entryCount:    count,
						entryTerm:     2,
						snapshotIndex: snap,
						snapshotTerm:  3,
						commit:        commit,
						stateTerm:     4,
					}, cov)
					n++
				}
			}
		}
	}
	if n < 250 {
		t.Fatalf("recovery sweep ran only %d cases; the coordinate sets have collapsed", n)
	}
	// Anti-vacuity: the sweep must actually reach the states its assertions
	// are about. Without these, a coordinate-set edit that pushed every case
	// into the unreachable region -- or a handleAppend change that made every
	// probe reject -- would leave the sweep green and empty.
	if cov.strictlyAsserted < n/2 {
		t.Errorf("only %d of %d recovery cases were strictly asserted; the sweep has drifted into the unreachable region",
			cov.strictlyAsserted, n)
	}
	if cov.clamped < 20 {
		t.Errorf("only %d recovery cases had their commit index clamped; the clamp under test is barely exercised",
			cov.clamped)
	}
	if cov.acknowledgements < 100 {
		t.Errorf("probes produced only %d non-rejecting acknowledgements; the acknowledgement property is vacuous",
			cov.acknowledgements)
	}
	t.Logf("recovery sweep: %d cases, %d strictly asserted, %d commit clamps, %d acknowledgements",
		n, cov.strictlyAsserted, cov.clamped, cov.acknowledgements)
}

// randomLogRecoveryCase derives a case from raw uint64s. It is shared by the
// deterministic sweep and the fuzz target so both explore the same space.
func randomLogRecoveryCase(a, b, c, d, e, f uint64) logRecoveryCase {
	// Bound the entry count: the case is about index arithmetic, not about
	// materialising a large log.
	return logRecoveryCase{
		firstIndex:    a % 64,
		entryCount:    b % 9,
		entryTerm:     c % 8,
		snapshotIndex: d % 128,
		snapshotTerm:  e % 8,
		commit:        f,
		stateTerm:     (c % 8) + 1,
	}
}

// TestLogRecoveryRandomSweep is the randomised half of the property test: a
// fixed-seed sweep over triples the hand-written coordinate sets do not
// contain, including commit indices drawn from the full uint64 range.
func TestLogRecoveryRandomSweep(t *testing.T) {
	quietLogs(t)
	rng := mathrand.New(mathrand.NewSource(0x10C_C0FFEE))
	for i := 0; i < 2000; i++ {
		c := randomLogRecoveryCase(
			rng.Uint64(), rng.Uint64(), rng.Uint64(),
			rng.Uint64(), rng.Uint64(), rng.Uint64(),
		)
		runLogRecoveryCase(t, c)
	}
}
