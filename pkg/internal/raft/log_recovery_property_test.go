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

// Property coverage for log recovery (newRaftLog) and for the acknowledgement
// a recovered replica gives to a probe.
//
// The defect this targets has a history in this repository: a recovered
// HardState.Commit ahead of what the log actually holds. getConflictIndex
// treats every index at or below committed as unable to conflict, so a
// replica that comes up claiming a commit index it cannot back with entries
// answers a probe with a NON-rejecting ReplicateResp for entries it does not
// have -- and the leader counts that phantom match toward the commit quorum.
// The clamp in newRaftLog exists for exactly that, and was itself once
// VACUOUS (the boundary was derived from state.Commit, so the clamp compared
// state.Commit against itself).
//
// A clamp that can be vacuous is a clamp that has to be property-tested over
// the whole (entries, snapshot, commit) space rather than at the three points
// someone thought of.

package raft

import (
	"errors"
	"fmt"
	mathrand "math/rand"
	"testing"

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

// reachableFromRecovery reports whether the case describes a LogDB state a
// real recovery can actually hand to newRaftLog.
//
// The distinction matters, and getting it wrong in the other direction is a
// documented trap in this repository: a fixture that manufactures an
// impossible state produces a "finding" that is really a mock defect. Both
// exclusions below are properties of the REAL LogReader, checked against it:
//
//   - A compacted prefix is only ever produced by a snapshot.
//     LogReader.setRangeLocked returns ErrLogGapDetected for an entry range
//     that starts above the reader's marker, so recoverLogReader FAILS
//     outright on an entry range beginning above 1 with no snapshot record;
//     the shard never starts, and newRaftLog never sees that pair.
//
//   - A snapshot index at the uint64 ceiling overflows newInMemory's
//     markerIndex (lastIndex+1), which needs 2^64 log entries to reach.
//
// Everything outside these two regions is asserted strictly. Inside them the
// harness still runs the case -- newRaftLog must be TOTAL, it may not panic
// or wedge on any input -- it just does not hold the stricter frontier
// post-conditions against a state the recovery path rejects first.
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
// newRaft refuses to open at all (#197 C): a snapshot, term or log tail at the
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

	// A snapshot index is COMMITTED by construction -- a snapshot is only
	// ever taken over committed state -- so a recovered replica must come up
	// with committed at or above it, whatever the HardState said. Without
	// this lower bound a replica restoring from a snapshot with a lost or
	// lagging HardState.Commit comes up with processed > committed and trips
	// the assertion on its very first commit.
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

// checkProbeAcknowledgements is the second half of the property: whatever
// newRaftLog decided, the replica must never answer a probe with a
// NON-rejecting ReplicateResp naming an index it cannot produce.
//
// A non-rejecting ReplicateResp is the leader's ONLY evidence that this
// replica holds everything through LogIndex; it is counted directly toward
// the commit quorum. Acknowledging an index the log has nothing for commits
// an entry a majority does not hold.
func checkProbeAcknowledgements(tb testing.TB, c logRecoveryCase, lr *testLogReader, cov *recoveryCoverage) {
	tb.Helper()

	cfg := raftTestCfg(adversarialShardID, adversarialSelfID)
	r, err := newRaft(cfg, lr)
	if c.outsideProtocolRange() {
		// The ceiling case used to be tolerated here as "unreachable, but
		// handleAppend must be total over it", with a note that the
		// markerIndex overflow made the replica ack the ceiling index. That
		// overflow is #197 C, and the answer is no longer tolerance: such a
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
				// ErrCompacted means the entry was captured by a snapshot and
				// is therefore held; ErrUnavailable means it is not there at
				// all, which is the phantom acknowledgement.
				if _, err := r.log.term(m.LogIndex); errors.Is(err, ErrUnavailable) {
					tb.Fatalf("%s: probe(index=%d term=%d) acknowledged index %d the log does not hold",
						c, probe, logTerm, m.LogIndex)
				}
			}
		}
	}
}

// recoveryCoverage counts what the recovery sweep actually exercised. A sweep
// whose every case lands in the unreachable region, or whose probes are all
// REJECTED, asserts nothing about the acknowledgement property while still
// reporting green. See TestLogRecoverySweep.
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
			// A snapshot-only replica whose HardState.Commit was LOST (or
			// lagged) behind the snapshot. Without the ss.Index lower bound
			// this comes up with processed > committed and trips the
			// assertion on the first commit.
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
			// Snapshot index at the uint64 ceiling. Unreachable (it needs
			// 2^64 entries) and excluded from the frontier assertions by
			// reachableFromRecovery. newRaftLog must still be total over it
			// -- newInMemory's markerIndex = lastIndex+1 overflows to 0 here
			// -- and newRaft refuses to open it (outsideProtocolRange), which
			// is what keeps that overflow from ever driving a replica.
			name: "snapshot-index-max",
			c: logRecoveryCase{
				snapshotIndex: ^uint64(0), snapshotTerm: 1,
				commit: ^uint64(0), stateTerm: 1,
			},
		},
		{
			// A log compacted PAST its own commit index with no snapshot
			// record to explain it. recoverLogReader rejects this pair
			// (LogReader.SetRange -> ErrLogGapDetected) before newRaftLog
			// sees it, so it is excluded from the frontier assertions -- but
			// see the report: newRaftLog seeds processed = firstIndex-1 while
			// committed stays at the recovered commit, so it would come up
			// with processed > committed if it ever were reached.
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
