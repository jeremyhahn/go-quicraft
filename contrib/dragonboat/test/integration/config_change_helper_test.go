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

//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// These tests pin the decision core of driveConfigChange without a cluster:
// which errors count as a submission, which are retried, and above all that
// "already exists" / "not found" / "stale ConfigChangeID" become a success
// ONLY behind a submitted attempt AND a linearizable read showing the exact
// change requested. Each rule has a test that fails when it is removed; the
// PR that introduced them lists the mutations.

// scriptedAttempts returns each error in errs in turn, then the last one
// forever, and counts the calls.
type scriptedAttempts struct {
	errs  []error
	calls int
}

func (s *scriptedAttempts) attempt(context.Context) error {
	i := s.calls
	s.calls++
	if i < len(s.errs) {
		return s.errs[i]
	}
	return s.errs[len(s.errs)-1]
}

// scriptedReads returns each membership (or error) in turn, then the last
// one forever, and counts the calls.
type scriptedReads struct {
	results []scriptedRead
	calls   int
}

type scriptedRead struct {
	m   *dragonboat.Membership
	err error
}

func (s *scriptedReads) read(context.Context) (*dragonboat.Membership, error) {
	i := s.calls
	s.calls++
	if i >= len(s.results) {
		i = len(s.results) - 1
	}
	return s.results[i].m, s.results[i].err
}

func membershipOf(nodes, nonVotings, witnesses map[uint64]dragonboat.Target) *dragonboat.Membership {
	m := &dragonboat.Membership{
		ConfigChangeID: 7,
		Nodes:          map[uint64]dragonboat.Target{1: "127.10.0.5:1", 2: "127.10.0.5:2", 3: "127.10.0.5:3"},
		NonVotings:     map[uint64]dragonboat.Target{},
		Witnesses:      map[uint64]dragonboat.Target{},
		Removed:        map[uint64]struct{}{},
	}
	for id, addr := range nodes {
		m.Nodes[id] = addr
	}
	for id, addr := range nonVotings {
		m.NonVotings[id] = addr
	}
	for id, addr := range witnesses {
		m.Witnesses[id] = addr
	}
	return m
}

func readsOf(ms ...*dragonboat.Membership) *scriptedReads {
	s := &scriptedReads{}
	for _, m := range ms {
		s.results = append(s.results, scriptedRead{m: m})
	}
	return s
}

// The two refusals the idempotence rule is about, as they reach a test
// through this adapter: quicraft's sentinels are unmapped, so translateError
// wraps them in AdapterError.
var (
	errAlreadyExistsHere = &dragonboat.AdapterError{Err: quicraft.ErrNodeAlreadyExists}
	errNotFoundHere      = &dragonboat.AdapterError{Err: quicraft.ErrNodeNotFound}
)

const (
	helperTestAddr  dragonboat.Target = "127.10.0.1:59999"
	helperOtherAddr dragonboat.Target = "127.10.0.2:59999"
)

func addTen() configChange {
	return configChange{kind: addReplica, shardID: 1, replicaID: 10, addr: helperTestAddr, identity: dbIdentityFor(10), configChangeID: 7}
}

func deleteTwo() configChange {
	return configChange{kind: deleteReplica, shardID: 1, replicaID: 2, configChangeID: 7}
}

// helperBudget is generous for a loop that never sleeps past leaderRetryInterval
// between attempts and short enough that the exhaustion tests stay quick.
const helperBudget = 3 * time.Second

func requireFailedWith(t *testing.T, err, want error) {
	t.Helper()
	var failed *configChangeFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("got %v (%T), want *configChangeFailedError", err, err)
	}
	if !errors.Is(err, want) {
		t.Fatalf("failed with %v, want it to wrap %v", err, want)
	}
}

func requireConflict(t *testing.T, err error) *membershipConflictError {
	t.Helper()
	var conflict *membershipConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("got %v (%T), want a *membershipConflictError", err, err)
	}
	return conflict
}

// The "after submission lands" cases script the membership the way the
// cluster produces it: the read taken right after the submitted-but-refused
// attempt does not show the change yet, and the read taken after the
// "already exists" / "not found" / "stale" refusal does. The refusal itself
// is therefore what the loop has to accept, not the first observation.

// postDeliverySubmitted is what the adapter produces when leadership moves
// AFTER the change reached Raft: the mapped sentinel with quicraft's submission
// bit intact, exactly as translateError now returns it (#156).
//
// These fixtures used a BARE dragonboat.ErrShardNotReady to mean "submitted",
// which worked only because translateError folded the post-delivery and
// pre-flight cases into that one sentinel and the helper had to arm the bit for
// both. With the fold gone a bare ErrShardNotReady means what it says -- the
// replica refused it, nothing was submitted -- so a fixture that means
// "submitted" has to say so.
func postDeliverySubmitted() error {
	return &quicraft.MaybeAppliedError{Err: dragonboat.ErrShardNotReady}
}

func TestIntegration_DB_ConfigChangeRetry_AlreadyExistsAfterSubmissionLands(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{postDeliverySubmitted(), errAlreadyExistsHere}}
	reads := readsOf(
		membershipOf(nil, nil, nil),
		membershipOf(map[uint64]dragonboat.Target{10: helperTestAddr}, nil, nil),
	)
	if err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read); err != nil {
		t.Fatalf("a submitted add refused as already-existing, with the voter at the requested address, must land: %v", err)
	}
	if attempts.calls != 2 || reads.calls != 2 {
		t.Fatalf("attempts=%d reads=%d, want 2 and 2: the refusal is accepted on the read that confirms it", attempts.calls, reads.calls)
	}
}

func TestIntegration_DB_ConfigChangeRetry_TimeoutIsASubmission(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{dragonboat.ErrTimeout, errAlreadyExistsHere}}
	reads := readsOf(
		membershipOf(nil, nil, nil),
		membershipOf(map[uint64]dragonboat.Target{10: helperTestAddr}, nil, nil),
	)
	if err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read); err != nil {
		t.Fatalf("an ErrTimeout is produced only after delivery, so the later already-exists must land: %v", err)
	}
	if attempts.calls != 2 || reads.calls != 2 {
		t.Fatalf("attempts=%d reads=%d, want 2 and 2", attempts.calls, reads.calls)
	}
}

func TestIntegration_DB_ConfigChangeRetry_AlreadyExistsAtDifferentAddressStillFails(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{postDeliverySubmitted(), errAlreadyExistsHere}}
	reads := readsOf(membershipOf(map[uint64]dragonboat.Target{10: helperOtherAddr}, nil, nil))
	err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read)
	if err == nil {
		t.Fatal("replica 10 is a voter at a DIFFERENT address; accepting the already-exists turns a replica-ID collision into a green test")
	}
	conflict := requireConflict(t, err)
	if conflict.addr != helperOtherAddr || conflict.role != roleVoter {
		t.Fatalf("conflict names %s at %q, want %s at %q", membershipRoleNames[conflict.role], conflict.addr, membershipRoleNames[roleVoter], helperOtherAddr)
	}
}

func TestIntegration_DB_ConfigChangeRetry_AlreadyExistsInAnotherRoleStillFails(t *testing.T) {
	cases := map[string]struct {
		cc   configChange
		m    *dragonboat.Membership
		role membershipRole
	}{
		"voter requested, non-voting present": {
			cc:   addTen(),
			m:    membershipOf(nil, map[uint64]dragonboat.Target{10: helperTestAddr}, nil),
			role: roleNonVoting,
		},
		"voter requested, witness present": {
			cc:   addTen(),
			m:    membershipOf(nil, nil, map[uint64]dragonboat.Target{10: helperTestAddr}),
			role: roleWitness,
		},
		"non-voting requested, voter present": {
			cc:   configChange{kind: addNonVoting, shardID: 1, replicaID: 10, addr: helperTestAddr, identity: dbIdentityFor(10), configChangeID: 7},
			m:    membershipOf(map[uint64]dragonboat.Target{10: helperTestAddr}, nil, nil),
			role: roleVoter,
		},
		"witness requested, non-voting present": {
			cc:   configChange{kind: addWitness, shardID: 1, replicaID: 10, addr: helperTestAddr, identity: dbIdentityFor(10), configChangeID: 7},
			m:    membershipOf(nil, map[uint64]dragonboat.Target{10: helperTestAddr}, nil),
			role: roleNonVoting,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			attempts := &scriptedAttempts{errs: []error{postDeliverySubmitted(), errAlreadyExistsHere}}
			err := driveConfigChange(tc.cc, helperBudget, attempts.attempt, readsOf(tc.m).read)
			if err == nil {
				t.Fatal("the replica ID is held in a different ROLE at the same address; that is a conflict, not a landed retry")
			}
			if conflict := requireConflict(t, err); conflict.role != tc.role {
				t.Fatalf("conflict names %s, want %s", membershipRoleNames[conflict.role], membershipRoleNames[tc.role])
			}
		})
	}
}

func TestIntegration_DB_ConfigChangeRetry_AlreadyExistsWithoutSubmissionStillFails(t *testing.T) {
	// Every refusal here is retryable and none is a submission: nothing this
	// loop did could have put replica 10 in the membership, so an "already
	// exists" that follows it -- even at the exact requested address -- is
	// somebody else's change, or an engine that applied what it refused.
	preflight := map[string]error{
		"no leader observed":    errNoLeaderObserved,
		"system busy":           dragonboat.ErrSystemBusy,
		"pending config change": dragonboat.ErrRejected,
		// The real sentinel, not a look-alike built from the same text. This
		// carried errors.New(...) while the engine's error was internal and
		// unmatchable; matching it by sentinel (#146) means a fixture that
		// only LOOKED like a transfer refusal would no longer be retried, so
		// the fixture has to be the thing it models.
		"transfer in progress":  &dragonboat.AdapterError{Err: quicraft.ErrTransferInProgress},
		"refused on first call": errAlreadyExistsHere,
		// The BARE sentinel, with no submission bit. Before #156 this was
		// indistinguishable from the post-delivery leadership change --
		// translateError folded both into ErrShardNotReady -- so the helper had
		// to arm the submission bit for it and this case could not be
		// expressed. It is the one an engine that applied a change it reported
		// as refused PRE-FLIGHT would slip through.
		"pre-flight not-leader": dragonboat.ErrShardNotReady,
	}
	landed := membershipOf(map[uint64]dragonboat.Target{10: helperTestAddr}, nil, nil)
	for name, first := range preflight {
		t.Run(name, func(t *testing.T) {
			attempts := &scriptedAttempts{errs: []error{first, errAlreadyExistsHere}}
			reads := readsOf(landed)
			err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read)
			if err == nil {
				t.Fatal("already-exists with NO submitted attempt behind it must not be reported as a landed change")
			}
			requireFailedWith(t, err, quicraft.ErrNodeAlreadyExists)
			if reads.calls != 0 {
				t.Fatalf("the membership was read %d time(s) before any attempt was submitted; the confirmation must be gated on submission", reads.calls)
			}
		})
	}
}

func TestIntegration_DB_ConfigChangeRetry_DeleteNotFoundAfterSubmissionLands(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{postDeliverySubmitted(), errNotFoundHere}}
	gone := membershipOf(nil, nil, nil)
	delete(gone.Nodes, 2)
	reads := readsOf(membershipOf(nil, nil, nil), gone) // still present, then gone
	if err := driveConfigChange(deleteTwo(), helperBudget, attempts.attempt, reads.read); err != nil {
		t.Fatalf("a submitted delete refused as not-found, with the replica holding no role, must land: %v", err)
	}
	if attempts.calls != 2 || reads.calls != 2 {
		t.Fatalf("attempts=%d reads=%d, want 2 and 2", attempts.calls, reads.calls)
	}
}

func TestIntegration_DB_ConfigChangeRetry_DeleteNotFoundWithoutSubmissionStillFails(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{dragonboat.ErrRejected, errNotFoundHere}}
	gone := membershipOf(nil, nil, nil)
	delete(gone.Nodes, 2)
	reads := readsOf(gone)
	err := driveConfigChange(deleteTwo(), helperBudget, attempts.attempt, reads.read)
	if err == nil {
		t.Fatal("not-found with NO submitted attempt behind it is a delete of a replica that was never there, not a landed one")
	}
	requireFailedWith(t, err, quicraft.ErrNodeNotFound)
}

func TestIntegration_DB_ConfigChangeRetry_DeleteStillPresentFails(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{postDeliverySubmitted(), errNotFoundHere}}
	reads := readsOf(membershipOf(nil, nil, nil)) // replica 2 still a voter
	err := driveConfigChange(deleteTwo(), helperBudget, attempts.attempt, reads.read)
	if err == nil {
		t.Fatal("not-found while the read still shows replica 2 as a voter: the change did not land and must not be reported as landed")
	}
	requireFailedWith(t, err, quicraft.ErrNodeNotFound)
}

func TestIntegration_DB_ConfigChangeRetry_StaleIDAfterSubmissionLands(t *testing.T) {
	// The caller's ConfigChangeID describes the configuration BEFORE its own
	// change; once that change commits under a new leader, the retry is
	// refused as stale. Only the read can say the change is the loop's own.
	attempts := &scriptedAttempts{errs: []error{postDeliverySubmitted(), dragonboat.ErrStaleConfigChange}}
	reads := readsOf(
		membershipOf(nil, nil, nil),
		membershipOf(map[uint64]dragonboat.Target{10: helperTestAddr}, nil, nil),
	)
	if err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read); err != nil {
		t.Fatalf("a submitted add refused as stale, with the voter at the requested address, must land: %v", err)
	}
	if attempts.calls != 2 || reads.calls != 2 {
		t.Fatalf("attempts=%d reads=%d, want 2 and 2", attempts.calls, reads.calls)
	}
}

func TestIntegration_DB_ConfigChangeRetry_StaleIDWithoutSubmissionFails(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{dragonboat.ErrStaleConfigChange}}
	reads := readsOf(membershipOf(map[uint64]dragonboat.Target{10: helperTestAddr}, nil, nil))
	err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read)
	if err == nil {
		t.Fatal("a stale ConfigChangeID on the FIRST attempt is the caller's own error and must be reported")
	}
	requireFailedWith(t, err, dragonboat.ErrStaleConfigChange)
	if attempts.calls != 1 {
		t.Fatalf("attempts=%d, want 1: a stale ID is not retryable", attempts.calls)
	}
}

func TestIntegration_DB_ConfigChangeRetry_StaleIDNotLandedFails(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{postDeliverySubmitted(), dragonboat.ErrStaleConfigChange}}
	reads := readsOf(membershipOf(nil, nil, nil)) // replica 10 absent
	err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read)
	if err == nil {
		t.Fatal("stale after a submission, but the read does not show replica 10: some OTHER change moved the ID, and this one did not land")
	}
	requireFailedWith(t, err, dragonboat.ErrStaleConfigChange)
}

func TestIntegration_DB_ConfigChangeRetry_ReadFailureIsNotEvidence(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{postDeliverySubmitted(), errAlreadyExistsHere}}
	reads := &scriptedReads{results: []scriptedRead{
		{err: errNoLeaderObserved},
		{err: dragonboat.ErrShardNotReady},
		{m: membershipOf(map[uint64]dragonboat.Target{10: helperTestAddr}, nil, nil)},
	}}
	if err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read); err != nil {
		t.Fatalf("a membership read that fails transiently answers nothing; the refusal it was checking must be re-checked, not made final: %v", err)
	}
	if reads.calls != 3 {
		t.Fatalf("reads=%d, want 3: two failed reads and the one that confirmed", reads.calls)
	}
}

func TestIntegration_DB_ConfigChangeRetry_NonRetryableIsFinal(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{dragonboat.ErrInvalidAddress}}
	reads := readsOf(membershipOf(nil, nil, nil))
	err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read)
	requireFailedWith(t, err, dragonboat.ErrInvalidAddress)
	if attempts.calls != 1 {
		t.Fatalf("attempts=%d, want 1", attempts.calls)
	}
	if reads.calls != 0 {
		t.Fatalf("reads=%d, want 0: nothing was submitted", reads.calls)
	}
}

func TestIntegration_DB_ConfigChangeRetry_NoLeaderExhaustsBudget(t *testing.T) {
	attempts := &scriptedAttempts{errs: []error{errNoLeaderObserved}}
	reads := readsOf(membershipOf(nil, nil, nil))
	started := time.Now()
	err := driveConfigChange(addTen(), helperBudget, attempts.attempt, reads.read)
	elapsed := time.Since(started)
	var exhausted *configChangeExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("got %v (%T), want *configChangeExhaustedError", err, err)
	}
	if !errors.Is(err, errNoLeaderObserved) {
		t.Fatalf("exhaustion reports %v, want it to wrap the last refusal, errNoLeaderObserved", err)
	}
	if elapsed > helperBudget+time.Second {
		t.Fatalf("gave up after %v, past the %v budget", elapsed, helperBudget)
	}
	if attempts.calls < 2 {
		t.Fatalf("attempts=%d: a leaderless moment must be retried, not reported on first sight", attempts.calls)
	}
	if reads.calls != 0 {
		t.Fatalf("reads=%d, want 0: a leaderless moment is not a submission", reads.calls)
	}
}

// countingResolver is a leaderResolver that never finds a leader and counts
// how often it was asked.
type countingResolver struct{ calls int }

func (r *countingResolver) resolve(uint64) (*dragonboat.NodeHost, int) {
	r.calls++
	return nil, -1
}

// TestIntegration_DB_ConfigChangeRetry_LeaderIsResolvedOnEveryAttempt pins
// the property the whole class is about: the leader is named at the point of
// EACH attempt, never sampled once before the loop. A hoisted resolve is
// asked exactly once; the loop below asks on every attempt for a whole
// budget.
func TestIntegration_DB_ConfigChangeRetry_LeaderIsResolvedOnEveryAttempt(t *testing.T) {
	resolver := &countingResolver{}
	const budget = 500 * time.Millisecond
	err := configChangeVia(resolver.resolve, addTen(), budget)
	var exhausted *configChangeExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("got %v (%T), want *configChangeExhaustedError", err, err)
	}
	if resolver.calls < 2 {
		t.Fatalf("the leader was resolved %d time(s) across a %v budget; it must be resolved on every attempt, not once before the loop", resolver.calls, budget)
	}
	t.Logf("leader resolved %d times across %v", resolver.calls, budget)
}

// TestIntegration_DB_ConfigChangeToLeader_NoLeaderExhaustsBudget runs the
// cluster-facing form against a shard that CANNOT elect -- a three-member
// membership with one replica running -- so the resolver never finds a leader.
// It pins that the budget is a bound rather than a hope, against the real
// resolver and real hosts.
func TestIntegration_DB_ConfigChangeToLeader_NoLeaderExhaustsBudget(t *testing.T) {
	cluster := newDBTestCluster(t, 3)
	defer cluster.close()

	members := make(map[uint64]dragonboat.Target, len(cluster.hosts))
	for i, addr := range cluster.addrs {
		members[uint64(i+1)] = addr
	}
	createFn, _ := dbKVCreateFunc()
	if err := cluster.hosts[0].StartReplica(members, false, createFn, cluster.shardConfig(1, 1)); err != nil {
		t.Fatalf("StartReplica on the lone replica failed: %v", err)
	}

	budget := 2 * electionRound
	started := time.Now()
	err := cluster.tryConfigChangeToLeader(addTen(), budget)
	elapsed := time.Since(started)
	var exhausted *configChangeExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("a shard with no quorum must exhaust the budget, got %v (%T)", err, err)
	}
	if !errors.Is(err, errNoLeaderObserved) {
		t.Fatalf("exhaustion reports %v, want errNoLeaderObserved: no attempt should ever have been sent", err)
	}
	if elapsed > budget+electionRound {
		t.Fatalf("gave up after %v, past the %v budget", elapsed, budget)
	}
	t.Logf("no leader: exhausted after %v against a %v budget: %v", elapsed.Round(time.Millisecond), budget, err)
}

// TestIntegration_DB_BudgetsDescribeTheClusterTheyBudgetFor pins the
// derivation beside the budgets to the configuration this suite actually
// starts.
//
// The constants are only worth having if the config builders keep reading
// them. If a future change gives dbTestShardConfig an explicit ElectionRTT, or
// hands dbTestNodeHostConfig a different RTTMillisecond, electionRound
// silently starts describing a cluster that no longer exists and every budget
// built on it is wrong by that factor -- exactly the drift the derivation
// replaced a literal to prevent. This is the only place that can notice.
func TestIntegration_DB_BudgetsDescribeTheClusterTheyBudgetFor(t *testing.T) {
	pki := dbTestPKIFor(t)
	if got := dbTestNodeHostConfig(t, "127.10.0.5:1", pki.leaf(t)).RTTMillisecond; got != testRTTMillisecond {
		t.Fatalf("dbTestNodeHostConfig sets RTTMillisecond=%d but the budgets are derived "+
			"from testRTTMillisecond=%d; one Raft tick is not what electionRound (%v) "+
			"thinks it is", got, uint64(testRTTMillisecond), electionRound)
	}
	shardConfigs := map[string]uint64{
		"dbTestShardConfig":             dbTestShardConfig(1, 1).ElectionRTT,
		"dbTestShardConfigWithSnapshot": dbTestShardConfigWithSnapshot(1, 1, 10, 5).ElectionRTT,
	}
	for name, got := range shardConfigs {
		if got != testElectionRTT {
			t.Fatalf("%s sets ElectionRTT=%d but the budgets are derived from "+
				"testElectionRTT=%d; electionRound (%v) and every budget built on it "+
				"are wrong by a factor of %.2f", name, got, testElectionRTT,
				electionRound, float64(got)/float64(testElectionRTT))
		}
	}
	if electionRound != 2*testElectionRTT*raftTick {
		t.Fatalf("electionRound=%v is not 2*ElectionRTT*tick", electionRound)
	}
	derived := map[string]struct{ got, want time.Duration }{
		"leaderElectionTimeout": {leaderElectionTimeout, coldStartElectionRounds * electionRound},
		"failoverTimeout":       {failoverTimeout, 20 * electionRound},
		"leaderTransferRound":   {leaderTransferRound, leaderTransferRounds * electionRound},
		"leaderTransferTimeout": {leaderTransferTimeout, failoverTimeout + leaderTransferRound},
		"membershipTimeout":     {membershipTimeout, failoverTimeout + defaultTimeout},
		"catchUpTimeout":        {catchUpTimeout, failoverTimeout + defaultTimeout},
	}
	for name, d := range derived {
		if d.got != d.want {
			t.Fatalf("%s=%v is no longer %v; a budget that stops tracking the shard config is a literal again", name, d.got, d.want)
		}
	}
	t.Logf("tick=%v electionRound=%v leaderElectionTimeout=%v (%d rounds) failoverTimeout=%v "+
		"leaderTransferRound=%v (%d rounds) leaderTransferTimeout=%v membershipTimeout=%v catchUpTimeout=%v",
		raftTick, electionRound, leaderElectionTimeout, coldStartElectionRounds, failoverTimeout,
		leaderTransferRound, leaderTransferRounds, leaderTransferTimeout, membershipTimeout, catchUpTimeout)
}
