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

// Package electionbudget derives how long an integration test may wait for a
// Raft election, from the shard configuration it is waiting on.
//
// # Why it is one package and not one copy per suite
//
// The derivation, its rationale and its pin test existed in FIVE places: the
// e2e, host, raft and linearizability suites, and contrib/dragonboat's. Each
// was ~15 lines of arithmetic under ~60 lines of reasoning, and the reasoning
// is the part that matters — it records why the budget is twice ElectionRTT and
// not once, why thirty rounds, and what was measured to arrive there. Five
// copies of a number is five places a correction has to land; five copies of
// the argument for it is worse, because four of them go stale silently.
//
// test/proposer is the precedent: its own doc records that four per-suite
// copies of the propose-retry loop were "four places a fix has to land", and
// consolidated them. #96 did the same for the propose budget. This is #133.
//
// # Why it takes numbers and not a Config
//
// The signature is (electionRTT, rttMillisecond) rather than a
// quicraft.Config, and that is deliberate: it is what lets contrib/dragonboat
// share this package. test/proposer could NOT be shared across that module
// boundary (#114) because its Resolver is typed on *quicraft.Host, which
// dragonboat's NodeHost does not expose. A budget is arithmetic over two
// integers and couples to nothing, so the boundary is not in its way.
//
// Each suite keeps a small pin test asserting that its OWN config builders set
// the ElectionRTT the budget was derived from. That check reads suite-local
// builders and cannot move here; the derivation it compares against can, and
// has.
package electionbudget

import "time"

// Round is the longest a SINGLE election attempt can run before the next
// candidate's timer fires and supersedes it.
//
// It is TWICE electionRTT, not once. Raft randomises each replica's election
// timer over [electionTimeout, 2*electionTimeout) —
// randomizedElectionTimeout = electionTimeout + rng.Intn(electionTimeout), see
// pkg/internal/raft — so the upper bound on one attempt is two election
// timeouts' worth of ticks. A budget written against the un-randomised value
// is half the size it appears to be, which is how a "10x headroom" constant
// turns out to be 5x. At ElectionRTT=10 and RTTMillisecond=50: 2*10*50ms = 1s.
func Round(electionRTT, rttMillisecond uint64) time.Duration {
	return 2 * time.Duration(electionRTT) * (time.Duration(rttMillisecond) * time.Millisecond)
}

// ColdStartRounds is how many worst-case election rounds LeaderElection allows
// a shard that was started moments ago to produce its FIRST leader.
//
// FOUR of them are the protocol's own worst case for a cold start, each
// bounded by Round:
//
//   - the QUIC handshake and mTLS exchange between all the hosts, which must
//     complete before the first RequestVote can even be answered;
//   - the PreVote round (SetDefaults turns PreVote on), which precedes the
//     real one;
//   - the vote round itself;
//   - one split-vote retry. On a cold start a split is likely rather than
//     exceptional: every replica arms its election timer within microseconds
//     of the others, which is the one situation the randomised timer has least
//     room to separate.
//
// The remaining factor is host saturation, and it is a MULTIPLIER rather than
// an addend. A Raft tick is a goroutine wakeup; on a box oversubscribed k
// times a 50ms tick lands roughly k times late, and the whole cold start
// stretches with it.
//
// Measured with this budget lifted to 120s so the tail was not truncated: full
// runs of the integration suites under -race on 24 cores at load average
// 120-190 (5-8x oversubscribed, the CI runners that box hosts). The
// distribution is not merely slow, it is heavy-tailed — the common case sits
// near the four rounds above and a small minority runs an order of magnitude
// past it. The distribution is recorded on #111.
//
// Thirty rounds is thirty seconds at the standard test tick. It is BOUNDED on
// purpose, and the bound is the point of the number rather than an
// afterthought: a shard that CANNOT elect — a lost quorum, a broken transport,
// a membership that does not agree — is still reported in half a minute,
// naming the shard, instead of running to the suite timeout. That case is
// verified directly rather than assumed: a three-member shard with only one
// replica running fails this wait in 30.0s.
const ColdStartRounds = 30

// FailoverRounds is how many election rounds Failover allows.
//
// A failover needs one election timeout for the survivors to notice the
// silence and a round or two to elect, over warm connections. Twenty rounds is
// its historical 20s value, re-expressed in rounds so that it tracks
// ElectionRTT and RTTMillisecond alongside LeaderElection instead of drifting
// away from them.
const FailoverRounds = 20

// LeaderElection is the budget for a freshly started shard to produce its
// FIRST leader. See ColdStartRounds for the derivation.
//
// It is a SETUP budget and nothing else. Every one of its call sites waits on
// a shard started moments earlier, before the test's subject is reached, so a
// test that exhausts it has asserted nothing at all — which is what made #111
// worth fixing rather than merely worth widening. A wait for a SUBSEQUENT
// leader, after one has been stopped or partitioned away, is a different
// question and takes Failover.
//
// It is LARGER than Failover even though the names suggest the opposite, and
// that is the correction #111 made. A cold start is the HARDER of the two
// waits: a failover happens over transport that is already established between
// survivors that already know one another, while a cold start pays for the
// handshakes first. The easier case had 20s and the harder one had 5s.
func LeaderElection(electionRTT, rttMillisecond uint64) time.Duration {
	return ColdStartRounds * Round(electionRTT, rttMillisecond)
}

// Failover is the budget for a shard that has already been led to elect a
// REPLACEMENT leader after the incumbent is stopped or partitioned away.
func Failover(electionRTT, rttMillisecond uint64) time.Duration {
	return FailoverRounds * Round(electionRTT, rttMillisecond)
}
