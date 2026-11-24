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

// Package electionbudget derives how long an integration test may wait for a Raft election from ElectionRTT and RTTMillisecond.
package electionbudget

import "time"

// Round is the longest one election attempt can run: twice electionRTT ticks, since election timers are randomized over [timeout, 2*timeout).
func Round(electionRTT, rttMillisecond uint64) time.Duration {
	return 2 * time.Duration(electionRTT) * (time.Duration(rttMillisecond) * time.Millisecond)
}

// ColdStartRounds is the number of rounds allowed for a fresh shard's first leader: handshake, PreVote, vote and a split retry, multiplied for host saturation.
const ColdStartRounds = 30

// FailoverRounds is the number of election rounds Failover allows.
const FailoverRounds = 20

// LeaderElection is the budget for a freshly started shard's first leader; see ColdStartRounds.
func LeaderElection(electionRTT, rttMillisecond uint64) time.Duration {
	return ColdStartRounds * Round(electionRTT, rttMillisecond)
}

// Failover is the budget for a shard to elect a replacement leader.
func Failover(electionRTT, rttMillisecond uint64) time.Duration {
	return FailoverRounds * Round(electionRTT, rttMillisecond)
}
