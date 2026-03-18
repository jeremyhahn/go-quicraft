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

// quiesceThresholdMultiplier is the number of election timeouts of inactivity
// required before entering quiesce mode.
const quiesceThresholdMultiplier = 10

// quiesceLivenessMultiplier is the multiplier for the liveness probe interval
// during quiesce. The leader sends a lightweight heartbeat at this frequency.
const quiesceLivenessMultiplier = 10

// quiesceElectionTimeoutMultiplier is the multiplier for the follower election
// timeout during quiesce. Followers use 3x the liveness interval to tolerate
// missed probes from network jitter or GC pauses.
const quiesceElectionTimeoutMultiplier = 3

// quiesceState tracks quiesce-related state for a raft node. It is
// embedded in the raft struct and managed by tick functions.
type quiesceState struct {
	// enabled indicates whether quiesce is configured for this shard.
	enabled bool
	// active indicates whether the node is currently in quiesce mode.
	active bool
	// tick counts ticks of inactivity since the last meaningful activity.
	tick int
	// threshold is the number of ticks required before entering quiesce.
	// Computed as electionTimeout * quiesceThresholdMultiplier.
	threshold int
	// livenessInterval is the interval between liveness probes during
	// quiesce. Computed as electionTimeout * quiesceLivenessMultiplier.
	livenessInterval int
	// livenessTick counts ticks since the last liveness probe.
	livenessTick int
}

// newQuiesceState creates a quiesce state tracker with the given
// configuration. The threshold and liveness intervals are derived from
// the election timeout.
func newQuiesceState(enabled bool, electionTimeout int) quiesceState {
	return quiesceState{
		enabled:          enabled,
		threshold:        electionTimeout * quiesceThresholdMultiplier,
		livenessInterval: electionTimeout * quiesceLivenessMultiplier,
	}
}

// tryEnter checks whether the node should enter quiesce mode.
// Returns true if quiesce was entered on this call.
func (q *quiesceState) tryEnter() bool {
	if !q.enabled || q.active {
		return false
	}
	q.tick++
	if q.tick >= q.threshold {
		q.active = true
		q.livenessTick = 0
		return true
	}
	return false
}

// exit leaves quiesce mode and resets all counters.
func (q *quiesceState) exit() {
	q.active = false
	q.tick = 0
	q.livenessTick = 0
}

// recordActivity resets the inactivity counter. Called when a proposal,
// config change, read index request, or non-heartbeat message is received.
func (q *quiesceState) recordActivity() {
	q.tick = 0
	if q.active {
		q.exit()
	}
}

// tickLiveness advances the liveness probe timer during quiesce.
// Returns true when a liveness probe should be sent.
func (q *quiesceState) tickLiveness() bool {
	if !q.active {
		return false
	}
	q.livenessTick++
	if q.livenessTick >= q.livenessInterval {
		q.livenessTick = 0
		return true
	}
	return false
}

// quiesceElectionTimeout returns the election timeout to use while
// quiesced. This is significantly longer than the normal timeout to
// prevent unnecessary elections from a single missed liveness probe.
func (q *quiesceState) quiesceElectionTimeout() int {
	return q.livenessInterval * quiesceElectionTimeoutMultiplier
}
