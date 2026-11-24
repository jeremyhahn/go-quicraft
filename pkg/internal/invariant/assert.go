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

// Package invariant provides assertion helpers for programmer errors.
// These are intended for impossible states and violated invariants,
// never for runtime error handling.
package invariant

// Assert panics with a descriptive message if the condition is false.
// Used ONLY for programmer errors / impossible states, never for runtime errors.
//
// Example:
//
//	invariant.Assert(index >= 0, "index must be non-negative")
//
// # Never assert on a value a peer chose
//
// Everything in the consensus engine that calls this runs on the raft step
// worker, which serves messages from remote replicas. Before adding a call,
// answer one question: can the asserted condition be made false by a field a
// PEER controls?
//
// If yes, this is the wrong tool, however impossible the state looks. A panic
// here kills the step worker; the engine recovers and restarts it, but the
// shard stalls while it does, and a peer that can trigger the condition once
// can trigger it on every retry — turning a message into an availability
// primitive. Such a condition must be a check at the point the message is
// handled, returning a typed error and logging at slog.Error, so that the
// MESSAGE dies instead of the worker. handleSnapshot's voting-member gate is
// the worked example: a snapshot naming more than MaxVotingMembers voters used
// to install the whole configuration and then panic rebuildReplicaToBit on
// this replica's next election, permanently.
//
// If no — the condition relates only to state this replica computed itself —
// keep it fatal. Continuing past a broken local invariant in a consensus
// engine corrupts the log or the quorum arithmetic silently, which is strictly
// worse than crashing.
//
// Note that "the data arrived in a peer message" and "the peer controls the
// condition" are different questions. Most of the assertions in the raft log
// sit on peer-driven paths but restate a relation the code establishes a few
// lines earlier (a caller's range guard, a contiguity precheck), and those are
// correctly fatal: no message can falsify them without a local bug first.
func Assert(condition bool, msg string) {
	if !condition {
		panic("invariant violated: " + msg)
	}
}
