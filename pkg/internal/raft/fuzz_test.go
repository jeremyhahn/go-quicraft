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

//go:build quicraft_fuzz

// Go-native fuzz targets over the raft state machine.
//
// Run:
//
//	make fuzz-raft                              # both targets, FUZZ_TIME each
//	go test -tags=quicraft_fuzz -run=Fuzz -fuzz=FuzzRaftStepSequence \
//	    -fuzztime=120s ./pkg/internal/raft/
//
// The harness, the invariant checker and the seed corpus live in
// adversarial_test.go, which carries NO build tag: the seeds are replayed by
// TestAdversarialStepSeeds and the property sweeps on every ordinary
// `go test ./pkg/...`, so the assertions here are not gated behind the tag.

package raft

import "testing"

// FuzzRaftStepSequence fuzzes a SEQUENCE of decoded proto.Message values
// stepped into a raft instance, asserting every Raft safety invariant after
// each one (see raftInvariants.check):
//
//   - the term never decreases
//   - committed never decreases and never exceeds lastIndex()
//   - processed never exceeds committed and never moves backwards
//   - a replica never holds leadership of a term it voted away
//   - a vote, once cast in a term, never changes within that term
//   - the membership / quorum() input set never changes from a wire message
//   - no outbound message is one the peer's own gates would drop, and no
//     non-rejecting ReplicateResp acknowledges an index the log cannot serve
//
// Every fuzzed message is stamped proto.OriginRemote. That is the attacker's
// actual capability and the whole point of the target: the origin is
// process-local state the wire format cannot express, so a peer cannot claim
// OriginLocal, and the local-injection exemptions in step() must therefore be
// unreachable from the network.
func FuzzRaftStepSequence(f *testing.F) {
	quietLogs(f)
	for _, seed := range adversarialSeeds() {
		f.Add(seed.data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runAdversarialStepSequence(t, data)
	})
}

// FuzzRaftLogRecovery fuzzes newRaftLog's recovery over arbitrary
// (entry range, snapshot index, HardState.Commit) triples and then probes the
// recovered replica, asserting that it never answers a probe with a
// NON-rejecting ReplicateResp for an index it does not hold.
//
// The commit index is taken raw from the fuzzer (the whole uint64 range); the
// remaining coordinates are bounded by randomLogRecoveryCase so a case
// materialises a small log rather than an enormous one.
func FuzzRaftLogRecovery(f *testing.F) {
	quietLogs(f)
	// Seeds: the pathological recoveries this project has actually hit.
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(25))   // empty log, non-zero commit
	f.Add(uint64(1), uint64(5), uint64(1), uint64(0), uint64(0), ^uint64(0))   // commit at the ceiling
	f.Add(uint64(1), uint64(5), uint64(1), uint64(20), uint64(3), uint64(20))  // snapshot above the entries
	f.Add(uint64(10), uint64(5), uint64(2), uint64(3), uint64(1), uint64(12))  // snapshot below the entries
	f.Add(uint64(0), uint64(0), uint64(0), uint64(30), uint64(5), uint64(0))   // snapshot only, lagging state
	f.Add(uint64(1), uint64(5), uint64(1), uint64(5), uint64(1), uint64(5))    // snapshot at the tail
	f.Add(uint64(1), uint64(5), uint64(1), uint64(0), uint64(0), uint64(500))  // commit past the last entry
	f.Add(uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0))    // fresh replica
	f.Add(uint64(100), uint64(3), uint64(7), uint64(0), uint64(0), uint64(50)) // compacted past commit

	f.Fuzz(func(t *testing.T, first, count, entryTerm, snapIndex, snapTerm, commit uint64) {
		runLogRecoveryCase(t, randomLogRecoveryCase(
			first, count, entryTerm, snapIndex, snapTerm, commit))
	})
}
