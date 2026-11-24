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

// Fuzz targets over the raft state machine (make fuzz-raft); the harness and seeds in adversarial_test.go run in every go test.

package raft

import "testing"

// FuzzRaftStepSequence steps a sequence of OriginRemote messages and checks every Raft safety invariant after each (raftInvariants.check).
func FuzzRaftStepSequence(f *testing.F) {
	quietLogs(f)
	for _, seed := range adversarialSeeds() {
		f.Add(seed.data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runAdversarialStepSequence(t, data)
	})
}

// FuzzRaftLogRecovery fuzzes newRaftLog recovery and asserts the replica never acknowledges an index it does not hold.
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
