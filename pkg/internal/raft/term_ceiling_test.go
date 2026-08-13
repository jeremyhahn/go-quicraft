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

import (
	"errors"
	"math"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestStep_RefusesTermInTheReservedCeiling covers a remotely reachable term
// REGRESSION.
//
// Term is a plain uint64 on the wire with nothing bounding it, and step()
// adopts a higher term verbatim. A peer naming a term at the top of the range
// therefore drives this replica to it, and becomeCandidate's r.term++ then
// wraps to 0 on the next campaign. A replica whose term went backwards can
// vote a second time in a term it has already voted in, which is an Election
// Safety violation reachable from one message.
func TestStep_RefusesTermInTheReservedCeiling(t *testing.T) {
	for _, msgType := range []uint64{
		proto.Heartbeat, proto.Replicate, proto.RequestVote, proto.TimeoutNow,
	} {
		name := proto.MessageTypeName(msgType)
		t.Run(name, func(t *testing.T) {
			r := newTestRaft(2)
			before := r.term

			err := r.step(proto.Message{
				Type:   msgType,
				From:   2,
				To:     r.replicaID,
				Term:   math.MaxUint64,
				Origin: proto.OriginRemote,
			})
			if !errors.Is(err, ErrTermCeilingExceeded) {
				t.Fatalf("step(term=MaxUint64) error = %v, want ErrTermCeilingExceeded", err)
			}
			if r.term != before {
				t.Fatalf("term = %d after a refused message, want %d unchanged", r.term, before)
			}
		})
	}
}

// TestBecomeCandidate_NeverWrapsTheTerm guards the increment itself, so the
// wrap is impossible even if some future path sets a ceiling term without
// going through step(). It asserts the term does not go BACKWARDS, which is
// the invariant that matters; refusing to advance is the acceptable outcome.
func TestBecomeCandidate_NeverWrapsTheTerm(t *testing.T) {
	r := newTestRaft(2)
	r.term = math.MaxUint64
	r.state = follower

	r.becomeCandidate()

	if r.term < math.MaxUint64 {
		t.Fatalf("term regressed to %d from MaxUint64; a replica whose term went "+
			"backwards can vote twice in one term", r.term)
	}
}
