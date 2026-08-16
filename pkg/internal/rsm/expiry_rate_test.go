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

package rsm

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestMaxExpirePerCheckpoint_ScalesWithTableCapacity pins the bound on how
// long a full session table takes to drain.
//
// A fixed cap of 64 made the recovery time depend on MaxTotalSessions: at the
// defaults (16384 sessions, a 100000-entry window) a full table needed 256
// windows — 25.6 million applied entries — during which every new session
// registration on the shard failed. A crashed client fleet reaches that state,
// and the recovery constant was three orders of magnitude larger than the
// failure.
//
// The cap must also stay a pure function of the REPLICATED policy: a
// node-local input here would expire different sessions on different replicas,
// which is the divergence the whole expiry design exists to avoid.
func TestMaxExpirePerCheckpoint_ScalesWithTableCapacity(t *testing.T) {
	cases := []struct {
		name          string
		maxTotal      uint64
		wantAtLeast   int
		wantDrainOnce int // windows to drain a full table
	}{
		{"default capacity", 16384, 2048, 8},
		{"small capacity keeps the floor", 128, 64, 2},
		{"tiny capacity keeps the floor", 8, 64, 1},
		{"large capacity", 1 << 20, 1 << 17, 8},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &StateMachine{sessionPolicy: proto.SessionPolicy{
				MaxTotalSessions: c.maxTotal,
			}}
			got := s.maxExpirePerCheckpoint()
			if got < c.wantAtLeast {
				t.Fatalf("maxExpirePerCheckpoint() = %d, want >= %d", got, c.wantAtLeast)
			}
			// The property that actually matters: a full table drains in a
			// bounded number of windows regardless of configured capacity.
			windows := (int(c.maxTotal) + got - 1) / got
			if windows > c.wantDrainOnce {
				t.Fatalf("a full table of %d sessions needs %d expiry windows "+
					"to drain (cap %d); want at most %d. Every new session "+
					"registration on the shard fails for that whole period",
					c.maxTotal, windows, got, c.wantDrainOnce)
			}
		})
	}
}

// TestMaxExpirePerCheckpoint_IsDeterministicAcrossReplicas pins that the cap
// comes from replicated state only. Two replicas with identical committed
// policy must compute the identical cap whatever their local configuration
// says, or they expire different sessions at different indexes.
func TestMaxExpirePerCheckpoint_IsDeterministicAcrossReplicas(t *testing.T) {
	replicated := proto.SessionPolicy{MaxTotalSessions: 16384}

	a := &StateMachine{
		sessionPolicy: replicated,
		localPolicy:   proto.SessionPolicy{MaxTotalSessions: 10},
	}
	b := &StateMachine{
		sessionPolicy: replicated,
		localPolicy:   proto.SessionPolicy{MaxTotalSessions: 1 << 20},
	}
	if a.maxExpirePerCheckpoint() != b.maxExpirePerCheckpoint() {
		t.Fatalf("two replicas with the same committed policy computed "+
			"different caps (%d vs %d): the cap is reading node-local "+
			"configuration and will expire different sessions on each",
			a.maxExpirePerCheckpoint(), b.maxExpirePerCheckpoint())
	}
}
