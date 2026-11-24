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

package electionbudget

import (
	"testing"
	"time"
)

// TestRoundIsTwiceTheElectionTimeout: one election attempt can take up to two election timeouts, since timers are randomised over [electionTimeout, 2*electionTimeout).
func TestRoundIsTwiceTheElectionTimeout(t *testing.T) {
	// The configuration the integration suites actually run: ElectionRTT=10,
	// RTTMillisecond=50. The package doc quotes this case, so a change that
	// contradicts it is a documentation defect as well as an arithmetic one.
	const (
		electionRTT    = 10
		rttMillisecond = 50
	)
	oneTimeout := time.Duration(electionRTT) * time.Duration(rttMillisecond) * time.Millisecond
	if oneTimeout != 500*time.Millisecond {
		t.Fatalf("one election timeout = %v, want 500ms; the fixture no longer "+
			"matches the suites and the assertions below mean something else", oneTimeout)
	}

	got := Round(electionRTT, rttMillisecond)
	if got != 2*oneTimeout {
		t.Errorf("Round = %v, want %v (TWICE one election timeout, because the "+
			"timer is randomised over [timeout, 2*timeout))", got, 2*oneTimeout)
	}
	if got != time.Second {
		t.Errorf("Round = %v, want 1s — the figure the package doc quotes", got)
	}
}

// TestColdStartIsTheLargerBudget: a cold start waits longer than a failover because it pays for QUIC and mTLS handshakes.
func TestColdStartIsTheLargerBudget(t *testing.T) {
	const electionRTT, rttMillisecond = 10, 50
	cold := LeaderElection(electionRTT, rttMillisecond)
	warm := Failover(electionRTT, rttMillisecond)
	if cold <= warm {
		t.Fatalf("LeaderElection=%v is not larger than Failover=%v; a cold start "+
			"is the harder wait, so its budget must be the larger one", cold, warm)
	}
	if cold != ColdStartRounds*Round(electionRTT, rttMillisecond) {
		t.Errorf("LeaderElection = %v, want %d rounds", cold, ColdStartRounds)
	}
	if warm != FailoverRounds*Round(electionRTT, rttMillisecond) {
		t.Errorf("Failover = %v, want %d rounds", warm, FailoverRounds)
	}
	// The wall-clock values the suites and their logs are written around.
	if cold != 30*time.Second {
		t.Errorf("LeaderElection = %v, want 30s at the suites' configuration", cold)
	}
	if warm != 20*time.Second {
		t.Errorf("Failover = %v, want 20s at the suites' configuration", warm)
	}
}

// TestBudgetsTrackTheConfiguration: every budget moves proportionally with either input.
func TestBudgetsTrackTheConfiguration(t *testing.T) {
	base := Round(10, 50)
	if doubled := Round(20, 50); doubled != 2*base {
		t.Errorf("doubling ElectionRTT gave %v, want %v", doubled, 2*base)
	}
	if doubled := Round(10, 100); doubled != 2*base {
		t.Errorf("doubling RTTMillisecond gave %v, want %v", doubled, 2*base)
	}
	if l := LeaderElection(20, 50); l != 2*LeaderElection(10, 50) {
		t.Errorf("LeaderElection did not track ElectionRTT: %v", l)
	}
	if f := Failover(10, 100); f != 2*Failover(10, 50) {
		t.Errorf("Failover did not track RTTMillisecond: %v", f)
	}
}
