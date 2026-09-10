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

package dragonboat_perf

import (
	"strings"
	"testing"
)

// TestProposeFloorVerdict pins #181 at both ends, because the defect had two
// ends and fixing one of them alone reintroduces the other.
//
// Before: the sequential loops called b.Fatalf on the FIRST error, so a single
// transient "shard is not ready" destroyed a whole run and the baseline could
// not be measured on a host carrying ordinary CI load at all.
//
// The naive fix — tolerate everything and log a count — is what the concurrent
// loops already did, and it is worse in a quieter way: a run in which EVERY
// proposal failed reported no samples and passed clean, so a comparison table
// could publish percentiles over nothing.
//
// The rows below are the two real runs that produced #181, plus the boundaries
// of the threshold between them.
func TestProposeFloorVerdict(t *testing.T) {
	for _, tc := range []struct {
		name      string
		completed int
		errored   int
		fatal     bool
		quiet     bool // no message at all
		wants     string
	}{
		{
			name: "clean run says nothing",
			// 399 proposals, none failed: the loop has nothing to report.
			completed: 399, errored: 0, quiet: true,
		},
		{
			name: "measured quiet-box rate is published with a note",
			// The real 2026-09-10 run at benchtime=5s: 24 of 399 (6.0%).
			// Ordinary Raft weather; the surviving samples describe the engine.
			completed: 375, errored: 24, fatal: false, wants: "6.0%",
		},
		{
			name: "measured contended rate is refused",
			// The real 2026-09-10 run at benchtime=30s: 21 of 24 (87.5%).
			// These samples describe the box, not the engine.
			completed: 3, errored: 21, fatal: true, wants: "contended host",
		},
		{
			name: "total failure is refused as measuring nothing",
			// The shape the tolerant-but-uncounted loops passed clean.
			completed: 0, errored: 50, fatal: true, wants: "measured nothing",
		},
		{
			name: "exactly at the limit is allowed",
			// 20 of 100 is 20%, which is not GREATER than the limit.
			completed: 80, errored: 20, fatal: false, wants: "20.0%",
		},
		{
			name:      "one past the limit is refused",
			completed: 79, errored: 21, fatal: true, wants: "contended host",
		},
		{
			name: "an empty loop is not a failure",
			// b.N can be 0; nothing was asked of the engine.
			completed: 0, errored: 0, quiet: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			note, fatal := proposeFloorVerdict(tc.completed, tc.errored)
			if tc.quiet {
				if note != "" || fatal {
					t.Fatalf("want silence, got fatal=%v %q", fatal, note)
				}
				return
			}
			if note == "" {
				t.Fatal("want a message, got silence")
			}
			if fatal != tc.fatal {
				t.Errorf("fatal = %v, want %v (message: %q)", fatal, tc.fatal, note)
			}
			if !strings.Contains(note, tc.wants) {
				t.Errorf("message does not mention %q: %s", tc.wants, note)
			}
		})
	}
}

// TestProposeFloorAdmitsTheQuietRunAndRefusesTheLoadedOne states the reason the
// threshold sits where it does, as an assertion rather than only a comment.
//
// maxProposeErrorRate is not a round number chosen for tidiness: it has to fall
// BETWEEN the two rates actually measured on 2026-09-10, or it either discards
// a usable run or publishes an unusable one. If someone tunes the constant,
// this fails and names both ends.
func TestProposeFloorAdmitsTheQuietRunAndRefusesTheLoadedOne(t *testing.T) {
	const (
		quietRate  = 24.0 / 399.0 // benchtime=5s,  load ~3.4/20 cores
		loadedRate = 21.0 / 24.0  // benchtime=30s, load ~14-24/20 cores
	)
	if maxProposeErrorRate < quietRate {
		t.Errorf("maxProposeErrorRate=%.2f is below the measured quiet-box rate "+
			"%.3f: a usable run would be discarded", maxProposeErrorRate, quietRate)
	}
	if maxProposeErrorRate >= loadedRate {
		t.Errorf("maxProposeErrorRate=%.2f is at or above the measured contended "+
			"rate %.3f: a run measuring the box would be published",
			maxProposeErrorRate, loadedRate)
	}
}
