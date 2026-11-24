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

	"github.com/jeremyhahn/dragonboat/v4"
)

// TestProposeFloorVerdict checks the error floor neither fails on one transient error nor passes a run with no samples.
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
			name: "calibration probe with one transient failure is NOT judged",
			// Go runs every benchmark at b.N=1 first. The concurrent helper
			// makes eight proposals there; one refusal is 12.5%, which a
			// rate-based floor would have called fatal and aborted calibration.
			completed: 7, errored: 1, fatal: false, wants: "not judged",
		},
		{
			name: "calibration probe that could not propose at all IS fatal",
			// A probe with no successes still fails.
			completed: 0, errored: 1, fatal: true, wants: "measured nothing",
		},
		{
			name: "the loaded run's raw 21 of 24 is below the sample floor",
			// The real figures, unscaled. Twenty-four attempts is not enough to
			// rate, so this is reported and admitted, and the calibrated run
			// that follows -- with thousands of attempts -- is what gets judged.
			completed: 3, errored: 21, fatal: false, wants: "not judged",
		},
		{
			name: "exactly at the limit is allowed",
			// 20 of 100 is not above the limit.
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
			note, fatal := proposeFloorVerdict("proposals", tc.completed, tc.errored)
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

// TestFloorNamesTheOperationItJudged pins that a read loop's failure says
// "reads", not "proposals". The tolerant loops include a SyncRead loop; a
// message calling its failures proposals would
// send an operator looking at the wrong code path.
func TestFloorNamesTheOperationItJudged(t *testing.T) {
	note, fatal := proposeFloorVerdict("reads", 0, 50)
	if !fatal {
		t.Fatal("total failure must be fatal regardless of operation")
	}
	if !strings.Contains(note, "50 reads failed") {
		t.Fatalf("message does not name the operation: %s", note)
	}
	if strings.Contains(note, "proposal") {
		t.Fatalf("a read loop's verdict mentions proposals: %s", note)
	}
}

// TestProposeFloorAdmitsTheQuietRunAndRefusesTheLoadedOne checks maxProposeErrorRate sits between the two measured rates.
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

// TestTolerableAdmitsWeatherAndRefusesRealResults checks only self-clearing errors are tolerated, per test/proposer.
func TestTolerableAdmitsWeatherAndRefusesRealResults(t *testing.T) {
	// The two transient failures actually observed, which the loops MUST keep
	// tolerating or a single transient aborts the run.
	for _, err := range []error{dragonboat.ErrShardNotReady, dragonboat.ErrTimeout} {
		if !tolerable(err) {
			t.Errorf("tolerable(%v) = false; this is transient Raft weather the loops "+
				"must survive, and refusing it aborts the run on the first transient", err)
		}
	}
	// Real answers from the engine: none of these clears by waiting.
	for _, err := range []error{
		dragonboat.ErrPayloadTooBig,
		dragonboat.ErrRejected,
		dragonboat.ErrInvalidSession,
		dragonboat.ErrInvalidOperation,
	} {
		if tolerable(err) {
			t.Errorf("tolerable(%v) = true; a permanent error would be counted as "+
				"weather and published at up to 20%%", err)
		}
	}
}
