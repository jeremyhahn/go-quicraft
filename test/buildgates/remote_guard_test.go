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

package buildgates

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// runRemotePath is the staging script whose active-run guard this file pins.
var runRemotePath = filepath.Join("..", "performance", "run-remote.sh")

// TestStagingGuardCannotMatchItself pins the bracket in the active-run check.
//
// run-remote.sh refuses to stage while a benchmark is running, because the
// rsync --delete that follows would clobber the working tree of a live run.
// The check is a pgrep executed over ssh, and pgrep -f matches FULL COMMAND
// LINES -- including the `bash -c '<the check>'` that ssh spawns to run it. A
// plain `pgrep -f perfcompare` therefore matches itself, fires on every
// invocation, and makes staging impossible. That is not hypothetical: it is
// how the guard shipped, and it blocked the first end-to-end run of the remote
// pipeline.
//
// `[p]erfcompare` is a regex matching the text "perfcompare" carried on a
// command line that does not contain that text, so the check sees real runs
// and not its own shell. This test fails if the brackets are ever removed as
// a simplification.
//
// It cannot pass vacuously: it asserts the file was read and contains a pgrep
// at all before judging the pattern, so a moved or renamed script fails here
// rather than silently passing with nothing to check.
func TestStagingGuardCannotMatchItself(t *testing.T) {
	raw, err := os.ReadFile(runRemotePath)
	if err != nil {
		t.Fatalf("read %s: %v", runRemotePath, err)
	}
	body := string(raw)

	pgrepLine := regexp.MustCompile(`(?m)^.*pgrep .*$`)
	lines := pgrepLine.FindAllString(body, -1)
	if len(lines) == 0 {
		t.Fatalf("%s contains no pgrep call. The active-run guard is what stops "+
			"rsync --delete running over a live benchmark; if it moved, re-point "+
			"this test rather than deleting it.", runRemotePath)
	}

	checked := 0
	for _, line := range lines {
		// Skip comments. The fix is documented in a comment that quotes the
		// broken pattern verbatim to explain it, and flagging the explanation
		// as the defect would make this test impossible to satisfy alongside
		// the prose that stops the bug being reintroduced.
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		checked++
		if strings.Contains(line, "[p]erfcompare") {
			continue
		}
		if strings.Contains(line, "perfcompare") {
			t.Errorf("pgrep pattern matches its own command line:\n  %s\n\n"+
				"pgrep -f matches full command lines, and ssh runs this check via "+
				"bash -c, so the literal pattern appears in the checking process's "+
				"own cmdline and the guard fires unconditionally. Use the bracket "+
				"form [p]erfcompare, which matches the text without containing it.",
				strings.TrimSpace(line))
		}
	}

	// Guard against the skip above hollowing out the test: if every pgrep line
	// were a comment, the loop would pass having judged nothing.
	if checked == 0 {
		t.Fatalf("%s has pgrep only in comments; the executable guard is gone. "+
			"Re-point this test at wherever the active-run check now lives.",
			runRemotePath)
	}
}
