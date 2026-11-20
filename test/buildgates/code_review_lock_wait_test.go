package buildgates

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// lockWaitNow is the fixed epoch the stubbed date prints.
const lockWaitNow = 1_000_000

// lockWaitLib holds the shell pieces of code-review.yml's lib.sh that
// lock_wait needs, taken verbatim from the workflow.
type lockWaitLib struct {
	die       string // the die() definition
	budget    string // the JOB_BUDGET= line
	fn        string // the lock_wait() { ... } function
	refresh   string // the refresh caller's NEED expression
	reviewArg string // the review caller's NEED expression
}

var lockWaitCallerRE = regexp.MustCompile(`wait_s=\$\(lock_wait (\$\(\(.*?\)\))\)`)

// extractLockWaitLib reads code-review.yml and pulls out the lock_wait
// function, its budget, die() and both callers' reserve expressions. A
// missing piece fails the test, so a template change cannot drop coverage.
func extractLockWaitLib(t *testing.T) lockWaitLib {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "code-review.yml"))
	if err != nil {
		t.Fatalf("read code-review.yml: %v", err)
	}
	lines := strings.Split(string(b), "\n")
	var lib lockWaitLib
	for i, l := range lines {
		s := strings.TrimSpace(l)
		switch {
		case lib.die == "" && strings.HasPrefix(s, "die() {"):
			lib.die = s
		case lib.budget == "" && strings.HasPrefix(s, "JOB_BUDGET="):
			lib.budget = s
		case lib.fn == "" && s == "lock_wait() {":
			indent := l[:len(l)-len(strings.TrimLeft(l, " "))]
			var fn []string
			for _, f := range lines[i:] {
				fn = append(fn, strings.TrimPrefix(f, indent))
				if f == indent+"}" {
					lib.fn = strings.Join(fn, "\n")
					break
				}
			}
		}
	}
	if lib.die == "" {
		t.Fatal("code-review.yml: no die() definition")
	}
	if lib.budget == "" {
		t.Fatal("code-review.yml: no JOB_BUDGET= line")
	}
	if lib.fn == "" {
		t.Fatal("code-review.yml: no complete lock_wait() { ... } function")
	}
	for _, m := range lockWaitCallerRE.FindAllStringSubmatch(string(b), -1) {
		switch {
		case strings.Contains(m[1], "limit"):
			lib.reviewArg = m[1]
		default:
			lib.refresh = m[1]
		}
	}
	if lib.refresh == "" {
		t.Fatal("code-review.yml: no refresh caller wait_s=$(lock_wait $(( ... )))")
	}
	if lib.reviewArg == "" {
		t.Fatal("code-review.yml: no review caller wait_s=$(lock_wait $(( limit ... )))")
	}
	return lib
}

type lockWaitRun struct {
	code   int
	stdout string
	stderr string
	work   string
}

// runLockWait runs body after lib.sh's shell options, die(), JOB_BUDGET
// and fn, with date stubbed to lockWaitNow. start is written to
// $WORK/job-start unless it is nil.
func runLockWait(t *testing.T, lib lockWaitLib, fn string, start *string, body string) lockWaitRun {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	dir := t.TempDir()
	work := filepath.Join(dir, "work")
	stubs := filepath.Join(dir, "bin")
	for _, d := range []string{work, stubs} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(stubs, "date"), []byte("#!/bin/sh\necho 1000000\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if start != nil {
		if err := os.WriteFile(filepath.Join(work, "job-start"), []byte(*start+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script := strings.Join([]string{
		// lib.sh's own options.
		"set -euo pipefail",
		"shopt -s inherit_errexit",
		"PATH=" + stubs + ":$PATH",
		"WORK=" + work,
		"CODEX_MASTER=" + filepath.Join(dir, "codex"),
		lib.die,
		lib.budget,
		fn,
		body,
		"",
	}, "\n")
	path := filepath.Join(dir, "run.sh")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), bash, "-euo", "pipefail", path)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	r := lockWaitRun{work: work}
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run bash: %v", err)
		}
		r.code = ee.ExitCode()
	}
	r.stdout, r.stderr = stdout.String(), stderr.String()
	return r
}

func ago(s int) *string {
	v := strconv.Itoa(lockWaitNow - s)
	return &v
}

type lockWaitCase struct {
	name   string
	start  *string
	body   string // shell that calls lock_wait
	want   string // printed seconds; "" when lock_wait must die
	reason string // a substring of the reason die records
}

func lockWaitCases(lib lockWaitLib) []lockWaitCase {
	malformed := "12a4"
	// Budget 4500 s; the stubbed clock is lockWaitNow.
	return []lockWaitCase{
		{name: "positive budget less elapsed and reserve", start: ago(600), body: "lock_wait 100", want: "3800"},
		{name: "zero left", start: ago(0), body: "lock_wait 4500", reason: "no time is left in the job's 4500 s"},
		{name: "negative left", start: ago(4000), body: "lock_wait 1000", reason: "it needs 1000 s after the lock"},
		{name: "missing job-start", start: nil, body: "lock_wait 100", reason: "cannot read the job's start time"},
		{name: "malformed job-start", start: &malformed, body: "lock_wait 100", reason: "is not a number"},
		// 130 + 360 + 2730 + 440 = 3660.
		{name: "refresh reserve", start: ago(60), body: "lock_wait " + lib.refresh, want: "780"},
		{name: "refresh reserve exhausted", start: ago(841), body: "lock_wait " + lib.refresh, reason: "it needs 3660 s after the lock"},
		// limit + 30 + 440.
		{name: "review reserve limit 1530", start: ago(60), body: "limit=1530; lock_wait " + lib.reviewArg, want: "2440"},
		{name: "review reserve limit 2730", start: ago(60), body: "limit=2730; lock_wait " + lib.reviewArg, want: "1240"},
		{name: "review reserve limit 2730 exhausted", start: ago(1300), body: "limit=2730; lock_wait " + lib.reviewArg, reason: "it needs 3200 s after the lock"},
	}
}

func checkLockWaitCase(t *testing.T, c lockWaitCase, r lockWaitRun) {
	t.Helper()
	if c.want != "" {
		if r.code != 0 || strings.TrimSpace(r.stdout) != c.want {
			t.Errorf("lock_wait printed %q, exit %d, stderr %q; want %q, exit 0", r.stdout, r.code, r.stderr, c.want)
		}
		return
	}
	if r.code == 0 {
		t.Errorf("lock_wait exited 0 printing %q; want it to die (%q)", r.stdout, c.reason)
	}
	if r.stdout != "" {
		t.Errorf("lock_wait printed %q before dying; want no output", r.stdout)
	}
	if !strings.Contains(r.stderr, c.reason) {
		t.Errorf("stderr %q does not give the reason %q", r.stderr, c.reason)
	}
	rec, _ := os.ReadFile(filepath.Join(r.work, "error"))
	if !strings.Contains(string(rec), c.reason) {
		t.Errorf("$WORK/error %q does not record the reason %q", rec, c.reason)
	}
}

// TestCodeReviewLockWait runs code-review.yml's lock_wait under a fixed
// clock: the wait is the 75-minute budget less the time since job-start
// less the caller's reserve, and no time left, or no readable start, dies.
func TestCodeReviewLockWait(t *testing.T) {
	lib := extractLockWaitLib(t)
	for _, c := range lockWaitCases(lib) {
		t.Run(c.name, func(t *testing.T) {
			checkLockWaitCase(t, c, runLockWait(t, lib, lib.fn, c.start, c.body))
		})
	}
}

// TestCodeReviewLockWaitStopsCaller checks that a lock_wait failure inside
// the callers' `wait_s=$(lock_wait ...)` stops the step before flock runs.
func TestCodeReviewLockWaitStopsCaller(t *testing.T) {
	lib := extractLockWaitLib(t)
	for _, c := range []struct {
		name, need string
		start      *string
		reason     string
	}{
		{"refresh", lib.refresh, ago(1000), "it needs 3660 s after the lock"},
		{"review", lib.reviewArg, ago(4000), "it needs 2000 s after the lock"},
		{"missing job-start", lib.refresh, nil, "cannot read the job's start time"},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := "limit=1530\nwait_s=$(lock_wait " + c.need + ")\ntouch \"$WORK/flock-ran\"\necho \"$wait_s\""
			r := runLockWait(t, lib, lib.fn, c.start, body)
			if r.code == 0 {
				t.Errorf("caller exited 0 printing %q; want it stopped", r.stdout)
			}
			if _, err := os.Stat(filepath.Join(r.work, "flock-ran")); err == nil {
				t.Error("the caller went on to flock after lock_wait died")
			}
			if !strings.Contains(r.stderr, c.reason) {
				t.Errorf("stderr %q does not give the reason %q", r.stderr, c.reason)
			}
			rec, _ := os.ReadFile(filepath.Join(r.work, "error"))
			if !strings.Contains(string(rec), c.reason) {
				t.Errorf("$WORK/error %q does not record the reason %q", rec, c.reason)
			}
		})
	}
}
