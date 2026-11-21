package buildgates

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The gate job of code-review.yml (the handbook template, v5.12.6) fails
// closed while a twin run (same head, same run-name) has its `Codex review`
// job queued or in progress and the head is not approved: that run's gate
// posts the head's check. An approval of the head passes. These tests run
// the gate's own shell (fail(), the twin read up to the standing-verdict
// block (b), and the verdict block) under bash with the forge API stubbed.

const (
	gateTwinSHA      = "0123456789abcdef0123456789abcdef01234567"
	gateTwinPR       = "42"
	gateTwinBase     = "main"
	gateTwinReviewer = "codex-reviewer"
	gateTwinRunID    = "1000"
	gateTwinWF       = "77"
	gateTwinRun      = "555"
)

type gateTwinScript struct {
	fail    string // the fail() definition
	twin    string // `twin=` up to the standing-verdict block (b)
	verdict string // `verdict=$(get_all ...` through its `case "$verdict"` esac
}

// sliceFrom returns lines[i:] up to and including the first line (after i)
// for which end returns true, with the leading indent of lines[i] removed.
func sliceFrom(lines []string, i int, end func(string) bool, inclusive bool) (string, bool) {
	l := lines[i]
	indent := l[:len(l)-len(strings.TrimLeft(l, " "))]
	out := []string{strings.TrimPrefix(l, indent)}
	for _, f := range lines[i+1:] {
		if end(f) {
			if inclusive {
				out = append(out, strings.TrimPrefix(f, indent))
			}
			return strings.Join(out, "\n"), true
		}
		out = append(out, strings.TrimPrefix(f, indent))
	}
	return "", false
}

func extractGateTwin(t *testing.T) gateTwinScript {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "code-review.yml"))
	if err != nil {
		t.Fatalf("read code-review.yml: %v", err)
	}
	lines := strings.Split(string(b), "\n")
	gate := -1
	for i, l := range lines {
		if strings.TrimSpace(l) == "gate:" {
			gate = i
		}
	}
	if gate < 0 {
		t.Fatal("code-review.yml: no gate: job")
	}
	var s gateTwinScript
	for i := gate; i < len(lines); i++ {
		l := lines[i]
		ts := strings.TrimSpace(l)
		switch {
		case s.fail == "" && strings.HasPrefix(ts, "fail() {"):
			s.fail = ts
		case s.fail != "" && s.twin == "" && ts == "twin=":
			s.twin, _ = sliceFrom(lines, i, func(f string) bool {
				return strings.HasPrefix(strings.TrimSpace(f), `# (b) The reviewer's latest standing verdict`)
			}, false)
		case s.twin != "" && s.verdict == "" && strings.HasPrefix(ts, `verdict=$(get_all "/pulls/$PR_NUM/reviews?"`):
			indent := l[:len(l)-len(strings.TrimLeft(l, " "))]
			seenCase := false
			s.verdict, _ = sliceFrom(lines, i, func(f string) bool {
				if strings.TrimSpace(f) == `case "$verdict" in` {
					seenCase = true
				}
				return seenCase && f == indent+"esac"
			}, true)
		}
	}
	if s.fail == "" {
		t.Fatal("code-review.yml: gate job has no fail() definition")
	}
	if s.twin == "" || !strings.Contains(s.twin, "twin=${twin:-$id}") {
		t.Fatal("code-review.yml: gate job has no twin read before its standing-verdict block (b)")
	}
	if s.verdict == "" {
		t.Fatal(`code-review.yml: gate job has no verdict=$(get_all ...) block through its case "$verdict" esac`)
	}
	return s
}

type gateTwinCase struct {
	name      string
	twinState string // "", "queued" or "in_progress"
	verdict   string // review state on the head: APPROVED, CHANGES_REQUESTED or ""
	wantPass  bool
}

func gateTwinAnswers(c gateTwinCase) map[string]string {
	title := fmt.Sprintf("Code Review: PR #%s into %s at %s", gateTwinPR, gateTwinBase, gateTwinSHA)
	runs := func(st string) string {
		if st != c.twinState {
			return `{"workflow_runs":[]}`
		}
		return fmt.Sprintf(`{"workflow_runs":[{"id":%s,"display_title":%q},{"id":%s,"display_title":%q}]}`,
			gateTwinRunID, title, gateTwinRun, title)
	}
	reviews := `[]`
	if c.verdict != "" {
		reviews = fmt.Sprintf(`[{"id":1,"user":{"login":%q},"commit_id":%q,"state":%q,"submitted_at":"2026-11-20T00:00:00Z","body":""}]`,
			gateTwinReviewer, gateTwinSHA, c.verdict)
	}
	list := func(st string) string {
		return "/actions/workflows/" + gateTwinWF + "/runs?head_sha=" + gateTwinSHA + "&status=" + st + "&per_page=100"
	}
	a := map[string]string{
		"/actions/runs/" + gateTwinRunID:     `{"workflow_id":` + gateTwinWF + `}`,
		list("queued"):                       runs("queued"),
		list("in_progress"):                  runs("in_progress"),
		"/pulls/" + gateTwinPR + "/reviews?": reviews,
	}
	if c.twinState != "" {
		a["/actions/runs/"+gateTwinRun+"/jobs?per_page=100"] = fmt.Sprintf(
			`{"jobs":[{"name":"Scope","status":"completed"},{"name":"Codex review","status":%q}]}`, c.twinState)
	}
	return a
}

func runGateTwin(t *testing.T, s gateTwinScript, c gateTwinCase) (int, string) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is not installed")
	}
	dir := t.TempDir()
	answers := gateTwinAnswers(c)
	keys := make([]string, 0, len(answers))
	for k := range answers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var stub strings.Builder
	stub.WriteString("answer() {\n  case \"$1\" in\n")
	for i, k := range keys {
		f := filepath.Join(dir, fmt.Sprintf("answer-%d.json", i))
		if err := os.WriteFile(f, []byte(answers[k]), 0o600); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&stub, "    %q) cat %q ;;\n", k, f)
	}
	stub.WriteString("    *) echo \"stub: unexpected API path $1\" >&2; return 1 ;;\n  esac\n}\n")
	stub.WriteString("get() { answer \"$1\"; }\n")
	stub.WriteString("get_all() { answer \"$1\"; }\n")

	script := strings.Join([]string{
		"set -euo pipefail",
		"FORGE=github",
		"EVENT_SHA=" + gateTwinSHA,
		"PR_NUM=" + gateTwinPR,
		"BASE_REF=" + gateTwinBase,
		"REVIEWER=" + gateTwinReviewer,
		"GITHUB_RUN_ID=" + gateTwinRunID,
		// The subsequent edited/labeled event: its review job skipped.
		"REVIEW_RESULT=skipped",
		"SCOPE_RESULT=success",
		"REVIEW_SKIP=",
		s.fail,
		stub.String(),
		s.twin,
		s.verdict,
		"",
	}, "\n")
	path := filepath.Join(dir, "gate.sh")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), bash, path)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	code := 0
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run bash: %v", err)
		}
		code = ee.ExitCode()
	}
	return code, out.String()
}

func TestCodeReviewGateTwin(t *testing.T) {
	s := extractGateTwin(t)
	for _, c := range []gateTwinCase{
		// The template (v5.12.12): a twin reviewing the head fails the gate
		// closed, naming the twin, approval or not; with no twin an approved
		// head passes.
		{name: "approved head, queued twin", twinState: "queued", verdict: "APPROVED"},
		{name: "approved head, in_progress twin", twinState: "in_progress", verdict: "APPROVED"},
		{name: "approved head, no twin", verdict: "APPROVED", wantPass: true},
		{name: "not approved, queued twin", twinState: "queued"},
		{name: "changes requested, in_progress twin", twinState: "in_progress", verdict: "CHANGES_REQUESTED"},
	} {
		t.Run(c.name, func(t *testing.T) {
			code, out := runGateTwin(t, s, c)
			if strings.Contains(out, "stub: unexpected API path") {
				t.Fatalf("gate asked an unstubbed path:\n%s", out)
			}
			if c.wantPass {
				if code != 0 {
					t.Fatalf("gate failed (exit %d), want pass:\n%s", code, out)
				}
				return
			}
			if code == 0 {
				t.Fatalf("gate passed, want it closed while twin run %s reviews the head:\n%s", gateTwinRun, out)
			}
			if !strings.Contains(out, "run "+gateTwinRun) {
				t.Fatalf("gate failed (exit %d) without naming twin run %s:\n%s", code, gateTwinRun, out)
			}
		})
	}
}
