package buildgates

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// codeReviewWorkflow is the part of .github/workflows/code-review.yml the
// repository's LOCAL review-gate blocks live in.
type codeReviewWorkflow struct {
	On struct {
		PullRequestTarget struct {
			Types []string `yaml:"types"`
		} `yaml:"pull_request_target"`
	} `yaml:"on"`
	Jobs map[string]struct {
		Env   map[string]string `yaml:"env"`
		Steps []struct {
			Name string `yaml:"name"`
			Run  string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

// loadCodeReviewWorkflow reads code-review.yml the way extractGateTwin does
// and returns its raw text and its parsed form.
func loadCodeReviewWorkflow(t *testing.T) (string, codeReviewWorkflow) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "code-review.yml"))
	if err != nil {
		t.Fatalf("read code-review.yml: %v", err)
	}
	var wf codeReviewWorkflow
	if err := yaml.Unmarshal(b, &wf); err != nil {
		t.Fatalf("parse code-review.yml: %v", err)
	}
	return string(b), wf
}

// jobRuns joins the run scripts of every step of a job.
func jobRuns(t *testing.T, wf codeReviewWorkflow, job string) string {
	t.Helper()
	j, ok := wf.Jobs[job]
	if !ok {
		t.Fatalf("code-review.yml has no %s job", job)
	}
	var sb strings.Builder
	for _, s := range j.Steps {
		sb.WriteString(s.Run)
		sb.WriteString("\n")
	}
	return sb.String()
}

// triggersOnEdited reports whether the workflow runs on pull_request_target
// `edited` events (a retarget emits only `edited`).
func triggersOnEdited(wf codeReviewWorkflow) bool {
	for _, ty := range wf.On.PullRequestTarget.Types {
		if ty == "edited" {
			return true
		}
	}
	return false
}

func requireAll(t *testing.T, text, what string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(text, w) {
			t.Errorf("code-review.yml dropped %s: no %q", what, w)
		}
	}
}

// TestCodeReview_LocalBlocksKept fails when a template move drops one of
// the repository's own review-gate protections (the `# LOCAL (` blocks)
// from code-review.yml.
func TestCodeReview_LocalBlocksKept(t *testing.T) {
	raw, wf := loadCodeReviewWorkflow(t)

	t.Run("EditedTrigger", func(t *testing.T) {
		if triggersOnEdited(wf) {
			return
		}
		t.Errorf("code-review.yml dropped `edited` from on.pull_request_target.types %v: a retarget emits only `edited`, so the gate would not re-run against the new base", wf.On.PullRequestTarget.Types)
	})

	// The trigger check must fail on a copy with `edited` removed.
	t.Run("EditedTriggerCheckBites", func(t *testing.T) {
		const kept = "ready_for_review, edited]"
		if !strings.Contains(raw, kept) {
			t.Fatalf("code-review.yml trigger types no longer end %q; update this check", kept)
		}
		var cut codeReviewWorkflow
		if err := yaml.Unmarshal([]byte(strings.Replace(raw, kept, "ready_for_review]", 1)), &cut); err != nil {
			t.Fatalf("parse the edited-less copy: %v", err)
		}
		if triggersOnEdited(cut) {
			t.Error("the EditedTrigger check passes on a copy without `edited`")
		}
	})

	t.Run("ReviewRequiredPaths", func(t *testing.T) {
		pat := wf.Jobs["gate"].Env["REVIEW_REQUIRED_PATHS"]
		if pat == "" {
			t.Fatal("code-review.yml dropped REVIEW_REQUIRED_PATHS from the gate job's env: workflow files and the signing trust registry would pass the no-review exemption")
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			t.Fatalf("REVIEW_REQUIRED_PATHS %q does not compile: %v", pat, err)
		}
		for _, p := range []string{".github/workflows/x.yml", ".gitea/workflows/x.yml", ".github/allowed_signers"} {
			if !re.MatchString(p) {
				t.Errorf("code-review.yml REVIEW_REQUIRED_PATHS %q dropped %s: it would pass the no-review exemption", pat, p)
			}
		}
		requireAll(t, jobRuns(t, wf, "gate"), "the scope exemption's REVIEW_REQUIRED_PATHS check",
			`guarded=$(grep -Ec "$REVIEW_REQUIRED_PATHS" <<<"$names" || true)`)
	})

	t.Run("ApprovalBoundToBase", func(t *testing.T) {
		requireAll(t, jobRuns(t, wf, "review"), "the approval-reuse patch/ctx comparison against the event's base",
			`reuse_patch=$(git patch-id --verbatim < "$WORK/reuse.diff")`,
			`approved_ctx=$(jq -r '.ctx' <<<"$approval")`,
			`if [ "$approved_patch" != "$reuse_patch" ] || [ "$approved_ctx" != "$reuse_ctx" ]; then`)
		requireAll(t, jobRuns(t, wf, "gate"), "the required gate's patch/ctx comparison against the event's base",
			`patch=$(git patch-id --verbatim < gate-pr.diff)`,
			`approved_ctx=$(jq -r '.ctx' <<<"$approval")`,
			`[ "$approved_patch" = "$patch" ] && [ "$approved_ctx" = "$ctx" ] ||`)
	})

	t.Run("FailedReReviewKeepsGateClosed", func(t *testing.T) {
		requireAll(t, jobRuns(t, wf, "gate"), "the failed re-review persistence",
			`failure|cancelled) fail "the Codex review job of this run ended $REVIEW_RESULT; re-add code-review" ;;`,
			`failed=$(jq -r --arg t "$approved_at" '`,
			`.conclusion == "timed_out"`,
			`[ -z "$failed" ] ||`,
			`fail "a Codex review of $EVENT_SHA requested after its approval ended $failed; re-add code-review"`)
	})

	t.Run("LocalMarkerCount", func(t *testing.T) {
		const minMarkers = 23
		if n := strings.Count(raw, "# LOCAL ("); n < minMarkers {
			t.Errorf("code-review.yml has %d `# LOCAL (` markers, want at least %d: a template move dropped a repository review-gate block", n, minMarkers)
		}
	})
}
