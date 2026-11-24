package buildgates

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// reviewJobIf is the `if:` of code-review.yml's review job (the folded
// scalar, joined into one line).
func reviewJobIf(t *testing.T, workflow string) string {
	t.Helper()
	i := strings.Index(workflow, "\n  review:\n")
	if i < 0 {
		t.Fatal("code-review.yml has no review job")
	}
	job := workflow[i+1:]
	if j := regexp.MustCompile(`\n  [a-z][a-z0-9-]*:\n`).FindStringIndex(job[1:]); j != nil {
		job = job[:j[0]+1]
	}
	m := regexp.MustCompile(`(?s)\n    if: >-\n(.*?)\n    [a-z#]`).FindStringSubmatch(job)
	if m == nil {
		t.Fatal("the review job has no folded if")
	}
	return strings.Join(strings.Fields(m[1]), " ")
}

// workflowsNeedReview reports whether REVIEW_REQUIRED_PATHS matches a workflow file.
func workflowsNeedReview(t *testing.T, workflow string) bool {
	t.Helper()
	m := regexp.MustCompile(`(?m)^      REVIEW_REQUIRED_PATHS: '([^']*)'$`).FindStringSubmatch(workflow)
	if m == nil {
		t.Fatal("the gate has no REVIEW_REQUIRED_PATHS")
	}
	return regexp.MustCompile(m[1]).MatchString(".github/workflows/ci.yml")
}

// TestReviewRunsWhereTheGateNeedsIt verifies the review job runs for every workflow change the gate requires reviewed.
func TestReviewRunsWhereTheGateNeedsIt(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "code-review.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if problem := reviewSkipProblem(t, string(b)); problem != "" {
		t.Error(problem)
	}
}

func reviewSkipProblem(t *testing.T, workflow string) string {
	t.Helper()
	if workflowsNeedReview(t, workflow) && strings.Contains(reviewJobIf(t, workflow), "needs.scope.outputs.skip") {
		return "the review job skips a change the scope job clears, but the gate requires a review of every workflow file"
	}
	return ""
}

func TestReviewRunsWhereTheGateNeedsIt_CanFail(t *testing.T) {
	const fixture = `jobs:
  scope:
    name: x
  review:
    name: Codex review
    if: >-
      github.event.pull_request.state == 'open'
      && needs.scope.outputs.skip != 'true'
    runs-on: codex
  gate:
    env:
      REVIEW_REQUIRED_PATHS: '^(\.github/workflows/.+)$'
`
	if reviewSkipProblem(t, fixture) == "" {
		t.Fatal("a review skipped on the scope verdict while the gate requires workflow reviews: no problem reported")
	}
	ok := strings.Replace(fixture, "\n      && needs.scope.outputs.skip != 'true'", "", 1)
	if p := reviewSkipProblem(t, ok); p != "" {
		t.Fatalf("the review job without the scope skip: %s", p)
	}
}
