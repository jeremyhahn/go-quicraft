package buildgates

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// TestNonCodePathsExcludeExecutableGitHubConfig: workflow and action files
// run in CI, so a change to them is code and gets the normal review; the
// repository's other .github configuration stays non-code.
func TestNonCodePathsExcludeExecutableGitHubConfig(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "code-review.yml"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^  NON_CODE_PATHS: '([^']*)'$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("code-review.yml has no NON_CODE_PATHS")
	}
	rx := regexp.MustCompile(string(m[1]))
	for _, p := range []string{
		".github/workflows/ci.yml", ".github/workflows/release.yaml",
		".github/actions/setup/action.yml", ".github/actions/setup/action.yaml",
		".devcontainer/devcontainer.json",
	} {
		if rx.MatchString(p) {
			t.Errorf("%s is classed as non-code", p)
		}
	}
	for _, p := range []string{
		".github/dependabot.yml", ".github/actionlint.yaml", ".github/CODEOWNERS",
		".github/ISSUE_TEMPLATE/bug.md", "README.md", "docs/a.md", "deploy/values.yaml", "config.toml",
	} {
		if !rx.MatchString(p) {
			t.Errorf("%s is classed as code", p)
		}
	}
}
