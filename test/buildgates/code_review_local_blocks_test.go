package buildgates

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// codeReviewTemplateHeader is the first line of the code-review template this
// repository carries as written.
const codeReviewTemplateHeader = "# code-review-template: v5.12.12 "

// TestCodeReview_AdoptedAsWritten checks that .github/workflows/code-review.yml
// is the code-review template as written: its header names the template
// version, and no repository LOCAL block patches it.
func TestCodeReview_AdoptedAsWritten(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(workflowsDir, "code-review.yml"))
	if err != nil {
		t.Fatalf("read code-review.yml: %v", err)
	}
	raw := string(b)
	if !strings.HasPrefix(raw, codeReviewTemplateHeader) {
		first, _, _ := strings.Cut(raw, "\n")
		t.Errorf("code-review.yml line 1 is %q, want it to start %q", first, codeReviewTemplateHeader)
	}
	if n := strings.Count(raw, "# LOCAL ("); n != 0 {
		t.Errorf("code-review.yml has %d `# LOCAL (` markers; the template is carried as written", n)
	}
}
