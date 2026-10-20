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

package releasability

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// gitattributesEntries are the .gitattributes entries that keep the private
// forge's own workflows and the private release trust anchor
// (privateWorkflowFiles) out of every archive of the tree; nothing else is
// export-ignored. dockerignoreEntries keep the agent files and those files out
// of every image build context: an image runs the code, so it carries no
// development tooling, published or not.
var (
	gitattributesEntries = []string{
		".github/workflows/automerge.yml export-ignore",
		".github/workflows/revert-red-main.yml export-ignore",
		"/.release-allowed-signers export-ignore",
	}
	dockerignoreEntries = []string{
		"**/CLAUDE.md", "**/AGENTS.md", "**/.claude", "**/.serena",
		".github/workflows/automerge.yml",
		".github/workflows/revert-red-main.yml",
		".release-allowed-signers",
	}
)

// repoRoot walks up from the test's working directory to the module root, which
// is the repository root. The gate scans the whole repository, nested modules
// included, because git's index is the ship manifest and it does not stop at a
// nested go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod above %s", dir)
		}
		dir = parent
	}
}

// TestTheTrackedTreeIsPubliclyReleasable is the gate itself. `make test` runs
// `go test ./...`, which includes this package, so a lab value cannot reach main
// without somebody reading this failure first.
func TestTheTrackedTreeIsPubliclyReleasable(t *testing.T) {
	root := repoRoot(t)
	findings, err := scanRepo(t.Context(), root)
	if err != nil {
		t.Fatalf("scanning %s: %v", root, err)
	}
	if len(findings) > 0 {
		t.Fatalf("this tree cannot be published as it stands:\n\n%s", report(findings))
	}
}

// TestThePlantedViolationIsCaught proves the gate above can fail. It plants one
// value of each class, in the kinds of file this repository ships, scans them
// under the real allow-list, and asserts every class comes back -- then that
// the same value passes once waived in place.
func TestThePlantedViolationIsCaught(t *testing.T) {
	planted := map[string]string{
		"Makefile":                 "REMOTE_HOST ?= " + plantedAddress + "\n",
		"docs/guide.md":            "Deploy it on " + plantedGPUHost + ", then check " + plantedHome + "/sources.\n",
		"README.md":                "Questions go to " + plantedAccount + ".\n",
		"pkg/seal/doc.go":          "// observed on " + plantedAgent + "\n",
		"scripts/release.sh":       "GIT_AUTHOR_EMAIL=" + plantedPrincipal + " make release\n",
		"test/performance/x":       "PERF_PROFILE=" + plantedBenchHost + "\n",
		".github/workflows/ci.yml": "# v1.15 (" + plantedCompanyRepo + "#211): push runs on main\n",
	}
	root, files := writeTree(t, planted)

	findings, err := scan(root, files, allowList)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	byClass := map[class]int{}
	for _, f := range findings {
		byClass[f.class]++
	}
	for c := range knownClasses() {
		if byClass[c] == 0 {
			t.Errorf("no %s finding in %v", c, findings)
		}
	}
	out := report(findings)
	for name := range planted {
		if !strings.Contains(out, name) {
			t.Errorf("report does not name %s:\n%s", name, out)
		}
	}

	waivedRoot, waivedFiles := writeTree(t, map[string]string{
		"docs/guide.md": "Deploy it on " + plantedGPUHost + " <!-- lab-string-ok: measurement provenance -->\n",
	})
	rest, err := scan(waivedRoot, waivedFiles, allowList)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("a waived line was still reported: %v", rest)
	}
}

// TestTheAllowListDoesNotReachTheValuesThisChangeRemoved pins the scrub: the
// files whose lab values were neutralised are scanned in full, not excused. If an allow-list
// entry ever widens to cover one of them, this fails.
func TestTheAllowListDoesNotReachTheValuesThisChangeRemoved(t *testing.T) {
	for _, name := range []string{
		"Makefile",
		"README.md",
		"CONTRIBUTING.md",
		"docs/testing/benchmarking.md",
		"docs/testing/infrastructure.md",
		"docs/testing/overview.md",
		"test/performance/README.md",
		"test/performance/run-remote.sh",
		"test/performance/cmd/perfcompare/main_test.go",
		"contrib/dragonboat/README.md",
		"test/buildgates/release_inputs_test.go",
		".github/dependabot.yml",
	} {
		if accepted := acceptedClasses(allowList, name); len(accepted) != 0 {
			t.Errorf("%s is allow-listed for %v; it must be clean or waived line by line", name, accepted)
		}
	}
	// The baselines are allowed fleet HOST names as provenance, never the
	// address or the account the old run-host header carried.
	accepted := acceptedClasses(allowList, "test/performance/baselines/x.txt")
	if accepted[classLabAddress] || accepted[classOwnerAccount] {
		t.Errorf("baselines accept %v; only %s is provenance", accepted, classLabHost)
	}
}

func TestIsPrivateWorkflowFile(t *testing.T) {
	cases := map[string]bool{
		".release-allowed-signers":               true,
		"sub/.release-allowed-signers":           false,
		".release-allowed-signers.sample":        false,
		".github/workflows/automerge.yml":        true,
		".github/workflows/revert-red-main.yml":  true,
		".github/workflows/automerge.yml.sample": false,
		"sub/.github/workflows/automerge.yml":    false,
		".github/workflows/batch.yml":            false,
		".github/workflows/ci.yml":               false,
		".github/workflows/release.yml":          false,
		".github/dependabot.yml":                 false,
		"CLAUDE.md":                              false,
		"AGENTS.md":                              false,
		"pkg/seal/CLAUDE.md":                     false,
		".claude/agents/go-quicraft.md":          false,
		".serena/project.yml":                    false,
		"go.mod":                                 false,
	}
	for name, want := range cases {
		if got := isPrivateWorkflowFile(name); got != want {
			t.Errorf("isPrivateWorkflowFile(%q) = %v, want %v", name, got, want)
		}
	}
}

// agentFilesAndWorkflows are tracked files this repository publishes under
// option (a) of the owner ruling on #406 and #407: the agent files and the
// CI template's workflows, neutralised like the rest of the tree.
var agentFilesAndWorkflows = []string{
	"AGENTS.md",
	"CLAUDE.md",
	".claude/agents/go-quicraft.md",
	".serena/project.yml",
	".github/workflows/batch.yml",
	".github/workflows/ci.yml",
	".github/workflows/release.yml",
}

// TestThePublishedTreeIsTheTrackedTreeLessThePrivateWorkflow is the exclusion
// gate on this repository: the private forge's own workflows and the release
// trust anchor stay tracked for the private forge and release recipe and are
// not published; every other tracked file is, the agent files and every other
// workflow included, so the gate above scans them.
func TestThePublishedTreeIsTheTrackedTreeLessThePrivateWorkflow(t *testing.T) {
	root := repoRoot(t)
	tracked, err := trackedFiles(t.Context(), root)
	if err != nil {
		t.Fatalf("trackedFiles: %v", err)
	}
	published, err := publishedFiles(t.Context(), root)
	if err != nil {
		t.Fatalf("publishedFiles: %v", err)
	}
	for _, name := range published {
		if isPrivateWorkflowFile(name) {
			t.Errorf("%s is published; add its path to .gitattributes as export-ignore", name)
		}
	}
	for _, name := range tracked {
		if !isPrivateWorkflowFile(name) && !slices.Contains(published, name) {
			t.Errorf("%s is export-ignored but is not in privateWorkflowFiles; the exclusion is too wide", name)
		}
	}
	for _, name := range append([]string{"go.mod"}, agentFilesAndWorkflows...) {
		if !slices.Contains(published, name) {
			t.Errorf("the published manifest lost %s; the exclusion is too wide", name)
		}
	}
}

// TestTheExclusionListsCarryTheirEntries pins both lists: .gitattributes
// for every archive git builds, .dockerignore for every image build context.
func TestTheExclusionListsCarryTheirEntries(t *testing.T) {
	root := repoRoot(t)
	for file, want := range map[string][]string{
		".gitattributes": gitattributesEntries,
		".dockerignore":  dockerignoreEntries,
	} {
		raw, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		lines := strings.Split(string(raw), "\n")
		for _, entry := range want {
			if !slices.Contains(lines, entry) {
				t.Errorf("%s does not carry the line %q", file, entry)
			}
		}
	}
}

// TestGitArchiveHonoursTheExclusionList proves the list does what it claims: a
// repository carrying this tree's own .gitattributes, archived the way a forge
// builds a release tarball, leaves out the private forge's workflows and the
// release trust anchor, keeps the agent files and every other workflow, and
// publishedFiles agrees with the archive.
func TestGitArchiveHonoursTheExclusionList(t *testing.T) {
	attrs, err := os.ReadFile(filepath.Join(repoRoot(t), ".gitattributes"))
	if err != nil {
		t.Fatalf("reading .gitattributes: %v", err)
	}
	root, _ := writeTree(t, map[string]string{
		".gitattributes":                        string(attrs),
		"go.mod":                                "module example.com/m\n",
		"pkg/a/a.go":                            "package a\n",
		"CLAUDE.md":                             "public\n",
		"AGENTS.md":                             "public\n",
		"pkg/a/CLAUDE.md":                       "public\n",
		".claude/agents/go-quicraft.md":         "public\n",
		".serena/memories/notes.md":             "public\n",
		"docs/claude-review.md":                 "public\n",
		".github/workflows/ci.yml":              "public\n",
		".github/dependabot.yml":                "public\n",
		".github/workflows/automerge.yml":       "private\n",
		".github/workflows/revert-red-main.yml": "private\n",
		".release-allowed-signers":              "private\n",
		"sub/.release-allowed-signers":          "public\n",
	})
	runGit(t, root, "init", "-q")
	runGit(t, root, "add", "-A")
	runGit(t, root, "-c", "user.name=gate", "-c", "user.email=gate@example.com",
		"-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "commit", "-q", "-m", "fixture")

	archived := archiveEntries(t, root)
	want := []string{
		".claude/agents/go-quicraft.md", ".gitattributes", ".github/dependabot.yml",
		".github/workflows/ci.yml", ".serena/memories/notes.md",
		"AGENTS.md", "CLAUDE.md", "docs/claude-review.md", "go.mod",
		"pkg/a/CLAUDE.md", "pkg/a/a.go", "sub/.release-allowed-signers",
	}
	if !slices.Equal(archived, want) {
		t.Errorf("git archive contains %v, want %v", archived, want)
	}
	published, err := publishedFiles(t.Context(), root)
	if err != nil {
		t.Fatalf("publishedFiles: %v", err)
	}
	if !slices.Equal(published, want) {
		t.Errorf("publishedFiles = %v, want the archive's %v", published, want)
	}
}

func TestPublishedFilesOutsideAWorkTreeIsAListError(t *testing.T) {
	_, err := publishedFiles(t.Context(), t.TempDir())
	var listErr *listError
	if !errors.As(err, &listErr) {
		t.Fatalf("publishedFiles outside a work tree = %v, want *listError", err)
	}
	if listErr.root == "" {
		t.Error("listError does not name the root it was given")
	}
}

func TestExportIgnoredReportsAGitFailure(t *testing.T) {
	_, err := exportIgnored(t.Context(), filepath.Join(t.TempDir(), "absent"), []string{"go.mod"})
	var listErr *listError
	if !errors.As(err, &listErr) {
		t.Fatalf("exportIgnored on a missing root = %v, want *listError", err)
	}
}

// runGit runs one git command in dir, failing the test on error.
func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	if out, err := gitCommand(t.Context(), dir, args...).CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// archiveEntries lists the regular files `git archive HEAD` produces, sorted.
func archiveEntries(t *testing.T, dir string) []string {
	t.Helper()
	out, err := gitCommand(t.Context(), dir, "archive", "--format=tar", "HEAD").Output()
	if err != nil {
		t.Fatalf("git archive: %v", err)
	}
	var names []string
	reader := tar.NewReader(bytes.NewReader(out))
	for {
		hdr, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("reading the archive: %v", err)
		}
		if hdr.Typeflag == tar.TypeReg {
			names = append(names, hdr.Name)
		}
	}
	slices.Sort(names)
	return names
}
