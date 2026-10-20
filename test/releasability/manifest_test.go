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
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strings"
)

// exportIgnoreAttr is the git attribute that keeps a tracked file out of every
// archive git builds from the tree, including the source tarball a forge serves
// for a tag. .gitattributes is therefore the one exclusion list.
const exportIgnoreAttr = "export-ignore"

// privateWorkflowFiles are exact root-relative paths that stay in the private
// repository: the private forge's own workflows (forge-side auto-merge and
// red-main revert automation, which are not the CI template) and the release
// trust anchor pinning the internal agent principal, which only the private
// release recipe reads. Everything else is published -- the agent files
// (CLAUDE.md, AGENTS.md, .claude/, .serena/) and every other workflow under
// .github/workflows included (owner ruling on #406 and #407, option (a)) --
// and scanned like any other file. The list is exact on purpose: a file of
// the same name elsewhere is published.
var privateWorkflowFiles = []string{
	".github/workflows/automerge.yml",
	".github/workflows/revert-red-main.yml",
	".release-allowed-signers",
}

// isPrivateWorkflowFile reports whether a slash-separated, root-relative path
// is one of privateWorkflowFiles.
func isPrivateWorkflowFile(name string) bool {
	return slices.Contains(privateWorkflowFiles, name)
}

// gitCommand builds a git invocation against root. safe.directory is set for
// root alone so the gate reads the index the same way when the checkout is
// owned by another uid -- a bind-mounted tree inside the devcontainer, where git
// would otherwise refuse the repository and the gate would fail on ownership
// rather than on content.
func gitCommand(ctx context.Context, root string, args ...string) *exec.Cmd {
	full := append([]string{"-c", "safe.directory=" + root, "-C", root}, args...)
	return exec.CommandContext(ctx, "git", full...)
}

// trackedFiles lists the files git has in the index at root, slash-separated,
// relative to it and sorted. The index is where the published manifest starts:
// untracked working-tree litter is nobody's release problem.
func trackedFiles(ctx context.Context, root string) ([]string, error) {
	cmd := gitCommand(ctx, root, "ls-files", "-z", "--cached")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, &listError{root: root, stderr: strings.TrimSpace(stderr.String()), err: err}
	}
	var files []string
	for _, name := range strings.Split(string(out), "\x00") {
		if name != "" {
			files = append(files, name)
		}
	}
	sort.Strings(files)
	return files, nil
}

// publishedFiles lists the tracked files that are published: the git index
// minus every path .gitattributes marks export-ignore. It is the manifest the
// gate scans.
func publishedFiles(ctx context.Context, root string) ([]string, error) {
	if err := statIndex(ctx, root); err != nil {
		return nil, err
	}
	tracked, err := trackedFiles(ctx, root)
	if err != nil {
		return nil, err
	}
	ignored, err := exportIgnored(ctx, root, tracked)
	if err != nil {
		return nil, err
	}
	published := make([]string, 0, len(tracked))
	for _, name := range tracked {
		if !ignored[name] {
			published = append(published, name)
		}
	}
	return published, nil
}

// statIndex stats the git index from this process, so that go test's result
// cache keys on it. The manifest is read by git in a subprocess, which the
// cache cannot see: without this, staging a new file — whose contents the
// previous run never opened — leaves a plain `go test` reporting a cached pass
// over a tree that now leaks. A missing index (a repository with nothing
// staged yet) is not an error; ls-files then reports an empty index anyway.
func statIndex(ctx context.Context, root string) error {
	out, err := gitCommand(ctx, root, "rev-parse", "--path-format=absolute", "--git-path", "index").Output()
	if err != nil {
		return &listError{root: root, err: err}
	}
	if _, err := os.Stat(strings.TrimSpace(string(out))); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return &listError{root: root, err: err}
	}
	return nil
}

// exportIgnored asks git which of files carry the export-ignore attribute, so
// the answer follows git's own attribute rules rather than a copy of them.
func exportIgnored(ctx context.Context, root string, files []string) (map[string]bool, error) {
	var stdin bytes.Buffer
	for _, name := range files {
		stdin.WriteString(name)
		stdin.WriteByte(0)
	}
	cmd := gitCommand(ctx, root, "check-attr", "-z", "--stdin", exportIgnoreAttr)
	cmd.Stdin = &stdin
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, &listError{root: root, stderr: strings.TrimSpace(stderr.String()), err: err}
	}

	// -z output is NUL-separated triples: path, attribute, value.
	fields := strings.Split(string(out), "\x00")
	ignored := make(map[string]bool)
	for i := 0; i+2 < len(fields); i += 3 {
		if fields[i+2] == "set" {
			ignored[fields[i]] = true
		}
	}
	return ignored, nil
}
