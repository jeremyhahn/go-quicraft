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
	"os/exec"
	"path"
	"sort"
	"strings"
)

// exportIgnoreAttr is the git attribute that keeps a tracked file out of every
// archive git builds from the tree, including the source tarball a forge serves
// for a tag. .gitattributes is therefore the one exclusion list.
const exportIgnoreAttr = "export-ignore"

// developmentToolingNames are file names that belong to the private
// development workflow wherever they appear: per-directory agent instructions.
var developmentToolingNames = []string{"CLAUDE.md", "AGENTS.md"}

// developmentToolingDirs are directories, at any depth, that belong to the
// private development workflow: agent definitions, skills and working notes.
var developmentToolingDirs = []string{".claude", ".serena"}

// isDevelopmentTooling reports whether a slash-separated, root-relative path is
// part of the private development workflow. Such files name the private forge
// and internal identities; they stay in the private repository.
func isDevelopmentTooling(name string) bool {
	for _, base := range developmentToolingNames {
		if path.Base(name) == base {
			return true
		}
	}
	segments := strings.Split(name, "/")
	for _, segment := range segments[:len(segments)-1] {
		for _, dir := range developmentToolingDirs {
			if segment == dir {
				return true
			}
		}
	}
	return false
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
