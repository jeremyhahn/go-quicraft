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
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// moduleCredentialFile is the one place a workstation keeps the Gitea module
// credentials the benchmark image builds need. It is a token on disk,
// so every path that copies the working tree somewhere has to leave it behind.
const moduleCredentialFile = ".devcontainer/.env"

// TestTheCredentialFileLeavesTheTreeByNoRoute pins the three copies together.
// .gitignore keeps the token out of git; that is the obvious one and the only
// one that was written first. Six Dockerfiles `COPY . .` the whole context, so
// .dockerignore is what keeps the token out of the builder layer that precedes
// the secret mount -- the mount is pointless if the COPY before it already
// carried the file. And run-remote.sh rsyncs the tree to the bench host, where
// rsync does not read .gitignore and --delete leaves excluded paths alone, so
// staging must both exclude the file and remove any copy an earlier staging
// left.
func TestTheCredentialFileLeavesTheTreeByNoRoute(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, tc := range []struct {
		file string
		want string
		why  string
	}{
		{".gitignore", moduleCredentialFile, "the token would be one `git add` from history"},
		{".dockerignore", moduleCredentialFile, "`COPY . .` would bake the token into the builder layer before the secret is even mounted"},
		{"test/performance/run-remote.sh", "--exclude '" + moduleCredentialFile + "'", "rsync would stage the token onto the bench host as a file"},
		{"test/performance/run-remote.sh", "rm -f '$REMOTE_DIR/" + moduleCredentialFile + "'", "--delete never removes an excluded path, so a copy an earlier staging left would stay on the bench host"},
	} {
		body := readRepoFile(t, filepath.Join(root, tc.file))
		if !containsLine(body, tc.want) {
			t.Errorf("%s does not carry %q: %s", tc.file, tc.want, tc.why)
		}
	}
}

// containsLine reports whether some line of body, comments and surrounding
// whitespace aside, contains want. A commented-out exclude is not an exclude.
func containsLine(body, want string) bool {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.Contains(line, want) {
			return true
		}
	}
	return false
}

// devImageIgnoreFile is the per-Dockerfile ignore file BuildKit reads in place
// of the root .dockerignore for the dev image. It differs from the root file
// by exactly one entry, .git/: the dev image runs `make ci`, and the
// releasability gate inside it reads the git index.
const devImageIgnoreFile = ".devcontainer/Dockerfile.dev.dockerignore"

// gitMetadataEntry is the one root .dockerignore entry the dev image omits.
const gitMetadataEntry = ".git/"

// ignoreEntries returns the patterns of a .dockerignore-format file: every
// non-blank line that is not a comment, trimmed.
func ignoreEntries(body string) []string {
	var entries []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		entries = append(entries, line)
	}
	return entries
}

// TestTheDevImageIgnoreFileRepeatsTheRootList stops the two ignore files from
// drifting. BuildKit uses the per-Dockerfile file INSTEAD of the root one, so
// an entry added only at the root -- a new credential, a private workflow
// file -- would silently enter the dev image.
func TestTheDevImageIgnoreFileRepeatsTheRootList(t *testing.T) {
	root := filepath.Join("..", "..")
	have := make(map[string]bool)
	for _, e := range ignoreEntries(readRepoFile(t, filepath.Join(root, devImageIgnoreFile))) {
		have[e] = true
	}
	rootEntries := ignoreEntries(readRepoFile(t, filepath.Join(root, ".dockerignore")))
	if len(rootEntries) == 0 {
		t.Fatal("the root .dockerignore has no entries; the comparison proves nothing")
	}
	for _, e := range rootEntries {
		if e == gitMetadataEntry {
			continue
		}
		if !have[e] {
			t.Errorf("%s lacks the root .dockerignore entry %q; BuildKit reads it "+
				"instead of the root file, so that path would enter the dev image",
				devImageIgnoreFile, e)
		}
	}
}

// TestTheCredentialFileIsInEveryDockerIgnore keeps the token out of every
// image, whichever ignore file its build reads.
func TestTheCredentialFileIsInEveryDockerIgnore(t *testing.T) {
	root := filepath.Join("..", "..")
	var files []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", ".serena", "target":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".dockerignore") {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(files) < 2 {
		t.Fatalf("found %d ignore file(s), want the root .dockerignore and %s at least",
			len(files), devImageIgnoreFile)
	}
	for _, f := range files {
		if !slices.Contains(ignoreEntries(readRepoFile(t, f)), moduleCredentialFile) {
			t.Errorf("%s does not exclude %s; a build reading it would COPY the token into a layer",
				f, moduleCredentialFile)
		}
	}
}
