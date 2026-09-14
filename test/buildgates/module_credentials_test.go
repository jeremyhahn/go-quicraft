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
	"path/filepath"
	"strings"
	"testing"
)

// moduleCredentialFile is the one place a workstation keeps the Gitea module
// credentials the benchmark image builds need (#184). It is a token on disk,
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
// left. A review caught the second and third missing after the first was in.
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
