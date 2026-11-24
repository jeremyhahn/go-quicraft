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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// requirePrivateWorkflowFile skips t when rel is export-ignored in .gitattributes and fails it when rel is missing otherwise.
func requirePrivateWorkflowFile(t *testing.T, rel string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(repoRoot, filepath.FromSlash(rel)))
	if err == nil {
		return
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat %s: %v", rel, err)
	}
	if exportIgnoresExactly(t, rel) {
		t.Skipf("%s is export-ignored and absent: this tree is a published export, and this "+
			"gate runs in the private repository", rel)
	}
	t.Fatalf("%s is missing and .gitattributes does not export-ignore it: %v", rel, err)
}

// exportIgnoresExactly reports whether .gitattributes carries an
// `export-ignore` line naming rel itself (optionally anchored with a leading
// slash). A missing .gitattributes excludes nothing.
func exportIgnoresExactly(t *testing.T, rel string) bool {
	t.Helper()
	attrs, err := os.ReadFile(filepath.Join(repoRoot, ".gitattributes"))
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatalf("read .gitattributes: %v", err)
	}
	for _, line := range strings.Split(string(attrs), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == "export-ignore" && strings.TrimPrefix(fields[0], "/") == rel {
			return true
		}
	}
	return false
}

// TestPrivateWorkflowFilesAreExportIgnored keeps requirePrivateWorkflowFile
// honest in the private repository: every file it may skip on is present here
// AND export-ignored, so the skip can only ever fire in a published export.
func TestPrivateWorkflowFilesAreExportIgnored(t *testing.T) {
	for _, rel := range []string{".release-allowed-signers"} {
		if !exportIgnoresExactly(t, rel) {
			t.Errorf("%s is read by a gate through requirePrivateWorkflowFile but .gitattributes "+
				"does not export-ignore it; the gate would fail, not skip, in an export", rel)
		}
	}
}
