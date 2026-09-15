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

package benchutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestProjectVersionIsTheVersionFile pins the two sources and their order:
// the build-time stamp wins, and without it the VERSION file at the
// repository root is what a benchmark records -- never a placeholder.
func TestProjectVersionIsTheVersionFile(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "VERSION"))
	if err != nil {
		t.Fatalf("read the repository's VERSION file: %v", err)
	}
	want := strings.TrimSpace(string(raw))
	if want == "" || want == "0.0.0" {
		t.Fatalf("VERSION file holds %q, which no result may record", want)
	}

	Version = ""
	got, err := ProjectVersion()
	if err != nil {
		t.Fatalf("ProjectVersion without a stamp: %v", err)
	}
	if got != want {
		t.Fatalf("ProjectVersion = %q, want the VERSION file's %q", got, want)
	}

	Version = "stamped"
	t.Cleanup(func() { Version = "" })
	if got, err := ProjectVersion(); err != nil || got != "stamped" {
		t.Fatalf("ProjectVersion with a stamp = %q, %v; want the stamp", got, err)
	}

	// Outside any repository there is nothing to read, and the answer is an
	// error, not a placeholder.
	Version = ""
	wd, _ := os.Getwd()
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })
	if got, err := ProjectVersion(); err == nil {
		t.Fatalf("ProjectVersion outside the repository returned %q; want an error", got)
	}
}

// TestModuleVersionFollowsTheLink pins that a linked module reports the
// version the binary carries, and that a module not linked -- or replaced
// by a directory, as go-quicraft is in this module -- is an error rather
// than a guess.
func TestModuleVersionFollowsTheLink(t *testing.T) {
	if _, err := ModuleVersion("example.com/not/linked"); err == nil {
		t.Fatal("an unlinked module reported a version")
	}
	// go-quicraft is replaced by ../../ in this module: linked, but versionless.
	if v, err := ModuleVersion("github.com/jeremyhahn/go-quicraft"); err == nil {
		t.Fatalf("a directory-replaced module reported version %q; want an error naming the replacement", v)
	}
}
