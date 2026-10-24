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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var depsCheckScript = filepath.Join("..", "..", "scripts", "deps-check.sh")

// depsCheckTree is a throwaway tree holding a copy of deps-check.sh and a
// nested module replaced by its parent directory, the shape #188 covers.
func depsCheckTree(t *testing.T, rootRequire string) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go is not on PATH; deps-check needs the go tool to run")
	}
	script, err := os.ReadFile(depsCheckScript)
	if err != nil {
		t.Fatalf("read %s: %v", depsCheckScript, err)
	}
	dir := t.TempDir()
	files := map[string]string{
		"go.mod": "module example.com/depsprobe\n\ngo 1.24\n" + rootRequire,
		"sub/go.mod": "module example.com/depsprobe/sub\n\ngo 1.24\n\n" +
			"require example.com/depsprobe v0.0.0\n\n" +
			"replace example.com/depsprobe => ../\n",
		"scripts/deps-check.sh": string(script),
	}
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// runDepsCheck runs the gate with no network: GOPROXY=off and an empty,
// private module cache, so no lookup can succeed and none is attempted.
func runDepsCheck(t *testing.T, dir string) (int, string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "./scripts/deps-check.sh")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GOPROXY=off", "GOFLAGS=-mod=mod", "GOWORK=off", "GOTOOLCHAIN=local",
		"GOMODCACHE="+filepath.Join(t.TempDir(), "modcache"),
	)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	default:
		t.Fatalf("run deps-check: %v\n%s", err, out)
		return -1, ""
	}
}

// TestDepsCheckFailedLookupIsNotCurrent proves the gate fails closed: `go
// list -m -u` exits 0 when a module's update lookup fails and leaves .Update
// empty, which the gate once read as "current". A failed lookup is an ERROR
// line and exit 2; a module replaced by a directory still has nothing to look
// up and stays a pass.
func TestDepsCheckFailedLookupIsNotCurrent(t *testing.T) {
	t.Run("failed lookup is an error", func(t *testing.T) {
		dir := depsCheckTree(t, "\nrequire example.com/absent v1.2.3\n")
		code, out := runDepsCheck(t, dir)
		if code != 2 {
			t.Fatalf("exit %d, want 2 for a module whose lookup failed:\n%s", code, out)
		}
		if !strings.Contains(out, "ERROR .: example.com/absent v1.2.3: update lookup failed: ") {
			t.Fatalf("no ERROR line for example.com/absent:\n%s", out)
		}
		if strings.Contains(out, "is current") {
			t.Fatalf("a failed lookup was summarised as current:\n%s", out)
		}
	})

	t.Run("directory replacement stays current", func(t *testing.T) {
		dir := depsCheckTree(t, "")
		code, out := runDepsCheck(t, dir)
		if code != 0 || !strings.Contains(out, "every direct dependency is current") {
			t.Fatalf("exit %d, want 0 and the current verdict:\n%s", code, out)
		}
		if strings.Contains(out, "ERROR") {
			t.Fatalf("a module replaced by a directory was reported as an error:\n%s", out)
		}
	})
}
