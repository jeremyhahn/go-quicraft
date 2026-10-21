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

var apiCheckScript = filepath.Join("..", "..", "scripts", "api-check.sh")

const apiGateModule = `module example.com/apigate

go 1.24
`

const apiGateSource = `package apigate

// Kept stays in every revision.
func Kept() {}

// Planted is removed after the base tag.
func Planted() {}
`

// apiGateRepo is a throwaway git repository holding one module and a copy of
// the gate, tagged at its first commit.
type apiGateRepo struct {
	t   *testing.T
	dir string
	env []string
}

func newAPIGateRepo(t *testing.T, tag string) *apiGateRepo {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not on PATH")
	}
	script, err := os.ReadFile(apiCheckScript)
	if err != nil {
		t.Fatalf("read %s: %v", apiCheckScript, err)
	}
	r := &apiGateRepo{t: t, dir: t.TempDir()}
	// The repository is built with no global or system git configuration, so
	// it needs no identity or signing setup of the host's.
	r.env = append(os.Environ(),
		"GIT_CONFIG_GLOBAL="+filepath.Join(r.dir, ".gitconfig-none"),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=gate", "GIT_AUTHOR_EMAIL=gate@example.com",
		"GIT_COMMITTER_NAME=gate", "GIT_COMMITTER_EMAIL=gate@example.com",
		"GOWORK=off", "GOFLAGS=-mod=mod",
	)
	r.write("go.mod", apiGateModule)
	r.write("apigate.go", apiGateSource)
	r.write("VERSION", strings.TrimPrefix(tag, "v")+"\n")
	r.write("scripts/api-check.sh", string(script))
	if err := os.Chmod(filepath.Join(r.dir, "scripts", "api-check.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	r.git("init", "-q")
	r.git("add", ".")
	r.git("commit", "-q", "-m", "base")
	r.git("tag", tag)
	return r
}

func (r *apiGateRepo) write(name, body string) {
	r.t.Helper()
	p := filepath.Join(r.dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

func (r *apiGateRepo) git(args ...string) {
	r.t.Helper()
	cmd := exec.CommandContext(r.t.Context(), "git", args...)
	cmd.Dir, cmd.Env = r.dir, r.env
	if out, err := cmd.CombinedOutput(); err != nil {
		r.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// removePlanted makes the working tree drop Planted and claim version.
func (r *apiGateRepo) removePlanted(version string) {
	r.write("apigate.go", strings.Replace(apiGateSource,
		"\n// Planted is removed after the base tag.\nfunc Planted() {}\n", "", 1))
	r.write("VERSION", version+"\n")
}

// run executes the gate and returns its exit code and combined output.
func (r *apiGateRepo) run(apidiff string) (int, string) {
	r.t.Helper()
	cmd := exec.CommandContext(r.t.Context(), "./scripts/api-check.sh")
	cmd.Dir = r.dir
	cmd.Env = append(r.env, "API_MODULES=.", "API_DIFF_BIN="+apidiff)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	default:
		r.t.Fatalf("run api-check: %v\n%s", err, out)
		return -1, ""
	}
}

// buildAPIDiff builds the apidiff the root go.mod pins.
func buildAPIDiff(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "apidiff")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", bin, "golang.org/x/exp/cmd/apidiff")
	cmd.Dir = filepath.Join("..", "..")
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the pinned apidiff: %v\n%s", err, out)
	}
	return bin
}

// TestAPICheckGate proves the exported-API gate's whole policy on a planted
// removal: it fails after v1.0.0, passes for a new major, and before v1.0.0
// prints the change without failing.
func TestAPICheckGate(t *testing.T) {
	if testing.Short() {
		t.Skip("builds apidiff and runs the gate on a temporary module")
	}
	apidiff := buildAPIDiff(t)

	t.Run("compatible change passes", func(t *testing.T) {
		r := newAPIGateRepo(t, "v1.0.0")
		r.write("apigate.go", apiGateSource+"\n// Added is new.\nfunc Added() {}\n")
		code, out := r.run(apidiff)
		if code != 0 || !strings.Contains(out, "compatible with v1.0.0") {
			t.Fatalf("exit %d, want 0 with a compatible verdict:\n%s", code, out)
		}
	})

	t.Run("removal after v1 fails", func(t *testing.T) {
		r := newAPIGateRepo(t, "v1.0.0")
		r.removePlanted("1.1.0")
		code, out := r.run(apidiff)
		if code != 1 || !strings.Contains(out, "Planted") {
			t.Fatalf("exit %d, want 1 naming the removed Planted:\n%s", code, out)
		}
	})

	t.Run("removal in a new major passes", func(t *testing.T) {
		r := newAPIGateRepo(t, "v1.0.0")
		r.removePlanted("2.0.0")
		code, out := r.run(apidiff)
		if code != 0 || !strings.Contains(out, "Planted") {
			t.Fatalf("exit %d, want 0 still reporting Planted:\n%s", code, out)
		}
	})

	t.Run("removal before v1 is reported and passes", func(t *testing.T) {
		r := newAPIGateRepo(t, "v0.23.0-alpha")
		r.removePlanted("0.24.0-alpha")
		code, out := r.run(apidiff)
		if code != 0 || !strings.Contains(out, "Planted") || !strings.Contains(out, "before v1.0.0") {
			t.Fatalf("exit %d, want 0 reporting Planted as pre-v1:\n%s", code, out)
		}
	})

	t.Run("removed package after v1 fails", func(t *testing.T) {
		r := newAPIGateRepo(t, "v1.0.0")
		r.write("sub/sub.go", "package sub\n\nfunc S() {}\n")
		r.git("add", "sub/sub.go")
		r.git("commit", "-q", "-m", "sub")
		r.git("tag", "-f", "v1.0.0")
		if err := os.RemoveAll(filepath.Join(r.dir, "sub")); err != nil {
			t.Fatal(err)
		}
		code, out := r.run(apidiff)
		if code != 1 || !strings.Contains(out, "package removed") {
			t.Fatalf("exit %d, want 1 reporting the removed package:\n%s", code, out)
		}
	})
}
