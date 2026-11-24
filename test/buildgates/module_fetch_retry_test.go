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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// toolchainDockerfile builds the CI toolchain image whose tools stage installs
// the lint and scan tools from the module proxy.
const toolchainDockerfile = "build/docker/devcontainer/Dockerfile"

// dragonboatMakefile holds the contrib module's CI targets.
const dragonboatMakefile = "contrib/dragonboat/Makefile"

// goFetchCommand matches a `go install` or `go get` invocation.
var goFetchCommand = regexp.MustCompile(`\bgo (install|get)\b`)

// dockerfileStage returns the lines of the named stage: from its
// `FROM ... AS <name>` line up to the next FROM.
func dockerfileStage(t *testing.T, body, name string) []string {
	t.Helper()
	var out []string
	in := false
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(trimmed), "FROM ") {
			if in {
				break
			}
			fields := strings.Fields(trimmed)
			in = len(fields) >= 4 && strings.EqualFold(fields[len(fields)-2], "AS") && fields[len(fields)-1] == name
			continue
		}
		if in {
			out = append(out, line)
		}
	}
	if len(out) == 0 {
		t.Fatalf("%s has no %q stage", toolchainDockerfile, name)
	}
	return out
}

// TestToolsStageRetriesEveryModuleFetch keeps one transient proxy error from
// failing the toolchain image build: every `go install` and `go get` in the
// tools stage goes through the stage's retry function.
func TestToolsStageRetriesEveryModuleFetch(t *testing.T) {
	stage := dockerfileStage(t, readFile(t, filepath.Join(repoRoot, toolchainDockerfile)), "tools")
	defined := false
	fetches := 0
	for _, line := range stage {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.Contains(trimmed, "retry()") {
			defined = true
		}
		for _, loc := range goFetchCommand.FindAllStringIndex(trimmed, -1) {
			fetches++
			if !strings.HasSuffix(trimmed[:loc[0]], "retry ") {
				t.Errorf("%s tools stage fetches modules without retry: %s", toolchainDockerfile, trimmed)
			}
		}
	}
	if !defined {
		t.Errorf("%s tools stage does not define a retry() function", toolchainDockerfile)
	}
	if fetches == 0 {
		t.Fatalf("%s tools stage has no go install/go get; the check proves nothing", toolchainDockerfile)
	}
}

// makeTargetPrereqs returns the prerequisites of a make rule `name: ...`.
func makeTargetPrereqs(body, name string) ([]string, bool) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+":") && !strings.HasPrefix(line, name+":=") {
			return strings.Fields(strings.TrimPrefix(line, name+":")), true
		}
	}
	return nil, false
}

// TestDragonboatCITargetsDownloadModulesFirst keeps the contrib CI targets
// from fetching modules mid-test with no retry: they depend on mod-download,
// which retries `go mod download`.
func TestDragonboatCITargetsDownloadModulesFirst(t *testing.T) {
	body := readFile(t, filepath.Join(repoRoot, dragonboatMakefile))
	if _, ok := makeTargetPrereqs(body, "mod-download"); !ok {
		t.Fatalf("%s has no mod-download target", dragonboatMakefile)
	}
	if !strings.Contains(body, "mod download") {
		t.Errorf("%s mod-download does not run go mod download", dragonboatMakefile)
	}
	for _, target := range []string{"ci-test", "ci-integration"} {
		prereqs, ok := makeTargetPrereqs(body, target)
		if !ok {
			t.Errorf("%s has no %s target", dragonboatMakefile, target)
			continue
		}
		if !slices.Contains(prereqs, "mod-download") {
			t.Errorf("%s %s does not depend on mod-download (prerequisites %v)", dragonboatMakefile, target, prereqs)
		}
	}
}

// stubGoScript is a stand-in for go: it records each call's arguments and
// exits 7 for its first GO_STUB_FAILS calls, then 0.
const stubGoScript = `#!/bin/sh
echo "$*" >> "$STUB_DIR/go.calls"
n=$(wc -l < "$STUB_DIR/go.calls")
[ "$n" -gt "$GO_STUB_FAILS" ] && exit 0
exit 7
`

// stubSleepScript is a stand-in for sleep: it records its argument and
// returns at once, so the retry backoff costs no wall time.
const stubSleepScript = `#!/bin/sh
echo "$*" >> "$STUB_DIR/sleep.calls"
`

// stubGoFailStatus is the status stubGoScript exits with on a failed call.
const stubGoFailStatus = 7

// retryCases are the behaviours every module fetch retry must show: the
// number of go calls, the backoff sleeps between them, and whether the
// fetch finally succeeds.
var retryCases = []struct {
	name    string
	fails   int
	goCalls int
	sleeps  []string
	ok      bool
}{
	{name: "immediate success", fails: 0, goCalls: 1, sleeps: nil, ok: true},
	{name: "fails once then succeeds", fails: 1, goCalls: 2, sleeps: []string{"5"}, ok: true},
	{name: "always fails", fails: 99, goCalls: 3, sleeps: []string{"5", "15"}, ok: false},
}

// requireTools skips the test when a tool it drives is not on PATH.
func requireTools(t *testing.T, tools ...string) {
	t.Helper()
	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not on PATH", tool)
		}
	}
}

// newRetryStub writes the stub go and sleep into a fresh directory and
// returns it; the stubs live in its bin subdirectory.
func newRetryStub(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, script := range map[string]string{"go": stubGoScript, "sleep": stubSleepScript} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o755); err != nil { //nolint:gosec // test stub must be executable
			t.Fatal(err)
		}
	}
	return dir
}

// stubEnv is the environment that puts the stubs in dir first in PATH and
// scripts the stub go to fail its first fails calls. Make's own variables
// are dropped so an outer make does not leak flags into the one under test.
func stubEnv(dir string, fails int) []string {
	var env []string
	for _, kv := range os.Environ() {
		key, _, _ := strings.Cut(kv, "=")
		switch key {
		case "PATH", "MAKEFLAGS", "MFLAGS", "MAKELEVEL", "STUB_DIR", "GO_STUB_FAILS":
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"PATH="+filepath.Join(dir, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"),
		"STUB_DIR="+dir,
		"GO_STUB_FAILS="+strconv.Itoa(fails),
	)
}

// stubCalls returns the argument lines a stub recorded, one per call.
func stubCalls(t *testing.T, dir, name string) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name+".calls")) //nolint:gosec // path is under t.TempDir
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

// toolsStageRetryFunction returns the retry() definition from the tools
// stage RUN body, its continuation lines joined the way the Dockerfile
// parser joins them.
func toolsStageRetryFunction(t *testing.T) string {
	t.Helper()
	stage := dockerfileStage(t, readFile(t, filepath.Join(repoRoot, toolchainDockerfile)), "tools")
	var b strings.Builder
	in := false
	for _, line := range stage {
		if !in && !strings.Contains(line, "retry() {") {
			continue
		}
		if !in {
			line = line[strings.Index(line, "retry() {"):]
			in = true
		}
		part := strings.TrimSpace(strings.TrimSuffix(strings.TrimRight(line, " \t"), "\\"))
		b.WriteString(part)
		b.WriteString(" ")
		if strings.HasSuffix(part, "};") {
			return b.String()
		}
	}
	t.Fatalf("%s tools stage has no complete retry() { ... }; definition", toolchainDockerfile)
	return ""
}

// TestToolsStageRetryFunctionRetriesTransientFailures runs the tools-stage
// retry function under sh with a stub go and sleep: it must stop on the
// first success, back off 5 then 15 seconds, give up after three attempts,
// and return the last failure's status so the RUN fails.
func TestToolsStageRetryFunctionRetriesTransientFailures(t *testing.T) {
	requireTools(t, "sh")
	fn := toolsStageRetryFunction(t)
	for _, tc := range retryCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := newRetryStub(t)
			script := filepath.Join(dir, "driver.sh")
			driver := "set -eu\n" + fn + "\nretry go install example.com/tool@v1\n"
			if err := os.WriteFile(script, []byte(driver), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), "sh", script)
			cmd.Env = stubEnv(dir, tc.fails)
			out, err := cmd.CombinedOutput()
			if tc.ok && err != nil {
				t.Fatalf("retry failed: %v\n%s", err, out)
			}
			if !tc.ok {
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != stubGoFailStatus {
					t.Fatalf("retry returned %v, want exit status %d from the last attempt\n%s", err, stubGoFailStatus, out)
				}
			}
			calls := stubCalls(t, dir, "go")
			if len(calls) != tc.goCalls {
				t.Errorf("go ran %d times %q, want %d", len(calls), calls, tc.goCalls)
			}
			for _, c := range calls {
				if c != "install example.com/tool@v1" {
					t.Errorf("go ran with %q, want the retried command's arguments", c)
				}
			}
			if sleeps := stubCalls(t, dir, "sleep"); !slices.Equal(sleeps, tc.sleeps) {
				t.Errorf("sleeps = %q, want %q", sleeps, tc.sleeps)
			}
		})
	}
}

// runDragonboatMake runs a contrib/dragonboat make target with the stub go
// as GO and the stub sleep first in PATH.
func runDragonboatMake(t *testing.T, dir string, fails int, target string) ([]byte, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "make", "-C", filepath.Join(repoRoot, filepath.Dir(dragonboatMakefile)), target,
		"GO="+filepath.Join(dir, "bin", "go"))
	cmd.Env = stubEnv(dir, fails)
	return cmd.CombinedOutput()
}

// TestDragonboatModDownloadRetriesTransientFailures runs make mod-download
// with a stub go and sleep: the same attempt count, backoff and final
// failure as the tools-stage retry.
func TestDragonboatModDownloadRetriesTransientFailures(t *testing.T) {
	requireTools(t, "sh", "make")
	for _, tc := range retryCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := newRetryStub(t)
			out, err := runDragonboatMake(t, dir, tc.fails, "mod-download")
			if tc.ok && err != nil {
				t.Fatalf("make mod-download failed: %v\n%s", err, out)
			}
			if !tc.ok && err == nil {
				t.Fatalf("make mod-download succeeded after every download failed\n%s", out)
			}
			calls := stubCalls(t, dir, "go")
			if len(calls) != tc.goCalls {
				t.Errorf("go ran %d times %q, want %d", len(calls), calls, tc.goCalls)
			}
			for _, c := range calls {
				if c != "mod download" {
					t.Errorf("go ran with %q, want mod download", c)
				}
			}
			if sleeps := stubCalls(t, dir, "sleep"); !slices.Equal(sleeps, tc.sleeps) {
				t.Errorf("sleeps = %q, want %q", sleeps, tc.sleeps)
			}
		})
	}
}

// TestDragonboatCITestStopsWhenDownloadsExhausted runs make ci-test with a
// go that always fails: make must fail and never reach go test.
func TestDragonboatCITestStopsWhenDownloadsExhausted(t *testing.T) {
	requireTools(t, "sh", "make")
	dir := newRetryStub(t)
	out, err := runDragonboatMake(t, dir, 99, "ci-test")
	if err == nil {
		t.Fatalf("make ci-test succeeded after every module download failed\n%s", out)
	}
	calls := stubCalls(t, dir, "go")
	if len(calls) == 0 {
		t.Fatalf("make ci-test never ran go\n%s", out)
	}
	for _, c := range calls {
		if c == "test" || strings.HasPrefix(c, "test ") {
			t.Errorf("make ci-test ran go %s after the module downloads were exhausted", c)
		}
	}
}
