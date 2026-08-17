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

// Package buildgates holds tests for the parts of the build that are not Go
// code and therefore have no compiler and no test binary of their own: the
// analyser-version pins the Makefile gates, and the shell in the integration
// Dockerfile that decides whether a failed build is reported as a failure.
//
// They are ordinary untagged Go tests, so they run in `make test` and `make
// ci` alongside everything else. Neither builds an image nor invokes the Go
// toolchain.
package buildgates

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// .devcontainer/Dockerfile.integration: a failed build must fail the build
//
// The historical line was
//
//	go test -c ... 2>/dev/null || echo "...placeholder" && touch ... && chmod ...
//
// `||` and `&&` are equal-precedence and left-associative, so it parsed as
// ((go test || echo) && touch) && chmod: a compile failure was hidden by
// 2>/dev/null, echo succeeded, and a ZERO-BYTE executable was installed.
// execve on an empty file returns ENOEXEC, the shell runs it as a script, an
// empty script exits 0, and `docker compose --exit-code-from test-runner`
// reported the whole integration suite green.
//
// The fix is shell semantics, not Go, so the only honest way to guard it is to
// RUN the shell. These tests lift the RUN body out of the real Dockerfile,
// substitute a stub for the compile command, and assert the exit status for
// each outcome the stub can produce. Nothing here builds an image; what is
// under test is the operator precedence and the two post-build assertions,
// which is exactly what regressed.
//
// The substitution is confined to the compile command. `set -eux`, the
// `test -s`, the `-test.list | grep` and every redirect around them are the
// Dockerfile's own text, so a reintroduced `|| echo ... && touch`, a restored
// `2>/dev/null`, or a deleted assertion changes what these tests execute and
// flips a case.
// ---------------------------------------------------------------------------

// repoRoot is the module root relative to this package's directory.
const repoRoot = "../.."

// integrationDockerfile is the file under test.
const integrationDockerfile = ".devcontainer/Dockerfile.integration"

// historicalBuildCommand is the construct that shipped a green integration
// suite over a failing compile, reproduced verbatim from the Dockerfile's own
// account of it. It is not read from any file: it is the counter-example these
// tests must be able to tell apart from the current body, and pinning it as a
// literal is what keeps that discrimination provable.
const historicalBuildCommand = `go test -c -o /integration-tests -tags=integration ` +
	`./test/integration/e2e/... 2>/dev/null || echo "no tests yet, creating placeholder" ` +
	`&& touch /integration-tests && chmod +x /integration-tests`

// buildRunBody returns the shell body of the RUN instruction that compiles the
// integration test binary, with line continuations joined as the shell sees
// them.
func buildRunBody(t *testing.T) string {
	t.Helper()

	path := filepath.Join(repoRoot, integrationDockerfile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(string(data), "\n")

	for i := range lines {
		if !strings.HasPrefix(strings.TrimSpace(lines[i]), "RUN ") {
			continue
		}
		var joined []string
		for j := i; j < len(lines); j++ {
			cur := strings.TrimRight(lines[j], " \t")
			more := strings.HasSuffix(cur, `\`)
			joined = append(joined, strings.TrimSuffix(cur, `\`))
			if !more {
				break
			}
		}
		block := strings.Join(joined, " ")
		if !strings.Contains(block, "go test -c") {
			continue
		}
		return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(block), "RUN "))
	}
	t.Fatalf("%s has no RUN instruction that compiles the integration binary "+
		"with `go test -c`; the build step this test guards has moved or gone",
		integrationDockerfile)
	return ""
}

// compileCommandDelimiters are the shell tokens that end the compile command.
// Everything from the first of these onward -- redirects, `||`, `&&`, `;`, a
// pipe -- is the Dockerfile's own error handling and is preserved verbatim,
// because it is what these tests exist to execute.
var compileCommandDelimiters = []string{";", "|", "&", "2>", ">", "<", "\n"}

// stubCompileCommand replaces the `go test -c ...` command in body with a call
// to "$BUILD" and repoints the produced binary at "$BIN", leaving every other
// token exactly as written.
func stubCompileCommand(t *testing.T, body string) string {
	t.Helper()

	start := strings.Index(body, "go test -c")
	if start < 0 {
		t.Fatalf("no `go test -c` command in %q", body)
	}
	end := len(body)
	for _, d := range compileCommandDelimiters {
		if i := strings.Index(body[start:], d); i >= 0 && start+i < end {
			end = start + i
		}
	}
	script := body[:start] + ` "$BUILD" ` + body[end:]
	return strings.ReplaceAll(script, "/integration-tests", `"$BIN"`)
}

// buildOutcome is what running a stubbed build produced.
type buildOutcome struct {
	// exitedZero is true when the build step reported success.
	exitedZero bool
	// binaryRuns is true when the file the build step left behind exists and
	// executes successfully -- the property that made an empty placeholder
	// look like a passing test suite.
	binaryRuns bool
	output     string
}

// runStubbedBuild executes body with its compile command replaced by stub.
func runStubbedBuild(t *testing.T, body, stub string) buildOutcome {
	t.Helper()

	dir := t.TempDir()
	buildPath := filepath.Join(dir, "stub-build.sh")
	if err := os.WriteFile(buildPath, []byte(stub), 0o700); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	binPath := filepath.Join(dir, "integration-tests")
	env := append(os.Environ(), "BUILD="+buildPath, "BIN="+binPath)

	// A bounded context, not context.Background: the shell under test is the
	// error-handling half of a Dockerfile RUN, and a construct that hangs
	// rather than exiting would otherwise stall the suite with no output.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", stubCompileCommand(t, body))
	cmd.Env = env
	out, err := cmd.CombinedOutput()

	// Executed through sh, not execve directly: an empty file makes execve
	// return ENOEXEC, and it is the SHELL's fallback to running it as a script
	// that turned the placeholder into a silent exit 0. The container's
	// entrypoint is `exec "$@"` from /bin/sh, so this is the real path.
	runs := false
	if _, statErr := os.Stat(binPath); statErr == nil {
		runs = exec.CommandContext(ctx, "sh", "-c", binPath).Run() == nil
	}
	return buildOutcome{exitedZero: err == nil, binaryRuns: runs, output: string(out)}
}

// failingCompile writes nothing and reports failure, like a compile error.
const failingCompile = "#!/bin/sh\necho 'compile error' >&2\nexit 1\n"

func TestIntegrationBuildFailsWhenTheCompileFails(t *testing.T) {
	got := runStubbedBuild(t, buildRunBody(t), failingCompile)
	if got.exitedZero {
		t.Fatalf("the build step exited 0 on a FAILING compile. `docker compose "+
			"--exit-code-from test-runner` would report the integration suite "+
			"green having run nothing.\noutput:\n%s", got.output)
	}
}

func TestIntegrationBuildFailsOnAZeroByteBinary(t *testing.T) {
	// The outcome the old `|| echo ... && touch` construct actually produced:
	// a zero-byte executable that exits 0 when run.
	stub := "#!/bin/sh\n: > \"$BIN\"\nchmod +x \"$BIN\"\nexit 0\n"
	got := runStubbedBuild(t, buildRunBody(t), stub)
	if got.exitedZero {
		t.Fatalf("the build step exited 0 with a ZERO-BYTE binary installed. "+
			"execve returns ENOEXEC, the shell runs it as an empty script, and "+
			"it exits 0 -- a green suite that ran nothing.\noutput:\n%s", got.output)
	}
}

func TestIntegrationBuildFailsWhenNoBinaryIsProduced(t *testing.T) {
	// `go test -c` on a package with NO test files writes no output file and
	// still exits 0, so a successful-looking compile can leave nothing behind.
	got := runStubbedBuild(t, buildRunBody(t), "#!/bin/sh\nexit 0\n")
	if got.exitedZero {
		t.Fatalf("the build step exited 0 having produced no binary at all.\n"+
			"output:\n%s", got.output)
	}
}

func TestIntegrationBuildFailsWhenTheBinaryContainsNoTests(t *testing.T) {
	// Non-empty and executable, so `test -s` is satisfied; it simply has no
	// tests in it. Only the -test.list assertion catches this.
	stub := "#!/bin/sh\nprintf '#!/bin/sh\\nexit 0\\n' > \"$BIN\"\nchmod +x \"$BIN\"\n"
	got := runStubbedBuild(t, buildRunBody(t), stub)
	if got.exitedZero {
		t.Fatalf("the build step exited 0 with a binary that lists NO tests; "+
			"`test -s` alone cannot tell a real test binary from any other "+
			"non-empty file.\noutput:\n%s", got.output)
	}
}

// TestIntegrationBuildSucceedsOnARealBinary is the positive control, and it is
// what stops the cases above from passing against a build step that refuses
// everything -- including a correct compile.
func TestIntegrationBuildSucceedsOnARealBinary(t *testing.T) {
	stub := "#!/bin/sh\nprintf '#!/bin/sh\\necho TestSomething\\n' > \"$BIN\"\n" +
		"chmod +x \"$BIN\"\n"
	got := runStubbedBuild(t, buildRunBody(t), stub)
	if !got.exitedZero {
		t.Fatalf("the build step rejected a non-empty binary that lists tests; "+
			"the gate refuses everything and the failure cases above prove "+
			"nothing.\noutput:\n%s", got.output)
	}
}

// TestTheHarnessDetectsTheSwallowingConstruct is the discrimination control.
//
// The four failure cases above assert that the CURRENT body fails. On their own
// they cannot show that the harness would have caught the OLD one -- a stub
// substitution that quietly neutered the shell would make them pass over any
// body at all. So the historical construct is run through the identical
// harness and must reproduce the original defect exactly: exit 0 on a failing
// compile, and leave behind a placeholder that itself exits 0.
//
// If this ever stops reproducing, the harness has stopped executing the
// Dockerfile's error handling and every case above is worthless.
func TestTheHarnessDetectsTheSwallowingConstruct(t *testing.T) {
	got := runStubbedBuild(t, historicalBuildCommand, failingCompile)
	if !got.exitedZero {
		t.Fatalf("the historical `|| echo ... && touch` construct did NOT exit 0 "+
			"on a failing compile under this harness, so the harness is not "+
			"executing the Dockerfile's error handling and the assertions in "+
			"this file prove nothing.\noutput:\n%s", got.output)
	}
	if !got.binaryRuns {
		t.Fatalf("the historical construct did not leave behind a placeholder "+
			"that exits 0; the harness is not reproducing the defect it is "+
			"calibrated against.\noutput:\n%s", got.output)
	}
}
