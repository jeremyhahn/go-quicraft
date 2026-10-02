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
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// The analyser build-toolchain gate
//
// GOLANGCI_VERSION pins WHICH ANALYSER, not WHICH GO BUILT IT, and only the
// first half is visible to the pin gate in tool_versions_test.go. The second
// half is what broke local linting when go.mod moved to `go 1.27.0`: a
// correctly pinned golangci-lint built with go1.26.2 refuses the module before
// reading a line of source, while CI's identically-versioned binary — built
// with the runner image's 1.27 — passes. Same version string, opposite
// outcome.
//
// `make check-lint-toolchain` closes that half by reading the build toolchain
// out of the binary's own self-report. Unlike the pin recipe, this one is
// EXECUTED here rather than re-implemented: it is short, it touches nothing
// outside the analyser it is pointed at, and re-implementing a comparison in
// Go would test a copy of the logic instead of the shell that actually runs.
// Each case points TOOLS_BIN at a stub that reports a chosen version string,
// which is the one input the guard reads.
// ---------------------------------------------------------------------------

// analyserReport is the shape golangci-lint's --version prints. The guard
// parses "has version X" and "built with goY" out of it, so the stub reproduces
// the surrounding noise too: a parser that only works on a tidied-up string is
// not the parser this repository needs.
const analyserReport = "golangci-lint has version %s built with go%s " +
	`from (unknown, modified: ?, mod sum: "h1:deadbeef") on (unknown)` + "\n"

// stubAnalyser writes an executable that reports the given versions and returns
// the directory holding it, ready to pass as TOOLS_BIN.
func stubAnalyser(t *testing.T, version, builtWith string) string {
	t.Helper()

	dir := t.TempDir()
	body := "#!/bin/sh\nprintf '%s'\n"
	script := fmt.Sprintf(body, fmt.Sprintf(analyserReport, version, builtWith))
	path := filepath.Join(dir, "golangci-lint")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write stub analyser: %v", err)
	}
	return dir
}

// runGuard runs `make check-lint-toolchain` against the given TOOLS_BIN and
// returns its combined output and whether it exited zero. extraEnv entries
// override the inherited environment.
func runGuard(t *testing.T, toolsBin string, extraEnv ...string) (string, bool) {
	t.Helper()

	if _, err := exec.LookPath("make"); err != nil {
		t.Skipf("make is not installed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "make", "-C", repoRoot,
		"check-lint-toolchain", "TOOLS_BIN="+toolsBin)
	cmd.Env = append(os.Environ(), extraEnv...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("make timed out: %v", ctx.Err())
	}
	return string(out), err == nil
}

// moduleGoDirective returns the `go` directive from go.mod, which is the floor
// the guard compares an analyser's build toolchain against.
var goDirective = regexp.MustCompile(`(?m)^go\s+(\S+)`)

func moduleGoDirective(t *testing.T) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	m := goDirective.FindSubmatch(data)
	if m == nil {
		t.Fatal("go.mod has no `go` directive; the toolchain gate has no floor " +
			"to compare against")
	}
	return string(m[1])
}

// pinnedAnalyserVersion is GOLANGCI_VERSION without its leading v, which is the
// form the tool itself reports. declaredGolangciVersion lives in
// tool_versions_test.go; both gates read the same single source of truth.
func pinnedAnalyserVersion(t *testing.T) string {
	t.Helper()
	return strings.TrimPrefix(declaredGolangciVersion(t), "v")
}

// TestGuardRejectsAnAnalyserBuiltByAnOlderGo pins the toolchain guard.
//
// The message matters as much as the exit code. Without the guard, a developer
// meets golangci-lint's own "the Go language version (go1.26) used to build
// golangci-lint is lower than the targeted Go version" with nothing saying
// which binary produced it or what to run — so the failure is diagnosed once
// per developer, and in the meantime nobody lints before pushing.
func TestGuardRejectsAnAnalyserBuiltByAnOlderGo(t *testing.T) {
	need := moduleGoDirective(t)
	bin := stubAnalyser(t, pinnedAnalyserVersion(t), "1.26.2")

	out, ok := runGuard(t, bin)
	if ok {
		t.Fatalf("guard passed an analyser built with go1.26.2 against a module "+
			"targeting %s. That analyser cannot lint this module at all:\n%s", need, out)
	}
	for _, want := range []string{"built with go1.26.2", need, "make install-lint"} {
		if !strings.Contains(out, want) {
			t.Fatalf("guard output does not mention %q; a developer cannot act on "+
				"it:\n%s", want, out)
		}
	}
}

// TestGuardComparesToolchainsNumerically pins the comparison itself.
//
// A lexical string compare passes go1.9 against a 1.27 module, because "1.9"
// sorts after "1.27". That is the one wrong answer a hand-rolled comparison
// reaches by default, and it fails OPEN: the analyser is ancient and the gate
// waves it through.
func TestGuardComparesToolchainsNumerically(t *testing.T) {
	version := pinnedAnalyserVersion(t)

	if out, ok := runGuard(t, stubAnalyser(t, version, "1.9.0")); ok {
		t.Fatalf("guard passed an analyser built with go1.9.0. The comparison is "+
			"lexical, not numeric, and lets every go1.9-era binary through:\n%s", out)
	}
	if out, ok := runGuard(t, stubAnalyser(t, version, "99.0.0")); !ok {
		t.Fatalf("guard rejected an analyser built with go99.0.0, which is newer "+
			"than any `go` directive this module can carry:\n%s", out)
	}
}

// TestGuardAcceptsTheModulesOwnDirective is the floor case: an analyser built
// by exactly the Go the module targets must pass, or the gate refuses
// everything and the rejections above prove nothing.
func TestGuardAcceptsTheModulesOwnDirective(t *testing.T) {
	need := moduleGoDirective(t)
	bin := stubAnalyser(t, pinnedAnalyserVersion(t), need)

	if out, ok := runGuard(t, bin); !ok {
		t.Fatalf("guard rejected an analyser built with exactly go%s, the version "+
			"this module targets:\n%s", need, out)
	}
}

// TestGuardRejectsTheWrongAnalyserVersion covers the other half. The pin gate
// checks the versions written in files; this one checks the version of the
// binary that will actually run, which no amount of grepping can see.
func TestGuardRejectsTheWrongAnalyserVersion(t *testing.T) {
	bin := stubAnalyser(t, "0.0.1", moduleGoDirective(t))

	out, ok := runGuard(t, bin)
	if ok {
		t.Fatalf("guard passed an analyser at 0.0.1 while this repo pins %s; a "+
			"developer would gate on a different rule set from CI while looking "+
			"like they passed the same gate:\n%s", declaredGolangciVersion(t), out)
	}
	if !strings.Contains(out, "make install-lint") {
		t.Fatalf("guard output does not say how to fix the version:\n%s", out)
	}
}

// TestGuardReportsAMissingAnalyser keeps the no-tool case actionable too. With
// nothing installed the old `make lint` died on "golangci-lint: command not
// found" from the shell, which names neither the pin nor the way to satisfy it.
func TestGuardReportsAMissingAnalyser(t *testing.T) {
	empty := t.TempDir()

	// A PATH holding the standard utilities the recipe uses (sh, sed, sort,
	// head, printf) but no analyser, so resolution finds nothing anywhere.
	out, ok := runGuard(t, empty, "PATH=/usr/bin:/bin")
	if ok {
		t.Skipf("a golangci-lint is present on the minimal PATH; the "+
			"missing-analyser branch cannot be exercised here:\n%s", out)
	}
	for _, want := range []string{"no golangci-lint found", "make install-lint"} {
		if !strings.Contains(out, want) {
			t.Fatalf("guard output does not mention %q:\n%s", want, out)
		}
	}
}

// TestLintRunsTheResolvedAnalyser pins the wiring. The guard is worth nothing
// if `make lint` then runs whatever golangci-lint PATH offers first — which is
// exactly what it did, and why an unusable binary in ~/go/bin was what every
// local lint attempt reached.
func TestLintRunsTheResolvedAnalyser(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	rule := regexp.MustCompile(`(?m)^lint:[^\n]*\n(?:\t[^\n]*\n)+`).Find(data)
	if rule == nil {
		t.Fatal("the Makefile has no `lint:` target")
	}
	text := string(rule)
	if !strings.Contains(text, "check-lint-toolchain") {
		t.Fatalf("`lint` does not depend on check-lint-toolchain (%q); it will "+
			"surface the analyser's own version error instead of the guard's "+
			"message", text)
	}
	if !strings.Contains(text, "$(GOLANGCI_BIN)") {
		t.Fatalf("`lint` does not run $(GOLANGCI_BIN) (%q); the guard would check "+
			"one binary and the recipe would lint with another", text)
	}
}

// TestToolchainGuardIsWiredIntoCI is the companion of
// TestCheckToolVersionsIsWiredIntoCI: a gate nothing runs is the same as no
// gate. `ci` reaches this one through check-tool-versions, so the two analyser
// checks — the pins in files, and the binary on disk — always run together.
func TestToolchainGuardIsWiredIntoCI(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	rule := regexp.MustCompile(`(?m)^check-tool-versions:[^\n]*`).Find(data)
	if rule == nil {
		t.Fatal("the Makefile has no check-tool-versions target")
	}
	if !strings.Contains(string(rule), "check-lint-toolchain") {
		t.Fatalf("check-tool-versions does not depend on check-lint-toolchain "+
			"(%q); `make ci` checks the pins in files and never the binary it "+
			"is about to lint with", string(rule))
	}
}
