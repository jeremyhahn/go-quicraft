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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// The golangci-lint pin gate
//
// GOLANGCI_VERSION in the Makefile is the single source of truth, but
// .github/workflows/quality.yml and the two devcontainer Dockerfiles cannot
// read a make variable and each carry a literal pin. They had drifted to THREE
// versions at once, which is worse than having no analyser: each one gates on a
// different rule set while reporting that it passed the same gate.
//
// `make check-tool-versions` enforces the agreement. Its recipe is shell built
// on `git grep`, and this package deliberately runs no git, so these tests do
// not execute the recipe -- they perform the SAME discovery in Go, over the
// same roots, using the regexes lifted out of the recipe itself. Two
// consequences worth being explicit about:
//
//   - a divergent pin fails here as well as in `make ci`, which is the
//     behaviour the gate exists for;
//   - the recipe's own shell is not executed, so a syntax error in it would
//     still only be caught by running make.
//
// Lifting the regexes rather than restating them is what keeps this from
// becoming a second, silently divergent gate: weakening the recipe's pattern
// weakens the pattern these tests use, and the fixture cases below then fail
// because a pattern that finds nothing cannot find the two pins they plant.
// ---------------------------------------------------------------------------

// pinSearchRoots are the paths the recipe searches. They are asserted against
// the recipe text rather than trusted, so a root added to or dropped from the
// recipe fails here instead of quietly narrowing what is checked.
var pinSearchRoots = []string{".github", ".devcontainer", "Makefile"}

// noPinsFoundError reports that discovery matched nothing at all. It is the
// floor the recipe calls out: a version gate over an empty set passes forever,
// so a renamed file must fail rather than pass.
type noPinsFoundError struct {
	Roots []string
}

func (e *noPinsFoundError) Error() string {
	return fmt.Sprintf("no golangci-lint version pins found under %s; "+
		"discovery has broken and this gate is verifying nothing",
		strings.Join(e.Roots, ", "))
}

// pinMismatchError reports pins that disagree with the declared version.
type pinMismatchError struct {
	Want  string
	Found []string
}

func (e *pinMismatchError) Error() string {
	return fmt.Sprintf("golangci-lint pins %s disagree with GOLANGCI_VERSION=%s",
		strings.Join(e.Found, ", "), e.Want)
}

// checkPins is the gate itself: it fails on an empty discovery and on any pin
// that is not exactly want.
func checkPins(found []string, want string) error {
	if len(found) == 0 {
		return &noPinsFoundError{Roots: pinSearchRoots}
	}
	for _, v := range found {
		if v != want {
			return &pinMismatchError{Want: want, Found: found}
		}
	}
	return nil
}

// checkToolVersionsRecipe returns the body of the check-tool-versions target,
// which is every line from the target through the first blank line.
func checkToolVersionsRecipe(t *testing.T) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	start := -1
	for i, ln := range lines {
		if strings.HasPrefix(ln, "check-tool-versions:") {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatal("the Makefile has no check-tool-versions target; the analyser " +
			"pin gate has been removed")
	}
	var body []string
	for i := start; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "" {
			break
		}
		body = append(body, lines[i])
	}
	return strings.Join(body, "\n")
}

// singleQuoted returns every single-quoted string in s, in order.
var singleQuoted = regexp.MustCompile(`'([^']*)'`)

// recipeRegexes lifts the two patterns the recipe greps with: the one that
// finds a pin, and the one that extracts the version out of it.
func recipeRegexes(t *testing.T) (pin, version *regexp.Regexp) {
	t.Helper()

	recipe := checkToolVersionsRecipe(t)
	for _, root := range pinSearchRoots {
		if !strings.Contains(recipe, root) {
			t.Fatalf("check-tool-versions no longer searches %q; this test and "+
				"the recipe are checking different file sets", root)
		}
	}

	var pinSrc, verSrc string
	for _, m := range singleQuoted.FindAllStringSubmatch(recipe, -1) {
		q := m[1]
		switch {
		case strings.Contains(q, "golangci-lint") && strings.Contains(q, "[0-9]"):
			if pinSrc == "" {
				pinSrc = q
			}
		case strings.HasPrefix(q, "v[0-9]"):
			if verSrc == "" {
				verSrc = q
			}
		}
	}
	if pinSrc == "" || verSrc == "" {
		t.Fatalf("could not lift the discovery patterns out of check-tool-versions "+
			"(pin=%q version=%q); the recipe has been rewritten and this test is "+
			"no longer testing what it runs", pinSrc, verSrc)
	}

	pin, err := regexp.Compile(pinSrc)
	if err != nil {
		t.Fatalf("compile pin pattern %q: %v", pinSrc, err)
	}
	version, err = regexp.Compile(verSrc)
	if err != nil {
		t.Fatalf("compile version pattern %q: %v", verSrc, err)
	}
	return pin, version
}

// declaredGolangciVersion returns GOLANGCI_VERSION from the Makefile.
var golangciAssignment = regexp.MustCompile(`(?m)^GOLANGCI_VERSION\s*:?=\s*(\S+)`)

func declaredGolangciVersion(t *testing.T) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	m := golangciAssignment.FindStringSubmatch(string(data))
	if m == nil {
		t.Fatal("the Makefile no longer declares GOLANGCI_VERSION; the pin gate " +
			"has nothing to compare against")
	}
	return m[1]
}

// collectPins walks the given roots under base and returns every version a pin
// names, sorted and de-duplicated the way the recipe's `sort -u` does.
func collectPins(t *testing.T, base string, roots []string,
	pin, version *regexp.Regexp) []string {

	t.Helper()

	seen := make(map[string]struct{})
	for _, root := range roots {
		path := filepath.Join(base, root)
		walkErr := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if d.IsDir() {
				return nil
			}
			data, readErr := os.ReadFile(p)
			if readErr != nil {
				return readErr
			}
			for _, hit := range pin.FindAll(data, -1) {
				if v := version.Find(hit); v != nil {
					seen[string(v)] = struct{}{}
				}
			}
			return nil
		})
		if walkErr != nil {
			t.Fatalf("walk %s: %v", path, walkErr)
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// TestGolangciPinsAgreeWithTheMakefileVariable is the gate applied to the real
// tree: every literal pin under .github, .devcontainer and the Makefile must
// equal GOLANGCI_VERSION, and there must be at least one.
func TestGolangciPinsAgreeWithTheMakefileVariable(t *testing.T) {
	pin, version := recipeRegexes(t)
	want := declaredGolangciVersion(t)
	found := collectPins(t, repoRoot, pinSearchRoots, pin, version)

	if err := checkPins(found, want); err != nil {
		t.Fatalf("%v\n\nEvery literal pin must equal GOLANGCI_VERSION. A developer "+
			"or a container linting on a different version gates on a different "+
			"rule set from CI while looking like it passed the same gate.", err)
	}
}

// writePinFixture creates a fixture tree under a temp dir. It is never written
// into the repository: a fixture holding a deliberately wrong pin inside
// .github or .devcontainer would be discovered by the real gate and would fail
// `make ci`.
func writePinFixture(t *testing.T, files map[string]string) string {
	t.Helper()

	base := t.TempDir()
	for name, content := range files {
		full := filepath.Join(base, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", full, err)
		}
	}
	return base
}

// dockerfilePin and workflowPin reproduce the two textual forms the repository
// actually uses, so a discovery pattern that stopped recognising either one
// fails the fixture cases instead of silently checking nothing.
const (
	dockerfilePin = "RUN go install github.com/golangci/golangci-lint/v2/cmd/" +
		"golangci-lint@%s && \\\n    go install example.com/other@latest\n"
	workflowPin = "        run: go install github.com/golangci/golangci-lint/v2/" +
		"cmd/golangci-lint@%s\n"
	// spacedPin exercises the pattern's second alternative, where the version
	// follows the tool name after other text rather than after an '@'.
	spacedPin = "# pinned: golangci-lint must be %s\n"
)

func TestPinCheckFailsWhenDiscoveryFindsNothing(t *testing.T) {
	pin, version := recipeRegexes(t)

	// A tree with the right shape and no pins in it at all -- what a renamed
	// or moved file leaves behind.
	base := writePinFixture(t, map[string]string{
		".devcontainer/Dockerfile":      "FROM golang:1.26\nRUN go install example.com/x@latest\n",
		".github/workflows/quality.yml": "jobs:\n  lint:\n    steps:\n      - run: echo hi\n",
		"Makefile":                      "all:\n\techo hi\n",
	})

	found := collectPins(t, base, pinSearchRoots, pin, version)
	if len(found) != 0 {
		t.Fatalf("fixture was expected to hold no pins, discovery found %v", found)
	}

	var missing *noPinsFoundError
	if err := checkPins(found, "v2.14.0"); !errors.As(err, &missing) {
		t.Fatalf("checkPins over an empty discovery = %v, want *noPinsFoundError. "+
			"A version gate that passes on zero inputs verifies nothing and "+
			"stays green through exactly the drift it exists to catch.", err)
	}
}

func TestPinCheckFailsWhenAPinDiverges(t *testing.T) {
	pin, version := recipeRegexes(t)

	base := writePinFixture(t, map[string]string{
		".devcontainer/Dockerfile":      fmt.Sprintf(dockerfilePin, "v1.2.3"),
		".github/workflows/quality.yml": fmt.Sprintf(workflowPin, "v9.9.9"),
	})

	found := collectPins(t, base, pinSearchRoots, pin, version)
	if len(found) != 2 {
		t.Fatalf("discovery found %v, want both planted pins. The pattern lifted "+
			"from the recipe no longer recognises the forms the repository "+
			"writes, so the real gate is checking less than it appears to.", found)
	}

	var mismatch *pinMismatchError
	if err := checkPins(found, "v1.2.3"); !errors.As(err, &mismatch) {
		t.Fatalf("checkPins over %v with want=v1.2.3 = %v, want *pinMismatchError",
			found, err)
	}
}

func TestPinCheckPassesWhenEveryPinAgrees(t *testing.T) {
	pin, version := recipeRegexes(t)

	base := writePinFixture(t, map[string]string{
		".devcontainer/Dockerfile":      fmt.Sprintf(dockerfilePin, "v1.2.3"),
		".devcontainer/Dockerfile.dev":  fmt.Sprintf(dockerfilePin, "v1.2.3"),
		".github/workflows/quality.yml": fmt.Sprintf(workflowPin, "v1.2.3"),
		"Makefile":                      fmt.Sprintf(spacedPin, "v1.2.3"),
	})

	found := collectPins(t, base, pinSearchRoots, pin, version)
	if len(found) != 1 || found[0] != "v1.2.3" {
		t.Fatalf("discovery found %v, want exactly [v1.2.3]; all four planted "+
			"forms must be recognised and collapse to one version", found)
	}
	if err := checkPins(found, "v1.2.3"); err != nil {
		t.Fatalf("checkPins over agreeing pins = %v, want nil. The gate refuses "+
			"everything and the failure cases above prove nothing.", err)
	}
}

// TestCheckToolVersionsIsWiredIntoCI pins the gate to the pipeline. A gate
// nothing runs is the same as no gate, and `ci` is the target both the local
// inner loop and .github/workflows use.
func TestCheckToolVersionsIsWiredIntoCI(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	ciRule := regexp.MustCompile(`(?m)^ci:[^\n]*`).Find(data)
	if ciRule == nil {
		t.Fatal("the Makefile has no `ci:` target")
	}
	if !strings.Contains(string(ciRule), "check-tool-versions") {
		t.Fatalf("`ci` does not depend on check-tool-versions (%q); the pin gate "+
			"exists but nothing runs it", string(ciRule))
	}
}
