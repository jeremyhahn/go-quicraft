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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// The FIPS 140-3 gate cannot be weakened quietly
//
// The gate's whole value is that it FAILS when a regulated path executes a
// non-approved primitive. Every way of making it stop doing that is a one-line
// edit that leaves CI green and looks like a fix:
//
//   * GODEBUG=fips140=only -> fips140=on. `on` loads the Go Cryptographic
//     Module but still permits MD5, SHA-1 and arbitrary-IV AES-GCM, so the job
//     passes on exactly the thing it exists to catch.
//   * another `grep -v` in verify-fips, moving whichever package went red out
//     of the gate.
//   * dropping the step that covers the one tree already exempt from
//     approved-only, which would turn a documented exemption into no coverage
//     at all.
//
// These tests are cheap and static: they read the Makefile and the workflow as
// text. They do not run the toolchain, and they say nothing about whether the
// gate passes -- only that what CI runs is still the gate.
// ---------------------------------------------------------------------------

var (
	makefilePath = filepath.Join("..", "..", "Makefile")

	// The FIPS job moved from ci.yml to nightly.yml (go-quicraft#22, adopting
	// ci-template v1.9): it is known-red under runner contention (#24) and a
	// known-red job burning runner-minutes per push verifies nothing. It
	// still runs -- nightly, plus on demand via workflow_dispatch -- so the
	// gate this file protects is "some workflow still executes the FIPS
	// targets," not "ci.yml specifically does."
	fipsWorkflow = filepath.Join("..", "..", ".github", "workflows", "nightly.yml")

	// fipsModePattern captures the GODEBUG=fips140 mode as a WHOLE WORD. The
	// word boundary is load-bearing: "on" is a prefix of "only", so a substring
	// test cannot tell the weakened mode from the enforcing one.
	fipsModePattern = regexp.MustCompile(`GODEBUG=fips140=([a-z]+)\b`)
)

// readRepoFile reads a repository file, failing rather than skipping if it has
// moved: a gate test that silently passes when its subject is gone is worse
// than no test.
func readRepoFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v. If it moved, re-point this test rather than "+
			"deleting it -- it is the only thing standing between the FIPS gate "+
			"and a one-line weakening.", path, err)
	}
	return string(raw)
}

// makeRecipe returns the recipe lines of a Makefile target: every line after
// `name:` that begins with a tab, comments dropped. It fails if the target is
// absent, so a renamed target fails here instead of vacuously passing.
func makeRecipe(t *testing.T, makefile, name string) []string {
	t.Helper()

	lines := strings.Split(makefile, "\n")
	start := -1
	for i, line := range lines {
		if line == name+":" || strings.HasPrefix(line, name+": ") {
			start = i + 1
			break
		}
	}
	if start < 0 {
		t.Fatalf("Makefile has no target %q. It is what CI runs for the FIPS "+
			"gate; if it was renamed, re-point this test.", name)
	}

	var recipe []string
	for _, line := range lines[start:] {
		if !strings.HasPrefix(line, "\t") {
			break
		}
		body := strings.TrimSpace(line)
		if strings.HasPrefix(body, "#") || strings.HasPrefix(body, "@echo") {
			continue
		}
		recipe = append(recipe, body)
	}
	if len(recipe) == 0 {
		t.Fatalf("Makefile target %q has an empty recipe; it runs nothing.", name)
	}
	return recipe
}

// TestVerifyFIPSRunsApprovedOnly pins the mode string in the compliance target.
// fips140=on would leave the job green on a non-approved primitive, which is
// the regression the target exists to prevent.
func TestVerifyFIPSRunsApprovedOnly(t *testing.T) {
	recipe := strings.Join(makeRecipe(t, readRepoFile(t, makefilePath), "verify-fips"), "\n")

	// Matched as a whole word rather than by substring: "fips140=on" is a
	// PREFIX of "fips140=only", so a Contains check for the weakened mode
	// reports every correct recipe as broken.
	mode := fipsModePattern.FindStringSubmatch(recipe)
	if mode == nil {
		t.Fatalf("verify-fips sets no GODEBUG=fips140 mode:\n%s\n\n"+
			"Without one the module is not enforcing, and the target tests "+
			"nothing a plain `go test` does not.", recipe)
	}
	if mode[1] != "only" {
		t.Errorf("verify-fips runs GODEBUG=fips140=%s, want only:\n%s\n\n"+
			"Anything short of `only` permits non-approved algorithms -- MD5, "+
			"SHA-1 and arbitrary-IV AES-GCM all succeed -- so the compliance gate "+
			"would report green on exactly what it exists to catch.", mode[1], recipe)
	}
	if !strings.Contains(recipe, "-tags fips") {
		t.Errorf("verify-fips does not pass -tags fips:\n%s\n\n"+
			"Untagged, the non-approved algorithms are still compiled in and the "+
			"gate proves only that nothing happened to call one.", recipe)
	}
}

// TestFIPSExclusionIsOneDocumentedTree pins the SIZE of the approved-only
// deny-list at one.
//
// The exclusion is legitimate and cited: QUIC packet protection (RFC 9001
// s5.3) and the OCSP CertID (RFC 6960 s4.1.1) are arbitrary-IV AES-GCM and
// SHA-1 by protocol definition, both inside dependencies. What must not happen
// is a second entry appearing because some package went red -- that is
// route-around-the-failure, and it is indistinguishable from the first entry
// once both are in the file.
func TestFIPSExclusionIsOneDocumentedTree(t *testing.T) {
	makefile := readRepoFile(t, makefilePath)

	assign := regexp.MustCompile(`(?m)^FIPS_EXCLUDED\s*:?=\s*(.*)$`).FindStringSubmatch(makefile)
	if assign == nil {
		t.Fatal("Makefile no longer defines FIPS_EXCLUDED; the approved-only gate's " +
			"scope is no longer stated in one place.")
	}
	if got := strings.Fields(assign[1]); len(got) != 1 {
		t.Errorf("FIPS_EXCLUDED names %d package trees (%v), want exactly 1.\n\n"+
			"Every entry is a tree the approved-only gate does not judge. Adding "+
			"one because a package failed is the forbidden fix; the failure is "+
			"the finding.", len(got), got)
	}

	// The deny-list is applied with grep -v. A second filter would shrink the
	// gate just as effectively as a second FIPS_EXCLUDED entry, and would not
	// show up in the assignment above.
	recipe := strings.Join(makeRecipe(t, makefile, "verify-fips"), "\n")
	if n := strings.Count(recipe, "grep -v"); n != 1 {
		t.Errorf("verify-fips applies %d `grep -v` filters, want exactly 1:\n%s\n\n"+
			"One filter, one documented exemption. A second one narrows the gate "+
			"without touching FIPS_EXCLUDED.", n, recipe)
	}
}

// TestExcludedTreeIsStillGated pins that the one tree outside approved-only is
// covered by a target of its own, at fips140=on.
//
// Without this, "excluded from the approved-only gate" and "not tested under
// the module at all" are the same file. They are not the same claim: `on` is
// what proves the transport's own key exchange is approved, and it is what
// caught the X25519-only curve list that failed every intra-cluster handshake.
func TestExcludedTreeIsStillGated(t *testing.T) {
	makefile := readRepoFile(t, makefilePath)
	recipe := strings.Join(makeRecipe(t, makefile, "verify-fips-transport"), "\n")

	if !strings.Contains(recipe, "$(FIPS_EXCLUDED)") {
		t.Errorf("verify-fips-transport does not run $(FIPS_EXCLUDED):\n%s\n\n"+
			"It must name the same variable verify-fips excludes, so the two "+
			"targets are a partition by construction rather than by coincidence.",
			recipe)
	}
	// `on` is what this tree is expected to run at today and `only` is where it
	// would go if quic-go and x/crypto/ocsp ever stopped needing the exemption,
	// so both pass. Anything else -- no mode, or fips140=off -- means the
	// excluded tree is not gated at all.
	switch mode := fipsModePattern.FindStringSubmatch(recipe); {
	case mode == nil:
		t.Errorf("verify-fips-transport sets no GODEBUG=fips140 mode:\n%s\n\n"+
			"Then it is an ordinary `go test` run and the excluded tree is not "+
			"gated at all.", recipe)
	case mode[1] != "on" && mode[1] != "only":
		t.Errorf("verify-fips-transport runs GODEBUG=fips140=%s:\n%s\n\n"+
			"Only `on` or `only` load the Go Cryptographic Module; anything else "+
			"leaves this tree ungated.", mode[1], recipe)
	}
	if !strings.Contains(recipe, "-tags fips") {
		t.Errorf("verify-fips-transport does not pass -tags fips:\n%s\n\n"+
			"The tagged build of the transport would then never run its tests "+
			"anywhere, and the FIPS curve list could break unnoticed.", recipe)
	}
}

// TestCIRunsTheFIPSGate pins that nightly.yml actually invokes the targets
// above. A target no workflow calls protects nothing.
func TestCIRunsTheFIPSGate(t *testing.T) {
	workflow := readRepoFile(t, fipsWorkflow)

	// Comments in this workflow quote the mode strings to explain them, so the
	// search is over run: lines only -- otherwise deleting the job while
	// leaving its comment block would pass.
	var runs []string
	for _, line := range strings.Split(workflow, "\n") {
		body := strings.TrimSpace(line)
		if strings.HasPrefix(body, "#") {
			continue
		}
		if strings.HasPrefix(body, "run:") {
			runs = append(runs, strings.TrimSpace(strings.TrimPrefix(body, "run:")))
		}
	}

	for _, target := range []string{"make build-fips", "make verify-fips", "make verify-fips-transport"} {
		var found bool
		for _, run := range runs {
			if run == target {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no step in %s runs %q.\n\n"+
				"The FIPS gate is only a gate if some workflow executes it; a "+
				"Makefile target nothing calls is documentation.", fipsWorkflow, target)
		}
	}
}
