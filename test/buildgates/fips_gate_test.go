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

// Static tests that the FIPS 140-3 gate still runs fips140=only with exactly one documented exclusion.

var (
	makefilePath = filepath.Join("..", "..", "Makefile")

	// The FIPS job runs in ci.yml's full tier (#293; ci-template v1.41): every
	// event, a non-draft pull request included, but a refused dispatch; a
	// docs-only PR skips it. CI Gate checks it as `code`.
	fipsWorkflow = filepath.Join("..", "..", ".github", "workflows", "ci.yml")

	// fipsModePattern captures the fips140 mode as a whole word.
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

	// Whole-word match: fips140=on is a prefix of fips140=only.
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

// fipsExcluded returns the single package tree named by FIPS_EXCLUDED, failing
// if the variable is gone or names anything other than exactly one tree.
func fipsExcluded(t *testing.T, makefile string) string {
	t.Helper()

	assign := regexp.MustCompile(`(?m)^FIPS_EXCLUDED\s*:?=\s*(.*)$`).FindStringSubmatch(makefile)
	if assign == nil {
		t.Fatal("Makefile no longer defines FIPS_EXCLUDED; the approved-only gate's " +
			"scope is no longer stated in one place.")
	}
	got := strings.Fields(assign[1])
	if len(got) != 1 {
		t.Fatalf("FIPS_EXCLUDED names %d package trees (%v), want exactly 1.\n\n"+
			"Every entry is a tree the approved-only gate does not judge. Adding "+
			"one because a package failed is the forbidden fix; the failure is "+
			"the finding.", len(got), got)
	}
	return got[0]
}

// TestFIPSExclusionIsOneDocumentedTree checks the approved-only deny-list holds one entry, the OCSP SHA-1 CertID tree (RFC 6960 4.1.1).
func TestFIPSExclusionIsOneDocumentedTree(t *testing.T) {
	makefile := readRepoFile(t, makefilePath)

	fipsExcluded(t, makefile)

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

// TestQUICTransportIsInsideApprovedOnly checks the excluded prefix does not cover the QUIC transport.
func TestQUICTransportIsInsideApprovedOnly(t *testing.T) {
	const (
		transportPkg  = "github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
		revocationPkg = transportPkg + "/revocation"
	)

	excluded := fipsExcluded(t, readRepoFile(t, makefilePath))

	if excluded != revocationPkg {
		t.Errorf("FIPS_EXCLUDED is %q, want %q.\n\n"+
			"The exemption is the OCSP CertID's SHA-1 (RFC 6960 s4.1.1) in "+
			"golang.org/x/crypto/ocsp, reached only from revocation.Checker's OCSP "+
			"path. Nothing else in pkg/internal/transport needs to be outside the "+
			"approved-only gate, and quic-go >= v0.60.0 is why.", excluded, revocationPkg)
	}

	// FIPS_EXCLUDED must not be a prefix of the transport's import path.
	if strings.HasPrefix(transportPkg, excluded) {
		t.Errorf("FIPS_EXCLUDED (%q) is a path prefix of %q, so the QUIC transport "+
			"is filtered out of the approved-only gate.\n\n"+
			"Packet protection stopped being an exemption at quic-go v0.60.0. The "+
			"transport passes under GODEBUG=fips140=only and must be judged there.",
			excluded, transportPkg)
	}
}

// TestExcludedTreeIsStillGated checks the excluded tree has its own fips140=on target.
func TestExcludedTreeIsStillGated(t *testing.T) {
	makefile := readRepoFile(t, makefilePath)
	recipe := strings.Join(makeRecipe(t, makefile, "verify-fips-exempt"), "\n")

	if !strings.Contains(recipe, "$(FIPS_EXCLUDED)") {
		t.Errorf("verify-fips-exempt does not run $(FIPS_EXCLUDED):\n%s\n\n"+
			"It must name the same variable verify-fips excludes, so the two "+
			"targets are a partition by construction rather than by coincidence.",
			recipe)
	}
	// `on` is what this tree is expected to run at today and `only` is where it
	// would go if x/crypto/ocsp ever stopped needing the exemption -- or if the
	// OCSP path were excluded from the run some other way -- so both pass. Anything else -- no mode, or fips140=off -- means the
	// excluded tree is not gated at all.
	switch mode := fipsModePattern.FindStringSubmatch(recipe); {
	case mode == nil:
		t.Errorf("verify-fips-exempt sets no GODEBUG=fips140 mode:\n%s\n\n"+
			"Then it is an ordinary `go test` run and the excluded tree is not "+
			"gated at all.", recipe)
	case mode[1] != "on" && mode[1] != "only":
		t.Errorf("verify-fips-exempt runs GODEBUG=fips140=%s:\n%s\n\n"+
			"Only `on` or `only` load the Go Cryptographic Module; anything else "+
			"leaves this tree ungated.", mode[1], recipe)
	}
	if !strings.Contains(recipe, "-tags fips") {
		t.Errorf("verify-fips-exempt does not pass -tags fips:\n%s\n\n"+
			"The tagged build of the transport would then never run its tests "+
			"anywhere, and the FIPS curve list could break unnoticed.", recipe)
	}
}

// TestCIRunsTheFIPSGate pins that ci.yml actually invokes the targets
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

	for _, target := range []string{"make build-fips", "make verify-fips", "make verify-fips-exempt"} {
		var found bool
		for _, run := range runs {
			// CI runs each target in the toolchain image (ci-template v1.21).
			if run == target || run == `scripts/in-toolchain-image.sh "$TOOLCHAIN_IMAGE" `+target {
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
