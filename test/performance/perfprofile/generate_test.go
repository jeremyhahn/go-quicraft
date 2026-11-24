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

package perfprofile

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// generatedAt is a fixed clock so a document's "generated" line is not the one
// thing that changes between two otherwise identical runs.
var generatedAt = time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

// realOmegaProfile copies the repository's omega profile into a temporary
// directory and returns the copy.
//
// The fixture is the real measured data on purpose. omega genuinely has
// quicraft, dragonboat and openraft results and genuinely has no etcd result —
// that run failed on a harness bug — so it is the honest input for the
// missing-system path. The preconditions are asserted rather than assumed: if
// somebody later measures etcd on omega, this test says so instead of quietly
// testing a case that no longer exists.
func realOmegaProfile(t *testing.T) *Profile {
	t.Helper()

	src, err := filepath.Abs(filepath.Join("..", "profiles", "omega"))
	if err != nil {
		t.Fatalf("resolve omega profile: %v", err)
	}
	if _, err := os.Stat(filepath.Join(src, resultsDirName)); err != nil {
		t.Fatalf("omega profile results not found at %s: %v", src, err)
	}

	for _, sys := range []string{"quicraft", "dragonboat", "openraft"} {
		if _, err := os.Stat(filepath.Join(src, resultsDirName, sys+".json")); err != nil {
			t.Fatalf("fixture precondition failed: omega has no %s results (%v). "+
				"This test drives the generator with the real omega dataset.", sys, err)
		}
	}
	if _, err := os.Stat(filepath.Join(src, resultsDirName, "etcd.json")); err == nil {
		t.Fatal("fixture precondition failed: omega now HAS etcd results. " +
			"This test exists to cover a genuinely unmeasured system; pick another " +
			"gap or drop the test rather than faking one.")
	}

	dst := filepath.Join(t.TempDir(), "omega")
	copyTree(t, src, dst)

	p, err := Open(dst)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// The repository copy already holds generated documents from the last run.
	// Drop them, so every assertion below is about what THIS run produced
	// rather than about what happened to be checked in.
	for _, sys := range KnownSystems {
		if err := os.Remove(p.DocPath(sys)); err != nil && !os.IsNotExist(err) {
			t.Fatalf("clear generated %s.md from the fixture: %v", sys, err)
		}
	}
	return p
}

// copyTree recursively copies a directory.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0644)
	})
	if err != nil {
		t.Fatalf("copy %s -> %s: %v", src, dst, err)
	}
}

// TestGenerateReportsUnmeasuredSystem is the regression test for a generator
// that emitted two of three reports and printed "Generation complete." A system
// with no results must produce no document, must be named as unmeasured in the
// summary, and must be named inside every document that was written.
func TestGenerateReportsUnmeasuredSystem(t *testing.T) {
	p := realOmegaProfile(t)

	result, err := p.Generate(generatedAt)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if got := strings.Join(result.Generated, ","); got != "dragonboat.md,openraft.md" {
		t.Fatalf("generated = %q; want dragonboat.md,openraft.md", got)
	}
	if got := strings.Join(result.NotMeasured, ","); got != "etcd" {
		t.Fatalf("NotMeasured = %v; want [etcd]", result.NotMeasured)
	}
	if result.Complete() {
		t.Fatal("Complete() is true with etcd unmeasured")
	}

	summary := result.Summary()
	if !strings.Contains(summary, "NOT MEASURED") || !strings.Contains(summary, "etcd") {
		t.Fatalf("summary does not report etcd as unmeasured:\n%s", summary)
	}

	// No etcd document, and no placeholder standing in for one. A file of N/A
	// rows is indistinguishable at a glance from a real result of zero.
	if _, err := os.Stat(p.DocPath("etcd")); !os.IsNotExist(err) {
		t.Fatalf("etcd.md exists (%v) for a system that was never measured", err)
	}

	for _, name := range result.Generated {
		body := readFile(t, filepath.Join(p.Dir(), name))
		if !strings.Contains(body, "Not yet measured on `omega`: etcd") {
			t.Fatalf("%s does not name etcd as unmeasured:\n%s", name, body)
		}
	}

	// The documents must be rendered from the real JSON, not from defaults.
	// 287.9 ops/s is QuicRaft's measured commit_latency_3n_wal on omega and
	// 100.0 is Dragonboat's; neither can appear unless the stored results were
	// read and formatted.
	dragonboat := readFile(t, p.DocPath("dragonboat"))
	for _, want := range []string{
		"# QuicRaft vs Dragonboat — `omega` profile",
		"Commit Latency 3N (WAL)",
		"287.9 ops/s",
		"100.0 ops/s",
	} {
		if !strings.Contains(dragonboat, want) {
			t.Fatalf("dragonboat.md is missing %q; it did not render from the stored results", want)
		}
	}
	if !strings.Contains(dragonboat, "["+AnalysisFile+"]") {
		t.Fatalf("dragonboat.md does not link to %s", AnalysisFile)
	}
}

// TestGenerateNeverWritesAnalysis asserts the hand-written document survives a
// run that rewrites its siblings.
//
// The test cannot pass by doing nothing: dragonboat.md is seeded with a
// sentinel first, and the run is only accepted if that sentinel is gone
// afterwards. So the generator demonstrably wrote in this directory, and
// analysis.md is demonstrably byte-identical anyway.
func TestGenerateNeverWritesAnalysis(t *testing.T) {
	p := realOmegaProfile(t)

	const analysisBody = "# omega analysis\n\nHand-written. 0xDEADBEEF-ANALYSIS-SENTINEL\n"
	const staleDoc = "0xDEADBEEF-STALE-SENTINEL: this must be overwritten\n"

	if err := os.WriteFile(p.AnalysisPath(), []byte(analysisBody), 0644); err != nil {
		t.Fatalf("seed analysis.md: %v", err)
	}
	if err := os.WriteFile(p.DocPath("dragonboat"), []byte(staleDoc), 0644); err != nil {
		t.Fatalf("seed dragonboat.md: %v", err)
	}
	before := digest(t, p.AnalysisPath())

	if _, err := p.Generate(generatedAt); err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if got := readFile(t, p.DocPath("dragonboat")); strings.Contains(got, "STALE-SENTINEL") {
		t.Fatal("generation did not rewrite dragonboat.md; the analysis.md check below would prove nothing")
	}
	if got := readFile(t, p.AnalysisPath()); got != analysisBody {
		t.Fatalf("analysis.md was modified by generation:\n--- got ---\n%s", got)
	}
	if after := digest(t, p.AnalysisPath()); after != before {
		t.Fatalf("analysis.md digest changed: %s -> %s", before, after)
	}
}

// TestWriteDocRefusesAnalysisFile covers the structural guard behind the test
// above: the single write path refuses the hand-written filename outright, so a
// future caller cannot reach it by passing the wrong system name.
func TestWriteDocRefusesAnalysisFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), AnalysisFile)
	if err := writeDoc(path, "generated"); err == nil {
		t.Fatal("writeDoc wrote analysis.md")
	} else if _, ok := err.(*ProtectedFileError); !ok {
		t.Fatalf("writeDoc error = %T (%v); want *ProtectedFileError", err, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("analysis.md was created despite the refusal (%v)", err)
	}
}

// TestGenerateLeavesOtherProfilesAlone asserts profile isolation: generating
// omega must not read, rewrite, or delete anything belonging to another
// machine. r640 in particular holds the only surviving copy of a baseline that
// cannot be re-measured until the hardware is repaired.
func TestGenerateLeavesOtherProfilesAlone(t *testing.T) {
	root := t.TempDir()

	omegaSrc := realOmegaProfile(t)
	omegaDir := filepath.Join(root, "omega")
	copyTree(t, omegaSrc.Dir(), omegaDir)

	// A stand-in r640 with the same shape as the real one: a hand-written
	// analysis, its charts, and a nested file.
	r640 := filepath.Join(root, "r640")
	mkfile(t, filepath.Join(r640, AnalysisFile), "# R640 preserved baseline\n52.6 ops/s\n")
	mkfile(t, filepath.Join(r640, chartsDirName, "comparison.svg"), "<svg>r640</svg>")
	mkfile(t, filepath.Join(r640, chartsDirName, "etcd", "comparison.svg"), "<svg>r640 etcd</svg>")
	mkfile(t, filepath.Join(r640, resultsDirName, ".keep"), "")
	before := snapshot(t, r640)

	p, err := Open(omegaDir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	result, err := p.Generate(generatedAt)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Generated) == 0 {
		t.Fatal("generation wrote nothing; isolation would be trivially satisfied")
	}

	after := snapshot(t, r640)
	if len(before) != len(after) {
		t.Fatalf("r640 file set changed: %d files before, %d after\nbefore=%v\nafter=%v",
			len(before), len(after), before, after)
	}
	for path, sum := range before {
		got, ok := after[path]
		if !ok {
			t.Fatalf("generation removed %s from the r640 profile", path)
		}
		if got != sum {
			t.Fatalf("generation modified %s in the r640 profile", path)
		}
	}
}

// TestGenerateRemovesStaleDocument covers the other half of the missing-system
// rule: a document left behind from a run when the system did have results must
// not survive beside freshly written siblings, where it reads as current.
func TestGenerateRemovesStaleDocument(t *testing.T) {
	p := realOmegaProfile(t)
	if err := os.WriteFile(p.DocPath("etcd"), []byte("# stale etcd numbers\n"), 0644); err != nil {
		t.Fatalf("seed stale etcd.md: %v", err)
	}

	result, err := p.Generate(generatedAt)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if _, err := os.Stat(p.DocPath("etcd")); !os.IsNotExist(err) {
		t.Fatalf("stale etcd.md survived generation (%v)", err)
	}
	if strings.Join(result.Removed, ",") != "etcd.md" {
		t.Fatalf("Removed = %v; want [etcd.md]", result.Removed)
	}
	if !strings.Contains(result.Summary(), "removed stale document") {
		t.Fatalf("summary does not mention the removal:\n%s", result.Summary())
	}
}

// TestGenerateWithoutEnvSaysSo covers the omega case as it stands today: real
// results, no machine capture. The documents must state that rather than print
// a hardware table nobody measured.
func TestGenerateWithoutEnvSaysSo(t *testing.T) {
	p := realOmegaProfile(t)
	if err := os.Remove(p.EnvPath()); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove env: %v", err)
	}

	result, err := p.Generate(generatedAt)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.EnvCaptured {
		t.Fatal("EnvCaptured is true with no env.json present")
	}
	body := readFile(t, p.DocPath("dragonboat"))
	if !strings.Contains(body, UncapturedSummary) {
		t.Fatalf("document does not say the machine was never captured:\n%s", body)
	}
}

// TestGenerateWithEnvRendersHardwareInline asserts a reader of a generated
// document is never a click away from knowing which machine produced a number.
func TestGenerateWithEnvRendersHardwareInline(t *testing.T) {
	p := realOmegaProfile(t)
	if err := os.WriteFile(p.EnvPath(),
		[]byte(strings.Replace(partialEnvJSON, `"testbox"`, `"omega"`, 1)), 0644); err != nil {
		t.Fatalf("write env: %v", err)
	}

	if _, err := p.Generate(generatedAt); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	body := readFile(t, p.DocPath("openraft"))
	for _, want := range []string{
		"| Profile | `omega` |",
		"8260",                      // the captured CPU, inline
		"2026-08-17T09:00:00Z",      // the capture date
		"| Docker | " + NotCaptured, // and the field the capture could not obtain
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("generated document is missing %q:\n%s", want, body)
		}
	}
}

// TestGenerateRequiresBaseline asserts a profile with no QuicRaft results
// produces no documents at all rather than a set of empty ones.
func TestGenerateRequiresBaseline(t *testing.T) {
	p := realOmegaProfile(t)
	if err := os.Remove(filepath.Join(p.ResultsDir(), "quicraft.json")); err != nil {
		t.Fatalf("remove baseline: %v", err)
	}

	// Seed the documents a previous run would have left. The refusal must leave
	// them exactly as they are: a run that cannot produce a comparison must not
	// half-replace the last one either.
	const sentinel = "0xDEADBEEF-PRIOR-RUN\n"
	for _, sys := range []string{"etcd", "dragonboat", "openraft"} {
		if err := os.WriteFile(p.DocPath(sys), []byte(sentinel), 0644); err != nil {
			t.Fatalf("seed %s.md: %v", sys, err)
		}
	}

	_, err := p.Generate(generatedAt)
	if err == nil {
		t.Fatal("Generate succeeded with no quicraft results")
	}
	if _, ok := err.(*MissingBaselineError); !ok {
		t.Fatalf("error = %T (%v); want *MissingBaselineError", err, err)
	}
	for _, sys := range []string{"etcd", "dragonboat", "openraft"} {
		if got := readFile(t, p.DocPath(sys)); got != sentinel {
			t.Fatalf("%s.md was rewritten despite the refusal:\n%s", sys, got)
		}
	}
}

// TestPresentSystemsIsStable keeps map iteration order out of the output.
func TestPresentSystemsIsStable(t *testing.T) {
	p := realOmegaProfile(t)
	suites, err := p.LoadResults()
	if err != nil {
		t.Fatalf("LoadResults: %v", err)
	}
	const want = "quicraft,dragonboat,openraft"
	for i := 0; i < 20; i++ {
		if got := strings.Join(PresentSystems(suites), ","); got != want {
			t.Fatalf("PresentSystems = %q; want %q", got, want)
		}
	}
	if got := strings.Join(NotMeasured(suites), ","); got != "etcd" {
		t.Fatalf("NotMeasured = %q; want etcd", got)
	}
}

// readFile reads a file the test expects to exist.
func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

// mkfile writes a file, creating its parents.
func mkfile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// digest returns the SHA-256 of a file.
func digest(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// snapshot digests every file under a directory, keyed by relative path.
func snapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[rel] = digest(t, path)
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot %s: %v", root, err)
	}
	if len(out) == 0 {
		t.Fatalf("snapshot of %s is empty; the isolation check would prove nothing", root)
	}
	return out
}
