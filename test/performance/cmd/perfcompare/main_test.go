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

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/performance/perfprofile"
	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
)

// newProfileDir creates an empty profile directory with the systems named
// already measured into it.
func newProfileDir(t *testing.T, systems ...string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "testprofile")
	profile, err := perfprofile.Open(dir)
	if err != nil {
		t.Fatalf("open profile: %v", err)
	}
	if err := profile.EnsureDirs(); err != nil {
		t.Fatalf("create profile dirs: %v", err)
	}
	for _, sys := range systems {
		writeSuiteJSON(t, profile.ResultsDir(), sys)
	}
	return dir
}

// generatedDocs lists the markdown documents in a profile directory.
func generatedDocs(t *testing.T, profileDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(profileDir)
	if err != nil {
		t.Fatalf("read profile dir: %v", err)
	}
	var docs []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".md") {
			docs = append(docs, entry.Name())
		}
	}
	return docs
}

// writeSuiteJSON writes a minimal but complete result file for one system.
func writeSuiteJSON(t *testing.T, dir, system string) {
	t.Helper()
	suite := perfresult.NewBenchmarkSuite(system, "test")
	rec := perfresult.NewLatencyRecorder(1)
	rec.Record(time.Millisecond)
	suite.AddResult(perfresult.ScenarioProposeSEQWAL, 1, time.Millisecond, rec)

	data, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		t.Fatalf("marshal %s suite: %v", system, err)
	}
	if err := os.WriteFile(filepath.Join(dir, system+".json"), data, 0644); err != nil {
		t.Fatalf("write %s suite: %v", system, err)
	}
}

// TestGenerateFailsWhenRequestedSystemMissing is the regression test for the
// smoke run that lost dragonboat and then printed
//
//	report: etcd.md
//	report: openraft.md
//	Generation complete.
//
// A three-way comparison rendered as two reports must not be announced as
// complete, and must not exit zero. Generation is asked here for all four known
// systems with dragonboat's results absent, exactly as after that run.
func TestGenerateFailsWhenRequestedSystemMissing(t *testing.T) {
	profileDir := newProfileDir(t, "quicraft", "etcd", "openraft")

	err := runGenerate([]string{"--profile=" + profileDir})
	if err == nil {
		t.Fatal("runGenerate returned nil with dragonboat results missing; " +
			"a partial comparison must not be reported as complete")
	}

	var missing *MissingResultsError
	if !asMissingResults(err, &missing) {
		t.Fatalf("runGenerate error = %T (%v); want *MissingResultsError", err, err)
	}
	if len(missing.Missing) != 1 || missing.Missing[0] != "dragonboat" {
		t.Fatalf("missing systems = %v; want [dragonboat]", missing.Missing)
	}
	if !strings.Contains(err.Error(), "dragonboat") {
		t.Fatalf("error %q does not name the missing system", err.Error())
	}
	if !strings.Contains(err.Error(), profileDir) {
		t.Fatalf("error %q does not name the results directory searched", err.Error())
	}

	// Nothing may have been written: the run refused, it did not half-do it.
	if docs := generatedDocs(t, profileDir); len(docs) != 0 {
		t.Fatalf("generation wrote %v despite refusing to complete", docs)
	}
}

// TestGenerateRequiresAProfile asserts there is no default machine. A document
// that does not name the machine its numbers came from cannot be interpreted,
// so the profile is never inferred.
func TestGenerateRequiresAProfile(t *testing.T) {
	err := runGenerate(nil)
	if err == nil {
		t.Fatal("runGenerate returned nil with no --profile")
	}
	if _, ok := err.(*NoProfileError); !ok {
		t.Fatalf("error = %T (%v); want *NoProfileError", err, err)
	}
}

// asMissingResults is a local errors.As without pulling the dependency into a
// package that has none.
func asMissingResults(err error, target **MissingResultsError) bool {
	m, ok := err.(*MissingResultsError)
	if ok {
		*target = m
	}
	return ok
}

// TestGenerateSucceedsWhenAllRequestedSystemsPresent is the positive half: the
// same call must still work, and still say so, when nothing is missing.
func TestGenerateSucceedsWhenAllRequestedSystemsPresent(t *testing.T) {
	profileDir := newProfileDir(t, knownSystems...)

	if err := runGenerate([]string{"--profile=" + profileDir}); err != nil {
		t.Fatalf("runGenerate with a complete result set: %v", err)
	}

	for _, sys := range []string{"etcd", "dragonboat", "openraft"} {
		if _, err := os.Stat(filepath.Join(profileDir, sys+".md")); err != nil {
			t.Fatalf("expected report %s.md: %v", sys, err)
		}
	}
}

// TestGenerateNarrowedExplicitly asserts the escape hatch works: a caller who
// genuinely wants a subset says so, and then it is not a silent omission.
func TestGenerateNarrowedExplicitly(t *testing.T) {
	profileDir := newProfileDir(t, "quicraft", "etcd")

	if err := runGenerate([]string{
		"--profile=" + profileDir,
		"--systems=quicraft,etcd",
	}); err != nil {
		t.Fatalf("runGenerate with an explicitly narrowed system list: %v", err)
	}
	if _, err := os.Stat(filepath.Join(profileDir, "etcd.md")); err != nil {
		t.Fatalf("expected report etcd.md: %v", err)
	}

	// Narrowing is permission to generate, not permission to go quiet. The
	// systems this profile has never measured are still named in what it wrote.
	body, err := os.ReadFile(filepath.Join(profileDir, "etcd.md"))
	if err != nil {
		t.Fatalf("read etcd.md: %v", err)
	}
	for _, want := range []string{"dragonboat", "openraft", "Not yet measured"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("narrowed generation omits %q from the document it wrote:\n%s", want, body)
		}
	}
}

// TestGenerateFailsWhenResultsDirEmpty covers the degenerate case: an empty
// directory must fail, not produce an empty "complete" run.
func TestGenerateFailsWhenResultsDirEmpty(t *testing.T) {
	err := runGenerate([]string{"--profile=" + newProfileDir(t)})
	if err == nil {
		t.Fatal("runGenerate returned nil for an empty results directory")
	}
}

// TestRunBenchmarksFailsWhenSystemProducesNoResults is the regression test for
// the other half of the smoke run: dragonboat printed ERROR and the process
// still exited successfully. runBenchmarks must return a non-nil error so main
// exits non-zero. Docker is not available in unit tests, so every system fails
// to run here — which is precisely the condition under test.
func TestRunBenchmarksFailsWhenSystemProducesNoResults(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // no docker binary reachable

	err := runBenchmarks([]string{"--benchtime=1x", "--no-build", "dragonboat"})
	if err == nil {
		t.Fatal("runBenchmarks returned nil after the only requested system produced no results")
	}
	if !strings.Contains(err.Error(), "dragonboat") {
		t.Fatalf("error %q does not name the system that failed", err.Error())
	}
}

func TestResolveSystems(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    []string
		wantErr bool
	}{
		{name: "all", args: []string{"all"}, want: knownSystems},
		{name: "subset", args: []string{"quicraft", "etcd"}, want: []string{"quicraft", "etcd"}},
		{name: "comma split with spaces", args: []string{"quicraft", " etcd "}, want: []string{"quicraft", "etcd"}},
		{name: "empty entries ignored", args: []string{"", "etcd"}, want: []string{"etcd"}},
		{name: "unknown", args: []string{"zookeeper"}, wantErr: true},
		{name: "none", args: nil, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveSystems(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveSystems(%v) = %v, nil; want error", tt.args, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSystems(%v): %v", tt.args, err)
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Fatalf("resolveSystems(%v) = %v; want %v", tt.args, got, tt.want)
			}
		})
	}
}

func TestMissingSystems(t *testing.T) {
	suites := map[string]*perfresult.BenchmarkSuite{
		"quicraft": perfresult.NewBenchmarkSuite("quicraft", "v"),
		"etcd":     perfresult.NewBenchmarkSuite("etcd", "v"),
	}
	got := missingSystems(knownSystems, suites)
	want := []string{"dragonboat", "openraft"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("missingSystems = %v; want %v", got, want)
	}
	if len(missingSystems([]string{"quicraft"}, suites)) != 0 {
		t.Fatal("missingSystems reported a system that is present")
	}
}

// TestExtractDiagnostic asserts the container failure summary picks out the
// lines that explain a failure and drops the benchmark result rows. The input
// is a trimmed copy of the real smoke-run output.
func TestExtractDiagnostic(t *testing.T) {
	output := strings.Join([]string{
		"goos: linux",
		"BenchmarkElectionTime_5N_Dragonboat",
		"BenchmarkElectionTime_5N_Dragonboat-20               \t       9\t 117598100 ns/op",
		"BenchmarkProposeThroughput_5N_Dragonboat",
		"    benchmark_test.go:220: NewNodeHost[3] failed: listen tcp 127.0.0.1:41461: bind: address already in use",
		"--- FAIL: BenchmarkProposeThroughput_5N_Dragonboat-20",
		"BenchmarkStaleRead_5N_Dragonboat",
		"    benchmark_test.go:282: seed propose failed: request dropped as the shard is not ready",
		"--- FAIL: BenchmarkStaleRead_5N_Dragonboat",
		"FAIL",
	}, "\n")

	got := extractDiagnostic(output)
	if len(got) != 5 {
		t.Fatalf("extractDiagnostic returned %d lines, want 5:\n%s", len(got), strings.Join(got, "\n"))
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{
		"bind: address already in use",
		"--- FAIL: BenchmarkProposeThroughput_5N_Dragonboat-20",
		"shard is not ready",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("diagnostic missing %q:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "ns/op") {
		t.Fatalf("diagnostic kept benchmark result rows:\n%s", joined)
	}
	if strings.Contains(joined, "goos:") {
		t.Fatalf("diagnostic kept the platform header:\n%s", joined)
	}
}

// TestExtractDiagnosticTruncates keeps a pathological container from burying
// the summary it is supposed to be.
func TestExtractDiagnosticTruncates(t *testing.T) {
	lines := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		lines = append(lines, "--- FAIL: BenchmarkSomething")
	}
	got := extractDiagnostic(strings.Join(lines, "\n"))
	if len(got) != maxDiagnosticLines+1 {
		t.Fatalf("extractDiagnostic returned %d lines; want %d plus a truncation marker",
			len(got), maxDiagnosticLines)
	}
	if !strings.Contains(got[len(got)-1], "truncated") {
		t.Fatalf("last line %q is not a truncation marker", got[len(got)-1])
	}
}

// TestDockerRunErrorNamesTheReason asserts an exit-status-1 container never
// reports only "exit status 1".
func TestDockerRunErrorNamesTheReason(t *testing.T) {
	err := &DockerRunError{
		System: "dragonboat",
		Err:    &exitStatusStub{},
		Diagnostic: []string{
			"benchmark_test.go:220: NewNodeHost[3] failed: listen tcp 127.0.0.1:41461: bind: address already in use",
		},
	}
	msg := err.Error()
	for _, want := range []string{"dragonboat", "exit status 1", "bind: address already in use"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("DockerRunError message %q missing %q", msg, want)
		}
	}

	bare := &DockerRunError{System: "dragonboat", Err: &exitStatusStub{}}
	if !strings.Contains(bare.Error(), "no failure lines found") {
		t.Fatalf("a container with no diagnostics must say so, got %q", bare.Error())
	}
}

type exitStatusStub struct{}

func (e *exitStatusStub) Error() string { return "exit status 1" }
