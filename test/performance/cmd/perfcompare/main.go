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

// perfcompare orchestrates Raft consensus benchmark runs and turns the stored
// results of one machine profile into SVG charts and markdown comparison
// documents.
//
// Everything it reads and writes lives inside a profile directory, because a
// benchmark number means nothing without the machine it was measured on. See
// test/performance/README.md.
//
// Usage:
//
//	perfcompare [flags] <system...>         Run benchmarks for specified systems
//	perfcompare [flags] all                 Run benchmarks for all systems
//	perfcompare generate --profile=<dir>    Generate documents from a profile
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/performance/perfprofile"
	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
)

// knownSystems lists all benchmark systems in preferred order. The profile
// package owns the list; a second copy here would be a second thing to update.
var knownSystems = perfprofile.KnownSystems

// dockerGoConfig describes how to build and run a Go benchmark system in a
// Docker container so every engine is benchmarked in an identical, isolated
// environment (--net=host).
type dockerGoConfig struct {
	image      string
	dockerfile string // relative to the perf dir (orchestrator cwd)
	privileged bool   // QuicRaft needs --privileged for UDP buffer tuning
}

// dockerGoSystems maps each Go benchmark system to its Docker configuration.
// OpenRaft is handled separately (different run-arg convention).
var dockerGoSystems = map[string]dockerGoConfig{
	"quicraft":   {image: "quicraft-bench-quicraft", dockerfile: "quicraft/Dockerfile", privileged: true},
	"etcd":       {image: "quicraft-bench-etcd", dockerfile: "etcd/Dockerfile"},
	"dragonboat": {image: "quicraft-bench-dragonboat", dockerfile: "dragonboat/Dockerfile"},
}

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Detect "generate" subcommand.
	var err error
	if os.Args[1] == "generate" {
		err = runGenerate(os.Args[2:])
	} else {
		err = runBenchmarks(os.Args[1:])
	}
	if err != nil {
		log.Printf("FAILED: %v", err)
		os.Exit(1)
	}
}

// runBenchmarks parses flags and executes benchmark runs for the requested
// systems. A system that produces no results fails the whole run: a comparison
// that quietly drops an engine is not a comparison, and an orchestrator that
// prints ERROR and exits 0 is a gate that cannot fail.
func runBenchmarks(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	benchtime := fs.String("benchtime", "30s", "Duration for each benchmark (go test -benchtime)")
	timeout := fs.String("timeout", "3600s", "Timeout for the entire benchmark run")
	profileDir := fs.String("profile", "", "Profile directory to store results in (e.g. profiles/omega)")
	noBuild := fs.Bool("no-build", false, "Skip Docker image rebuild")
	fs.Parse(args)

	systems, err := resolveSystems(fs.Args())
	if err != nil {
		return err
	}
	if len(systems) == 0 {
		return &NoSystemsError{}
	}

	var profile *perfprofile.Profile
	if *profileDir != "" {
		profile, err = perfprofile.Open(*profileDir)
		if err != nil {
			return err
		}
		if err := profile.EnsureDirs(); err != nil {
			return err
		}
		fmt.Printf("Profile: %s (%s)\n", profile.Name(), profile.Dir())
	}

	fmt.Printf("Running benchmarks: %s\n", strings.Join(systems, ", "))
	fmt.Printf("  benchtime=%s  timeout=%s\n\n", *benchtime, *timeout)

	table := perfresult.NewComparisonTable()
	var failed []string

	for _, sys := range systems {
		fmt.Printf("=== %s ===\n", sys)
		start := time.Now()

		suite, err := runSystem(sys, *benchtime, *timeout, *noBuild)
		if err != nil {
			log.Printf("ERROR [%s]: %v", sys, err)
			failed = append(failed, sys)
			continue
		}

		elapsed := time.Since(start)
		fmt.Printf("  completed in %s (%d scenarios)\n\n", elapsed.Round(time.Second), len(suite.Results))

		table.Add(suite)

		// Store the result in the profile. A result that cannot be persisted is
		// a failed system: the generate phase would silently omit it.
		if profile != nil {
			path, err := profile.SaveSuite(suite)
			if err != nil {
				log.Printf("ERROR saving results for %s: %v", sys, err)
				failed = append(failed, sys)
				continue
			}
			fmt.Printf("  saved %s\n", path)
		}
	}

	// Report whatever did run before failing, so a partial run is still
	// diagnostic — but never let the exit status call it a success.
	if len(table.Suites) > 0 {
		report := table.Compare()
		report.PrintTerminal(os.Stdout)
	}

	if len(failed) > 0 {
		return &SystemsFailedError{Requested: systems, Failed: failed}
	}
	if len(table.Suites) == 0 {
		return &NoResultsError{Requested: systems}
	}
	return nil
}

// runGenerate turns one profile's stored results into charts and comparison
// documents. It refuses to generate — and refuses to report completion —
// unless every system the comparison is supposed to cover has results on disk.
// Reporting on a subset without saying so is how a three-way comparison came to
// be published as two reports and a "Generation complete."
//
// A profile that is genuinely missing an engine is generated by naming what is
// there: --systems=quicraft,dragonboat,openraft. The gap is then an explicit
// argument rather than a silent one, and every document written still says on
// its face which systems this profile has never measured.
func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	profileDir := fs.String("profile", "", "Profile directory to generate (e.g. profiles/omega)")
	systemsFlag := fs.String("systems", "all", "Comma-separated systems the generated documents must cover")
	fs.Parse(args)

	if *profileDir == "" {
		return &NoProfileError{}
	}

	expected, err := resolveSystems(strings.Split(*systemsFlag, ","))
	if err != nil {
		return err
	}
	if len(expected) == 0 {
		return &NoSystemsError{}
	}

	profile, err := perfprofile.Open(*profileDir)
	if err != nil {
		return err
	}

	suites, err := profile.LoadResults()
	if err != nil {
		return err
	}

	if missing := missingSystems(expected, suites); len(missing) > 0 {
		return &MissingResultsError{
			Missing:   missing,
			Expected:  expected,
			Present:   perfprofile.PresentSystems(suites),
			ResultDir: profile.ResultsDir(),
		}
	}

	result, err := profile.Generate(time.Now())
	if err != nil {
		return err
	}

	fmt.Println(result.Summary())
	if result.Complete() {
		fmt.Println("\nGeneration complete: every known system is covered.")
	} else {
		fmt.Printf("\nGeneration finished with %d system(s) unmeasured on this profile. "+
			"This profile is NOT a complete comparison.\n", len(result.NotMeasured))
	}
	return nil
}

// missingSystems returns the expected systems that have no loaded result suite,
// in the order they were expected.
func missingSystems(expected []string, suites map[string]*perfresult.BenchmarkSuite) []string {
	var missing []string
	for _, sys := range expected {
		if _, ok := suites[sys]; !ok {
			missing = append(missing, sys)
		}
	}
	return missing
}

// resolveSystems expands "all" and validates system names.
func resolveSystems(args []string) ([]string, error) {
	var systems []string
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			continue
		}
		if arg == "all" {
			return knownSystems, nil
		}
		if !slices.Contains(knownSystems, arg) {
			return nil, &UnknownSystemError{System: arg}
		}
		systems = append(systems, arg)
	}
	return systems, nil
}

// runSystem executes the benchmark for a single system and returns the parsed suite.
func runSystem(system, benchtime, timeout string, noBuild bool) (*perfresult.BenchmarkSuite, error) {
	if _, ok := dockerGoSystems[system]; ok {
		return runGoDocker(system, benchtime, timeout, noBuild)
	}
	if system == "openraft" {
		return runOpenraft(benchtime, timeout, noBuild)
	}
	return nil, fmt.Errorf("unknown system: %s", system)
}

// runGoDocker builds (unless --no-build) and runs a Go benchmark system inside
// a Docker container, then parses the sentinel-delimited JSON from stdout.
// Every engine runs with --net=host; QuicRaft additionally runs --privileged
// so its entrypoint can raise the kernel UDP buffers QUIC needs.
func runGoDocker(system, benchtime, timeout string, noBuild bool) (*perfresult.BenchmarkSuite, error) {
	cfg := dockerGoSystems[system]

	repoRoot, err := findRepoRoot()
	if err != nil {
		return nil, fmt.Errorf("cannot find repo root: %w", err)
	}

	if !noBuild {
		fmt.Printf("  building Docker image %s...\n", cfg.image)
		buildCmd := exec.Command("docker", "build", "-t", cfg.image, "-f", cfg.dockerfile, repoRoot)
		buildCmd.Stdout = os.Stdout
		buildCmd.Stderr = os.Stderr
		if err := buildCmd.Run(); err != nil {
			return nil, fmt.Errorf("docker build failed: %w", err)
		}
	}

	runArgs := []string{"run", "--rm", "--network=host"}
	if cfg.privileged {
		runArgs = append(runArgs, "--privileged")
	}
	runArgs = append(runArgs, benchTmpMount()...)
	runArgs = append(runArgs, cfg.image,
		"-test.bench=.", "-test.run=^$",
		"-test.benchtime="+benchtime, "-test.timeout="+timeout, "-test.v")

	fmt.Printf("  docker %s\n", strings.Join(runArgs, " "))
	runCmd := exec.Command("docker", runArgs...)
	runCmd.Stderr = os.Stderr

	output, err := runCmd.Output()
	if err != nil {
		return nil, &DockerRunError{
			System:     system,
			Err:        err,
			Diagnostic: extractDiagnostic(string(output)),
		}
	}

	suite, err := perfresult.ReadSuiteFromOutput(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse results: %w", err)
	}

	return suite, nil
}

// benchTmpMount returns docker run arguments that bind-mount a host scratch
// directory (BENCH_TMPDIR) into the container at /bench-tmp and point TMPDIR at
// it. Every engine writes its WAL through the testing temp dir (Go's
// b.TempDir / Rust's tempfile::tempdir, both honoring $TMPDIR), so this routes
// all fsync I/O to a chosen disk — e.g. an NVMe mount — for parity with the
// production storage tier. Returns nil when BENCH_TMPDIR is unset (containers
// then use their overlay filesystem, the local default).
func benchTmpMount() []string {
	host := os.Getenv("BENCH_TMPDIR")
	if host == "" {
		return nil
	}
	return []string{"-v", host + ":/bench-tmp", "-e", "TMPDIR=/bench-tmp"}
}

// openraftImage is the Docker image name for the OpenRaft benchmark.
const openraftImage = "quicraft-openraft-bench"

// runOpenraft builds (unless --no-build) and runs the OpenRaft Docker benchmark.
func runOpenraft(benchtime, timeout string, noBuild bool) (*perfresult.BenchmarkSuite, error) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return nil, fmt.Errorf("cannot find repo root: %w", err)
	}

	if !noBuild {
		fmt.Println("  building Docker image...")
		buildCmd := exec.Command("docker", "build",
			"-t", openraftImage,
			"-f", "openraft/Dockerfile",
			repoRoot,
		)
		buildCmd.Stdout = os.Stdout
		buildCmd.Stderr = os.Stderr
		if err := buildCmd.Run(); err != nil {
			return nil, fmt.Errorf("docker build failed: %w", err)
		}
	}

	fmt.Println("  running OpenRaft benchmarks in Docker...")
	// The OpenRaft binary's flag parser only recognizes the "--flag=value"
	// form (not space-separated), so pass benchtime/timeout with "=".
	runArgs := []string{"run", "--rm", "--network=host"}
	runArgs = append(runArgs, benchTmpMount()...)
	runArgs = append(runArgs, openraftImage,
		"--benchtime="+benchtime,
		"--timeout="+timeout,
	)
	runCmd := exec.Command("docker", runArgs...)
	runCmd.Stderr = os.Stderr

	output, err := runCmd.Output()
	if err != nil {
		return nil, &DockerRunError{
			System:     "openraft",
			Err:        err,
			Diagnostic: extractDiagnostic(string(output)),
		}
	}

	suite, err := perfresult.ReadSuiteFromOutput(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse results: %w", err)
	}

	return suite, nil
}

// findRepoRoot walks up from the working directory to find the repository root
// (directory containing go.mod with the module name).
func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no .git directory found")
		}
		dir = parent
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage:
  perfcompare [flags] <system...>          Run benchmarks for specified systems
  perfcompare [flags] all                  Run benchmarks for all systems
  perfcompare generate --profile=<dir>     Generate documents from a profile

Systems: %s

Run flags:
  --benchtime=30s       Duration for each benchmark
  --timeout=3600s       Timeout for entire run
  --profile=<dir>       Profile directory; results are stored in <dir>/results
  --no-build            Skip Docker image rebuild

Generate flags:
  --profile=<dir>       Profile directory to generate (required)
  --systems=all         Systems the generated documents must cover; generation
                        refuses to run if any of them has no results

A profile is one machine. %s is written by hand and never generated.
`, strings.Join(knownSystems, ", "), perfprofile.AnalysisFile)
}
