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

// perfcompare orchestrates Raft consensus benchmark runs, collects JSON
// results, generates SVG charts and markdown comparison reports.
//
// Usage:
//
//	perfcompare [flags] <system...>         Run benchmarks for specified systems
//	perfcompare [flags] all                 Run benchmarks for all systems
//	perfcompare generate [flags]            Generate reports from saved JSON results
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
)

// knownSystems lists all benchmark systems in preferred order.
var knownSystems = []string{"quicraft", "etcd", "dragonboat", "openraft"}

// dockerGoConfig describes how to build and run a Go benchmark system in a
// Docker container so every engine is benchmarked in an identical, isolated
// environment (--net=host).
type dockerGoConfig struct {
	image      string
	dockerfile string                      // relative to the perf dir (orchestrator cwd)
	privileged bool                        // QuicRaft needs --privileged for UDP buffer tuning
	stage      func(repoRoot string) error // optional pre-build staging step
}

// dockerGoSystems maps each Go benchmark system to its Docker configuration.
// OpenRaft is handled separately (different run-arg convention).
var dockerGoSystems = map[string]dockerGoConfig{
	"quicraft":   {image: "quicraft-bench-quicraft", dockerfile: "quicraft/Dockerfile", privileged: true},
	"etcd":       {image: "quicraft-bench-etcd", dockerfile: "etcd/Dockerfile"},
	"dragonboat": {image: "quicraft-bench-dragonboat", dockerfile: "dragonboat/Dockerfile", stage: stageDragonboat},
}

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Detect "generate" subcommand.
	if os.Args[1] == "generate" {
		runGenerate(os.Args[2:])
		return
	}

	runBenchmarks(os.Args[1:])
}

// runBenchmarks parses flags and executes benchmark runs for the requested systems.
func runBenchmarks(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	benchtime := fs.String("benchtime", "30s", "Duration for each benchmark (go test -benchtime)")
	timeout := fs.String("timeout", "3600s", "Timeout for the entire benchmark run")
	visualize := fs.Bool("visualize", false, "Generate SVG charts after benchmarks")
	pairwiseCharts := fs.Bool("pairwise-charts", false, "Generate pairwise comparison charts")
	chartDir := fs.String("chart-dir", "", "Directory for SVG chart output")
	jsonDir := fs.String("json-dir", "", "Directory to save JSON result files")
	noBuild := fs.Bool("no-build", false, "Skip Docker image rebuild for OpenRaft")
	fs.Parse(args)

	systems := resolveSystems(fs.Args())
	if len(systems) == 0 {
		log.Fatal("no systems specified")
	}

	fmt.Printf("Running benchmarks: %s\n", strings.Join(systems, ", "))
	fmt.Printf("  benchtime=%s  timeout=%s\n\n", *benchtime, *timeout)

	table := perfresult.NewComparisonTable()

	for _, sys := range systems {
		fmt.Printf("=== %s ===\n", sys)
		start := time.Now()

		suite, err := runSystem(sys, *benchtime, *timeout, *noBuild)
		if err != nil {
			log.Printf("ERROR [%s]: %v", sys, err)
			continue
		}

		elapsed := time.Since(start)
		fmt.Printf("  completed in %s (%d scenarios)\n\n", elapsed.Round(time.Second), len(suite.Results))

		table.Add(suite)

		// Save JSON if requested.
		if *jsonDir != "" {
			if err := saveJSON(suite, *jsonDir); err != nil {
				log.Printf("ERROR saving JSON for %s: %v", sys, err)
			}
		}
	}

	if len(table.Suites) == 0 {
		log.Fatal("no benchmark results collected")
	}

	// Print terminal report.
	report := table.Compare()
	report.PrintTerminal(os.Stdout)

	// Generate charts if requested.
	if *visualize && *chartDir != "" {
		if err := generateCharts(table, *chartDir, *pairwiseCharts); err != nil {
			log.Printf("ERROR generating charts: %v", err)
		}
	}
}

// runGenerate reads saved JSON results and produces reports and charts.
func runGenerate(args []string) {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	resultsDir := fs.String("results-dir", "results", "Directory containing JSON result files")
	outputDir := fs.String("output-dir", ".", "Directory for markdown output")
	chartDir := fs.String("chart-dir", "charts", "Directory for SVG chart output")
	fs.Parse(args)

	suites, err := loadAllJSON(*resultsDir)
	if err != nil {
		log.Fatalf("failed to load results: %v", err)
	}
	if len(suites) == 0 {
		log.Fatal("no JSON result files found in " + *resultsDir)
	}

	fmt.Printf("Loaded results for: ")
	names := make([]string, 0, len(suites))
	for sys := range suites {
		names = append(names, sys)
	}
	fmt.Println(strings.Join(names, ", "))

	// Build comparison table.
	table := perfresult.NewComparisonTable()
	for _, suite := range suites {
		table.Add(suite)
	}

	// Generate all charts (individual + comparison + pairwise).
	if err := generateCharts(table, *chartDir, true); err != nil {
		log.Printf("ERROR generating charts: %v", err)
	}

	// Generate markdown reports.
	if err := generateMarkdownReports(table, suites, *outputDir, *chartDir); err != nil {
		log.Printf("ERROR generating markdown: %v", err)
	}

	fmt.Println("\nGeneration complete.")
}

// resolveSystems expands "all" and validates system names.
func resolveSystems(args []string) []string {
	if len(args) == 0 {
		return nil
	}

	var systems []string
	for _, arg := range args {
		if arg == "all" {
			return knownSystems
		}
		valid := false
		for _, known := range knownSystems {
			if arg == known {
				valid = true
				break
			}
		}
		if !valid {
			log.Fatalf("unknown system: %s (known: %s)", arg, strings.Join(knownSystems, ", "))
		}
		systems = append(systems, arg)
	}
	return systems
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

	if cfg.stage != nil {
		if err := cfg.stage(repoRoot); err != nil {
			return nil, err
		}
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
		return nil, fmt.Errorf("docker run failed: %w\n%s", err, string(output))
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

// dragonboatSrc returns the path to the dragonboat fork source, honoring the
// DRAGONBOAT_SRC env var and falling back to the standard workspace checkout.
func dragonboatSrc() string {
	if v := os.Getenv("DRAGONBOAT_SRC"); v != "" {
		return v
	}
	return "/home/jhahn/sources/dragonboat"
}

// stageDragonboat copies the dragonboat fork into the repo's .build-cache so the
// Docker build context contains it (the module path github.com/lni/dragonboat/v4
// tagged v4.0.0 is not published upstream). If the cache already exists — for
// example rsynced onto a remote bench host — it is reused as-is.
func stageDragonboat(repoRoot string) error {
	dst := filepath.Join(repoRoot, ".build-cache", "dragonboat")
	if _, err := os.Stat(filepath.Join(dst, "go.mod")); err == nil {
		fmt.Printf("  using staged dragonboat source at %s\n", dst)
		return nil
	}

	src := dragonboatSrc()
	if _, err := os.Stat(filepath.Join(src, "go.mod")); err != nil {
		return fmt.Errorf("dragonboat source not found at %s; set DRAGONBOAT_SRC or pre-stage %s: %w", src, dst, err)
	}

	fmt.Printf("  staging dragonboat source %s -> %s\n", src, dst)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	cp := exec.Command("rsync", "-a", "--exclude", ".git", src+"/", dst+"/")
	cp.Stdout = os.Stdout
	cp.Stderr = os.Stderr
	if err := cp.Run(); err != nil {
		return fmt.Errorf("staging dragonboat via rsync failed: %w", err)
	}
	return nil
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
		return nil, fmt.Errorf("docker run failed: %w\n%s", err, string(output))
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

// saveJSON writes a benchmark suite as a JSON file to the given directory.
func saveJSON(suite *perfresult.BenchmarkSuite, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	path := filepath.Join(dir, suite.System+".json")
	data, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		return err
	}
	fmt.Printf("  saved %s\n", path)
	return os.WriteFile(path, data, 0644)
}

// loadAllJSON reads all .json files from a directory and returns them keyed by system name.
func loadAllJSON(dir string) (map[string]*perfresult.BenchmarkSuite, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	suites := make(map[string]*perfresult.BenchmarkSuite)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		var suite perfresult.BenchmarkSuite
		if err := json.Unmarshal(data, &suite); err != nil {
			return nil, fmt.Errorf("parse %s: %w", entry.Name(), err)
		}
		suites[suite.System] = &suite
	}
	return suites, nil
}

// generateCharts creates SVG charts from the collected benchmark suites.
func generateCharts(table *perfresult.ComparisonTable, chartDir string, pairwise bool) error {
	if err := os.MkdirAll(chartDir, 0755); err != nil {
		return err
	}

	// Per-system charts.
	for sys, suite := range table.Suites {
		path := filepath.Join(chartDir, sys+".svg")
		if err := perfresult.GenerateSystemChart(suite, path); err != nil {
			log.Printf("  chart %s: %v", sys, err)
			continue
		}
		fmt.Printf("  chart: %s\n", path)
	}

	// Combined comparison chart (all systems).
	if len(table.Suites) > 1 {
		path := filepath.Join(chartDir, "comparison.svg")
		if err := perfresult.GenerateComparisonChart(table.Suites, path); err != nil {
			log.Printf("  comparison chart: %v", err)
		} else {
			fmt.Printf("  chart: %s\n", path)
		}
	}

	// Pairwise charts (quicraft vs each other system).
	if pairwise && len(table.Suites) > 1 {
		quicraftSuite, hasQuicraft := table.Suites["quicraft"]
		if hasQuicraft {
			for sys, suite := range table.Suites {
				if sys == "quicraft" {
					continue
				}
				pairSuites := map[string]*perfresult.BenchmarkSuite{
					"quicraft": quicraftSuite,
					sys:        suite,
				}
				path := filepath.Join(chartDir, "quicraft_vs_"+sys+".svg")
				if err := perfresult.GenerateComparisonChart(pairSuites, path); err != nil {
					log.Printf("  pairwise chart %s: %v", sys, err)
					continue
				}
				fmt.Printf("  chart: %s\n", path)
			}
		}
	}

	return nil
}

// generateMarkdownReports creates markdown comparison docs from benchmark results.
func generateMarkdownReports(table *perfresult.ComparisonTable, suites map[string]*perfresult.BenchmarkSuite, outputDir, chartDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	// Generate pairwise markdown reports (quicraft vs X).
	quicraftSuite, hasQuicraft := suites["quicraft"]
	if !hasQuicraft {
		return nil
	}

	for _, sys := range []string{"etcd", "dragonboat", "openraft"} {
		otherSuite, ok := suites[sys]
		if !ok {
			continue
		}
		path := filepath.Join(outputDir, sys+".md")
		if err := writePairwiseMarkdown(quicraftSuite, otherSuite, path, chartDir); err != nil {
			log.Printf("  markdown %s: %v", sys, err)
			continue
		}
		fmt.Printf("  report: %s\n", path)
	}

	return nil
}

// writePairwiseMarkdown generates a markdown comparison report between QuicRaft
// and another system.
func writePairwiseMarkdown(quicraftSuite, otherSuite *perfresult.BenchmarkSuite, outPath, chartDir string) error {
	table := perfresult.NewComparisonTable()
	table.Add(quicraftSuite)
	table.Add(otherSuite)
	report := table.Compare()

	other := otherSuite.System

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("# QuicRaft vs %s Performance Comparison\n\n", titleCase(other)))
	sb.WriteString(fmt.Sprintf("Generated: %s\n\n", time.Now().UTC().Format("2006-01-02 15:04:05 UTC")))

	// Chart reference.
	chartPath := filepath.Join(chartDir, "quicraft_vs_"+other+".svg")
	sb.WriteString(fmt.Sprintf("![QuicRaft vs %s](%s)\n\n", titleCase(other), chartPath))

	// Results table.
	sb.WriteString("## Results\n\n")
	sb.WriteString(fmt.Sprintf("| Scenario | QuicRaft ops/s | %s ops/s | Winner | Speedup |\n", titleCase(other)))
	sb.WriteString("|----------|---------------|")
	sb.WriteString(strings.Repeat("-", len(titleCase(other))+8))
	sb.WriteString("|--------|--------:|\n")

	wins := map[string]int{"quicraft": 0, other: 0}

	for _, sc := range report.Scenarios {
		label := perfresult.ScenarioLabels[sc.Scenario]
		if label == "" {
			label = sc.Scenario
		}

		qr, qOK := sc.Results["quicraft"]
		or, oOK := sc.Results[other]

		qOps := "N/A"
		if qOK {
			qOps = perfresult.FormatOps(qr.OpsPerSec)
		}
		oOps := "N/A"
		if oOK {
			oOps = perfresult.FormatOps(or.OpsPerSec)
		}

		winner := ""
		speedup := ""
		if sc.ThroughputWinner != "" {
			winner = sc.ThroughputWinner
			wins[winner]++
			if ratio, ok := sc.ThroughputSpeedups[sc.ThroughputWinner]; ok && ratio > 1.005 {
				speedup = fmt.Sprintf("%.2fx", ratio)
			}
		}

		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s |\n",
			label, qOps, oOps, winner, speedup))
	}

	sb.WriteString(fmt.Sprintf("\n**Score: QuicRaft %d/%d, %s %d/%d**\n",
		wins["quicraft"], len(report.Scenarios),
		titleCase(other), wins[other], len(report.Scenarios)))

	return os.WriteFile(outPath, []byte(sb.String()), 0644)
}

// titleCase capitalizes the first letter of a string.
func titleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage:
  perfcompare [flags] <system...>     Run benchmarks for specified systems
  perfcompare [flags] all             Run benchmarks for all systems
  perfcompare generate [flags]        Generate reports from saved JSON

Systems: %s

Run flags:
  --benchtime=30s       Duration for each benchmark
  --timeout=3600s       Timeout for entire run
  --visualize           Generate SVG charts
  --pairwise-charts     Generate pairwise comparison charts
  --chart-dir=<dir>     Directory for SVG output
  --json-dir=<dir>      Directory to save JSON results
  --no-build            Skip Docker rebuild for OpenRaft

Generate flags:
  --results-dir=<dir>   Directory with JSON result files (default: results)
  --output-dir=<dir>    Directory for markdown output (default: .)
  --chart-dir=<dir>     Directory for SVG chart output (default: charts)
`, strings.Join(knownSystems, ", "))
}
