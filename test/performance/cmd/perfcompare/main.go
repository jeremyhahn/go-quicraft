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
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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

// runConfig carries the options every system's run shares. logDir is where each
// container's console is written; it is never empty, because a run whose output
// cannot be read back is a run whose failures cannot be explained.
type runConfig struct {
	benchtime string
	timeout   string
	noBuild   bool
	logDir    string
}

// defaultLogDir holds container consoles for a run made without a profile. With
// a profile the logs live inside it, beside the results they explain.
const defaultLogDir = "logs"

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
	profileDir := fs.String("profile", "", "Profile directory to store results in (e.g. profiles/<name>)")
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

	cfg := runConfig{
		benchtime: *benchtime,
		timeout:   *timeout,
		noBuild:   *noBuild,
		logDir:    defaultLogDir,
	}
	if profile != nil {
		cfg.logDir = profile.LogsDir()
	}
	if err := os.MkdirAll(cfg.logDir, 0755); err != nil {
		return err
	}

	fmt.Printf("Running benchmarks: %s\n", strings.Join(systems, ", "))
	fmt.Printf("  benchtime=%s  timeout=%s  logs=%s\n\n", *benchtime, *timeout, cfg.logDir)

	table := perfresult.NewComparisonTable()
	var failed []string

	for _, sys := range systems {
		fmt.Printf("=== %s ===\n", sys)
		start := time.Now()

		suite, err := runSystem(sys, cfg)
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
	profileDir := fs.String("profile", "", "Profile directory to generate (e.g. profiles/<name>)")
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
func runSystem(system string, cfg runConfig) (*perfresult.BenchmarkSuite, error) {
	if _, ok := dockerGoSystems[system]; ok {
		return runGoDocker(system, cfg)
	}
	if system == "openraft" {
		return runOpenraft(cfg)
	}
	return nil, fmt.Errorf("unknown system: %s", system)
}

// runGoDocker builds (unless --no-build) and runs a Go benchmark system inside
// a Docker container, then parses the sentinel-delimited JSON from stdout.
// Every engine runs with --net=host; QuicRaft additionally runs --privileged
// so its entrypoint can raise the kernel UDP buffers QUIC needs.
func runGoDocker(system string, cfg runConfig) (*perfresult.BenchmarkSuite, error) {
	docker := dockerGoSystems[system]

	repoRoot, err := findRepoRoot()
	if err != nil {
		return nil, fmt.Errorf("cannot find repo root: %w", err)
	}

	if !cfg.noBuild {
		if err := buildImage(docker.image, docker.dockerfile, repoRoot); err != nil {
			return nil, err
		}
	}

	runArgs := []string{"run", "--rm", "--network=host"}
	if docker.privileged {
		runArgs = append(runArgs, "--privileged")
	}
	runArgs = append(runArgs, benchTmpMount()...)
	runArgs = append(runArgs, docker.image,
		"-test.bench=.", "-test.run=^$",
		"-test.benchtime="+cfg.benchtime, "-test.timeout="+cfg.timeout, "-test.v")

	fmt.Printf("  docker %s\n", strings.Join(runArgs, " "))
	runCmd := exec.Command("docker", runArgs...)

	logPath := filepath.Join(cfg.logDir, system+".log")
	fmt.Printf("  container console: %s\n", logPath)

	output, err := runContainer(runCmd, logPath)
	if err != nil {
		return nil, &DockerRunError{
			System:     system,
			Err:        err,
			LogPath:    logPath,
			Diagnostic: extractDiagnostic(string(output)),
		}
	}

	suite, err := perfresult.ReadSuiteFromOutput(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse results: %w", err)
	}

	return suite, nil
}

// moduleAuthEnv names both the BuildKit secret the benchmark Dockerfiles mount
// for `go mod download` and the environment variable that carries its payload.
// One name at every entrypoint that builds an image, shared with the sibling
// repositories' images, so no two paths to the same image can disagree about
// how it authenticates.
const moduleAuthEnv = "GOMODAUTH"

// moduleAuthVars are the four variables of dependency-resolution.md section 6,
// in the order the payload lists them. GIT_CONFIG_KEY_0 is the one that
// decides presence: without the rewrite there is nothing to authenticate.
var moduleAuthVars = [...]string{"GOPRIVATE", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"}

// moduleAuth returns the payload the Dockerfiles source before `go mod
// download`: GOPRIVATE and the git environment-config rewrite that sends
// github.com/jeremyhahn/* to Gitea with a token, as K='V' lines. It is taken
// verbatim from GOMODAUTH when a caller has already composed it, and otherwise
// assembled from the four variables a Gitea Actions runner injects (or an
// operator exports). Empty when neither is present -- the correct shape for
// a public build, which resolves from the proxy alone -- so the same image
// builds inside and outside the Gitea network.
func moduleAuth(getenv func(string) string) string {
	if payload := getenv(moduleAuthEnv); payload != "" {
		return payload
	}
	if getenv("GIT_CONFIG_KEY_0") == "" {
		return ""
	}
	var b strings.Builder
	for _, k := range moduleAuthVars {
		fmt.Fprintf(&b, "%s='%s'\n", k, strings.ReplaceAll(getenv(k), "'", `'\''`))
	}
	return b.String()
}

// buildImage builds one benchmark image with the repository root as its
// context. Module credentials, when present, reach `go mod download` as a
// BuildKit secret: mounted for that one RUN, sourced, and never exported into
// a layer, so the token appears in neither the image, its metadata, nor a
// command line -- the payload travels to the docker CLI through its
// environment, not an argument. The secret is optional at both ends, and a
// Dockerfile that does not mount it ignores it, so every image is built the
// same way. Which way was taken is printed, because a build that resolved
// from the proxy when it needed Gitea fails later with an error that reads
// like a missing repository.
func buildImage(image, dockerfile, root string) error {
	fmt.Printf("  building Docker image %s...\n", image)
	args := []string{"build", "-t", image, "-f", dockerfile}
	env := os.Environ()
	if payload := moduleAuth(os.Getenv); payload != "" {
		args = append(args, "--secret", "id=gomodauth,env="+moduleAuthEnv)
		env = append(env, moduleAuthEnv+"="+payload)
		fmt.Println("  module credentials: mounted as a build secret")
	} else {
		fmt.Println("  module credentials: none; github.com/jeremyhahn/* resolves from the public proxy only")
	}
	cmd := exec.Command("docker", append(args, root)...)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker build %s failed: %w", image, err)
	}
	return nil
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
func runOpenraft(cfg runConfig) (*perfresult.BenchmarkSuite, error) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return nil, fmt.Errorf("cannot find repo root: %w", err)
	}

	if !cfg.noBuild {
		if err := buildImage(openraftImage, "openraft/Dockerfile", repoRoot); err != nil {
			return nil, err
		}
	}

	fmt.Println("  running OpenRaft benchmarks in Docker...")
	// The OpenRaft binary's flag parser only recognizes the "--flag=value"
	// form (not space-separated), so pass benchtime/timeout with "=".
	runArgs := []string{"run", "--rm", "--network=host"}
	runArgs = append(runArgs, benchTmpMount()...)
	runArgs = append(runArgs, openraftImage,
		"--benchtime="+cfg.benchtime,
		"--timeout="+cfg.timeout,
	)
	runCmd := exec.Command("docker", runArgs...)

	logPath := filepath.Join(cfg.logDir, "openraft.log")
	fmt.Printf("  container console: %s\n", logPath)

	output, err := runContainer(runCmd, logPath)
	if err != nil {
		return nil, &DockerRunError{
			System:     "openraft",
			Err:        err,
			LogPath:    logPath,
			Diagnostic: extractDiagnostic(string(output)),
		}
	}

	suite, err := perfresult.ReadSuiteFromOutput(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse results: %w", err)
	}

	return suite, nil
}

// runContainer runs a benchmark container, returning its stdout for parsing and
// writing its complete console — stdout and stderr, verbatim — to logPath.
//
// The orchestrator only ever wanted the sentinel-delimited JSON, so it read
// stdout into a buffer and dropped the buffer on the floor when the container
// failed. Everything `go test -test.v` prints goes to that stdout: every
// "=== RUN", every relayed b.Errorf, and the "--- FAIL:" naming the benchmark
// that failed. A 41 MB run log of one such failure therefore contained not one
// of them (#184), and the container's exit status was the only surviving fact.
//
// Stderr is still relayed live to the orchestrator's own stderr, so a run in
// progress still shows the engines talking; it is teed into the same file so
// the file is the console as the container produced it, in order.
//
// A log that cannot be opened fails the run. There is no quiet fallback: the
// whole point is that the next failure is explainable.
func runContainer(cmd *exec.Cmd, logPath string) ([]byte, error) {
	file, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open container log %s: %w", logPath, err)
	}

	console := &consoleLog{w: file}
	var stdout bytes.Buffer
	cmd.Stdout = io.MultiWriter(&stdout, console)
	cmd.Stderr = io.MultiWriter(os.Stderr, console)

	runErr := cmd.Run()

	// The log's own failures are reported SEPARATELY from the container's
	// exit, and neither is allowed to hide the other. Two things went wrong
	// with the obvious `defer file.Close()`:
	//
	//   - A write error on the log (disk full, an NFS mount going away) used
	//     to stop that stream's copier, and a container blocked on a full
	//     pipe stalls the benchmark. consoleLog keeps draining regardless.
	//   - os/exec discards a copier's write error when the process also exits
	//     non-zero, and a deferred Close discards its own error always. So a
	//     truncated console -- missing exactly the late `--- FAIL:` lines the
	//     file exists to preserve -- would have been reported as a complete
	//     one. That is the failure mode #184 was opened for, one layer down.
	logErr := console.err()
	if closeErr := file.Close(); closeErr != nil && logErr == nil {
		logErr = closeErr
	}
	if logErr != nil {
		logErr = fmt.Errorf("container log %s is INCOMPLETE: %w", logPath, logErr)
		if runErr != nil {
			return stdout.Bytes(), errors.Join(runErr, logErr)
		}
		return stdout.Bytes(), logErr
	}
	return stdout.Bytes(), runErr
}

// consoleLog serializes both of a container's streams into one file and keeps
// accepting writes after the file has failed.
//
// Serialization: os/exec copies stdout and stderr on separate goroutines, and
// both are teed into the same log, so without the mutex the two copiers would
// interleave mid-line.
//
// Draining: Write always reports the full length as written, even once the
// underlying file has errored. Returning the short write instead would make
// os/exec stop copying that stream, and a container with nobody reading its
// pipe blocks -- turning a full log disk into a hung benchmark. The first
// error is kept and surfaced by err() after the process exits, so the
// operator learns the console is incomplete rather than trusting it.
type consoleLog struct {
	mu   sync.Mutex
	w    io.Writer
	fail error
}

// Write implements io.Writer. It never short-writes; see the type comment.
func (c *consoleLog) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		return len(p), nil
	}
	if _, err := c.w.Write(p); err != nil {
		c.fail = err
	}
	return len(p), nil
}

// err reports the first write failure, or nil if the whole console reached
// the file.
func (c *consoleLog) err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fail
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
