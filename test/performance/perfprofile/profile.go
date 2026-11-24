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

// Package perfprofile owns the on-disk shape of a machine profile: the captured
// machine description, the stored result JSON, and the comparison documents
// generated from them.
//
// A profile is one machine class tracked over time. Its name is supplied by the
// operator and cannot be derived from the hardware — no amount of lscpu output
// says "this is our desktop-class reference box" — while everything in env.json
// is derived on the target at run time, so the specification table beside a
// number is never hand-maintained.
//
// Within a profile directory:
//
//	env.json      captured on the target, never edited
//	results/      one JSON suite per benchmarked system
//	logs/         one verbatim container console per benchmarked system
//	charts/       SVG charts, regenerated wholesale
//	<system>.md   generated comparison documents, deleted and recreated freely
//	analysis.md   HAND-WRITTEN. The generator never writes it.
package perfprofile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
)

// KnownSystems lists every benchmark system in display order, baseline first.
var KnownSystems = []string{"quicraft", "etcd", "dragonboat", "openraft"}

// BaselineSystem is the engine every generated document compares against.
const BaselineSystem = "quicraft"

// AnalysisFile is the hand-written document inside every profile. It is named
// here so the write path can refuse it by construction rather than by
// convention.
const AnalysisFile = "analysis.md"

// Directory names inside a profile.
const (
	resultsDirName = "results"
	chartsDirName  = "charts"
	logsDirName    = "logs"
)

// Profile is one machine profile on disk.
type Profile struct {
	name string
	dir  string
}

// Open returns the profile rooted at dir. The profile name is the directory's
// base name, which is what the operator passed to the run.
func Open(dir string) (*Profile, error) {
	clean := filepath.Clean(dir)
	name := filepath.Base(clean)
	if name == "." || name == string(filepath.Separator) || name == "" {
		return nil, &UnknownSystemError{System: dir}
	}
	return &Profile{name: name, dir: clean}, nil
}

// Name is the operator-supplied profile name.
func (p *Profile) Name() string { return p.name }

// Dir is the profile's root directory.
func (p *Profile) Dir() string { return p.dir }

// ResultsDir holds one JSON suite per benchmarked system.
func (p *Profile) ResultsDir() string { return filepath.Join(p.dir, resultsDirName) }

// LogsDir holds one verbatim container console per benchmarked system. A
// benchmark number is reproducible; the reason a container failed exists only
// in what it printed while failing, so that text is kept beside the results
// rather than in the orchestrator's memory.
func (p *Profile) LogsDir() string { return filepath.Join(p.dir, logsDirName) }

// ChartsDir holds the generated SVG charts.
func (p *Profile) ChartsDir() string { return filepath.Join(p.dir, chartsDirName) }

// EnvPath is the captured machine description.
func (p *Profile) EnvPath() string { return filepath.Join(p.dir, EnvFile) }

// AnalysisPath is the hand-written document. It is exposed so callers can talk
// about it and link to it, never so they can write it.
func (p *Profile) AnalysisPath() string { return filepath.Join(p.dir, AnalysisFile) }

// DocPath is the generated comparison document for one system.
func (p *Profile) DocPath(system string) string {
	return filepath.Join(p.dir, system+".md")
}

// chartRelPath is the pairwise chart link as written inside a generated
// document, which sits in the profile root beside the charts directory.
func chartRelPath(system string) string {
	return chartsDirName + "/" + BaselineSystem + "_vs_" + system + ".svg"
}

// EnsureDirs creates the directories a benchmark run writes into.
func (p *Profile) EnsureDirs() error {
	for _, dir := range []string{p.dir, p.ResultsDir(), p.ChartsDir(), p.LogsDir()} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}

// LoadEnv reads the profile's machine capture, checking that it describes this
// profile and not another one.
func (p *Profile) LoadEnv() (*Env, error) {
	env, err := LoadEnv(p.EnvPath())
	if err != nil {
		return nil, err
	}
	if env.Profile != "" && env.Profile != p.name {
		return nil, &EnvProfileMismatchError{
			Path:      p.EnvPath(),
			Directory: p.name,
			Captured:  env.Profile,
		}
	}
	return env, nil
}

// SaveSuite writes one system's results into the profile.
func (p *Profile) SaveSuite(suite *perfresult.BenchmarkSuite) (string, error) {
	if err := os.MkdirAll(p.ResultsDir(), 0755); err != nil {
		return "", err
	}
	data, err := json.MarshalIndent(suite, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(p.ResultsDir(), suite.System+".json")
	return path, os.WriteFile(path, data, 0644)
}

// LoadResults reads every stored result suite, keyed by system name.
func (p *Profile) LoadResults() (map[string]*perfresult.BenchmarkSuite, error) {
	entries, err := os.ReadDir(p.ResultsDir())
	if err != nil {
		return nil, err
	}
	suites := make(map[string]*perfresult.BenchmarkSuite)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(p.ResultsDir(), entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var suite perfresult.BenchmarkSuite
		if err := json.Unmarshal(data, &suite); err != nil {
			return nil, &EnvParseError{Path: path, Detail: err.Error()}
		}
		suites[suite.System] = &suite
	}
	return suites, nil
}

// PresentSystems returns the systems that have results, in KnownSystems order
// so output never depends on map iteration.
func PresentSystems(suites map[string]*perfresult.BenchmarkSuite) []string {
	present := make([]string, 0, len(suites))
	for _, sys := range KnownSystems {
		if _, ok := suites[sys]; ok {
			present = append(present, sys)
		}
	}
	for sys := range suites {
		if !slices.Contains(KnownSystems, sys) {
			present = append(present, sys)
		}
	}
	return present
}

// NotMeasured returns the known systems that have no results, in KnownSystems
// order. These are the gaps a generated document must name rather than omit.
func NotMeasured(suites map[string]*perfresult.BenchmarkSuite) []string {
	var missing []string
	for _, sys := range KnownSystems {
		if _, ok := suites[sys]; !ok {
			missing = append(missing, sys)
		}
	}
	return missing
}

// writeDoc writes one generated document, refusing by construction to write the
// hand-written analysis file. Every generated write goes through here, so the
// guarantee holds for any future caller as well as the present ones.
func writeDoc(path, content string) error {
	if filepath.Base(path) == AnalysisFile {
		return &ProtectedFileError{Path: path}
	}
	return os.WriteFile(path, []byte(content), 0644)
}
