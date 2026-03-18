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

package perfresult

import (
	"encoding/xml"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGenerateSystemChartValidSVG(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	suite.AddResult(ScenarioProposeSEQWAL, 5000, time.Second, nil)
	suite.AddResult(ScenarioElectionWAL, 100, 500*time.Millisecond, nil)

	outPath := filepath.Join(t.TempDir(), "charts", "system.svg")
	if err := GenerateSystemChart(suite, outPath); err != nil {
		t.Fatalf("GenerateSystemChart failed: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	// Verify valid XML.
	if err := xml.Unmarshal(data, new(interface{})); err != nil {
		t.Fatalf("output is not valid XML: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "<svg") {
		t.Fatal("expected SVG root element")
	}
	if !strings.Contains(content, "</svg>") {
		t.Fatal("expected closing SVG tag")
	}
	if !strings.Contains(content, "quicraft") {
		t.Fatal("expected system name in SVG")
	}
}

func TestGenerateSystemChartCreatesDirectory(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	suite.AddResult(ScenarioElectionWAL, 10, time.Second, nil)

	outPath := filepath.Join(t.TempDir(), "nested", "deep", "chart.svg")
	if err := GenerateSystemChart(suite, outPath); err != nil {
		t.Fatalf("GenerateSystemChart failed: %v", err)
	}

	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		t.Fatal("expected output file to exist")
	}
}

func TestGenerateSystemChartNoResults(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")

	outPath := filepath.Join(t.TempDir(), "chart.svg")
	err := GenerateSystemChart(suite, outPath)
	if err == nil {
		t.Fatal("expected error for suite with no results")
	}

	var noResults *ErrNoResults
	if !errors.As(err, &noResults) {
		t.Fatalf("expected ErrNoResults, got %T: %v", err, err)
	}
}

func TestGenerateSystemChartWithLatency(t *testing.T) {
	suite := NewBenchmarkSuite("etcd", "3.5.0")
	rec := NewLatencyRecorder(100)
	for i := 1; i <= 100; i++ {
		rec.Record(time.Duration(i) * time.Microsecond)
	}
	suite.AddResult(ScenarioCommitLatencyWAL, 1000, time.Second, rec)

	outPath := filepath.Join(t.TempDir(), "etcd.svg")
	if err := GenerateSystemChart(suite, outPath); err != nil {
		t.Fatalf("GenerateSystemChart failed: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	if err := xml.Unmarshal(data, new(interface{})); err != nil {
		t.Fatalf("output is not valid XML: %v", err)
	}
}

func TestGenerateComparisonChartValidSVG(t *testing.T) {
	suites := map[string]*BenchmarkSuite{}

	qSuite := NewBenchmarkSuite("quicraft", "1.0.0")
	qSuite.AddResult(ScenarioProposeSEQWAL, 5000, time.Second, nil)
	suites["quicraft"] = qSuite

	eSuite := NewBenchmarkSuite("etcd", "3.5.0")
	eSuite.AddResult(ScenarioProposeSEQWAL, 3000, time.Second, nil)
	suites["etcd"] = eSuite

	outPath := filepath.Join(t.TempDir(), "comparison.svg")
	if err := GenerateComparisonChart(suites, outPath); err != nil {
		t.Fatalf("GenerateComparisonChart failed: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	if err := xml.Unmarshal(data, new(interface{})); err != nil {
		t.Fatalf("output is not valid XML: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "quicraft") {
		t.Fatal("expected quicraft in comparison chart")
	}
	if !strings.Contains(content, "etcd") {
		t.Fatal("expected etcd in comparison chart")
	}
	if !strings.Contains(content, "Performance Comparison") {
		t.Fatal("expected title in comparison chart")
	}
}

func TestGenerateComparisonChartCreatesDirectory(t *testing.T) {
	suites := map[string]*BenchmarkSuite{}
	qSuite := NewBenchmarkSuite("quicraft", "1.0.0")
	qSuite.AddResult(ScenarioElectionWAL, 100, time.Second, nil)
	suites["quicraft"] = qSuite

	outPath := filepath.Join(t.TempDir(), "nested", "compare.svg")
	if err := GenerateComparisonChart(suites, outPath); err != nil {
		t.Fatalf("GenerateComparisonChart failed: %v", err)
	}

	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		t.Fatal("expected output file to exist")
	}
}

func TestGenerateComparisonChartNoResults(t *testing.T) {
	suites := map[string]*BenchmarkSuite{
		"quicraft": NewBenchmarkSuite("quicraft", "1.0.0"),
	}

	outPath := filepath.Join(t.TempDir(), "chart.svg")
	err := GenerateComparisonChart(suites, outPath)
	if err == nil {
		t.Fatal("expected error for empty suites")
	}

	var noResults *ErrNoResults
	if !errors.As(err, &noResults) {
		t.Fatalf("expected ErrNoResults, got %T: %v", err, err)
	}
}

func TestGenerateComparisonChartMissingScenario(t *testing.T) {
	suites := map[string]*BenchmarkSuite{}

	// quicraft has two scenarios, etcd has only one.
	qSuite := NewBenchmarkSuite("quicraft", "1.0.0")
	qSuite.AddResult(ScenarioProposeSEQWAL, 5000, time.Second, nil)
	qSuite.AddResult(ScenarioElectionWAL, 100, 500*time.Millisecond, nil)
	suites["quicraft"] = qSuite

	eSuite := NewBenchmarkSuite("etcd", "3.5.0")
	eSuite.AddResult(ScenarioProposeSEQWAL, 3000, time.Second, nil)
	suites["etcd"] = eSuite

	outPath := filepath.Join(t.TempDir(), "chart.svg")
	if err := GenerateComparisonChart(suites, outPath); err != nil {
		t.Fatalf("GenerateComparisonChart failed: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	if err := xml.Unmarshal(data, new(interface{})); err != nil {
		t.Fatalf("output is not valid XML: %v", err)
	}

	content := string(data)
	// etcd should have N/A for the election scenario.
	if !strings.Contains(content, "N/A") {
		t.Fatal("expected N/A for missing etcd scenario")
	}
}

func TestGenerateComparisonChartFourSystems(t *testing.T) {
	suites := map[string]*BenchmarkSuite{}

	for _, sys := range []string{"quicraft", "etcd", "dragonboat", "openraft"} {
		s := NewBenchmarkSuite(sys, "1.0.0")
		s.AddResult(ScenarioProposeSEQWAL, 1000, time.Second, nil)
		suites[sys] = s
	}

	outPath := filepath.Join(t.TempDir(), "four.svg")
	if err := GenerateComparisonChart(suites, outPath); err != nil {
		t.Fatalf("GenerateComparisonChart failed: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	if err := xml.Unmarshal(data, new(interface{})); err != nil {
		t.Fatalf("output is not valid XML: %v", err)
	}

	content := string(data)
	for _, sys := range []string{"quicraft", "etcd", "dragonboat", "openraft"} {
		if !strings.Contains(content, sys) {
			t.Fatalf("expected %q in comparison chart", sys)
		}
	}
}

func TestEscapeXMLSpecialCharacters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ampersand",
			input:    "A & B",
			expected: "A &amp; B",
		},
		{
			name:     "less_than",
			input:    "A < B",
			expected: "A &lt; B",
		},
		{
			name:     "greater_than",
			input:    "A > B",
			expected: "A &gt; B",
		},
		{
			name:     "double_quote",
			input:    `A "B" C`,
			expected: "A &quot;B&quot; C",
		},
		{
			name:     "no_special_chars",
			input:    "plain text",
			expected: "plain text",
		},
		{
			name:     "all_special_chars",
			input:    `<tag attr="val">&data</tag>`,
			expected: "&lt;tag attr=&quot;val&quot;&gt;&amp;data&lt;/tag&gt;",
		},
		{
			name:     "empty_string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeXML(tt.input)
			if got != tt.expected {
				t.Fatalf("escapeXML(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestColorFor(t *testing.T) {
	tests := []struct {
		name     string
		system   string
		expected string
	}{
		{name: "quicraft", system: "quicraft", expected: "#2196F3"},
		{name: "etcd", system: "etcd", expected: "#4CAF50"},
		{name: "dragonboat", system: "dragonboat", expected: "#FF9800"},
		{name: "openraft", system: "openraft", expected: "#F44336"},
		{name: "unknown", system: "my-custom-raft", expected: defaultColor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := colorFor(tt.system)
			if got != tt.expected {
				t.Fatalf("colorFor(%q) = %q, want %q", tt.system, got, tt.expected)
			}
		})
	}
}

func TestScaleBar(t *testing.T) {
	tests := []struct {
		name     string
		ops      float64
		maxOps   float64
		maxWidth int
		expected int
	}{
		{name: "zero_ops", ops: 0, maxOps: 1000, maxWidth: 800, expected: 0},
		{name: "zero_max", ops: 500, maxOps: 0, maxWidth: 800, expected: 0},
		{name: "full_width", ops: 1000, maxOps: 1000, maxWidth: 800, expected: 800},
		{name: "half_width", ops: 500, maxOps: 1000, maxWidth: 800, expected: 400},
		{name: "tiny_ops_min_1", ops: 1, maxOps: 1_000_000, maxWidth: 800, expected: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scaleBar(tt.ops, tt.maxOps, tt.maxWidth)
			if got != tt.expected {
				t.Fatalf("scaleBar(%f, %f, %d) = %d, want %d", tt.ops, tt.maxOps, tt.maxWidth, got, tt.expected)
			}
		})
	}
}

func TestCollectPresentScenariosEmpty(t *testing.T) {
	suites := map[string]*BenchmarkSuite{
		"quicraft": NewBenchmarkSuite("quicraft", "1.0.0"),
	}
	scenarios := collectPresentScenarios(suites)
	if len(scenarios) != 0 {
		t.Fatalf("expected 0 present scenarios, got %d", len(scenarios))
	}
}

func TestCollectPresentScenariosOrdered(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	// Add scenarios out of AllScenarios order.
	suite.AddResult(ScenarioProposeSEQWAL, 1000, time.Second, nil)
	suite.AddResult(ScenarioElectionWAL, 100, time.Second, nil)

	suites := map[string]*BenchmarkSuite{"quicraft": suite}
	scenarios := collectPresentScenarios(suites)

	if len(scenarios) != 2 {
		t.Fatalf("expected 2 scenarios, got %d", len(scenarios))
	}
	// ElectionWAL comes before ProposeSEQWAL in AllScenarios.
	if scenarios[0] != ScenarioElectionWAL {
		t.Fatalf("expected first scenario %q, got %q", ScenarioElectionWAL, scenarios[0])
	}
	if scenarios[1] != ScenarioProposeSEQWAL {
		t.Fatalf("expected second scenario %q, got %q", ScenarioProposeSEQWAL, scenarios[1])
	}
}

func TestFindMaxOps(t *testing.T) {
	suites := map[string]*BenchmarkSuite{}

	qSuite := NewBenchmarkSuite("quicraft", "1.0.0")
	qSuite.AddResult(ScenarioProposeSEQWAL, 5000, time.Second, nil)
	suites["quicraft"] = qSuite

	eSuite := NewBenchmarkSuite("etcd", "3.5.0")
	eSuite.AddResult(ScenarioProposeSEQWAL, 3000, time.Second, nil)
	suites["etcd"] = eSuite

	scenarios := []string{ScenarioProposeSEQWAL}
	maxOps := findMaxOps(suites, scenarios)
	if maxOps != 5000 {
		t.Fatalf("expected maxOps=5000, got %f", maxOps)
	}
}

func TestFindMaxOpsNoResults(t *testing.T) {
	suites := map[string]*BenchmarkSuite{
		"quicraft": NewBenchmarkSuite("quicraft", "1.0.0"),
	}
	maxOps := findMaxOps(suites, []string{ScenarioElectionWAL})
	if maxOps != 0 {
		t.Fatalf("expected maxOps=0, got %f", maxOps)
	}
}

func TestOrderedSystemKeys(t *testing.T) {
	suites := map[string]*BenchmarkSuite{
		"openraft":   NewBenchmarkSuite("openraft", "0.8.0"),
		"quicraft":   NewBenchmarkSuite("quicraft", "1.0.0"),
		"dragonboat": NewBenchmarkSuite("dragonboat", "4.0.0"),
	}

	keys := orderedSystemKeys(suites)
	if keys[0] != "quicraft" {
		t.Fatalf("expected quicraft first, got %q", keys[0])
	}
	if keys[1] != "dragonboat" {
		t.Fatalf("expected dragonboat second, got %q", keys[1])
	}
	if keys[2] != "openraft" {
		t.Fatalf("expected openraft third, got %q", keys[2])
	}
}

func TestOrderedSystemKeysUnknown(t *testing.T) {
	suites := map[string]*BenchmarkSuite{
		"custom": NewBenchmarkSuite("custom", "1.0.0"),
	}

	keys := orderedSystemKeys(suites)
	if len(keys) != 1 || keys[0] != "custom" {
		t.Fatalf("expected [custom], got %v", keys)
	}
}

func TestGenerateSystemChartXMLEscapedContent(t *testing.T) {
	// Use a system name with XML-special characters.
	suite := NewBenchmarkSuite("sys<&>", "1.0.0")
	suite.AddResult(ScenarioElectionWAL, 100, time.Second, nil)

	outPath := filepath.Join(t.TempDir(), "escape.svg")
	if err := GenerateSystemChart(suite, outPath); err != nil {
		t.Fatalf("GenerateSystemChart failed: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	// The output should be valid XML despite the special characters.
	if err := xml.Unmarshal(data, new(interface{})); err != nil {
		t.Fatalf("output is not valid XML with escaped content: %v", err)
	}
}
