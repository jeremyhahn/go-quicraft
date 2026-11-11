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
	"bytes"
	"strings"
	"testing"
)

func TestPrintTerminalEmptyReport(t *testing.T) {
	report := &ComparisonReport{
		Systems:   []string{},
		Scenarios: nil,
	}

	var buf bytes.Buffer
	if err := report.PrintTerminal(&buf); err != nil {
		t.Fatalf("PrintTerminal: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "No benchmark results to report.") {
		t.Fatalf("expected 'No benchmark results' message, got: %s", output)
	}
}

func TestPrintTerminalContainsScenarioLabelAndWinner(t *testing.T) {
	report := buildSampleReport()

	var buf bytes.Buffer
	if err := report.PrintTerminal(&buf); err != nil {
		t.Fatalf("PrintTerminal: %v", err)
	}
	output := buf.String()

	// scenarioLabels maps ScenarioProposeSEQWAL to "Propose Sequential (WAL)".
	if !strings.Contains(output, "Propose Sequential (WAL)") {
		t.Fatalf("expected scenario label in output, got:\n%s", output)
	}
	if !strings.Contains(output, "WINNER") {
		t.Fatal("expected 'WINNER' label in output")
	}
}

func TestPrintTerminalMissingSystemShowsNA(t *testing.T) {
	report := &ComparisonReport{
		Systems: []string{"quicraft", "etcd"},
		Scenarios: []*ScenarioComparison{
			{
				Scenario: ScenarioElectionWAL,
				Results: map[string]*ScenarioResult{
					"quicraft": {Ops: 100, OpsPerSec: 100},
					// etcd is missing.
				},
				ThroughputSpeedups: map[string]float64{},
			},
		},
	}

	var buf bytes.Buffer
	if err := report.PrintTerminal(&buf); err != nil {
		t.Fatalf("PrintTerminal: %v", err)
	}
	output := buf.String()

	// etcd row should show N/A for all values (7 columns: Ops/sec, P50, P90, P95, P99, P99.9, Mean).
	lines := strings.Split(output, "\n")
	foundEtcdNA := false
	for _, line := range lines {
		if strings.Contains(line, "etcd") && strings.Count(line, "N/A") >= 7 {
			foundEtcdNA = true
			break
		}
	}
	if !foundEtcdNA {
		t.Fatal("expected etcd row with N/A values for missing results")
	}
}

func TestPrintTerminalSpeedupRow(t *testing.T) {
	report := &ComparisonReport{
		Systems: []string{"quicraft", "etcd"},
		Scenarios: []*ScenarioComparison{
			{
				Scenario: ScenarioProposeSEQWAL,
				Results: map[string]*ScenarioResult{
					"quicraft": {Ops: 3000, OpsPerSec: 3000},
					"etcd":     {Ops: 1000, OpsPerSec: 1000},
				},
				ThroughputWinner:   "quicraft",
				ThroughputSpeedups: map[string]float64{"quicraft": 3.0, "etcd": 1.0},
			},
		},
	}

	var buf bytes.Buffer
	if err := report.PrintTerminal(&buf); err != nil {
		t.Fatalf("PrintTerminal: %v", err)
	}
	output := buf.String()

	if !strings.Contains(output, "speedup") {
		t.Fatalf("expected 'speedup' in output for 3x difference, got:\n%s", output)
	}
}

func TestPrintTerminalSingleSystemNoWinnerRow(t *testing.T) {
	report := &ComparisonReport{
		Systems: []string{"quicraft"},
		Scenarios: []*ScenarioComparison{
			{
				Scenario: ScenarioElectionWAL,
				Results: map[string]*ScenarioResult{
					"quicraft": {Ops: 100, OpsPerSec: 100},
				},
				ThroughputWinner:   "",
				LatencyWinner:      "",
				ThroughputSpeedups: map[string]float64{},
			},
		},
	}

	var buf bytes.Buffer
	if err := report.PrintTerminal(&buf); err != nil {
		t.Fatalf("PrintTerminal: %v", err)
	}
	output := buf.String()

	// When there's only one system and no winners, the WINNER row should be suppressed.
	if strings.Contains(output, "WINNER") {
		t.Fatal("expected no WINNER row for single-system benchmark")
	}
	// But the benchmark title should reflect single-system mode.
	if !strings.Contains(output, "Performance Benchmark: quicraft") {
		t.Fatalf("expected single-system title, got:\n%s", output)
	}
}

func TestPrintTerminalUnknownScenarioKeyUsedAsLabel(t *testing.T) {
	report := &ComparisonReport{
		Systems: []string{"quicraft"},
		Scenarios: []*ScenarioComparison{
			{
				Scenario: "custom_scenario_xyz",
				Results: map[string]*ScenarioResult{
					"quicraft": {Ops: 100, OpsPerSec: 100},
				},
				ThroughputWinner:   "quicraft",
				ThroughputSpeedups: map[string]float64{},
			},
		},
	}

	var buf bytes.Buffer
	if err := report.PrintTerminal(&buf); err != nil {
		t.Fatalf("PrintTerminal: %v", err)
	}
	output := buf.String()

	// Unknown scenario key should be used directly as label.
	if !strings.Contains(output, "custom_scenario_xyz") {
		t.Fatal("expected unknown scenario key to be used as label")
	}
}

func TestFormatOps(t *testing.T) {
	tests := []struct {
		name string
		ops  float64
		want string
	}{
		{name: "zero_na", ops: 0, want: "N/A"},
		{name: "small", ops: 500.5, want: "500.5"},
		{name: "k_lower_bound", ops: 1000, want: "1.00K"},
		{name: "m_lower_bound", ops: 1_000_000, want: "1.00M"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatOps(tt.ops); got != tt.want {
				t.Fatalf("FormatOps(%v) = %q, want %q", tt.ops, got, tt.want)
			}
		})
	}
}

func TestFormatDurationNs(t *testing.T) {
	tests := []struct {
		name string
		ns   int64
		want string
	}{
		{name: "zero", ns: 0, want: "N/A"},
		{name: "nanoseconds", ns: 500, want: "500ns"},
		{name: "ns_upper_bound", ns: 999, want: "999ns"},
		{name: "us_lower_bound", ns: 1000, want: "1.0us"},
		{name: "microseconds", ns: 5000, want: "5.0us"},
		{name: "us_upper_bound", ns: 999_999, want: "1000.0us"},
		{name: "ms_lower_bound", ns: 1_000_000, want: "1.00ms"},
		{name: "milliseconds", ns: 5_000_000, want: "5.00ms"},
		{name: "ms_upper_bound", ns: 999_999_999, want: "1000.00ms"},
		{name: "s_lower_bound", ns: 1_000_000_000, want: "1.000s"},
		{name: "seconds", ns: 2_500_000_000, want: "2.500s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatDurationNs(tt.ns); got != tt.want {
				t.Fatalf("FormatDurationNs(%d) = %q, want %q", tt.ns, got, tt.want)
			}
		})
	}
}

func TestFormatDurationWrapsFormatDurationNs(t *testing.T) {
	// FormatDuration should delegate to FormatDurationNs.
	got := FormatDuration(5 * 1_000_000) // 5ms in nanoseconds as Duration
	expected := FormatDurationNs(5_000_000)
	if got != expected {
		t.Fatalf("FormatDuration(%v)=%q != FormatDurationNs(%d)=%q", 5*1_000_000, got, 5_000_000, expected)
	}
}

func TestTruncateExactLength(t *testing.T) {
	got := Truncate("hello", 5)
	if got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
}

func TestTruncateLongString(t *testing.T) {
	got := Truncate("hello world, this is a very long string", 15)
	if got != "hello world,..." {
		t.Fatalf("expected 'hello world,...', got %q", got)
	}
	if len(got) != 15 {
		t.Fatalf("expected length 15, got %d", len(got))
	}
}

// buildSampleReport creates a ComparisonReport with two systems and one scenario.
func buildSampleReport() *ComparisonReport {
	return &ComparisonReport{
		Systems: []string{"quicraft", "etcd"},
		Scenarios: []*ScenarioComparison{
			{
				Scenario: ScenarioProposeSEQWAL,
				Results: map[string]*ScenarioResult{
					"quicraft": {
						Ops: 5000, OpsPerSec: 5000, DurationNs: 1_000_000_000,
						Latency: LatencyStats{
							P50Ns: 100_000, P90Ns: 200_000, P95Ns: 300_000,
							P99Ns: 500_000, P999Ns: 750_000, MinNs: 50_000, MaxNs: 800_000,
							MeanNs: 150_000, Count: 5000,
						},
					},
					"etcd": {
						Ops: 3000, OpsPerSec: 3000, DurationNs: 1_000_000_000,
						Latency: LatencyStats{
							P50Ns: 200_000, P90Ns: 400_000, P95Ns: 600_000,
							P99Ns: 900_000, P999Ns: 1_100_000, MinNs: 100_000, MaxNs: 1_200_000,
							MeanNs: 300_000, Count: 3000,
						},
					},
				},
				ThroughputWinner:   "quicraft",
				LatencyWinner:      "quicraft",
				ThroughputSpeedups: map[string]float64{"quicraft": 1.67, "etcd": 1.0},
			},
		},
	}
}
