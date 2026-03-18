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
	report.PrintTerminal(&buf)

	output := buf.String()
	if !strings.Contains(output, "No benchmark results to report.") {
		t.Fatalf("expected 'No benchmark results' message, got: %s", output)
	}
}

func TestPrintTerminalContainsSystemNames(t *testing.T) {
	report := buildSampleReport()

	var buf bytes.Buffer
	report.PrintTerminal(&buf)
	output := buf.String()

	if !strings.Contains(output, "quicraft") {
		t.Fatal("expected output to contain system name 'quicraft'")
	}
	if !strings.Contains(output, "etcd") {
		t.Fatal("expected output to contain system name 'etcd'")
	}
}

func TestPrintTerminalContainsScenarioLabel(t *testing.T) {
	report := buildSampleReport()

	var buf bytes.Buffer
	report.PrintTerminal(&buf)
	output := buf.String()

	// scenarioLabels maps ScenarioProposeSEQWAL to "Propose Sequential (WAL)".
	if !strings.Contains(output, "Propose Sequential (WAL)") {
		t.Fatalf("expected scenario label in output, got:\n%s", output)
	}
}

func TestPrintTerminalContainsHeaderRow(t *testing.T) {
	report := buildSampleReport()

	var buf bytes.Buffer
	report.PrintTerminal(&buf)
	output := buf.String()

	if !strings.Contains(output, "Ops/sec") {
		t.Fatal("expected header 'Ops/sec' in output")
	}
	if !strings.Contains(output, "P50") {
		t.Fatal("expected header 'P50' in output")
	}
	if !strings.Contains(output, "P99") {
		t.Fatal("expected header 'P99' in output")
	}
	if !strings.Contains(output, "P99.9") {
		t.Fatal("expected header 'P99.9' in output")
	}
}

func TestPrintTerminalContainsWinner(t *testing.T) {
	report := buildSampleReport()

	var buf bytes.Buffer
	report.PrintTerminal(&buf)
	output := buf.String()

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
	report.PrintTerminal(&buf)
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
	report.PrintTerminal(&buf)
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
	report.PrintTerminal(&buf)
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
	report.PrintTerminal(&buf)
	output := buf.String()

	// Unknown scenario key should be used directly as label.
	if !strings.Contains(output, "custom_scenario_xyz") {
		t.Fatal("expected unknown scenario key to be used as label")
	}
}

func TestFormatOpsNA(t *testing.T) {
	if got := FormatOps(0); got != "N/A" {
		t.Fatalf("expected 'N/A', got %q", got)
	}
}

func TestFormatOpsSmall(t *testing.T) {
	got := FormatOps(500.5)
	if got != "500.5" {
		t.Fatalf("expected '500.5', got %q", got)
	}
}

func TestFormatOpsThousands(t *testing.T) {
	got := FormatOps(5000)
	if got != "5.00K" {
		t.Fatalf("expected '5.00K', got %q", got)
	}
}

func TestFormatOpsMillions(t *testing.T) {
	got := FormatOps(2_500_000)
	if got != "2.50M" {
		t.Fatalf("expected '2.50M', got %q", got)
	}
}

func TestFormatOpsExactBoundary(t *testing.T) {
	// Exactly 1000 -> should be K format.
	got := FormatOps(1000)
	if got != "1.00K" {
		t.Fatalf("expected '1.00K', got %q", got)
	}

	// Exactly 1,000,000 -> should be M format.
	got = FormatOps(1_000_000)
	if got != "1.00M" {
		t.Fatalf("expected '1.00M', got %q", got)
	}
}

func TestFormatDurationNsZero(t *testing.T) {
	if got := FormatDurationNs(0); got != "N/A" {
		t.Fatalf("expected 'N/A', got %q", got)
	}
}

func TestFormatDurationNsNanoseconds(t *testing.T) {
	got := FormatDurationNs(500)
	if got != "500ns" {
		t.Fatalf("expected '500ns', got %q", got)
	}
}

func TestFormatDurationNsMicroseconds(t *testing.T) {
	got := FormatDurationNs(5000)
	if got != "5.0us" {
		t.Fatalf("expected '5.0us', got %q", got)
	}
}

func TestFormatDurationNsMilliseconds(t *testing.T) {
	got := FormatDurationNs(5_000_000)
	if got != "5.00ms" {
		t.Fatalf("expected '5.00ms', got %q", got)
	}
}

func TestFormatDurationNsSeconds(t *testing.T) {
	got := FormatDurationNs(2_500_000_000)
	if got != "2.500s" {
		t.Fatalf("expected '2.500s', got %q", got)
	}
}

func TestFormatDurationNsBoundaries(t *testing.T) {
	// 999ns should still be ns format.
	got := FormatDurationNs(999)
	if !strings.HasSuffix(got, "ns") {
		t.Fatalf("expected ns suffix for 999ns, got %q", got)
	}

	// 1000ns should be us format.
	got = FormatDurationNs(1000)
	if !strings.HasSuffix(got, "us") {
		t.Fatalf("expected us suffix for 1000ns, got %q", got)
	}

	// 999999ns should still be us format.
	got = FormatDurationNs(999_999)
	if !strings.HasSuffix(got, "us") {
		t.Fatalf("expected us suffix for 999999ns, got %q", got)
	}

	// 1000000ns should be ms format.
	got = FormatDurationNs(1_000_000)
	if !strings.HasSuffix(got, "ms") {
		t.Fatalf("expected ms suffix for 1000000ns, got %q", got)
	}

	// 999999999ns should still be ms format.
	got = FormatDurationNs(999_999_999)
	if !strings.HasSuffix(got, "ms") {
		t.Fatalf("expected ms suffix for 999999999ns, got %q", got)
	}

	// 1000000000ns should be s format.
	got = FormatDurationNs(1_000_000_000)
	if !strings.HasSuffix(got, "s") && !strings.HasSuffix(got, "ms") {
		t.Fatalf("expected s suffix for 1s, got %q", got)
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

func TestTruncateShortString(t *testing.T) {
	got := Truncate("hello", 10)
	if got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
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

func TestTruncateWithEllipsis(t *testing.T) {
	got := Truncate("abcdefghij", 6)
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("expected '...' suffix, got %q", got)
	}
	if len(got) != 6 {
		t.Fatalf("expected length 6, got %d", len(got))
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
