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

// ComparisonTable holds benchmark suites for N systems keyed by system name.
type ComparisonTable struct {
	Suites map[string]*BenchmarkSuite
}

// NewComparisonTable creates an empty comparison table.
func NewComparisonTable() *ComparisonTable {
	return &ComparisonTable{
		Suites: make(map[string]*BenchmarkSuite),
	}
}

// Add registers a benchmark suite for comparison.
func (ct *ComparisonTable) Add(suite *BenchmarkSuite) {
	ct.Suites[suite.System] = suite
}

// ComparisonReport is the output of Compare(), containing per-scenario
// analysis across all systems.
type ComparisonReport struct {
	Systems   []string
	Scenarios []*ScenarioComparison
}

// ScenarioComparison holds comparison data for a single scenario.
type ScenarioComparison struct {
	Scenario string
	// Results maps system name to its ScenarioResult (nil if not available).
	Results map[string]*ScenarioResult
	// ThroughputWinner is the system name with highest ops/sec.
	ThroughputWinner string
	// LatencyWinner is the system name with lowest P99 latency.
	LatencyWinner string
	// ThroughputSpeedups maps system name to speedup ratio vs the slowest.
	ThroughputSpeedups map[string]float64
}

// metricComparator defines comparison direction for a metric.
type metricComparator struct {
	extract    func(*ScenarioResult) float64
	higherWins bool
}

var metricComparators = map[string]metricComparator{
	"throughput": {
		extract:    func(r *ScenarioResult) float64 { return r.OpsPerSec },
		higherWins: true,
	},
	"p99_latency": {
		extract:    func(r *ScenarioResult) float64 { return float64(r.Latency.P99Ns) },
		higherWins: false,
	},
}

// Compare analyzes all suites in the table and produces a ComparisonReport.
func (ct *ComparisonTable) Compare() *ComparisonReport {
	systems := ct.orderedSystems()
	report := &ComparisonReport{
		Systems: systems,
	}

	for _, scenario := range AllScenarios {
		sc := &ScenarioComparison{
			Scenario:           scenario,
			Results:            make(map[string]*ScenarioResult),
			ThroughputSpeedups: make(map[string]float64),
		}

		hasAny := false
		for _, sys := range systems {
			suite, ok := ct.Suites[sys]
			if !ok {
				continue
			}
			if result, exists := suite.Results[scenario]; exists {
				sc.Results[sys] = result
				hasAny = true
			}
		}

		if !hasAny {
			continue
		}

		// Only compute winners/speedups when comparing 2+ systems.
		if len(sc.Results) > 1 {
			sc.ThroughputWinner = findWinner(sc.Results, metricComparators["throughput"])
			sc.LatencyWinner = findWinner(sc.Results, metricComparators["p99_latency"])
			computeSpeedups(sc)
		}

		report.Scenarios = append(report.Scenarios, sc)
	}

	return report
}

// orderedSystems returns system names in a stable order.
func (ct *ComparisonTable) orderedSystems() []string {
	// Preferred order: quicraft first, then alphabetical.
	preferred := []string{"quicraft", "etcd", "dragonboat", "openraft"}
	var result []string
	seen := make(map[string]bool)

	for _, sys := range preferred {
		if _, ok := ct.Suites[sys]; ok {
			result = append(result, sys)
			seen[sys] = true
		}
	}

	for sys := range ct.Suites {
		if !seen[sys] {
			result = append(result, sys)
		}
	}
	return result
}

// findWinner returns the system name that wins for the given metric comparator.
func findWinner(results map[string]*ScenarioResult, mc metricComparator) string {
	var winner string
	var winnerVal float64
	first := true

	for sys, r := range results {
		val := mc.extract(r)
		if val == 0 {
			continue
		}
		if first {
			winner = sys
			winnerVal = val
			first = false
			continue
		}
		if mc.higherWins && val > winnerVal {
			winner = sys
			winnerVal = val
		} else if !mc.higherWins && val < winnerVal {
			winner = sys
			winnerVal = val
		}
	}
	return winner
}

// computeSpeedups calculates throughput speedup ratios relative to the
// slowest system.
func computeSpeedups(sc *ScenarioComparison) {
	var minOps float64
	first := true
	for _, r := range sc.Results {
		if r.OpsPerSec > 0 {
			if first || r.OpsPerSec < minOps {
				minOps = r.OpsPerSec
				first = false
			}
		}
	}

	if minOps == 0 {
		return
	}

	for sys, r := range sc.Results {
		if r.OpsPerSec > 0 {
			sc.ThroughputSpeedups[sys] = r.OpsPerSec / minOps
		}
	}
}
