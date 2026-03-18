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
	"testing"
	"time"
)

func TestNewComparisonTable(t *testing.T) {
	ct := NewComparisonTable()
	if ct.Suites == nil {
		t.Fatal("expected non-nil suites map")
	}
	if len(ct.Suites) != 0 {
		t.Fatalf("expected empty suites map, got %d entries", len(ct.Suites))
	}
}

func TestComparisonTableAdd(t *testing.T) {
	ct := NewComparisonTable()
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	ct.Add(suite)

	if len(ct.Suites) != 1 {
		t.Fatalf("expected 1 suite, got %d", len(ct.Suites))
	}
	if ct.Suites["quicraft"] != suite {
		t.Fatal("expected stored suite to match")
	}
}

func TestComparisonTableAddOverwrite(t *testing.T) {
	ct := NewComparisonTable()
	suite1 := NewBenchmarkSuite("quicraft", "1.0.0")
	suite2 := NewBenchmarkSuite("quicraft", "2.0.0")

	ct.Add(suite1)
	ct.Add(suite2)

	if len(ct.Suites) != 1 {
		t.Fatalf("expected 1 suite after overwrite, got %d", len(ct.Suites))
	}
	if ct.Suites["quicraft"].Version != "2.0.0" {
		t.Fatal("expected suite to be overwritten with v2")
	}
}

func TestCompareEmptyTable(t *testing.T) {
	ct := NewComparisonTable()
	report := ct.Compare()

	if len(report.Systems) != 0 {
		t.Fatalf("expected 0 systems, got %d", len(report.Systems))
	}
	if len(report.Scenarios) != 0 {
		t.Fatalf("expected 0 scenarios, got %d", len(report.Scenarios))
	}
}

func TestCompareSingleSystem(t *testing.T) {
	ct := NewComparisonTable()
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	suite.AddResult(ScenarioProposeSEQWAL, 1000, time.Second, nil)
	ct.Add(suite)

	report := ct.Compare()

	if len(report.Systems) != 1 {
		t.Fatalf("expected 1 system, got %d", len(report.Systems))
	}
	if report.Systems[0] != "quicraft" {
		t.Fatalf("expected system %q, got %q", "quicraft", report.Systems[0])
	}
	if len(report.Scenarios) != 1 {
		t.Fatalf("expected 1 scenario, got %d", len(report.Scenarios))
	}
	sc := report.Scenarios[0]
	if sc.Scenario != ScenarioProposeSEQWAL {
		t.Fatalf("expected scenario %q, got %q", ScenarioProposeSEQWAL, sc.Scenario)
	}
	// Single-system comparison should NOT declare a winner.
	if sc.ThroughputWinner != "" {
		t.Fatalf("expected no throughput winner for single system, got %q", sc.ThroughputWinner)
	}
	if sc.LatencyWinner != "" {
		t.Fatalf("expected no latency winner for single system, got %q", sc.LatencyWinner)
	}
	if len(sc.ThroughputSpeedups) != 0 {
		t.Fatalf("expected no speedups for single system, got %d entries", len(sc.ThroughputSpeedups))
	}
}

func TestCompareMultipleSystems(t *testing.T) {
	ct := NewComparisonTable()

	// quicraft: 2000 ops/sec, p99 = 500us
	qSuite := NewBenchmarkSuite("quicraft", "1.0.0")
	qRec := NewLatencyRecorder(100)
	for i := 1; i <= 100; i++ {
		qRec.Record(time.Duration(i*5) * time.Microsecond) // 5us - 500us
	}
	qSuite.AddResult(ScenarioProposeSEQWAL, 2000, time.Second, qRec)
	ct.Add(qSuite)

	// etcd: 1000 ops/sec, p99 = 1000us
	eSuite := NewBenchmarkSuite("etcd", "3.5.0")
	eRec := NewLatencyRecorder(100)
	for i := 1; i <= 100; i++ {
		eRec.Record(time.Duration(i*10) * time.Microsecond) // 10us - 1000us
	}
	eSuite.AddResult(ScenarioProposeSEQWAL, 1000, time.Second, eRec)
	ct.Add(eSuite)

	report := ct.Compare()

	if len(report.Systems) != 2 {
		t.Fatalf("expected 2 systems, got %d", len(report.Systems))
	}

	// Verify order: quicraft first (preferred order).
	if report.Systems[0] != "quicraft" {
		t.Fatalf("expected quicraft first, got %q", report.Systems[0])
	}
	if report.Systems[1] != "etcd" {
		t.Fatalf("expected etcd second, got %q", report.Systems[1])
	}

	if len(report.Scenarios) != 1 {
		t.Fatalf("expected 1 scenario, got %d", len(report.Scenarios))
	}
	sc := report.Scenarios[0]

	// Throughput: quicraft wins (2000 > 1000).
	if sc.ThroughputWinner != "quicraft" {
		t.Fatalf("expected throughput winner %q, got %q", "quicraft", sc.ThroughputWinner)
	}

	// Latency: quicraft wins (lower p99).
	if sc.LatencyWinner != "quicraft" {
		t.Fatalf("expected latency winner %q, got %q", "quicraft", sc.LatencyWinner)
	}
}

func TestCompareThroughputHigherWins(t *testing.T) {
	ct := NewComparisonTable()

	// System A: 500 ops/sec
	suiteA := NewBenchmarkSuite("etcd", "3.5.0")
	suiteA.AddResult(ScenarioElectionWAL, 500, time.Second, nil)
	ct.Add(suiteA)

	// System B: 1500 ops/sec
	suiteB := NewBenchmarkSuite("dragonboat", "4.0.0")
	suiteB.AddResult(ScenarioElectionWAL, 1500, time.Second, nil)
	ct.Add(suiteB)

	report := ct.Compare()
	if len(report.Scenarios) < 1 {
		t.Fatal("expected at least 1 scenario")
	}

	sc := report.Scenarios[0]
	if sc.ThroughputWinner != "dragonboat" {
		t.Fatalf("expected throughput winner dragonboat (higher ops/sec), got %q", sc.ThroughputWinner)
	}
}

func TestCompareLatencyLowerWins(t *testing.T) {
	ct := NewComparisonTable()

	// quicraft: low latency
	qSuite := NewBenchmarkSuite("quicraft", "1.0.0")
	qRec := NewLatencyRecorder(100)
	for i := 1; i <= 100; i++ {
		qRec.Record(time.Duration(i) * time.Microsecond) // 1us - 100us
	}
	qSuite.AddResult(ScenarioCommitLatencyWAL, 1000, time.Second, qRec)
	ct.Add(qSuite)

	// etcd: high latency
	eSuite := NewBenchmarkSuite("etcd", "3.5.0")
	eRec := NewLatencyRecorder(100)
	for i := 1; i <= 100; i++ {
		eRec.Record(time.Duration(i*100) * time.Microsecond) // 100us - 10000us
	}
	eSuite.AddResult(ScenarioCommitLatencyWAL, 1000, time.Second, eRec)
	ct.Add(eSuite)

	report := ct.Compare()
	var sc *ScenarioComparison
	for _, s := range report.Scenarios {
		if s.Scenario == ScenarioCommitLatencyWAL {
			sc = s
			break
		}
	}
	if sc == nil {
		t.Fatal("expected commit latency scenario in report")
	}
	if sc.LatencyWinner != "quicraft" {
		t.Fatalf("expected latency winner quicraft (lower p99), got %q", sc.LatencyWinner)
	}
}

func TestCompareSpeedupComputation(t *testing.T) {
	ct := NewComparisonTable()

	// quicraft: 3000 ops/sec
	qSuite := NewBenchmarkSuite("quicraft", "1.0.0")
	qSuite.AddResult(ScenarioProposeSEQWAL, 3000, time.Second, nil)
	ct.Add(qSuite)

	// etcd: 1000 ops/sec
	eSuite := NewBenchmarkSuite("etcd", "3.5.0")
	eSuite.AddResult(ScenarioProposeSEQWAL, 1000, time.Second, nil)
	ct.Add(eSuite)

	report := ct.Compare()
	sc := report.Scenarios[0]

	// Speedup is relative to the slowest system (etcd at 1000 ops/sec).
	qSpeedup, ok := sc.ThroughputSpeedups["quicraft"]
	if !ok {
		t.Fatal("expected quicraft speedup entry")
	}
	if qSpeedup < 2.99 || qSpeedup > 3.01 {
		t.Fatalf("expected quicraft speedup ~3.0x, got %f", qSpeedup)
	}

	eSpeedup, ok := sc.ThroughputSpeedups["etcd"]
	if !ok {
		t.Fatal("expected etcd speedup entry")
	}
	if eSpeedup < 0.99 || eSpeedup > 1.01 {
		t.Fatalf("expected etcd speedup ~1.0x, got %f", eSpeedup)
	}
}

func TestCompareMissingScenarioShowsNil(t *testing.T) {
	ct := NewComparisonTable()

	// quicraft has the scenario, etcd does not.
	qSuite := NewBenchmarkSuite("quicraft", "1.0.0")
	qSuite.AddResult(ScenarioProposeSEQWAL, 1000, time.Second, nil)
	ct.Add(qSuite)

	eSuite := NewBenchmarkSuite("etcd", "3.5.0")
	// No results added.
	ct.Add(eSuite)

	report := ct.Compare()
	if len(report.Scenarios) < 1 {
		t.Fatal("expected at least 1 scenario")
	}

	sc := report.Scenarios[0]
	if _, ok := sc.Results["etcd"]; ok {
		t.Fatal("expected etcd result to be absent (nil)")
	}
	if _, ok := sc.Results["quicraft"]; !ok {
		t.Fatal("expected quicraft result to be present")
	}
}

func TestCompareNoScenariosWithResults(t *testing.T) {
	ct := NewComparisonTable()

	// Both systems have no results.
	ct.Add(NewBenchmarkSuite("quicraft", "1.0.0"))
	ct.Add(NewBenchmarkSuite("etcd", "3.5.0"))

	report := ct.Compare()
	if len(report.Scenarios) != 0 {
		t.Fatalf("expected 0 scenarios when no results, got %d", len(report.Scenarios))
	}
}

func TestOrderedSystemsPreferred(t *testing.T) {
	ct := NewComparisonTable()
	ct.Add(NewBenchmarkSuite("openraft", "0.8.0"))
	ct.Add(NewBenchmarkSuite("quicraft", "1.0.0"))
	ct.Add(NewBenchmarkSuite("etcd", "3.5.0"))
	ct.Add(NewBenchmarkSuite("dragonboat", "4.0.0"))

	systems := ct.orderedSystems()

	expected := []string{"quicraft", "etcd", "dragonboat", "openraft"}
	if len(systems) != len(expected) {
		t.Fatalf("expected %d systems, got %d", len(expected), len(systems))
	}
	for i, sys := range expected {
		if systems[i] != sys {
			t.Fatalf("expected systems[%d]=%q, got %q", i, sys, systems[i])
		}
	}
}

func TestOrderedSystemsUnknownAppended(t *testing.T) {
	ct := NewComparisonTable()
	ct.Add(NewBenchmarkSuite("quicraft", "1.0.0"))
	ct.Add(NewBenchmarkSuite("custom-raft", "0.1.0"))

	systems := ct.orderedSystems()

	if systems[0] != "quicraft" {
		t.Fatalf("expected quicraft first, got %q", systems[0])
	}
	if len(systems) != 2 {
		t.Fatalf("expected 2 systems, got %d", len(systems))
	}
	if systems[1] != "custom-raft" {
		t.Fatalf("expected custom-raft second, got %q", systems[1])
	}
}

func TestFindWinnerAllZeroValues(t *testing.T) {
	results := map[string]*ScenarioResult{
		"a": {OpsPerSec: 0},
		"b": {OpsPerSec: 0},
	}
	mc := metricComparators["throughput"]
	winner := findWinner(results, mc)
	// All values are 0, so no winner should be found.
	if winner != "" {
		t.Fatalf("expected empty winner for all-zero values, got %q", winner)
	}
}

func TestFindWinnerLatencyLowerWins(t *testing.T) {
	results := map[string]*ScenarioResult{
		"a": {Latency: LatencyStats{P99Ns: 5000}},
		"b": {Latency: LatencyStats{P99Ns: 1000}},
	}
	mc := metricComparators["p99_latency"]
	winner := findWinner(results, mc)
	if winner != "b" {
		t.Fatalf("expected latency winner %q, got %q", "b", winner)
	}
}

func TestComputeSpeedupsAllZeroOps(t *testing.T) {
	sc := &ScenarioComparison{
		Results: map[string]*ScenarioResult{
			"a": {OpsPerSec: 0},
			"b": {OpsPerSec: 0},
		},
		ThroughputSpeedups: make(map[string]float64),
	}
	computeSpeedups(sc)

	if len(sc.ThroughputSpeedups) != 0 {
		t.Fatalf("expected no speedup entries for zero ops, got %d", len(sc.ThroughputSpeedups))
	}
}

func TestCompareMultipleScenarios(t *testing.T) {
	ct := NewComparisonTable()

	qSuite := NewBenchmarkSuite("quicraft", "1.0.0")
	qSuite.AddResult(ScenarioElectionWAL, 100, time.Second, nil)
	qSuite.AddResult(ScenarioProposeSEQWAL, 5000, time.Second, nil)
	ct.Add(qSuite)

	eSuite := NewBenchmarkSuite("etcd", "3.5.0")
	eSuite.AddResult(ScenarioElectionWAL, 50, time.Second, nil)
	eSuite.AddResult(ScenarioProposeSEQWAL, 3000, time.Second, nil)
	ct.Add(eSuite)

	report := ct.Compare()

	if len(report.Scenarios) != 2 {
		t.Fatalf("expected 2 scenarios, got %d", len(report.Scenarios))
	}

	// Verify ordering matches AllScenarios order.
	if report.Scenarios[0].Scenario != ScenarioElectionWAL {
		t.Fatalf("expected first scenario %q, got %q", ScenarioElectionWAL, report.Scenarios[0].Scenario)
	}
	if report.Scenarios[1].Scenario != ScenarioProposeSEQWAL {
		t.Fatalf("expected second scenario %q, got %q", ScenarioProposeSEQWAL, report.Scenarios[1].Scenario)
	}
}
