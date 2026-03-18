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
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestNewBenchmarkSuite(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")

	if suite.System != "quicraft" {
		t.Fatalf("expected system %q, got %q", "quicraft", suite.System)
	}
	if suite.Version != "1.0.0" {
		t.Fatalf("expected version %q, got %q", "1.0.0", suite.Version)
	}
	if suite.Timestamp == "" {
		t.Fatal("expected non-empty timestamp")
	}
	if _, err := time.Parse(time.RFC3339, suite.Timestamp); err != nil {
		t.Fatalf("expected RFC3339 timestamp, got parse error: %v", err)
	}
	if suite.Results == nil {
		t.Fatal("expected non-nil results map")
	}
	if len(suite.Results) != 0 {
		t.Fatalf("expected empty results map, got %d entries", len(suite.Results))
	}
}

func TestNewBenchmarkSuiteDefaultConfig(t *testing.T) {
	suite := NewBenchmarkSuite("etcd", "3.5.0")

	if suite.Config.PayloadSize != 128 {
		t.Fatalf("expected payload size 128, got %d", suite.Config.PayloadSize)
	}
	if suite.Config.ConcurrentGoroutines != 8 {
		t.Fatalf("expected concurrent goroutines 8, got %d", suite.Config.ConcurrentGoroutines)
	}
	if suite.Config.TLS {
		t.Fatal("expected TLS to be false by default (single-node has no transport)")
	}
	if suite.Config.RTTMs != 1 {
		t.Fatalf("expected RTT 1ms, got %d", suite.Config.RTTMs)
	}
	if suite.Config.ElectionRTT != 10 {
		t.Fatalf("expected election RTT 10, got %d", suite.Config.ElectionRTT)
	}
	if suite.Config.HeartbeatRTT != 1 {
		t.Fatalf("expected heartbeat RTT 1, got %d", suite.Config.HeartbeatRTT)
	}
}

func TestAddResultWithRecorder(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	rec := NewLatencyRecorder(100)
	for i := 1; i <= 100; i++ {
		rec.Record(time.Duration(i) * time.Microsecond)
	}

	suite.AddResult(ScenarioProposeSEQWAL, 1000, 2*time.Second, rec)

	r, ok := suite.Results[ScenarioProposeSEQWAL]
	if !ok {
		t.Fatal("expected result for scenario")
	}
	if r.Ops != 1000 {
		t.Fatalf("expected ops 1000, got %d", r.Ops)
	}
	if r.DurationNs != (2 * time.Second).Nanoseconds() {
		t.Fatalf("expected duration 2s in ns, got %d", r.DurationNs)
	}
	if r.OpsPerSec != 500.0 {
		t.Fatalf("expected 500 ops/sec, got %f", r.OpsPerSec)
	}
	if r.Latency.Count != 100 {
		t.Fatalf("expected latency count 100, got %d", r.Latency.Count)
	}
	if r.Latency.P999Ns <= 0 {
		t.Fatal("expected positive P999 latency")
	}
	if r.Latency.MinNs <= 0 {
		t.Fatal("expected positive min latency")
	}
	if r.Latency.MaxNs <= 0 {
		t.Fatal("expected positive max latency")
	}
}

func TestAddResultNilRecorder(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	suite.AddResult(ScenarioElectionWAL, 50, 500*time.Millisecond, nil)

	r := suite.Results[ScenarioElectionWAL]
	if r.Latency.Count != 0 {
		t.Fatalf("expected zero latency count with nil recorder, got %d", r.Latency.Count)
	}
	if r.Latency.P50Ns != 0 || r.Latency.P99Ns != 0 || r.Latency.P999Ns != 0 {
		t.Fatal("expected zero latency stats with nil recorder")
	}
}

func TestAddResultEmptyRecorder(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	rec := NewLatencyRecorder(10)
	suite.AddResult(ScenarioElectionMemory, 10, time.Second, rec)

	r := suite.Results[ScenarioElectionMemory]
	if r.Latency.Count != 0 {
		t.Fatalf("expected zero latency count with empty recorder, got %d", r.Latency.Count)
	}
}

func TestAddResultZeroDuration(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	suite.AddResult(ScenarioElectionWAL, 100, 0, nil)

	r := suite.Results[ScenarioElectionWAL]
	if r.OpsPerSec != 0 {
		t.Fatalf("expected 0 ops/sec for zero duration, got %f", r.OpsPerSec)
	}
}

func TestAddResultOverwrite(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	suite.AddResult(ScenarioElectionWAL, 100, time.Second, nil)
	suite.AddResult(ScenarioElectionWAL, 200, time.Second, nil)

	r := suite.Results[ScenarioElectionWAL]
	if r.Ops != 200 {
		t.Fatalf("expected overwritten ops 200, got %d", r.Ops)
	}
}

func TestWriteSuiteToAndReadSuiteFromOutput(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	rec := NewLatencyRecorder(10)
	for i := 1; i <= 10; i++ {
		rec.Record(time.Duration(i) * time.Millisecond)
	}
	suite.AddResult(ScenarioProposeSEQWAL, 500, time.Second, rec)

	var buf bytes.Buffer
	if err := WriteSuiteTo(&buf, suite); err != nil {
		t.Fatalf("WriteSuiteTo failed: %v", err)
	}

	output := buf.Bytes()

	// Verify sentinel markers are present.
	if !bytes.Contains(output, []byte(SentinelBegin)) {
		t.Fatal("output missing begin sentinel")
	}
	if !bytes.Contains(output, []byte(SentinelEnd)) {
		t.Fatal("output missing end sentinel")
	}

	// Parse it back.
	parsed, err := ReadSuiteFromOutput(output)
	if err != nil {
		t.Fatalf("ReadSuiteFromOutput failed: %v", err)
	}
	if parsed.System != "quicraft" {
		t.Fatalf("expected system %q, got %q", "quicraft", parsed.System)
	}
	if parsed.Version != "1.0.0" {
		t.Fatalf("expected version %q, got %q", "1.0.0", parsed.Version)
	}
	r, ok := parsed.Results[ScenarioProposeSEQWAL]
	if !ok {
		t.Fatal("expected result for scenario after round-trip")
	}
	if r.Ops != 500 {
		t.Fatalf("expected ops 500, got %d", r.Ops)
	}
}

func TestWriteSuiteToMixedOutput(t *testing.T) {
	suite := NewBenchmarkSuite("etcd", "3.5.0")
	suite.AddResult(ScenarioElectionWAL, 10, 100*time.Millisecond, nil)

	var buf bytes.Buffer
	// Simulate mixed test output surrounding the sentinel block.
	buf.WriteString("PASS\nok  some/package 0.123s\n")
	if err := WriteSuiteTo(&buf, suite); err != nil {
		t.Fatalf("WriteSuiteTo failed: %v", err)
	}
	buf.WriteString("more output after sentinel\n")

	parsed, err := ReadSuiteFromOutput(buf.Bytes())
	if err != nil {
		t.Fatalf("ReadSuiteFromOutput with mixed output failed: %v", err)
	}
	if parsed.System != "etcd" {
		t.Fatalf("expected system %q, got %q", "etcd", parsed.System)
	}
}

func TestReadSuiteFromOutputMissingBeginSentinel(t *testing.T) {
	output := []byte("no sentinel here\n===PERF_JSON_END===\n")
	_, err := ReadSuiteFromOutput(output)
	if err == nil {
		t.Fatal("expected error for missing begin sentinel")
	}
	var noSentinel *ErrNoSentinel
	if !errors.As(err, &noSentinel) {
		t.Fatalf("expected ErrNoSentinel, got %T: %v", err, err)
	}
}

func TestReadSuiteFromOutputMissingEndSentinel(t *testing.T) {
	output := []byte("===PERF_JSON_BEGIN===\n{}\n")
	_, err := ReadSuiteFromOutput(output)
	if err == nil {
		t.Fatal("expected error for missing end sentinel")
	}
	var noSentinel *ErrNoSentinel
	if !errors.As(err, &noSentinel) {
		t.Fatalf("expected ErrNoSentinel, got %T: %v", err, err)
	}
}

func TestReadSuiteFromOutputEmptyInput(t *testing.T) {
	_, err := ReadSuiteFromOutput([]byte{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}
	var noSentinel *ErrNoSentinel
	if !errors.As(err, &noSentinel) {
		t.Fatalf("expected ErrNoSentinel, got %T: %v", err, err)
	}
}

func TestReadSuiteFromOutputMalformedJSON(t *testing.T) {
	output := []byte("===PERF_JSON_BEGIN===\n{not-valid-json}\n===PERF_JSON_END===\n")
	_, err := ReadSuiteFromOutput(output)
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
	var parseErr *ErrParseFailure
	if !errors.As(err, &parseErr) {
		t.Fatalf("expected ErrParseFailure, got %T: %v", err, err)
	}
}

func TestBenchmarkSuiteJSONRoundTrip(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "2.0.0")
	suite.Config.PayloadSize = 256
	suite.Config.ConcurrentGoroutines = 16
	suite.AddResult(ScenarioProposeSEQWAL, 10000, 5*time.Second, nil)

	data, err := json.Marshal(suite)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded BenchmarkSuite
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.System != suite.System {
		t.Fatalf("system mismatch: got %q, want %q", decoded.System, suite.System)
	}
	if decoded.Config.PayloadSize != 256 {
		t.Fatalf("expected payload size 256, got %d", decoded.Config.PayloadSize)
	}
	if decoded.Config.ConcurrentGoroutines != 16 {
		t.Fatalf("expected concurrent goroutines 16, got %d", decoded.Config.ConcurrentGoroutines)
	}
	r, ok := decoded.Results[ScenarioProposeSEQWAL]
	if !ok {
		t.Fatal("expected result after JSON round-trip")
	}
	if r.Ops != 10000 {
		t.Fatalf("expected ops 10000, got %d", r.Ops)
	}
}

func TestSentinelConstants(t *testing.T) {
	if SentinelBegin == "" {
		t.Fatal("SentinelBegin must not be empty")
	}
	if SentinelEnd == "" {
		t.Fatal("SentinelEnd must not be empty")
	}
	if SentinelBegin == SentinelEnd {
		t.Fatal("SentinelBegin and SentinelEnd must be different")
	}
}

func TestCoreAndAllScenariosNotEmpty(t *testing.T) {
	if len(CoreScenarios) == 0 {
		t.Fatal("CoreScenarios must not be empty")
	}
	if len(AllScenarios) == 0 {
		t.Fatal("AllScenarios must not be empty")
	}
	if len(AllScenarios) < len(CoreScenarios) {
		t.Fatal("AllScenarios must contain at least all CoreScenarios")
	}

	allSet := make(map[string]bool, len(AllScenarios))
	for _, s := range AllScenarios {
		allSet[s] = true
	}
	for _, s := range CoreScenarios {
		if !allSet[s] {
			t.Fatalf("CoreScenario %q missing from AllScenarios", s)
		}
	}
}

// errWriter is an io.Writer that always returns an error.
type errWriter struct{}

func (ew *errWriter) Write([]byte) (int, error) {
	return 0, &ErrParseFailure{Detail: "write failed"}
}

func TestWriteSuiteToWriterError(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	err := WriteSuiteTo(&errWriter{}, suite)
	if err == nil {
		t.Fatal("expected error from failing writer")
	}
}

func TestWriteSuiteToStdoutCompiles(t *testing.T) {
	// We cannot easily capture stdout in a test, but we verify the function
	// signature exists and compiles. A deeper test is done via WriteSuiteTo.
	_ = WriteSuiteToStdout
}

func TestErrorTypes(t *testing.T) {
	t.Run("ErrNoResults", func(t *testing.T) {
		err := &ErrNoResults{}
		msg := err.Error()
		if msg == "" {
			t.Fatal("expected non-empty error message")
		}
		if msg != "no benchmark results collected" {
			t.Fatalf("unexpected error message: %q", msg)
		}
	})

	t.Run("ErrParseFailure", func(t *testing.T) {
		err := &ErrParseFailure{Detail: "bad json"}
		msg := err.Error()
		if msg == "" {
			t.Fatal("expected non-empty error message")
		}
		if msg != "failed to parse benchmark output: bad json" {
			t.Fatalf("unexpected error message: %q", msg)
		}
	})

	t.Run("ErrNoSentinel", func(t *testing.T) {
		err := &ErrNoSentinel{}
		msg := err.Error()
		if msg == "" {
			t.Fatal("expected non-empty error message")
		}
		if msg != "sentinel markers not found in benchmark output" {
			t.Fatalf("unexpected error message: %q", msg)
		}
	})

	t.Run("ErrSystemUnknown", func(t *testing.T) {
		err := &ErrSystemUnknown{System: "myraft"}
		msg := err.Error()
		if msg == "" {
			t.Fatal("expected non-empty error message")
		}
		if msg != "unknown benchmark system: myraft" {
			t.Fatalf("unexpected error message: %q", msg)
		}
	})
}

// limitWriter writes at most N bytes and then returns an error.
type limitWriter struct {
	limit   int
	written int
}

func (lw *limitWriter) Write(p []byte) (int, error) {
	remaining := lw.limit - lw.written
	if remaining <= 0 {
		return 0, &ErrParseFailure{Detail: "limit exceeded"}
	}
	n := len(p)
	if n > remaining {
		n = remaining
	}
	lw.written += n
	return n, nil
}

func TestWriteSuiteToPartialWriteErrors(t *testing.T) {
	suite := NewBenchmarkSuite("quicraft", "1.0.0")
	suite.AddResult(ScenarioElectionWAL, 100, 500*time.Millisecond, nil)

	// Allow only a small number of bytes to be written, then fail.
	// This tests the intermediate write error paths in WriteSuiteTo.
	// The sentinel begin line is ~22 bytes.
	lw := &limitWriter{limit: 25}
	err := WriteSuiteTo(lw, suite)
	if err == nil {
		t.Fatal("expected error from limit writer")
	}
}
