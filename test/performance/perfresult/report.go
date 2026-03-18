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
	"fmt"
	"io"
	"strings"
	"time"
)

// ScenarioLabels maps scenario keys to human-readable display names.
var ScenarioLabels = map[string]string{
	// Single-node (1N) scenarios.
	ScenarioElectionWAL:         "Election (WAL)",
	ScenarioElectionMemory:      "Election (Memory)",
	ScenarioProposeSEQWAL:       "Propose Sequential (WAL)",
	ScenarioProposeSEQMemory:    "Propose Sequential (Memory)",
	ScenarioProposeConc8WAL:     "Propose Concurrent/8 (WAL)",
	ScenarioProposeConc8Memory:  "Propose Concurrent/8 (Memory)",
	ScenarioCommitLatencyWAL:    "Commit Latency (WAL)",
	ScenarioCommitLatencyMemory: "Commit Latency (Memory)",
	ScenarioSyncReadWAL:         "Sync Read (WAL)",
	ScenarioSyncReadMemory:      "Sync Read (Memory)",
	ScenarioStaleReadWAL:        "Stale Read (WAL)",
	ScenarioStaleReadMemory:     "Stale Read (Memory)",

	// Multi-node 3N scenarios.
	ScenarioElection3NWAL:      "Election 3N (WAL)",
	ScenarioProposeSEQ3NWAL:    "Propose Sequential 3N (WAL)",
	ScenarioProposeConc83NWAL:  "Propose Concurrent/8 3N (WAL)",
	ScenarioCommitLatency3NWAL: "Commit Latency 3N (WAL)",
	ScenarioSyncRead3NWAL:      "Sync Read 3N (WAL)",
	ScenarioStaleRead3NWAL:     "Stale Read 3N (WAL)",

	// Multi-node 5N scenarios.
	ScenarioElection5NWAL:      "Election 5N (WAL)",
	ScenarioProposeSEQ5NWAL:    "Propose Sequential 5N (WAL)",
	ScenarioProposeConc85NWAL:  "Propose Concurrent/8 5N (WAL)",
	ScenarioCommitLatency5NWAL: "Commit Latency 5N (WAL)",
	ScenarioSyncRead5NWAL:      "Sync Read 5N (WAL)",
	ScenarioStaleRead5NWAL:     "Stale Read 5N (WAL)",

	// QuicRaft-only LeaseRead variants.
	ScenarioSyncRead3NWALLeaseRead: "Sync Read 3N (WAL+LeaseRead)",
	ScenarioSyncRead5NWALLeaseRead: "Sync Read 5N (WAL+LeaseRead)",
}

// PrintTerminal writes a formatted comparison report to the writer.
func (cr *ComparisonReport) PrintTerminal(w io.Writer) {
	if len(cr.Scenarios) == 0 {
		fmt.Fprintln(w, "No benchmark results to report.")
		return
	}

	width := 143
	sep := strings.Repeat("=", width)
	thinSep := strings.Repeat("-", width)

	fmt.Fprintln(w, sep)
	if len(cr.Systems) == 1 {
		fmt.Fprintf(w, "  Performance Benchmark: %s\n", cr.Systems[0])
	} else {
		fmt.Fprintf(w, "  Performance Comparison: %s\n", strings.Join(cr.Systems, " vs "))
	}
	fmt.Fprintln(w, sep)
	fmt.Fprintln(w)

	for _, sc := range cr.Scenarios {
		label := ScenarioLabels[sc.Scenario]
		if label == "" {
			label = sc.Scenario
		}

		fmt.Fprintf(w, "  Scenario: %s\n", label)
		fmt.Fprintln(w, thinSep)

		// Header row.
		fmt.Fprintf(w, "  %-20s %15s %12s %12s %12s %12s %12s %12s\n",
			"System", "Ops/sec", "P50", "P90", "P95", "P99", "P99.9", "Mean")
		fmt.Fprintln(w, thinSep)

		// Data rows.
		for _, sys := range cr.Systems {
			r, ok := sc.Results[sys]
			if !ok {
				fmt.Fprintf(w, "  %-20s %15s %12s %12s %12s %12s %12s %12s\n",
					sys, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A")
				continue
			}

			fmt.Fprintf(w, "  %-20s %15s %12s %12s %12s %12s %12s %12s\n",
				sys,
				FormatOps(r.OpsPerSec),
				FormatDurationNs(r.Latency.P50Ns),
				FormatDurationNs(r.Latency.P90Ns),
				FormatDurationNs(r.Latency.P95Ns),
				FormatDurationNs(r.Latency.P99Ns),
				FormatDurationNs(r.Latency.P999Ns),
				FormatDurationNs(r.Latency.MeanNs))
		}

		// Winner row (only shown when 2+ systems have results for this scenario).
		if sc.ThroughputWinner != "" || sc.LatencyWinner != "" {
			fmt.Fprintln(w, thinSep)
			throughputWinner := sc.ThroughputWinner
			if throughputWinner == "" {
				throughputWinner = "N/A"
			}
			latencyWinner := sc.LatencyWinner
			if latencyWinner == "" {
				latencyWinner = "N/A"
			}
			fmt.Fprintf(w, "  %-20s %15s %48s\n",
				"WINNER", "T: "+throughputWinner, "L: "+latencyWinner)

			// Speedup ratios.
			for _, sys := range cr.Systems {
				if ratio, ok := sc.ThroughputSpeedups[sys]; ok && ratio > 1.005 {
					fmt.Fprintf(w, "  %-20s %14.2fx speedup\n", sys, ratio)
				}
			}
		}

		fmt.Fprintln(w)
	}

	fmt.Fprintln(w, sep)
}

// FormatOps formats an ops/sec value for display.
func FormatOps(opsPerSec float64) string {
	if opsPerSec == 0 {
		return "N/A"
	}
	if opsPerSec >= 1_000_000 {
		return fmt.Sprintf("%.2fM", opsPerSec/1_000_000)
	}
	if opsPerSec >= 1_000 {
		return fmt.Sprintf("%.2fK", opsPerSec/1_000)
	}
	return fmt.Sprintf("%.1f", opsPerSec)
}

// FormatDuration formats a time.Duration into a human-readable string.
func FormatDuration(d time.Duration) string {
	return FormatDurationNs(d.Nanoseconds())
}

// FormatDurationNs formats nanoseconds into a human-readable latency string.
func FormatDurationNs(ns int64) string {
	if ns == 0 {
		return "N/A"
	}
	switch {
	case ns < 1_000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1_000_000:
		return fmt.Sprintf("%.1fus", float64(ns)/1_000)
	case ns < 1_000_000_000:
		return fmt.Sprintf("%.2fms", float64(ns)/1_000_000)
	default:
		return fmt.Sprintf("%.3fs", float64(ns)/1_000_000_000)
	}
}

// Truncate shortens a string to maxLen characters.
func Truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
