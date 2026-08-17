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

package perfprofile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jeremyhahn/go-quicraft/test/performance/perfresult"
)

// notMeasuredCell is what a table cell says when a system has no result for a
// scenario. It is deliberately not "N/A": a harness that never implemented a
// scenario and a harness that measured zero must not render identically.
const notMeasuredCell = "_not measured_"

// GenerateResult describes what one generation run did, so the caller can
// report it without re-deriving it from the filesystem.
type GenerateResult struct {
	// Profile is the profile name that was generated.
	Profile string
	// Generated lists the documents written, in KnownSystems order.
	Generated []string
	// Measured lists the systems that had results.
	Measured []string
	// NotMeasured lists the known systems with no results in this profile.
	NotMeasured []string
	// Removed lists stale generated documents deleted because their system is
	// no longer measured here. A stale document is worse than none: at a glance
	// it is indistinguishable from a current one.
	Removed []string
	// EnvCaptured reports whether the profile had a machine description.
	EnvCaptured bool
}

// Complete reports whether every known system was measured.
func (r *GenerateResult) Complete() bool { return len(r.NotMeasured) == 0 }

// Summary is the terminal report. It never claims completeness it does not
// have: a three-way comparison rendered as two documents says so, in the same
// breath as it says what it wrote.
func (r *GenerateResult) Summary() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "profile %s: wrote %d document(s)", r.Profile, len(r.Generated))
	if len(r.Generated) > 0 {
		sb.WriteString(" (" + strings.Join(r.Generated, ", ") + ")")
	}
	sb.WriteString("\n  measured here: " + strings.Join(r.Measured, ", "))
	if len(r.NotMeasured) > 0 {
		files := make([]string, 0, len(r.NotMeasured))
		for _, sys := range r.NotMeasured {
			files = append(files, resultsDirName+"/"+sys+".json")
		}
		sb.WriteString("\n  NOT MEASURED on this profile: " + strings.Join(r.NotMeasured, ", ") +
			" — no " + strings.Join(files, ", ") + ", so no document was written for " +
			plural(r.NotMeasured, "it", "them"))
	}
	for _, removed := range r.Removed {
		sb.WriteString("\n  removed stale document: " + removed)
	}
	if !r.EnvCaptured {
		sb.WriteString("\n  machine specifications were NOT captured for this profile (" +
			EnvFile + " absent); the documents say so on their face")
	}
	return sb.String()
}

// Generate writes the profile's charts and comparison documents from its stored
// results. It rewrites every document it owns and never touches analysis.md.
//
// A system with no results gets no document, and its absence is reported both
// in the returned result and inside every document that was written. Generating
// on a partial set is a legitimate state — a profile can be measured one engine
// at a time — but it is never silently a complete one.
func (p *Profile) Generate(now time.Time) (*GenerateResult, error) {
	suites, err := p.LoadResults()
	if err != nil {
		return nil, err
	}

	baseline, ok := suites[BaselineSystem]
	if !ok {
		return nil, &MissingBaselineError{
			Profile:    p.name,
			ResultsDir: p.ResultsDir(),
			Present:    PresentSystems(suites),
		}
	}

	env, err := p.LoadEnv()
	envCaptured := true
	if err != nil {
		var notCaptured *EnvNotCapturedError
		if !errors.As(err, &notCaptured) {
			return nil, err
		}
		envCaptured = false
		env = &Env{Profile: p.name}
	}

	if err := p.writeCharts(suites); err != nil {
		return nil, err
	}

	result := &GenerateResult{
		Profile:     p.name,
		Measured:    PresentSystems(suites),
		NotMeasured: NotMeasured(suites),
		EnvCaptured: envCaptured,
	}

	for _, sys := range KnownSystems {
		if sys == BaselineSystem {
			continue
		}
		path := p.DocPath(sys)
		other, measured := suites[sys]
		if !measured {
			// Remove a document left over from a run when this system did have
			// results; leaving it beside fresh siblings makes stale numbers look
			// current.
			if err := os.Remove(path); err == nil {
				result.Removed = append(result.Removed, filepath.Base(path))
			} else if !os.IsNotExist(err) {
				return nil, err
			}
			continue
		}
		doc := renderPairwise(p, env, envCaptured, baseline, other,
			result.Measured, result.NotMeasured, now)
		if err := writeDoc(path, doc); err != nil {
			return nil, err
		}
		result.Generated = append(result.Generated, filepath.Base(path))
	}

	return result, nil
}

// writeCharts regenerates every SVG for the profile: one per system, one
// combined comparison, and one per pair.
func (p *Profile) writeCharts(suites map[string]*perfresult.BenchmarkSuite) error {
	if err := os.MkdirAll(p.ChartsDir(), 0755); err != nil {
		return err
	}
	for _, sys := range PresentSystems(suites) {
		path := filepath.Join(p.ChartsDir(), sys+".svg")
		if err := perfresult.GenerateSystemChart(suites[sys], path); err != nil {
			return err
		}
	}
	if len(suites) > 1 {
		path := filepath.Join(p.ChartsDir(), "comparison.svg")
		if err := perfresult.GenerateComparisonChart(suites, path); err != nil {
			return err
		}
	}
	baseline, ok := suites[BaselineSystem]
	if !ok {
		return nil
	}
	for _, sys := range PresentSystems(suites) {
		if sys == BaselineSystem {
			continue
		}
		pair := map[string]*perfresult.BenchmarkSuite{
			BaselineSystem: baseline,
			sys:            suites[sys],
		}
		path := filepath.Join(p.dir, chartRelPath(sys))
		if err := perfresult.GenerateComparisonChart(pair, path); err != nil {
			return err
		}
	}
	return nil
}

// renderPairwise builds one generated comparison document. Everything a reader
// needs in order to know which machine produced the numbers is inline: the
// profile, the capture date, the hardware, and the link to the profile's
// hand-written analysis.
func renderPairwise(p *Profile, env *Env, envCaptured bool,
	baseline, other *perfresult.BenchmarkSuite, measured, notMeasured []string, now time.Time) string {

	sys := other.System
	title := titleCase(sys)

	table := perfresult.NewComparisonTable()
	table.Add(baseline)
	table.Add(other)
	report := table.Compare()

	var sb strings.Builder

	fmt.Fprintf(&sb, "# QuicRaft vs %s — `%s` profile\n\n", title, p.Name())

	sb.WriteString("> **Generated file.** `perfcompare generate` deletes and rewrites this\n")
	sb.WriteString("> document from `" + resultsDirName + "/*.json`; edits made here are lost on the next\n")
	sb.WriteString("> run. Written analysis belongs in [" + AnalysisFile + "](" + AnalysisFile + "), which the\n")
	sb.WriteString("> generator never touches.\n\n")

	hardware := UncapturedSummary
	capturedAt := NotCaptured
	if envCaptured {
		hardware = env.Summary()
		if strings.TrimSpace(env.CapturedAt) != "" {
			capturedAt = env.CapturedAt
		}
	}

	sb.WriteString("| | |\n|---|---|\n")
	fmt.Fprintf(&sb, "| Profile | `%s` |\n", p.Name())
	fmt.Fprintf(&sb, "| Hardware | %s |\n", hardware)
	fmt.Fprintf(&sb, "| Specs captured | %s |\n", capturedAt)
	fmt.Fprintf(&sb, "| QuicRaft run | %s (version %s) |\n",
		orNotCaptured(baseline.Timestamp), orNotCaptured(baseline.Version))
	fmt.Fprintf(&sb, "| %s run | %s (version %s) |\n",
		title, orNotCaptured(other.Timestamp), orNotCaptured(other.Version))
	fmt.Fprintf(&sb, "| Document generated | %s |\n\n", now.UTC().Format("2006-01-02 15:04:05 UTC"))

	sb.WriteString("Numbers below were measured on this profile's machine only. Nothing here is\n")
	sb.WriteString("comparable with a number from another profile; do not compute a ratio across two.\n\n")

	// Coverage. This section is why a missing system cannot be a silent
	// omission: it is stated in the artifact, not only in the terminal.
	sb.WriteString("## Coverage on this profile\n\n")
	fmt.Fprintf(&sb, "This document compares %s against %s. Systems measured on `%s`: %s.\n\n",
		baseline.System, other.System, p.Name(), strings.Join(measured, ", "))
	if len(notMeasured) > 0 {
		fmt.Fprintf(&sb, "**Not yet measured on `%s`: %s.** No results file exists for %s,\n",
			p.Name(), strings.Join(notMeasured, ", "), plural(notMeasured, "it", "them"))
		fmt.Fprintf(&sb, "so no document was generated for %s. This is a gap in the data, not a result.\n\n",
			plural(notMeasured, "it", "them"))
	} else {
		sb.WriteString("Every known system has results on this profile.\n\n")
	}

	// Machine.
	sb.WriteString("## Machine\n\n")
	if envCaptured {
		sb.WriteString("Captured on the target at run time by `capture-env.sh`; never hand-edited.\n\n")
		sb.WriteString(env.MarkdownTable())
		if missing := env.Missing(); len(missing) > 0 {
			fmt.Fprintf(&sb, "\nThe capture could not obtain: %s.\n", strings.Join(missing, ", "))
		}
		sb.WriteString("\n")
	} else {
		fmt.Fprintf(&sb, "%s. There is no `%s` in this profile, so the hardware\n",
			UncapturedSummary, EnvFile)
		sb.WriteString("these numbers came from is not recorded. Capture it with\n")
		fmt.Fprintf(&sb, "`make perf-env PERF_HOST=<user@host> PERF_PROFILE=%s` and regenerate.\n\n", p.Name())
	}

	// Charts.
	sb.WriteString("## Charts\n\n")
	fmt.Fprintf(&sb, "![QuicRaft vs %s](%s)\n\n", title, chartRelPath(sys))

	// Head-to-head.
	head, quicraftOnly, otherOnly := splitScenarios(report, sys)

	wins := map[string]int{BaselineSystem: 0, sys: 0}
	sb.WriteString("## Head-to-head\n\n")
	if len(head) == 0 {
		sb.WriteString("No scenario was measured for both systems on this profile.\n\n")
	} else {
		fmt.Fprintf(&sb, "| Scenario | QuicRaft | %s | Winner | Ratio |\n|---|---|---|---|---|\n", title)
		for _, sc := range head {
			winner := sc.ThroughputWinner
			ratio := ""
			if winner != "" {
				wins[winner]++
				if r, ok := sc.ThroughputSpeedups[winner]; ok && r > 1.005 {
					ratio = strconv.FormatFloat(r, 'f', 2, 64) + "x"
				}
			}
			fmt.Fprintf(&sb, "| %s | %s | %s | %s | %s |\n",
				scenarioLabel(sc.Scenario),
				cell(sc.Results[BaselineSystem]),
				cell(sc.Results[sys]),
				winner, ratio)
		}
		fmt.Fprintf(&sb, "\n**Score: QuicRaft %d, %s %d, across %d head-to-head scenarios.**\n\n",
			wins[BaselineSystem], title, wins[sys], len(head))
	}

	writeExclusive(&sb, "Measured for QuicRaft only", quicraftOnly, BaselineSystem,
		"The "+title+" harness implements no equivalent scenario.")
	writeExclusive(&sb, "Measured for "+title+" only", otherOnly, sys,
		"The QuicRaft harness implements no equivalent scenario.")

	// Run configuration.
	sb.WriteString("## Run configuration\n\n")
	fmt.Fprintf(&sb, "| Parameter | QuicRaft | %s |\n|---|---|---|\n", title)
	for _, row := range configRows(baseline.Config, other.Config) {
		fmt.Fprintf(&sb, "| %s | %s | %s |\n", row[0], row[1], row[2])
	}
	sb.WriteString("\n")

	fmt.Fprintf(&sb, "Written analysis of these numbers lives in [%s](%s).\n", AnalysisFile, AnalysisFile)

	return sb.String()
}

// plural picks the singular or plural pronoun for a list.
func plural(items []string, one, many string) string {
	if len(items) == 1 {
		return one
	}
	return many
}

// splitScenarios divides the comparison into scenarios both systems measured
// and those only one of them did.
func splitScenarios(report *perfresult.ComparisonReport, other string) (head, baselineOnly, otherOnly []*perfresult.ScenarioComparison) {
	for _, sc := range report.Scenarios {
		_, hasBase := sc.Results[BaselineSystem]
		_, hasOther := sc.Results[other]
		switch {
		case hasBase && hasOther:
			head = append(head, sc)
		case hasBase:
			baselineOnly = append(baselineOnly, sc)
		case hasOther:
			otherOnly = append(otherOnly, sc)
		}
	}
	return head, baselineOnly, otherOnly
}

// writeExclusive renders the scenarios only one system measured.
func writeExclusive(sb *strings.Builder, heading string, scenarios []*perfresult.ScenarioComparison,
	system, why string) {
	if len(scenarios) == 0 {
		return
	}
	fmt.Fprintf(sb, "## %s\n\n%s\n\n", heading, why)
	sb.WriteString("| Scenario | Throughput | Latency |\n|---|---|---|\n")
	for _, sc := range scenarios {
		r := sc.Results[system]
		fmt.Fprintf(sb, "| %s | %s ops/s | %s |\n",
			scenarioLabel(sc.Scenario), perfresult.FormatOps(r.OpsPerSec), latencies(r))
	}
	sb.WriteString("\n")
}

// configRows pairs the two suites' run configurations for side-by-side display.
func configRows(a, b perfresult.BenchmarkConfig) [][3]string {
	return [][3]string{
		{"Payload", strconv.Itoa(a.PayloadSize) + " bytes", strconv.Itoa(b.PayloadSize) + " bytes"},
		{"Concurrent goroutines", strconv.Itoa(a.ConcurrentGoroutines), strconv.Itoa(b.ConcurrentGoroutines)},
		{"TLS", yesNo(a.TLS), yesNo(b.TLS)},
		{"RTT", strconv.Itoa(a.RTTMs) + "ms", strconv.Itoa(b.RTTMs) + "ms"},
		{"Election RTT", strconv.Itoa(a.ElectionRTT) + " ticks", strconv.Itoa(b.ElectionRTT) + " ticks"},
		{"Heartbeat RTT", strconv.Itoa(a.HeartbeatRTT) + " ticks", strconv.Itoa(b.HeartbeatRTT) + " ticks"},
	}
}

// yesNo renders a boolean configuration flag.
func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}

// cell renders one system's result for one scenario, or says it was not
// measured.
func cell(r *perfresult.ScenarioResult) string {
	if r == nil {
		return notMeasuredCell
	}
	return perfresult.FormatOps(r.OpsPerSec) + " ops/s, " + latencies(r)
}

// latencies renders the percentile summary carried in every result cell.
func latencies(r *perfresult.ScenarioResult) string {
	return "P50 " + perfresult.FormatDurationNs(r.Latency.P50Ns) +
		", P95 " + perfresult.FormatDurationNs(r.Latency.P95Ns) +
		", P99 " + perfresult.FormatDurationNs(r.Latency.P99Ns)
}

// scenarioLabel is the human-readable name of a scenario key, falling back to
// the key itself so a newly added scenario is never rendered as blank.
func scenarioLabel(key string) string {
	if label := perfresult.ScenarioLabels[key]; label != "" {
		return label
	}
	return key
}

// orNotCaptured renders a suite field that a harness may have left empty.
func orNotCaptured(value string) string {
	if strings.TrimSpace(value) == "" {
		return NotCaptured
	}
	return value
}

// titleCase capitalizes the first letter of a system name.
func titleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
