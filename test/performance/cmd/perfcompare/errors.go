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

package main

import (
	"slices"
	"strconv"
	"strings"
)

// SystemsFailedError reports that one or more requested benchmark systems
// produced no results. It exists so a run that drops an engine from the
// comparison cannot also exit zero.
type SystemsFailedError struct {
	Requested []string
	Failed    []string
}

// Error implements the error interface.
func (e *SystemsFailedError) Error() string {
	return strconv.Itoa(len(e.Failed)) + " of " + strconv.Itoa(len(e.Requested)) +
		" requested systems produced no results: " + strings.Join(e.Failed, ", ") +
		" (requested: " + strings.Join(e.Requested, ", ") + "). " +
		"The comparison is incomplete; see the ERROR lines above for each failure."
}

// NoResultsError reports that no system at all produced results.
type NoResultsError struct {
	Requested []string
}

// Error implements the error interface.
func (e *NoResultsError) Error() string {
	return "no benchmark results collected for any of: " + strings.Join(e.Requested, ", ")
}

// NoSystemsError reports that no system was named on the command line.
type NoSystemsError struct{}

// Error implements the error interface.
func (e *NoSystemsError) Error() string {
	return "no systems specified"
}

// NoProfileError reports a generate run with no profile to generate. There is
// no default: a benchmark document that does not name its machine is a document
// whose numbers cannot be interpreted, so the machine is always named.
type NoProfileError struct{}

// Error implements the error interface.
func (e *NoProfileError) Error() string {
	return "no profile specified: pass --profile=<dir> (e.g. --profile=profiles/omega). " +
		"Every result belongs to one machine profile; there is no default"
}

// UnknownSystemError reports an unrecognized system name.
type UnknownSystemError struct {
	System string
}

// Error implements the error interface.
func (e *UnknownSystemError) Error() string {
	return "unknown system: " + e.System + " (known: " + strings.Join(knownSystems, ", ") + ")"
}

// MissingResultsError reports that the generate phase was asked to produce a
// comparison covering systems that have no results on disk.
type MissingResultsError struct {
	Missing   []string
	Expected  []string
	Present   []string
	ResultDir string
}

// Error implements the error interface.
func (e *MissingResultsError) Error() string {
	var sb strings.Builder
	sb.WriteString("cannot generate reports: no results for ")
	sb.WriteString(strings.Join(e.Missing, ", "))
	if e.ResultDir != "" {
		sb.WriteString(" in ")
		sb.WriteString(e.ResultDir)
	}
	sb.WriteString(". Expected ")
	sb.WriteString(strings.Join(e.Expected, ", "))
	sb.WriteString("; found ")
	if len(e.Present) == 0 {
		sb.WriteString("nothing")
	} else {
		sb.WriteString(strings.Join(e.Present, ", "))
	}
	sb.WriteString(". Re-run the missing system, or narrow the comparison " +
		"explicitly with --systems=" + strings.Join(e.Present, ","))
	return sb.String()
}

// DockerRunError reports a failed benchmark container together with the lines
// of its output that explain why. The orchestrator used to attach the whole
// captured stdout, hundreds of lines of benchmark rows in which the two lines
// that mattered were invisible; "docker run failed: exit status 1" on its own
// says nothing at all.
type DockerRunError struct {
	System     string
	Err        error
	LogPath    string
	Diagnostic []string
}

// Error implements the error interface.
func (e *DockerRunError) Error() string {
	var sb strings.Builder
	sb.WriteString("benchmark container for ")
	sb.WriteString(e.System)
	sb.WriteString(" failed: ")
	sb.WriteString(e.Err.Error())
	if len(e.Diagnostic) == 0 {
		sb.WriteString("\n  (no failure lines found in container output; " +
			"the container produced no test diagnostics, which usually means it " +
			"died before the benchmarks ran)")
	} else {
		sb.WriteString("\n  container reported:")
		for _, line := range e.Diagnostic {
			sb.WriteString("\n    ")
			sb.WriteString(line)
		}
	}
	if e.LogPath != "" {
		sb.WriteString("\n  full container console: ")
		sb.WriteString(e.LogPath)
	}
	return sb.String()
}

// Unwrap exposes the underlying exec error.
func (e *DockerRunError) Unwrap() error { return e.Err }

// maxDiagnosticLines caps the extracted diagnostic so a pathological run cannot
// bury the summary it is meant to be.
const maxDiagnosticLines = 40

// verdictPrefixes are the line prefixes that carry the VERDICT: which benchmark
// failed, or how the binary died. These are what an operator reads first and
// the summary must never be without them.
var verdictPrefixes = []string{
	"--- FAIL",
	"FAIL",
	"panic:",
	"fatal error:",
	"SIGQUIT",
	"signal:",
}

// detailPrefixes carry supporting detail: a message the engine printed on its
// way to failing. Useful, but expendable when the budget is tight.
var detailPrefixes = []string{
	"Error:",
	"error:",
}

// diagLine is one extracted line and where it appeared, so the summary can
// spend its budget by importance and still print in the container's own order.
type diagLine struct {
	pos     int
	text    string
	verdict bool
}

// extractDiagnostic pulls the lines that explain a benchmark container failure
// out of its output: the FAIL markers, the b.Fatalf / b.Errorf messages that
// precede them, and any panic. Benchmark result rows are dropped.
//
// The budget is spent on verdicts first. `go test` prints every relayed message
// BEFORE the "--- FAIL:" that names the benchmark, so a summary that simply kept
// the first maxDiagnosticLines matches cut off at exactly the line the operator
// needed: a run that logged 40 tolerated propose errors reported 40 propose
// errors and no verdict at all (#184). Detail lines are dropped to make room
// for verdicts, never the reverse.
func extractDiagnostic(output string) []string {
	var verdicts, details []diagLine
	truncated := false

	for pos, raw := range strings.Split(output, "\n") {
		line := strings.TrimRight(raw, "\r")
		verdict, ok := classifyDiagnostic(line)
		if !ok {
			continue
		}
		entry := diagLine{pos: pos, text: strings.TrimSpace(line), verdict: verdict}
		// Both slices are capped as they fill so a container that emits
		// millions of matching lines cannot be read into memory here.
		switch {
		case verdict && len(verdicts) < maxDiagnosticLines:
			verdicts = append(verdicts, entry)
		case !verdict && len(details) < maxDiagnosticLines:
			details = append(details, entry)
		default:
			truncated = true
		}
	}

	if room := maxDiagnosticLines - len(verdicts); len(details) > room {
		details = details[:room]
		truncated = true
	}

	found := slices.Concat(verdicts, details)
	slices.SortFunc(found, func(a, b diagLine) int { return a.pos - b.pos })

	lines := make([]string, 0, len(found)+1)
	for _, entry := range found {
		lines = append(lines, entry.text)
	}
	if truncated {
		lines = append(lines, "... (diagnostic truncated; every verdict line above is kept)")
	}
	return lines
}

// classifyDiagnostic reports whether a line of `go test` output explains a
// failure, and whether it is a verdict rather than supporting detail.
func classifyDiagnostic(line string) (verdict, ok bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false, false
	}
	for _, prefix := range verdictPrefixes {
		if strings.HasPrefix(trimmed, prefix) {
			return true, true
		}
	}
	for _, prefix := range detailPrefixes {
		if strings.HasPrefix(trimmed, prefix) {
			return false, true
		}
	}
	// Indented "<file>_test.go:NN: message" lines are the b.Fatalf/b.Errorf
	// text, which is where the actual reason lives.
	if line != trimmed && strings.Contains(trimmed, "_test.go:") {
		return false, true
	}
	// Indented "<file>.go:NN: message" from a helper package, same idea.
	if line != trimmed && strings.Contains(trimmed, ".go:") && strings.Contains(trimmed, ": ") {
		return false, true
	}
	return false, false
}
