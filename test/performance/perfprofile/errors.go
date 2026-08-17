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

import "strings"

// EnvNotCapturedError reports that a profile has no env.json. It is not
// automatically fatal — a profile can be generated before its machine has been
// described — but the resulting documents say so on their face.
type EnvNotCapturedError struct {
	Path string
}

// Error implements the error interface.
func (e *EnvNotCapturedError) Error() string {
	return "no machine capture at " + e.Path +
		" (run capture-env.sh on the target, or make perf-remote which does it for you)"
}

// EnvParseError reports an env.json that exists but cannot be read as one.
type EnvParseError struct {
	Path   string
	Detail string
}

// Error implements the error interface.
func (e *EnvParseError) Error() string {
	return "cannot parse machine capture " + e.Path + ": " + e.Detail
}

// EnvProfileMismatchError reports an env.json captured for a different profile
// than the directory it sits in. Attributing one machine's specifications to
// another machine's numbers is the exact failure the capture exists to prevent,
// so it is fatal rather than a warning.
type EnvProfileMismatchError struct {
	Path      string
	Directory string
	Captured  string
}

// Error implements the error interface.
func (e *EnvProfileMismatchError) Error() string {
	return "machine capture " + e.Path + " names profile " + e.Captured +
		" but sits in profile " + e.Directory +
		"; these specifications describe a different machine than these results"
}

// MissingBaselineError reports that a profile has no QuicRaft results. Every
// generated document is QuicRaft against another engine, so without it there is
// nothing any of them could say.
type MissingBaselineError struct {
	Profile    string
	ResultsDir string
	Present    []string
}

// Error implements the error interface.
func (e *MissingBaselineError) Error() string {
	present := "nothing"
	if len(e.Present) > 0 {
		present = strings.Join(e.Present, ", ")
	}
	return "profile " + e.Profile + " has no " + BaselineSystem + " results in " +
		e.ResultsDir + " (found " + present + "); every generated document compares " +
		BaselineSystem + " against another engine, so none can be written"
}

// ProtectedFileError reports an attempt to have the generator write a file the
// generator does not own. analysis.md is the human's document; the generator
// deletes and recreates its siblings freely and must never reach this one.
type ProtectedFileError struct {
	Path string
}

// Error implements the error interface.
func (e *ProtectedFileError) Error() string {
	return "refusing to write " + e.Path + ": " + AnalysisFile +
		" is hand-written and the generator never owns it"
}

// UnknownSystemError reports a system name that is not a benchmark system.
type UnknownSystemError struct {
	System string
}

// Error implements the error interface.
func (e *UnknownSystemError) Error() string {
	return "unknown system: " + e.System + " (known: " + strings.Join(KnownSystems, ", ") + ")"
}
