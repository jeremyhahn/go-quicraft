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

package releasability

import (
	"path"
	"strconv"
	"strings"
)

// allowRule accepts some classes of lab value in the files its path pattern
// matches. It exists for what an inline waiver cannot reach: a whole tree of
// files, a format with no comment syntax, or a file whose content another owner
// controls.
//
// Every field is required. A rule with no reason is rejected by
// validateAllowList, because the reason is the entire difference between an
// allow-list and a blindfold.
type allowRule struct {
	// path is a slash-separated pattern. `*` matches within one path
	// segment; a `**` segment matches any number of segments, including
	// none. An exact path matches only itself.
	path string

	// classes are the classes this rule accepts in those files. Naming them
	// keeps the rule narrow: a tree allowed to mention a fleet hostname is
	// not thereby allowed to hardcode the owner's home directory.
	classes []class

	// reason says why publishing this value is acceptable, or who owns
	// removing it.
	reason string
}

// benchmarkProvenance is the reason shared by the benchmark-record entries.
const benchmarkProvenance = "Benchmark provenance: profile names record which machine measured " +
	"the numbers. A throughput figure is a property of its machine, so the " +
	"machine's name is the measurement's meaning, not a leak."

// allowList is the accepted set for THIS repository. Every entry is a
// deliberate decision, and the list is meant to shrink. Lab values were removed
// everywhere they were removable, and the private forge's own configuration is
// export-ignored rather than allowed (manifest_test.go: privateWorkflowFiles);
// what remains is benchmark provenance.
var allowList = []allowRule{
	{
		path:    "test/performance/profiles/**",
		classes: []class{classLabHost},
		reason:  benchmarkProvenance + " Each profile directory, its env.json and its generated documents are named after that machine.",
	},
	{
		path:    "test/performance/baselines/**",
		classes: []class{classLabHost},
		reason:  benchmarkProvenance + " The file name and the run-host/build-host header lines are the provenance stamp scripts/bench-compare.sh writes.",
	},
	{
		path:    "test/performance/perfprofile/generate_test.go",
		classes: []class{classLabHost},
		reason: "Drives the document generator with the real stored dataset under " +
			"test/performance/profiles/, so it names that profile directory and " +
			"asserts on the profile name the generated documents carry.",
	},
	{
		path:    "CHANGELOG.md",
		classes: []class{classLabHost},
		reason: "Historical release notes recording where measurements ran. Released " +
			"entries are not rewritten; new entries name roles, not machines.",
	},
	{
		path:    "docs/references/**",
		classes: []class{classLabAddress},
		reason:  "Verbatim IETF text: the RFCs use RFC 5737 documentation addresses, and a reference copy is never edited.",
	},
}

// validateAllowList reports the first unusable entry. scan calls it, so a
// malformed rule fails the gate instead of quietly widening it.
func validateAllowList(rules []allowRule) error {
	known := knownClasses()
	for i, r := range rules {
		if r.path == "" {
			return &allowRuleError{index: i, problem: "no path pattern"}
		}
		if strings.TrimSpace(r.reason) == "" {
			return &allowRuleError{index: i, path: r.path, problem: "no reason given"}
		}
		if len(r.classes) == 0 {
			return &allowRuleError{index: i, path: r.path, problem: "no classes listed"}
		}
		for _, c := range r.classes {
			if !known[c] {
				return &allowRuleError{index: i, path: r.path, problem: "unknown class " + string(c)}
			}
		}
		for _, segment := range strings.Split(r.path, "/") {
			if segment == "**" {
				continue
			}
			if _, err := path.Match(segment, "probe"); err != nil {
				return &allowRuleError{index: i, path: r.path, problem: "malformed path pattern: " + err.Error()}
			}
		}
	}
	return nil
}

// acceptedClasses collects the classes the allow-list accepts for one file.
// Rules are validated before this runs, so a malformed pattern cannot silently
// match nothing.
func acceptedClasses(rules []allowRule, name string) map[class]bool {
	accepted := make(map[class]bool, len(patterns))
	for _, r := range rules {
		if ok, err := matchPath(r.path, name); err != nil || !ok {
			continue
		}
		for _, c := range r.classes {
			accepted[c] = true
		}
	}
	return accepted
}

// matchPath reports whether a slash-separated path matches an allowRule pattern.
func matchPath(pattern, name string) (bool, error) {
	return matchSegments(strings.Split(pattern, "/"), strings.Split(name, "/"))
}

// matchSegments walks pattern and path segment by segment. `**` is the only
// construct that may span a separator, which keeps a rule for one directory
// from reaching into its siblings.
func matchSegments(pattern, segments []string) (bool, error) {
	for len(pattern) > 0 {
		if pattern[0] == "**" {
			if len(pattern) == 1 {
				return true, nil
			}
			for i := 0; i <= len(segments); i++ {
				ok, err := matchSegments(pattern[1:], segments[i:])
				if err != nil || ok {
					return ok, err
				}
			}
			return false, nil
		}
		if len(segments) == 0 {
			return false, nil
		}
		ok, err := path.Match(pattern[0], segments[0])
		if err != nil || !ok {
			return false, err
		}
		pattern, segments = pattern[1:], segments[1:]
	}
	return len(segments) == 0, nil
}

// allowRuleError is returned when an allow-list entry is not usable. The
// allow-list is the gate's own configuration: a malformed or unexplained entry
// would silently accept whatever it covers, so it fails the gate instead.
type allowRuleError struct {
	index   int
	path    string
	problem string
}

func (e *allowRuleError) Error() string {
	return "releasability: allow-list entry " + strconv.Itoa(e.index) + " (" + e.path + "): " + e.problem
}
