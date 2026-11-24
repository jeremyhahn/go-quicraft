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
	"errors"
	"strings"
	"testing"
)

func TestMatchPath(t *testing.T) {
	cases := []struct {
		pattern string
		path    string
		want    bool
	}{
		{"**/*_test.go", "pkg/seal/barrier_test.go", true},
		{"**/*_test.go", "barrier_test.go", true},
		{"**/*_test.go", "pkg/seal/barrier.go", false},
		{"CHANGELOG.md", "CHANGELOG.md", true},
		{"CHANGELOG.md", "docs/CHANGELOG.md", false},
		{"test/performance/profiles/**", "test/performance/profiles/bench-01/env.json", true},
		{"test/performance/profiles/**", "test/performance/profiles", true},
		{"test/performance/profiles/**", "test/performance/README.md", false},
		{".github/workflows/*.yml", ".github/workflows/ci.yml", true},
		{".github/workflows/*.yml", ".github/workflows/nested/ci.yml", false},
		{"test/**/testdata/*.log", "test/a/testdata/x.log", true},
		{"test/**/testdata/*.log", "test/a/b/testdata/x.log", true},
		{"test/**/testdata/*.log", "test/a/x.log", false},
	}

	for _, tc := range cases {
		t.Run(tc.pattern+" vs "+tc.path, func(t *testing.T) {
			got, err := matchPath(tc.pattern, tc.path)
			if err != nil {
				t.Fatalf("matchPath: %v", err)
			}
			if got != tc.want {
				t.Errorf("matchPath(%q, %q) = %v, want %v", tc.pattern, tc.path, got, tc.want)
			}
		})
	}
}

func TestMatchPathReportsAMalformedPattern(t *testing.T) {
	if _, err := matchPath("pkg/[a-", "pkg/x"); err == nil {
		t.Fatal("a malformed character class matched silently")
	}
}

func TestValidateAllowListRefusesEveryUnusableEntry(t *testing.T) {
	cases := []struct {
		name    string
		rule    allowRule
		problem string
	}{
		{"no path", allowRule{classes: []class{classLabHost}, reason: "because"}, "no path pattern"},
		{"no reason", allowRule{path: "pkg/**", classes: []class{classLabHost}}, "no reason given"},
		{"whitespace reason", allowRule{path: "pkg/**", classes: []class{classLabHost}, reason: "   "}, "no reason given"},
		{"no classes", allowRule{path: "pkg/**", reason: "because"}, "no classes listed"},
		{"unknown class", allowRule{path: "pkg/**", classes: []class{"whatever"}, reason: "because"}, "unknown class whatever"},
		{"malformed pattern", allowRule{path: "pkg/[a-", classes: []class{classLabHost}, reason: "because"}, "malformed path pattern"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAllowList([]allowRule{tc.rule})
			var ruleErr *allowRuleError
			if !errors.As(err, &ruleErr) {
				t.Fatalf("err = %v, want *allowRuleError", err)
			}
			if !strings.Contains(ruleErr.Error(), tc.problem) {
				t.Errorf("error %q does not report %q", ruleErr.Error(), tc.problem)
			}
		})
	}
}

// TestAllowListIsUsableAndExplainsItself validates the shipped list. A one-word
// reason is not a reason: the list is the record of what this repository
// knowingly publishes, so each entry has to tell the next reader why.
func TestAllowListIsUsableAndExplainsItself(t *testing.T) {
	if err := validateAllowList(allowList); err != nil {
		t.Fatalf("the shipped allow-list is not usable: %v", err)
	}
	every := len(knownClasses())
	for _, rule := range allowList {
		if len(strings.Fields(rule.reason)) < 5 {
			t.Errorf("%s: reason is too thin to be a decision: %q", rule.path, rule.reason)
		}
		// No entry may blanket a path with every class: that is an
		// exemption from the gate, not an allowance within it.
		if len(rule.classes) >= every {
			t.Errorf("%s accepts every class; name only the classes the files need", rule.path)
		}
	}
}

// TestAllowListEntriesStillMatchSomething stops the list from outliving what it
// allows: an entry whose path matches no published file is dead weight that
// would silently accept a future file of that name.
func TestAllowListEntriesStillMatchSomething(t *testing.T) {
	published, err := publishedFiles(t.Context(), repoRoot(t))
	if err != nil {
		t.Fatalf("publishedFiles: %v", err)
	}
	for _, rule := range allowList {
		var matched bool
		for _, name := range published {
			if ok, _ := matchPath(rule.path, name); ok {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("allow-list entry %s matches no published file; delete it", rule.path)
		}
	}
}

func TestAcceptedClassesUnionsEveryMatchingRule(t *testing.T) {
	rules := []allowRule{
		{path: "**/*_test.go", classes: []class{classLabHost}, reason: "fleet hostnames in fixtures"},
		{path: "pkg/x/y_test.go", classes: []class{classOwnerPath}, reason: "one file, owned elsewhere"},
	}
	accepted := acceptedClasses(rules, "pkg/x/y_test.go")
	if !accepted[classLabHost] || !accepted[classOwnerPath] {
		t.Fatalf("accepted = %v, want both classes", accepted)
	}
	if accepted[classLabAddress] {
		t.Error("a class no rule names was accepted")
	}
	if acceptedClasses(rules, "pkg/x/z_test.go")[classOwnerPath] {
		t.Error("the single-file rule reached a sibling file")
	}
}

// TestScanNeverReadsAFileWhoseEveryClassIsAllowed pins the shortcut. The named
// file sits under a regular file, so any attempt to stat it fails with ENOTDIR
// and surfaces as a readError: a clean result proves it was never touched.
func TestScanNeverReadsAFileWhoseEveryClassIsAllowed(t *testing.T) {
	root, _ := writeTree(t, map[string]string{"vendored": "a file, not a directory\n"})
	files := []string{"vendored/thing.js"}

	var unread *readError
	if _, err := scan(root, files, nil); !errors.As(err, &unread) {
		t.Fatalf("precondition: reading the file must fail, got %v", err)
	}

	every := make([]class, 0, len(patterns))
	for c := range knownClasses() {
		every = append(every, c)
	}
	rules := []allowRule{{
		path:    "vendored/**",
		classes: every,
		reason:  "a tree this repository does not author",
	}}
	findings, err := scan(root, files, rules)
	if err != nil {
		t.Fatalf("scan read a file every class is allowed in: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected no findings, got %v", findings)
	}
}
