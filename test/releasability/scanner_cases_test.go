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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The placeholder values the gate's own tests plant. They are the values the
// rules exist to find, so each line carries a waiver; every other test in the
// repository uses neutral fixtures.
const (
	plantedAddress   = "198.51.100.51"        // lab-string-ok: planted fixture for the lab-address rule
	plantedGPUHost   = "gpu-host"             // lab-string-ok: planted fixture for the lab-host rule
	plantedBenchHost = "omega"                // lab-string-ok: planted fixture for the lab-host rule
	plantedRunner    = "ci-node-2"            // lab-string-ok: planted fixture for the lab-host rule
	plantedAgent     = "vm-host-4"            // lab-string-ok: planted fixture for the lab-host rule
	plantedHome      = plantedMount + "/home" // lab-string-ok: planted fixture for the owner-path rule, a home under the lab mount
	plantedMount     = "/mnt/vm-host"         // lab-string-ok: planted fixture for the owner-path rule
	plantedAccount   = "jhahn"                // lab-string-ok: planted fixture for the owner-account rule

	plantedDomain        = "example.internal"       // lab-string-ok: planted fixture for the internal-identity rule
	plantedPrincipal     = "agent-01@example.com"   // lab-string-ok: planted fixture for the agent-principal rule
	plantedSibling       = "agent-02@forge.example" // lab-string-ok: an agent principal outside the internal domain is still one
	plantedHandbookIssue = "handbook #392"          // lab-string-ok: planted fixture for the internal-repository rule
	plantedAgentsRepo    = "claude-agents"          // lab-string-ok: planted fixture for the internal-repository rule
)

// writeTree writes files into a temporary directory and returns the root plus
// the file list in the form scan takes.
func writeTree(t *testing.T, files map[string]string) (string, []string) {
	t.Helper()
	root := t.TempDir()
	names := make([]string, 0, len(files))
	for name, content := range files {
		full := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", name, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		names = append(names, name)
	}
	return root, names
}

func TestScanReportsEveryClassWithItsLineAndMatch(t *testing.T) {
	cases := []struct {
		name      string
		content   string
		wantClass class
		wantMatch string
		wantLine  int
	}{
		{
			name:      "fleet address in a Makefile default",
			content:   "# remote offload\n\nREMOTE_HOST ?= " + plantedAddress + "\n",
			wantClass: classLabAddress,
			wantMatch: plantedAddress,
			wantLine:  3,
		},
		{
			name:      "fleet hostname shouted in a doc",
			content:   "# Runbook\n\nRun it on " + strings.ToUpper(plantedGPUHost) + " first.\n",
			wantClass: classLabHost,
			wantMatch: strings.ToUpper(plantedGPUHost),
			wantLine:  3,
		},
		{
			name:      "bench host in an example",
			content:   "make perf-all PERF_PROFILE=" + plantedBenchHost + "\n",
			wantClass: classLabHost,
			wantMatch: plantedBenchHost,
			wantLine:  1,
		},
		{
			name:      "runner hostname in a captured log",
			content:   "2026-09-12T00:00:00Z " + plantedRunner + "(version:v3.1.0)\n",
			wantClass: classLabHost,
			wantMatch: plantedRunner,
			wantLine:  1,
		},
		{
			name:      "agent hostname in a comment",
			content:   "// observed on " + plantedAgent + "\n",
			wantClass: classLabHost,
			wantMatch: plantedAgent,
			wantLine:  1,
		},
		{
			name:      "lab NFS mount in a script default",
			content:   "SRC=${SRC:-" + plantedMount + "-01/go-quicraft}\n",
			wantClass: classOwnerPath,
			wantMatch: plantedMount,
			wantLine:  1,
		},
		{
			name:      "agent principal in an allowed-signers file",
			content:   "# trust anchor\n" + plantedPrincipal + " ssh-ed25519 AAAAC3Nz\n",
			wantClass: classInternalIdentity,
			wantMatch: plantedPrincipal,
			wantLine:  2,
		},
		{
			name:      "agent principal on any domain in a script",
			content:   "GIT_AUTHOR_EMAIL=" + strings.ToUpper(plantedSibling) + " make release\n",
			wantClass: classInternalIdentity,
			wantMatch: strings.ToUpper(plantedSibling),
			wantLine:  1,
		},
		{
			name:      "internal mail domain in a doc",
			content:   "Mail ops@" + plantedDomain + " for access.\n",
			wantClass: classInternalIdentity,
			wantMatch: plantedDomain,
			wantLine:  1,
		},
		{
			name:      "handbook issue in a doc",
			content:   "# Notes\n\nSee " + strings.ToUpper(plantedHandbookIssue) + " for the ruling.\n",
			wantClass: classInternalRepository,
			wantMatch: strings.ToUpper(plantedHandbookIssue),
			wantLine:  3,
		},
		{
			name:      "agent tooling repository in a script",
			content:   "# from the " + plantedAgentsRepo + " repository\n",
			wantClass: classInternalRepository,
			wantMatch: plantedAgentsRepo,
			wantLine:  1,
		},
		{
			name:      "owner account in a baseline header",
			content:   "# command: PERF_HOST='" + plantedAccount + "@bench' make bench-baseline\n",
			wantClass: classOwnerAccount,
			wantMatch: plantedAccount,
			wantLine:  1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, files := writeTree(t, map[string]string{"shipping/file.txt": tc.content})
			findings, err := scan(root, files, nil)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			var got *finding
			for i := range findings {
				if findings[i].class == tc.wantClass {
					got = &findings[i]
					break
				}
			}
			if got == nil {
				t.Fatalf("no %s finding; got %v", tc.wantClass, findings)
			}
			if got.match != tc.wantMatch {
				t.Errorf("match = %q, want %q", got.match, tc.wantMatch)
			}
			if got.line != tc.wantLine {
				t.Errorf("line = %d, want %d", got.line, tc.wantLine)
			}
			if got.guidance == "" {
				t.Error("finding carries no guidance; the report would not say what to do")
			}
			if !strings.Contains(got.String(), "shipping/file.txt:"+strconv.Itoa(tc.wantLine)) {
				t.Errorf("String() is not file:line clickable: %s", got.String())
			}
		})
	}
}

func TestScanAcceptsWhatItShould(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"documentation-range literals that are not the placeholder subnet", "dial 192.0.2.10:8443 or 203.0.113.4 or 198.51.10.7\n"},
		{"RFC 1918 literals that are not the fleet subnet", "peers: 192.168.1.10, 192.168.100.4, 10.0.0.1\n"},
		{"role names", "make remote-test REMOTE_HOST=build-host.example PERF_HOST=user@bench-host.example\n"},
		{"a waiver on the offending line", "Measured on " + plantedGPUHost + ". <!-- lab-string-ok: measurement provenance -->\n"},
		{"a waiver whose reason ends at a comment closer", "x := 1 /* lab-string-ok: provenance */ // " + plantedGPUHost + "\n"},
		{"a fleet name inside a longer word", "un" + plantedGPUHost + "ness, " + plantedBenchHost + "Dir and OMEGA_HOST are not hosts\n"},
		{"a fleet prefix without its number", "ci-node-x and vm-host-N name no machine\n"},
		{"agent words that are not principals", "agent-" + "git.sh go-quicraft commit; User-Agent: x; agent-01 runs it\n"},
		{"a templated principal with no name", "user.email=\"agent-$repo@example.invalid\"\n"},
		{"a neutral example identity", "release@example.invalid and ops@forge.example\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, files := writeTree(t, map[string]string{"clean.txt": tc.content})
			findings, err := scan(root, files, nil)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			if len(findings) != 0 {
				t.Fatalf("expected no findings, got %v", findings)
			}
		})
	}
}

// TestScanAWaiverWithNoReasonIsNotAWaiver pins the rule that the reason is the
// waiver: a bare marker, or one followed only by a comment closer or blanks,
// accepts nothing.
func TestScanAWaiverWithNoReasonIsNotAWaiver(t *testing.T) {
	root, files := writeTree(t, map[string]string{
		"bare.md":       "Measured on " + plantedGPUHost + ". <!-- lab-string-ok: -->\n",
		"whitespace.md": "Measured on " + plantedGPUHost + ". # lab-string-ok:    \n",
		"closer.go":     "x := 1 // " + plantedGPUHost + " /* lab-string-ok: */\n",
		"above.sh":      "# lab-string-ok:\necho " + plantedGPUHost + "\n",
	})
	findings, err := scan(root, files, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(findings) != len(files) {
		t.Fatalf("want %d findings (one per file), got %d: %v", len(files), len(findings), findings)
	}
}

// TestScanAWaiverReachesOnlyTheNextLine stops a waiver from silencing a whole
// paragraph below it.
func TestScanAWaiverReachesOnlyTheNextLine(t *testing.T) {
	content := "# lab-string-ok: provenance\nline one " + plantedGPUHost + "\nline two " + plantedGPUHost + "\n"
	root, files := writeTree(t, map[string]string{"doc.md": content})
	findings, err := scan(root, files, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(findings) != 1 || findings[0].line != 3 {
		t.Fatalf("want exactly the line-3 finding, got %v", findings)
	}
}

func TestScanAnAllowRuleAcceptsOnlyTheClassesItNames(t *testing.T) {
	content := "host " + plantedAgent + " under " + plantedHome + "/sources\n"
	root, files := writeTree(t, map[string]string{"pkg/thing_test.go": content})

	rules := []allowRule{{
		path:    "**/*_test.go",
		classes: []class{classLabHost},
		reason:  "test fixtures may name a fleet host",
	}}
	findings, err := scan(root, files, rules)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(findings) != 1 || findings[0].class != classOwnerPath {
		t.Fatalf("want exactly the owner-path finding, got %v", findings)
	}
}

func TestScanRejectsAnUnusableAllowList(t *testing.T) {
	root, files := writeTree(t, map[string]string{"clean.txt": "nothing here\n"})
	rules := []allowRule{{path: "**/*.go", classes: []class{classLabHost}}}

	_, err := scan(root, files, rules)
	var ruleErr *allowRuleError
	if !errors.As(err, &ruleErr) {
		t.Fatalf("err = %v, want *allowRuleError", err)
	}
	if !strings.Contains(ruleErr.Error(), "no reason given") {
		t.Errorf("error does not name the defect: %v", ruleErr)
	}
}

func TestScanSkipsWhatCannotLeakText(t *testing.T) {
	root, files := writeTree(t, map[string]string{
		"binary.bin":  plantedGPUHost + "\x00more bytes\n",
		"present.txt": "clean\n",
	})
	files = append(files, "deleted-from-the-working-tree.txt")

	findings, err := scan(root, files, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected no findings, got %v", findings)
	}
}

func TestScanSkipsAFileTooLargeToBeSource(t *testing.T) {
	root := t.TempDir()
	body := make([]byte, maxFileSize+1)
	for i := range body {
		body[i] = 'a'
	}
	copy(body, plantedGPUHost)
	if err := os.WriteFile(filepath.Join(root, "huge.txt"), body, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	findings, err := scan(root, []string{"huge.txt"}, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected the oversized file to be skipped, got %v", findings)
	}
}

// TestScanReadsASymlinkTarget covers a tracked link into the owner's machine,
// which publishes the path even though the link has no content of its own.
func TestScanReadsASymlinkTarget(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink(plantedMount+"-01/go-quicraft/", filepath.Join(root, "go-quicraft")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	findings, err := scan(root, []string{"go-quicraft"}, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	var sawOwnerPath bool
	for _, f := range findings {
		if f.path != "go-quicraft" {
			t.Errorf("finding path = %q, want the link itself", f.path)
		}
		sawOwnerPath = sawOwnerPath || f.class == classOwnerPath
	}
	if !sawOwnerPath {
		t.Fatalf("the link target was not scanned: %v", findings)
	}
}

// TestScanReportsAValueOnceByItsMostSpecificRule pins the span dedupe: the
// account name inside an owner path is one leak, reported as the path.
func TestScanReportsAValueOnceByItsMostSpecificRule(t *testing.T) {
	root, files := writeTree(t, map[string]string{"docs/a.md": "cd " + plantedHome + "/sources, ask " + plantedAccount + "\n"})
	findings, err := scan(root, files, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(findings) != 2 || findings[0].class != classOwnerPath || findings[1].class != classOwnerAccount {
		t.Fatalf("want the path once and the bare name once, got %v", findings)
	}
}

// TestScanReportsAnAgentPrincipalOnce pins the rule order: the internal domain
// inside an agent principal is part of that one identity, not a second leak.
func TestScanReportsAnAgentPrincipalOnce(t *testing.T) {
	principal := "agent-01@" + plantedDomain
	root, files := writeTree(t, map[string]string{"signers": principal + " ssh-ed25519 AAAA " + principal + "\n"})
	findings, err := scan(root, files, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("want the principal once per occurrence and the domain never, got %v", findings)
	}
	for _, f := range findings {
		if f.class != classInternalIdentity || f.match != principal {
			t.Errorf("finding = %v, want the whole principal", f)
		}
	}
}

func TestScanQuotesALongLineBounded(t *testing.T) {
	long := "const x=\"" + strings.Repeat("padding;", 200) + plantedGPUHost + "\";"
	root, files := writeTree(t, map[string]string{"dist/bundle.js": long + "\n"})

	findings, err := scan(root, files, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("want one finding, got %v", findings)
	}
	if got := findings[0].excerpt; len(got) > excerptLimit+len(" ...") || !strings.HasSuffix(got, " ...") {
		t.Errorf("excerpt is not bounded and marked as truncated: %q", got)
	}
}

func TestReportSaysWhatToDo(t *testing.T) {
	out := report([]finding{{
		path:     "docs/thing.md",
		line:     7,
		class:    classLabHost,
		match:    plantedAgent,
		excerpt:  "observed on " + plantedAgent,
		guidance: "name the role",
	}})
	for _, want := range []string{"docs/thing.md:7", plantedAgent, "name the role", waiverPrefix, "public-releasability.md"} {
		if !strings.Contains(out, want) {
			t.Errorf("report omits %q:\n%s", want, out)
		}
	}
}

func TestErrorsNameTheirSubjectAndUnwrap(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		contains []string
		wrapped  error
	}{
		{
			name:     "list failure names the root and what git said",
			err:      &listError{root: "/srv/tree", stderr: "not a git repository", err: os.ErrNotExist},
			contains: []string{"/srv/tree", "not a git repository"},
			wrapped:  os.ErrNotExist,
		},
		{
			name:     "read failure names the file",
			err:      &readError{path: "pkg/x/y.go", err: os.ErrPermission},
			contains: []string{"pkg/x/y.go"},
			wrapped:  os.ErrPermission,
		},
		{
			name:     "allow-rule failure names the entry and the defect",
			err:      &allowRuleError{index: 3, path: "pkg/**", problem: "no reason given"},
			contains: []string{"entry 3", "pkg/**", "no reason given"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, want := range tc.contains {
				if !strings.Contains(tc.err.Error(), want) {
					t.Errorf("%q omits %q", tc.err.Error(), want)
				}
			}
			if tc.wrapped != nil && !errors.Is(tc.err, tc.wrapped) {
				t.Errorf("%v does not unwrap to %v", tc.err, tc.wrapped)
			}
		})
	}
}

// TestPatternsEveryNeedleIsImpliedByItsExpr pins the sieve's invariant: a
// pattern whose needle its own matches do not contain would be skipped for
// every file, and the gate would pass while seeing nothing.
func TestPatternsEveryNeedleIsImpliedByItsExpr(t *testing.T) {
	samples := map[string][]string{
		`\b198\.51\.100\.\d{1,3}\b`: {plantedAddress + ":3001", "at " + plantedAddress},
		`(?i)\bgpu-host\b`:          {"on " + plantedGPUHost, "http://" + strings.ToUpper(plantedGPUHost) + ".internal:8077"}, // lab-string-ok: the rule's own source text, as the map key
		`(?i)\bci-node-\d+\b`:       {"runner " + plantedRunner, strings.ToUpper(plantedRunner)},
		`(?i)\bvm-host-\d+\b`:       {"agent-" + plantedAgent, strings.ToUpper(plantedAgent) + " rebooted"},
		`(?i)\bomega\b`:             {"ssh user@" + plantedBenchHost + ":22", strings.ToUpper(plantedBenchHost) + " reported Online"},
		`/mnt/vm-host`:              {plantedMount + "-01/go-qrdb"}, // lab-string-ok: the rule's own source text, as the map key
		`(?i)\bagent-[a-z0-9-]+@[a-z0-9-]+(\.[a-z0-9-]+)*`: {plantedPrincipal, "user.email=" + strings.ToUpper(plantedSibling)},
		`(?i)\bexample\.internal\b`:                        {"ops@" + plantedDomain, strings.ToUpper(plantedDomain) + ":22"},
		`(?i)\bhandbook ?#\d+`:                             {plantedHandbookIssue, "Handbook#44"},                                                 // lab-string-ok: a planted sample for the internal-repository rule
		`(?i)\bclaude-agents\b`:                            {plantedAgentsRepo + "#92", strings.ToUpper(plantedAgentsRepo)},                       // lab-string-ok: the rule's own source text, as the map key
		`(?i)\bjhahn\b`:                                    {plantedAccount + "@example.com", "signed off by " + strings.ToUpper(plantedAccount)}, // lab-string-ok: the rule's own source text, as the map key
	}

	for _, p := range patterns {
		t.Run(p.expr.String(), func(t *testing.T) {
			if p.needle == "" {
				t.Fatal("pattern has no sieve needle")
			}
			if p.needle != strings.ToLower(p.needle) {
				t.Errorf("needle %q is not lowercase; the sieve folds case before searching", p.needle)
			}
			got, ok := samples[p.expr.String()]
			if !ok {
				t.Fatal("no sample for this pattern: add one so its needle stays honest")
			}
			for _, sample := range got {
				if !p.expr.MatchString(sample) {
					t.Errorf("%q does not match its own sample %q", p.expr, sample)
				}
				if !strings.Contains(strings.ToLower(sample), p.needle) {
					t.Errorf("sample %q matches but does not contain needle %q: the sieve would skip it", sample, p.needle)
				}
			}
		})
	}
}
