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

// Package releasability is the release gate that keeps this repository
// publicly releasable: it reports every published file that names a host, an
// address, a filesystem path or an identity belonging to the private
// development fleet, and it pins the exclusion list that keeps the private
// development workflow out of every published form of the tree.
//
// WHY THIS IS A GATE AND NOT A CONVENTION. go-quicraft ships publicly as a Go
// module and as release archives. Anything committed here is published unless
// .gitattributes marks it export-ignore (see publishedFiles). Before this gate
// the Makefile defaulted its remote-offload target to a fleet address and the
// owner's account, benchmark baselines carried the same pair in their headers,
// and a test fixture spelled out the private forge's address. None of it was
// load-bearing and nothing prevented any of it.
//
// WHAT IS REPORTED, and why each class is a leak rather than a style problem:
//
//   - classLabAddress: an address on the fleet's subnet. Unreachable for every
//     public reader, so a copy-pasteable example containing one is broken
//     documentation, and it publishes the operator's network layout.
//   - classLabHost: a fleet hostname. Same, plus it names the machines an
//     attacker would go looking for.
//   - classOwnerPath: a path under the owner's home directory or the lab's NFS
//     mount. Meaningless off that one machine, and it leaks the account name.
//   - classOwnerAccount: the owner's account name on its own. It identifies a
//     person, and it is how the paths above get rebuilt by hand.
//   - classInternalIdentity: a principal in the internal mail domain, or an
//     agent principal (`agent-<name>@...`). It publishes who and what holds
//     signing and write access on the private forge, and no public reader can
//     do anything with it.
//
// TWO WAYS TO ACCEPT A LINE, both explicit:
//
//   - An inline waiver: the text `lab-string-ok: <reason>` on the offending
//     line or the line immediately above it, in whatever comment syntax the
//     file uses. An empty reason is not a waiver.
//   - An entry in allowList (allowlist_test.go), for whole trees and for
//     files that cannot carry a comment. Each entry names the classes it
//     accepts and a reason.
//
// WHERE THIS LIVES. The package is test-only -- every file is a _test.go file
// -- so it adds nothing to the module's importable API, and it sits in the tree
// `go test ./...` walks, so `make test` runs the gate with no target to
// remember. Resolution is textual: it needs no build tags and no parser, so it
// covers Markdown, YAML, shell and Makefiles as readily as Go.
//
// docs/release/public-releasability.md is the policy this package enforces.
package releasability

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// class groups findings by what the matched text leaks. It is a string so a
// report, an allow-list entry and a test name all spell it the same way.
type class string

const (
	// classLabAddress is an IP address on the development fleet's subnet.
	classLabAddress class = "lab-address"

	// classLabHost is a hostname belonging to the development fleet.
	classLabHost class = "lab-host"

	// classOwnerPath is a filesystem path that exists only on the owner's
	// machine or on the lab's network mount.
	classOwnerPath class = "owner-path"

	// classOwnerAccount is the owner's account name outside a path.
	classOwnerAccount class = "owner-account"

	// classInternalIdentity is an identity of the private development
	// workflow: an address in the internal mail domain, or an agent
	// principal.
	classInternalIdentity class = "internal-identity"

	// classInternalRepository is a repository of the private forge that is
	// not this project: the company repository, the handbook's issue
	// tracker, the agent tooling repository. A public reader can open none
	// of them, so a citation of one is a dead reference.
	classInternalRepository class = "internal-repository"
)

// waiverPrefix marks a line whose lab value is the point of the line. The text
// after it is the reason and must not be empty.
const waiverPrefix = "lab-string-ok:"

// pattern is one rule the scanner applies to every line.
type pattern struct {
	// class says what a match leaks.
	class class

	// expr matches the offending text. It matches the value, not the whole
	// line, so the report can quote exactly what has to change.
	expr *regexp.Regexp

	// needle is a lowercase literal every string expr can match must
	// contain. It is the cheap precondition the sieve uses: a substring
	// search runs at memory bandwidth where the regexp runs at a few
	// megabytes a second. A needle not implied by expr would blind its own
	// pattern, so TestPatternsEveryNeedleIsImpliedByItsExpr pins it.
	needle string

	// guidance is what the engineer who tripped this rule should do instead.
	guidance string
}

// patterns is the rule set. It names the placeholder vocabulary the published
// tree uses for the development fleet's subnet, hostnames, path prefixes and
// internal mail domain, so a placeholder copied into new prose or a new fixture
// is caught like the value it stands for.
//
// The values a private scan of an unpublished tree needs live outside this
// repository.
//
// Note what is NOT here: the documentation and RFC 1918 ranges at large. Tests
// across this tree use such literals as opaque addresses that are never dialed;
// banning whole ranges would bury real findings under them. The placeholder
// subnet is named precisely instead.
var patterns = []pattern{
	{
		class:    classLabAddress,
		expr:     regexp.MustCompile(`\b198\.51\.100\.\d{1,3}\b`),
		needle:   "198.51.100.",
		guidance: "use an address from another RFC 5737 documentation range (192.0.2.0/24, 203.0.113.0/24) or a name under .example",
	},
	{
		class:    classLabHost,
		expr:     regexp.MustCompile(`(?i)\bgpu-host\b`), // lab-string-ok: the hostname this rule exists to find
		needle:   "gpu-host",                             // lab-string-ok: the hostname this rule exists to find
		guidance: "name the role, not the machine: review-host.example, build-host.example",
	},
	{
		class:    classLabHost,
		expr:     regexp.MustCompile(`(?i)\bci-node-\d+\b`),
		needle:   "ci-node-",
		guidance: "a CI runner's or bench machine's own hostname is never load-bearing: bench-01 reads the same",
	},
	{
		class:    classLabHost,
		expr:     regexp.MustCompile(`(?i)\bvm-host-\d+\b`),
		needle:   "vm-host-",
		guidance: "name the role, not the machine: build-host.example, bench-01, agent-01",
	},
	{
		class:    classLabHost,
		expr:     regexp.MustCompile(`(?i)\bomega\b`), // lab-string-ok: the hostname this rule exists to find
		needle:   "omega",                             // lab-string-ok: the hostname this rule exists to find
		guidance: "name the role, not the machine: build-host.example, bench-host.example",
	},
	{
		class:    classOwnerPath,
		expr:     regexp.MustCompile(`/mnt/vm-host`), // lab-string-ok: the mount prefix this rule exists to find
		needle:   "/mnt/vm-host",                     // lab-string-ok: the mount prefix this rule exists to find
		guidance: "the lab's NFS mount does not exist anywhere else; use a repository-relative path",
	},
	{
		// Before the domain rule on purpose: an agent principal is reported
		// once, whole, and the internal domain inside it is not reported again.
		class:    classInternalIdentity,
		expr:     regexp.MustCompile(`(?i)\bagent-[a-z0-9-]+@[a-z0-9-]+(\.[a-z0-9-]+)*`),
		needle:   "agent-",
		guidance: "an agent principal identifies the private workflow, not the project: use a neutral example identity such as release@example.invalid",
	},
	{
		class:    classInternalIdentity,
		expr:     regexp.MustCompile(`(?i)\bexample\.internal\b`), // lab-string-ok: the mail domain this rule exists to find
		needle:   "example.internal",                              // lab-string-ok: the mail domain this rule exists to find
		guidance: "the internal mail domain resolves nowhere public: use a name under .example or .invalid",
	},
	{
		class:    classInternalRepository,
		expr:     regexp.MustCompile(`(?i)\bhandbook ?#\d+`), // lab-string-ok: the issue reference this rule exists to find
		needle:   "handbook",
		guidance: internalRepositoryGuidance,
	},
	{
		class:    classInternalRepository,
		expr:     regexp.MustCompile(`(?i)\bclaude-agents\b`), // lab-string-ok: the repository name this rule exists to find
		needle:   "claude-agents",                             // lab-string-ok: the repository name this rule exists to find
		guidance: internalRepositoryGuidance,
	},
	{
		// Last on purpose: a match inside a span an earlier rule already
		// reported (the account inside an owner path) is not reported twice.
		class:    classOwnerAccount,
		expr:     regexp.MustCompile(`(?i)\bjhahn\b`), // lab-string-ok: the account name this rule exists to find
		needle:   "jhahn",                             // lab-string-ok: the account name this rule exists to find
		guidance: "name a role, not a person: operator, user, or the project's own module path",
	},
}

// internalRepositoryGuidance is what every internal-repository rule asks for.
const internalRepositoryGuidance = "a private-forge repository and its issue numbers resolve nowhere public: state the decision itself, or cite a public document"

// sieveNeedles is every pattern's needle, in the form the sieve wants. Almost
// no file contains a lab value, so settling "nothing here" has to be cheap.
var sieveNeedles = func() [][]byte {
	needles := make([][]byte, 0, len(patterns))
	for _, p := range patterns {
		needles = append(needles, []byte(p.needle))
	}
	return needles
}()

// mayContainLabValue reports whether any pattern could possibly match inside
// content. Case is folded because a hostname is as likely to appear shouted in a
// log line as lowercased in a config file.
func mayContainLabValue(content []byte) bool {
	folded := bytes.ToLower(content)
	for _, needle := range sieveNeedles {
		if bytes.Contains(folded, needle) {
			return true
		}
	}
	return false
}

// finding is one line that must change before this tree can be published.
type finding struct {
	path     string // slash-separated, relative to the scan root
	line     int    // 1-based, so the report is clickable
	class    class
	match    string // the offending text exactly as it appears
	excerpt  string // the whole line, trimmed and bounded
	guidance string
}

// String renders a finding in the file:line form every editor and CI log viewer
// already knows how to jump to.
func (f finding) String() string {
	return fmt.Sprintf("%s:%d: [%s] %q -- %s\n\t%s", f.path, f.line, f.class, f.match, f.guidance, f.excerpt)
}

// excerptLimit bounds the quoted line, so one long line cannot fill the report.
const excerptLimit = 160

// maxFileSize bounds what the scanner reads into memory. Nothing tracked here is
// close to it; a file that large is data, not something a reader copies from.
const maxFileSize = 16 << 20

// binarySniff is how much of a file is examined for a NUL byte before deciding
// it is binary. It matches what git itself uses.
const binarySniff = 8000

// scanRepo is the gate: it enumerates the published files under root and
// reports every one that names a lab value and is neither waived in place nor
// covered by allowList.
func scanRepo(ctx context.Context, root string) ([]finding, error) {
	files, err := publishedFiles(ctx, root)
	if err != nil {
		return nil, err
	}
	return scan(root, files, allowList)
}

// scan reports the lab values in the named files, interpreted relative to root.
// Rules are validated first: an allow-list that does not say why it allows
// something is a defect in the gate, not a pass. A file absent from the working
// tree, binary, or larger than maxFileSize is skipped.
func scan(root string, files []string, rules []allowRule) ([]finding, error) {
	if err := validateAllowList(rules); err != nil {
		return nil, err
	}
	var findings []finding
	for _, name := range files {
		accepted := acceptedClasses(rules, name)
		if len(accepted) == len(knownClasses()) {
			// Every class is allowed here; reading the file cannot
			// produce a finding.
			continue
		}
		content, ok, err := readCandidate(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		findings = append(findings, scanContent(name, content, accepted)...)
	}
	return findings, nil
}

// knownClasses is the set of classes the patterns report.
func knownClasses() map[class]bool {
	known := make(map[class]bool, len(patterns))
	for _, p := range patterns {
		known[p.class] = true
	}
	return known
}

// readCandidate reads a file the scanner may have to look at. The bool reports
// whether the content is worth scanning; a missing, oversized or binary file is
// not an error. A symlink is scanned as its target text, because a link into an
// owner's home directory is itself the leak.
func readCandidate(path string) ([]byte, bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, &readError{path: path, err: err}
	}
	if info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(path)
		if err != nil {
			return nil, false, &readError{path: path, err: err}
		}
		return []byte(target), true, nil
	}
	if !info.Mode().IsRegular() || info.Size() > maxFileSize {
		return nil, false, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, false, &readError{path: path, err: err}
	}
	if isBinary(content) {
		return nil, false, nil
	}
	return content, true, nil
}

// isBinary reports whether content holds a NUL byte in its first binarySniff
// bytes, which is git's own test.
func isBinary(content []byte) bool {
	head := content
	if len(head) > binarySniff {
		head = head[:binarySniff]
	}
	return bytes.IndexByte(head, 0) >= 0
}

// scanContent applies every pattern whose class is not accepted for this file
// to every line of it.
func scanContent(name string, content []byte, accepted map[class]bool) []finding {
	if !mayContainLabValue(content) {
		return nil
	}
	lines := strings.Split(string(content), "\n")
	var findings []finding
	for i, line := range lines {
		if waived(line) || (i > 0 && waived(lines[i-1])) {
			continue
		}
		var reported [][]int
		for _, p := range patterns {
			if accepted[p.class] {
				continue
			}
			for _, span := range p.expr.FindAllStringIndex(line, -1) {
				if within(span, reported) {
					continue
				}
				reported = append(reported, span)
				findings = append(findings, finding{
					path:     name,
					line:     i + 1,
					class:    p.class,
					match:    line[span[0]:span[1]],
					excerpt:  excerpt(line),
					guidance: p.guidance,
				})
			}
		}
	}
	return findings
}

// within reports whether span lies inside a span already reported on its line,
// so one value is reported once, by the most specific rule.
func within(span []int, reported [][]int) bool {
	for _, r := range reported {
		if span[0] >= r[0] && span[1] <= r[1] {
			return true
		}
	}
	return false
}

// waived reports whether a line carries an inline waiver with a reason. The
// reason is what makes it a waiver: `lab-string-ok:` on its own says only that
// somebody wanted the gate to stop talking.
func waived(line string) bool {
	idx := strings.Index(line, waiverPrefix)
	if idx < 0 {
		return false
	}
	reason := strings.TrimSpace(line[idx+len(waiverPrefix):])
	// Trim the comment closers the host syntaxes need, so a reason that ends
	// at `-->` or `*/` is still a reason and a bare closer is not one.
	for _, closer := range []string{"-->", "*/"} {
		reason = strings.TrimSpace(strings.TrimSuffix(reason, closer))
	}
	return reason != ""
}

// excerpt trims and bounds a line for the report.
func excerpt(line string) string {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) <= excerptLimit {
		return trimmed
	}
	return trimmed[:excerptLimit] + " ..."
}

// report renders findings in a form an engineer can act on without opening
// this package. A gate whose output does not say what to do next gets waived
// wholesale by the next person who trips it.
func report(findings []finding) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d lab value(s) in published files -- this tree ships publicly:\n\n", len(findings))
	for _, f := range findings {
		fmt.Fprintf(&b, "%s\n\n", f)
	}
	b.WriteString("Replace the value, move it to CI or infra configuration, or -- if the\n")
	b.WriteString("value is the point (benchmark provenance) -- waive the line with a\n")
	b.WriteString("`" + waiverPrefix + " <reason>` comment. See docs/release/public-releasability.md.\n")
	return b.String()
}

// listError is returned when the published-file enumeration fails. The gate
// reads the git index because that is the ship manifest, so a tree with no
// index -- an extracted archive, a module cache entry -- cannot be gated, and
// says so rather than reporting a clean tree.
type listError struct {
	root   string
	stderr string
	err    error
}

func (e *listError) Error() string {
	msg := "releasability: cannot list published files in " + e.root + ": " + e.err.Error()
	if e.stderr != "" {
		msg += ": " + e.stderr
	}
	return msg
}

func (e *listError) Unwrap() error { return e.err }

// readError is returned when a tracked file exists but cannot be read.
type readError struct {
	path string
	err  error
}

func (e *readError) Error() string {
	return "releasability: cannot read " + e.path + ": " + e.err.Error()
}

func (e *readError) Unwrap() error { return e.err }
