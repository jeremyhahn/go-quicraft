// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package buildgates

import (
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestReleaseWireGateFetchesItsBaseline pins that the wire version gate of
// make release and make release-candidate reads its baseline from the release
// remote, not only from the local tags: a --no-tags or single-branch clone
// must not skip the gate, and an unreachable remote is not an absent baseline.
func TestReleaseWireGateFetchesItsBaseline(t *testing.T) {
	for _, tc := range []struct{ target, version string }{
		{"release", "v0.9.5"},
		{"release-candidate", "v0.9.5-rc.1"},
	} {
		t.Run(tc.target, func(t *testing.T) {
			for _, sc := range []struct{ name, localTag string }{
				{"remote-only baseline is used", ""},
				// A local tag the remote never advertised must not become the
				// baseline, even when it sorts above every remote tag.
				{"higher local-only tag is ignored", "v0.9.9-rc.1"},
			} {
				t.Run(sc.name, func(t *testing.T) {
					key, pub := newSigningKey(t)
					const maintainer = "release@example.invalid"
					f := newReleaseFixture(t, true)
					f.signWith(maintainer, key, true, true)
					signers := writeSigners(t, maintainer+" "+pub)

					const baseline = "v0.9.4-rc.1"
					f.git("-c", "tag.gpgsign=false", "tag", baseline)
					f.git("push", "-q", fixtureRemote, "refs/tags/"+baseline)
					f.git("tag", "-d", baseline)
					pushed := f.remoteRefs()

					golden := filepath.Join(f.dir, "pkg/proto/testdata/wire_layout.golden")
					moved := regexp.MustCompile(`(?m)^sha256 .*$`).ReplaceAllString(readFile(t, golden), "sha256 "+strings.Repeat("0", 64))
					writeFile(t, golden, moved, 0o644)
					f.git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qam", "move the wire layout")
					if sc.localTag != "" {
						f.git("-c", "tag.gpgsign=false", "tag", sc.localTag)
					}
					before := f.version()

					out, err := f.make(tc.target, "VERSION="+tc.version, "RELEASE_REMOTE="+fixtureRemote, "RELEASE_ALLOWED_SIGNERS="+signers)
					if err == nil {
						t.Fatalf("%s succeeded with the wire layout moved since %s on the remote:\n%s", tc.target, baseline, out)
					}
					if !strings.Contains(out, "WireVersion did not") || strings.Contains(out, "skipping the wire version gate") {
						t.Errorf("%s did not compare against the remote's %s:\n%s", tc.target, baseline, out)
					}
					if got := f.version(); got != before {
						t.Errorf("VERSION = %q after a refused release, want %q", got, before)
					}
					if refs := f.remoteRefs(); refs != pushed {
						t.Errorf("the remote changed after a refused release:\n%s\nwant:\n%s", refs, pushed)
					}
				})
			}
			t.Run("unreachable remote is refused", func(t *testing.T) {
				key, pub := newSigningKey(t)
				const maintainer = "release@example.invalid"
				f := newReleaseFixture(t, true)
				f.signWith(maintainer, key, true, true)
				signers := writeSigners(t, maintainer+" "+pub)
				f.git("remote", "set-url", fixtureRemote, filepath.Join(t.TempDir(), "absent.git"))
				before := f.version()

				out, err := f.make(tc.target, "VERSION="+tc.version, "RELEASE_REMOTE="+fixtureRemote, "RELEASE_ALLOWED_SIGNERS="+signers)
				if err == nil {
					t.Fatalf("%s succeeded against an unreachable remote:\n%s", tc.target, out)
				}
				if !strings.Contains(out, "could not fetch tags from "+fixtureRemote) {
					t.Errorf("%s was not refused by the wire gate's fetch:\n%s", tc.target, out)
				}
				if got := f.version(); got != before {
					t.Errorf("VERSION = %q after a refused release, want %q", got, before)
				}
			})
		})
	}
}
