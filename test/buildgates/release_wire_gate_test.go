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
			for _, sc := range []struct {
				name, localTag string
				// pruneTags sets these git config keys to true in the fixture
				// clone. git adds refs/tags/*:refs/tags/* to a pruning fetch
				// only when no refspec is named on the command line (the
				// baseline fetches always name one, and also turn tag pruning
				// off), so the local-only tag must survive the gate; a
				// refspec-less fetch afterwards proves the config is live.
				pruneTags []string
			}{
				{"remote-only baseline is used", "", nil},
				// A local tag the remote never advertised must not become the
				// baseline, even when it sorts above every remote tag.
				{"higher local-only tag is ignored", "v0.9.9-rc.1", nil},
				{"fetch.pruneTags keeps local-only tags", "v0.9.9-rc.1", []string{"fetch.pruneTags", "fetch.prune"}},
				{"remote pruneTags keeps local-only tags", "v0.9.9-rc.1", []string{"remote." + fixtureRemote + ".pruneTags", "remote." + fixtureRemote + ".prune"}},
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
					for _, key := range sc.pruneTags {
						f.git("config", key, "true")
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
					if sc.localTag != "" {
						if got := strings.TrimSpace(f.git("tag", "-l", sc.localTag)); got != sc.localTag {
							t.Errorf("local-only tag %s was deleted by the baseline fetch (config %v)", sc.localTag, sc.pruneTags)
						}
					}
					if len(sc.pruneTags) > 0 {
						f.git("fetch", "--quiet", "--no-tags", fixtureRemote)
						if got := strings.TrimSpace(f.git("tag", "-l", sc.localTag)); got != "" {
							t.Errorf("config %v did not make a refspec-less fetch prune local-only tag %s; the case above proves nothing", sc.pruneTags, sc.localTag)
						}
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
			// A configured remote.<name>.fetch mapping still updates matching
			// local refs when a refspec is named on the command line, unless
			// the fetch passes --refmap=. A forced tag mapping must not
			// overwrite a local baseline tag that differs from the remote's.
			t.Run("forced tag mapping keeps a clashing local tag", func(t *testing.T) {
				key, pub := newSigningKey(t)
				const maintainer = "release@example.invalid"
				f := newReleaseFixture(t, true)
				f.signWith(maintainer, key, true, true)
				signers := writeSigners(t, maintainer+" "+pub)

				const baseline = "v0.9.4"
				f.git("-c", "tag.gpgsign=false", "tag", baseline)
				f.git("push", "-q", fixtureRemote, "refs/tags/"+baseline)
				f.git("tag", "-d", baseline)
				pushed := f.remoteRefs()
				f.git("-c", "commit.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "diverge")
				f.git("-c", "tag.gpgsign=false", "tag", baseline)
				local := strings.TrimSpace(f.git("rev-parse", "refs/tags/"+baseline))
				f.git("config", "--add", "remote."+fixtureRemote+".fetch", "+refs/tags/*:refs/tags/*")
				before := f.version()

				out, err := f.make(tc.target, "VERSION="+tc.version, "RELEASE_REMOTE="+fixtureRemote, "RELEASE_ALLOWED_SIGNERS="+signers)
				if err == nil {
					t.Fatalf("%s succeeded with a local %s that differs from the remote's:\n%s", tc.target, baseline, out)
				}
				if !strings.Contains(out, "could not fetch the wire version baseline tag "+baseline) {
					t.Errorf("%s was not refused by the clashing baseline tag:\n%s", tc.target, out)
				}
				if got := strings.TrimSpace(f.git("rev-parse", "refs/tags/"+baseline)); got != local {
					t.Errorf("local tag %s = %s after the gate, want %s: the configured forced mapping overwrote it", baseline, got, local)
				}
				if got := f.version(); got != before {
					t.Errorf("VERSION = %q after a refused release, want %q", got, before)
				}
				if refs := f.remoteRefs(); refs != pushed {
					t.Errorf("the remote changed after a refused release:\n%s\nwant:\n%s", refs, pushed)
				}
			})
		})
	}
}
