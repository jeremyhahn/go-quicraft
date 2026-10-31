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

package netaddr

import (
	"net"
	"testing"
)

// TestCanonical_NormalisesPortSpelling pins the quorum-safety property this
// package exists for.
//
// Canonical lower-cased the host but passed the PORT through verbatim, so
// "node-b:5001", "node-b:05001" and "node-b:+5001" were three distinct keys.
// net.ResolveUDPAddr accepts all three and returns the same endpoint, so a
// single host could be named twice in one configuration: the duplicate-address
// check in StartShard, the addressHolder guard and the sharedAddresses
// fail-closed net all key on Canonical, and the receive-side certificate check
// drops the port entirely. One machine then held two votes and two match
// indices in a quorum that believed they were independent replicas.
func TestCanonical_NormalisesPortSpelling(t *testing.T) {
	want := Canonical("node-b:5001")
	for _, spelling := range []string{
		"node-b:05001",
		"node-b:+5001",
		"NODE-B:05001",
		"node-b:0005001",
	} {
		if got := Canonical(spelling); got != want {
			t.Errorf("Canonical(%q) = %q, want %q — one endpoint spelled two "+
				"ways passes the duplicate-address guard and holds two votes",
				spelling, got, want)
		}
	}

	// Guard the premise: these spellings really are one endpoint. If
	// ResolveUDPAddr ever stopped accepting them the test above would be
	// pinning a normalisation nothing needs.
	base, err := net.ResolveUDPAddr("udp", "127.0.0.1:5001")
	if err != nil {
		t.Fatalf("ResolveUDPAddr(base): %v", err)
	}
	for _, spelling := range []string{"127.0.0.1:05001", "127.0.0.1:+5001"} {
		got, rerr := net.ResolveUDPAddr("udp", spelling)
		if rerr != nil {
			t.Fatalf("ResolveUDPAddr(%q): %v", spelling, rerr)
		}
		if got.String() != base.String() {
			t.Fatalf("premise broken: %q resolves to %v, not %v",
				spelling, got, base)
		}
	}
}

// TestCanonical_IPv6AndServiceNames covers the shapes the numeric path must
// not damage.
func TestCanonical_IPv6AndServiceNames(t *testing.T) {
	if got, want := Canonical("[::1]:05001"), Canonical("[::1]:5001"); got != want {
		t.Errorf("IPv6 port normalisation: %q != %q", got, want)
	}
	// A service name is not a pure function of the string, so it is left as
	// written (trimmed and lower-cased) rather than resolved.
	if got := Canonical("node-b:HTTPS"); got != "node-b:https" {
		t.Errorf("Canonical(%q) = %q, want %q", "node-b:HTTPS", got, "node-b:https")
	}
}
