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

import "testing"

// TestCanonical_IPv6FormsWithAndWithoutPort pins the IPv6 spellings that only
// reached CanonicalHost before: an IPv4-mapped literal with a port collapses
// to the IPv4 endpoint (so it cannot pass a duplicate-address guard as a
// second replica), and a bare or bracketed literal with no port falls back to
// CanonicalHost and comes out unbracketed and portless.
func TestCanonical_IPv6FormsWithAndWithoutPort(t *testing.T) {
	cases := []struct {
		name    string
		address string
		want    string
	}{
		{"mapped literal with port", "[::ffff:10.0.0.5]:5001", "10.0.0.5:5001"},
		{"plain IPv4 with port", "10.0.0.5:5001", "10.0.0.5:5001"},
		{"expanded literal, no port", "0:0:0:0:0:0:0:1", "::1"},
		{"bracketed expanded literal, no port", "[0:0:0:0:0:0:0:1]", "::1"},
		{"mapped literal, zero-padded port", "[::ffff:10.0.0.5]:05001", "10.0.0.5:5001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Canonical(tc.address); got != tc.want {
				t.Errorf("Canonical(%q) = %q, want %q", tc.address, got, tc.want)
			}
		})
	}
	if a, b := Canonical("[::ffff:10.0.0.5]:5001"), Canonical("10.0.0.5:5001"); a != b {
		t.Errorf("mapped and IPv4 spellings of one endpoint differ: %q != %q", a, b)
	}
}

// TestHost_IPv6FormMappedLiteral pins that Host extracts the host component
// as written (its documented contract: it does not canonicalise), and that
// CanonicalHost of that component is the IPv4 spelling Canonical uses.
func TestHost_IPv6FormMappedLiteral(t *testing.T) {
	const address = "[::ffff:10.0.0.5]:5001"
	host := Host(address)
	if host != "::ffff:10.0.0.5" {
		t.Errorf("Host(%q) = %q, want %q", address, host, "::ffff:10.0.0.5")
	}
	if got := CanonicalHost(host); got != "10.0.0.5" {
		t.Errorf("CanonicalHost(Host(%q)) = %q, want %q", address, got, "10.0.0.5")
	}
}
