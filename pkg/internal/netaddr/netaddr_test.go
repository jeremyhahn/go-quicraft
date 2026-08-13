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

// TestCanonicalHost_MatchesVerifyHostnameSemantics pins the normalisations
// x509.VerifyHostname performs. Anything it treats as one host must canonicalise
// to one string, or a caller deciding "is this the same endpoint?" by comparison
// disagrees with the certificate match it is standing in for.
func TestCanonicalHost_MatchesVerifyHostnameSemantics(t *testing.T) {
	same := []struct {
		name string
		a, b string
	}{
		{"case", "NODE-B.EXAMPLE.COM", "node-b.example.com"},
		{"absolute FQDN dot", "node-b.example.com.", "node-b.example.com"},
		{"surrounding space", "  node-b.example.com  ", "node-b.example.com"},
		{"expanded IPv6", "0:0:0:0:0:0:0:1", "::1"},
		{"bracketed IPv6", "[::1]", "::1"},
		{"IPv4-mapped IPv6", "::ffff:10.0.0.5", "10.0.0.5"},
		{"IPv4 leading zeros are NOT an IP", "10.0.0.05", "10.0.0.05"},
	}
	for _, tt := range same {
		t.Run(tt.name, func(t *testing.T) {
			if got, want := CanonicalHost(tt.a), CanonicalHost(tt.b); got != want {
				t.Fatalf("CanonicalHost(%q) = %q, CanonicalHost(%q) = %q; these are one host "+
					"to x509.VerifyHostname and must compare equal", tt.a, got, tt.b, want)
			}
		})
	}

	// Distinct hosts must stay distinct. Without this the canonicaliser could
	// satisfy every case above by returning a constant.
	distinct := [][2]string{
		{"node-a.example.com", "node-b.example.com"},
		{"10.0.0.5", "10.0.0.6"},
		// A hostname and the IP it might resolve to stay distinct: resolving
		// would make config-change validation depend on DNS at the moment of
		// the call, which replicated logic cannot tolerate.
		{"localhost", "127.0.0.1"},
	}
	for _, pair := range distinct {
		if CanonicalHost(pair[0]) == CanonicalHost(pair[1]) {
			t.Fatalf("CanonicalHost collapsed distinct hosts %q and %q", pair[0], pair[1])
		}
	}

	if CanonicalHost("") != "" {
		t.Fatal("an empty host must stay empty; callers read that as \"no host to check\"")
	}
	// A single dot is not a trailing-dot FQDN and must not be trimmed to "".
	if got := CanonicalHost("."); got != "." {
		t.Fatalf("CanonicalHost(\".\") = %q, want \".\"", got)
	}
}

// TestCanonical_PreservesThePort is the property that keeps co-located replicas
// working. Two replicas on one machine at different ports are a supported
// deployment and genuinely distinct endpoints, so the address canonicaliser must
// normalise the SPELLING without collapsing the port — which is exactly what
// distinguishes it from CanonicalHost.
func TestCanonical_PreservesThePort(t *testing.T) {
	if Canonical("NODE-B.example.com:5001") != Canonical("node-b.example.com:5001") {
		t.Fatal("two spellings of one endpoint did not canonicalise to one value")
	}
	if Canonical("node-b.example.com:5001") == Canonical("node-b.example.com:5002") {
		t.Fatal("Canonical collapsed two ports on one host: co-located replicas would " +
			"be rejected as duplicate members")
	}

	// An IPv6 literal must come back re-parseable. Dropping the brackets would
	// concatenate the host and port into a different literal entirely.
	if got, want := Canonical("[0:0:0:0:0:0:0:1]:5001"), "[::1]:5001"; got != want {
		t.Fatalf("Canonical(IPv6) = %q, want %q", got, want)
	}
	// A bare host with no port canonicalises as a host.
	if got, want := Canonical("NODE-B.example.com"), "node-b.example.com"; got != want {
		t.Fatalf("Canonical(bare host) = %q, want %q", got, want)
	}
	if Canonical("") != "" {
		t.Fatal("an empty address must stay empty")
	}
}

// TestHost_DropsThePort pins that Host answers the certificate question, where
// the port is never part of the decision, and tolerates an address with no port.
func TestHost_DropsThePort(t *testing.T) {
	if got, want := Host("node-b.example.com:5001"), "node-b.example.com"; got != want {
		t.Fatalf("Host = %q, want %q", got, want)
	}
	if got, want := Host("[::1]:5001"), "::1"; got != want {
		t.Fatalf("Host(IPv6) = %q, want %q", got, want)
	}
	if got, want := Host("node-b.example.com"), "node-b.example.com"; got != want {
		t.Fatalf("Host(no port) = %q, want %q", got, want)
	}
}
