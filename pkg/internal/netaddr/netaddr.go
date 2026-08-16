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

// Package netaddr canonicalises Raft node addresses so that two SPELLINGS of
// one endpoint compare equal.
//
// It exists because the same normalisation is needed by two packages that must
// not depend on each other. The transport matches a peer address against a
// certificate's SANs; raft refuses a configuration that names one address
// twice. Both decisions are "is this the same endpoint?", and while the
// transport held the only normaliser, raft compared raw strings — so
// "NODE-B:5001" and "node-b:5001" were one endpoint to the certificate check
// and two distinct members to the configuration validator. Importing transport
// from raft would invert the layering (transport already depends on raft's
// data types through pkg/proto and pkg/config), so the normalisation lives
// here, below both.
//
// Everything here is a PURE STRING FUNCTION. Nothing resolves DNS, consults
// the network, or reads any state. That is a hard requirement, not an
// accident: raft uses these results inside config-change validation, which is
// replicated-state logic that must produce the identical answer on every
// replica and on every replay of the same log. A resolver would make the
// answer depend on when and where it ran.
package netaddr

import (
	"net"
	"strconv"
	"strings"
)

// CanonicalHost normalises a host the way x509.VerifyHostname compares it, so
// two spellings of the SAME host are recognised as one.
//
// VerifyHostname lower-cases DNS names, tolerates a single trailing dot (the
// absolute-FQDN form), and parses an IP literal before matching it against the
// IPAddresses SANs. Any check that decides "is this the same host?" with a raw
// string compare while the MATCH is done by VerifyHostname is inconsistent by
// construction: it will call two names different that the certificate calls
// the same. That inconsistency is what made the certificate-exclusivity check
// reject legitimate deployments — N1.EXAMPLE.COM against n1.example.com, the
// FQDN form against the relative one, ::1 against 0:0:0:0:0:0:0:1.
//
// IP literals are canonicalised through net.IP.String(), which also collapses
// the IPv4-mapped IPv6 form (::ffff:127.0.0.1) onto its IPv4 spelling. An
// empty host stays empty; callers treat that as "no host to check".
//
// It deliberately does NOT resolve names: a hostname and the IP it currently
// resolves to stay distinct. Folding them would make the result depend on DNS
// at the moment of the call, which config-change validation cannot tolerate.
func CanonicalHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	// A single trailing dot marks an absolute FQDN and is not part of the
	// name for matching purposes.
	if len(host) > 1 && strings.HasSuffix(host, ".") {
		host = host[:len(host)-1]
	}
	// Bracketed IPv6 literals reach here when the address had no port.
	if len(host) > 1 && host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return host
}

// Host extracts the host component of a "host:port" Raft address. An address
// with no port is returned verbatim, so a bare host is its own host.
//
// The port is deliberately dropped: a certificate is matched on host alone,
// which is exactly why callers that decide certificate questions must not see
// it. Callers that need the whole endpoint use Canonical.
func Host(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// address may be a bare host with no port; fall back to it verbatim.
		return address
	}
	return host
}

// Canonical normalises a whole "host:port" Raft address: the host through
// CanonicalHost, the port verbatim. An address with no port canonicalises as a
// bare host.
//
// The PORT is part of the result, unlike Host. Two replicas on one machine at
// different ports are a supported deployment and are genuinely distinct
// endpoints, so a duplicate-address check must compare the whole address —
// while still recognising that "NODE-B:5001" and "node-b:5001" are one of them,
// not two.
func Canonical(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return CanonicalHost(address)
	}
	host = CanonicalHost(host)
	port = canonicalPort(port)
	// An IPv6 literal must go back inside brackets or the result is not a
	// re-parseable address (and "::1" + ":5001" would concatenate into a
	// different literal entirely).
	if strings.Contains(host, ":") {
		return "[" + host + "]:" + port
	}
	return host + ":" + port
}

// canonicalPort normalises a port NUMERICALLY rather than accepting whatever
// spelling the caller used.
//
// "5001", "05001" and "+5001" all resolve to the same UDP endpoint —
// net.ResolveUDPAddr accepts every one of them and returns :5001 — so leaving
// them as distinct strings let one endpoint be named twice in a configuration.
// Both the duplicate-address check in StartShard and the addressHolder /
// sharedAddresses guards in membership key on Canonical, and the receive-side
// certificate check drops the port entirely, so a single host behind
// "node-b:5001" and "node-b:05001" satisfied all of them as two "distinct"
// replicas — and then held two votes and two match indices in a quorum that
// believed they were independent. That is precisely the outcome those guards
// exist to prevent, defeated by one character.
//
// A non-numeric port (a service name) is lower-cased and trimmed but otherwise
// left alone: resolving it is not a pure function of the string, and this
// package must stay one.
func canonicalPort(port string) string {
	trimmed := strings.TrimSpace(port)
	if n, err := strconv.ParseUint(strings.TrimPrefix(trimmed, "+"), 10, 16); err == nil {
		return strconv.FormatUint(n, 10)
	}
	return strings.ToLower(trimmed)
}
