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

// Package netaddr canonicalises Raft node addresses with pure string functions, so raft and the transport agree on endpoint identity.
package netaddr

import (
	"net"
	"strconv"
	"strings"
)

// CanonicalHost normalises a host as x509.VerifyHostname compares it (lower case, no trailing dot, canonical IP), without DNS.
func CanonicalHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	// A trailing dot marks an absolute FQDN and is ignored.
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

// Host returns the host of a host:port address, or the address when it has no port.
func Host(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// address may be a bare host with no port; fall back to it verbatim.
		return address
	}
	return host
}

// Canonical normalises a host:port address: CanonicalHost for the host, canonicalPort for the port.
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

// canonicalPort normalises a numeric port ("05001" and "+5001" become "5001"); a service name is lower-cased and trimmed only.
func canonicalPort(port string) string {
	trimmed := strings.TrimSpace(port)
	if n, err := strconv.ParseUint(strings.TrimPrefix(trimmed, "+"), 10, 16); err == nil {
		return strconv.FormatUint(n, 10)
	}
	return strings.ToLower(trimmed)
}
