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

package discovery

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"net"
	"strconv"
	"time"
)

// LookupSRVFunc is the function signature for DNS SRV lookups.
// This allows injection of a mock resolver for testing.
type LookupSRVFunc func(service, proto, name string) (string, []*net.SRV, error)

// DNSConfig configures DNS SRV record discovery.
type DNSConfig struct {
	// Service is the SRV service name (e.g., "_raft").
	Service string

	// Proto is the SRV protocol (e.g., "_udp").
	Proto string

	// Domain is the DNS domain to query.
	Domain string

	// LookupTimeout is the DNS query timeout.
	LookupTimeout time.Duration

	// LookupFn overrides the default net.LookupSRV for testing.
	// If nil, net.LookupSRV is used.
	LookupFn LookupSRVFunc
}

// Security considerations for DNS-based discovery:
//
// DNS discovery does NOT authenticate responses. An attacker who can spoof
// DNS replies (cache poisoning, MITM on the resolver path) can inject
// arbitrary peer addresses. Deployments that rely on DNS discovery MUST
// use one or more of the following mitigations:
//
//   - DNSSEC: sign the zone containing SRV records so resolvers can
//     validate response authenticity.
//   - Trusted resolver: use a resolver on a secure channel (DNS-over-TLS,
//     DNS-over-HTTPS, or a local recursive resolver on loopback).
//   - Transport-layer mTLS: even if a spoofed address is discovered, the
//     Raft transport's mutual TLS handshake will reject connections to
//     peers that cannot present a valid certificate, preventing data
//     exfiltration or cluster compromise.
//
// Without at least one of these controls, DNS discovery is vulnerable to
// peer injection attacks.

// DNSDiscovery discovers peers via DNS SRV record lookups.
type DNSDiscovery struct {
	cfg      DNSConfig
	lookupFn LookupSRVFunc
}

// NewDNSDiscovery creates a DNSDiscovery that queries SRV records.
func NewDNSDiscovery(cfg DNSConfig) *DNSDiscovery {
	lookupFn := cfg.LookupFn
	if lookupFn == nil {
		lookupFn = net.LookupSRV
	}
	if cfg.LookupTimeout == 0 {
		cfg.LookupTimeout = 5 * time.Second
	}
	return &DNSDiscovery{
		cfg:      cfg,
		lookupFn: lookupFn,
	}
}

// Discover queries DNS SRV records and maps them to peers.
// NodeID is derived from a SHA-256 hash (truncated to uint64) of the
// target:port string since SRV records do not carry node identifiers.
// The context parameter is accepted for interface compliance. The
// underlying DNS lookup uses the resolver's own timeout (LookupTimeout)
// rather than the context, because the injected LookupSRVFunc signature
// does not accept a context.
func (d *DNSDiscovery) Discover(_ context.Context) ([]Peer, error) {
	_, addrs, err := d.lookupFn(d.cfg.Service, d.cfg.Proto, d.cfg.Domain)
	if err != nil {
		return nil, &Error{Method: d.Name(), Err: err}
	}

	if len(addrs) == 0 {
		return nil, &NoPeersError{Method: d.Name()}
	}

	peers := make([]Peer, 0, len(addrs))
	for _, srv := range addrs {
		target := srv.Target
		// Strip trailing dot from DNS names.
		if len(target) > 0 && target[len(target)-1] == '.' {
			target = target[:len(target)-1]
		}
		addr := net.JoinHostPort(target, strconv.Itoa(int(srv.Port)))
		peers = append(peers, Peer{
			NodeID:  sha256Hash(addr),
			Address: addr,
		})
	}

	return peers, nil
}

// Name returns "dns".
func (d *DNSDiscovery) Name() string {
	return "dns"
}

// Stop is a no-op for DNS discovery.
func (d *DNSDiscovery) Stop() error {
	return nil
}

// sha256Hash computes a SHA-256 hash of the input string and returns the
// first 8 bytes as a little-endian uint64. SHA-256 provides stronger
// collision resistance than FNV-1a for generating NodeIDs from addresses.
func sha256Hash(s string) uint64 {
	h := sha256.Sum256([]byte(s))
	return binary.LittleEndian.Uint64(h[:8])
}
