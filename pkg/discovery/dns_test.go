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
	"errors"
	"net"
	"testing"
)

func mockLookupSRV(records []*net.SRV, err error) LookupSRVFunc {
	return func(service, proto, name string) (string, []*net.SRV, error) {
		return "", records, err
	}
}

func TestDNSDiscovery_Discover(t *testing.T) {
	records := []*net.SRV{
		{Target: "node1.example.com.", Port: 5000, Priority: 1, Weight: 1},
		{Target: "node2.example.com.", Port: 5001, Priority: 1, Weight: 1},
		{Target: "node3.example.com.", Port: 5002, Priority: 1, Weight: 1},
	}

	dns := NewDNSDiscovery(DNSConfig{
		Service:  "_raft",
		Proto:    "_udp",
		Domain:   "example.com",
		LookupFn: mockLookupSRV(records, nil),
	})

	peers, err := dns.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d", len(peers))
	}

	// Verify addresses have trailing dot stripped.
	for _, p := range peers {
		if p.Address == "" {
			t.Error("peer address should not be empty")
		}
		if p.NodeID == 0 {
			t.Error("peer NodeID should not be zero")
		}
	}

	// Verify first peer address format.
	if peers[0].Address != "node1.example.com:5000" {
		t.Errorf("peer[0].Address = %q, want %q", peers[0].Address, "node1.example.com:5000")
	}
}

func TestDNSDiscovery_LookupError(t *testing.T) {
	lookupErr := errors.New("dns: no such host")
	dns := NewDNSDiscovery(DNSConfig{
		Service:  "_raft",
		Proto:    "_udp",
		Domain:   "nonexistent.example.com",
		LookupFn: mockLookupSRV(nil, lookupErr),
	})

	_, err := dns.Discover(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var de *Error
	if !errors.As(err, &de) {
		t.Errorf("expected Error, got %T: %v", err, err)
	}
	if !errors.Is(err, lookupErr) {
		t.Errorf("expected wrapped lookup error, got: %v", err)
	}
}

func TestDNSDiscovery_NoRecords(t *testing.T) {
	dns := NewDNSDiscovery(DNSConfig{
		Service:  "_raft",
		Proto:    "_udp",
		Domain:   "empty.example.com",
		LookupFn: mockLookupSRV([]*net.SRV{}, nil),
	})

	_, err := dns.Discover(context.Background())
	if err == nil {
		t.Fatal("expected NoPeersError, got nil")
	}
	var npe *NoPeersError
	if !errors.As(err, &npe) {
		t.Errorf("expected NoPeersError, got %T: %v", err, err)
	}
}

func TestDNSDiscovery_TrailingDotStripped(t *testing.T) {
	records := []*net.SRV{
		{Target: "host.example.com.", Port: 8080, Priority: 1, Weight: 1},
	}
	dns := NewDNSDiscovery(DNSConfig{
		Service:  "_raft",
		Proto:    "_udp",
		Domain:   "example.com",
		LookupFn: mockLookupSRV(records, nil),
	})

	peers, err := dns.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if peers[0].Address != "host.example.com:8080" {
		t.Errorf("Address = %q, want %q", peers[0].Address, "host.example.com:8080")
	}
}

func TestDNSDiscovery_NoTrailingDot(t *testing.T) {
	records := []*net.SRV{
		{Target: "host.example.com", Port: 8080, Priority: 1, Weight: 1},
	}
	dns := NewDNSDiscovery(DNSConfig{
		Service:  "_raft",
		Proto:    "_udp",
		Domain:   "example.com",
		LookupFn: mockLookupSRV(records, nil),
	})

	peers, err := dns.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if peers[0].Address != "host.example.com:8080" {
		t.Errorf("Address = %q, want %q", peers[0].Address, "host.example.com:8080")
	}
}

func TestDNSDiscovery_SHA256HashDeterministic(t *testing.T) {
	h1 := sha256Hash("host.example.com:8080")
	h2 := sha256Hash("host.example.com:8080")
	if h1 != h2 {
		t.Error("sha256Hash should be deterministic")
	}
}

func TestDNSDiscovery_SHA256HashUnique(t *testing.T) {
	h1 := sha256Hash("host1.example.com:8080")
	h2 := sha256Hash("host2.example.com:8080")
	if h1 == h2 {
		t.Error("different inputs should produce different hashes (with high probability)")
	}
}

func TestDNSDiscovery_UniqueNodeIDs(t *testing.T) {
	records := []*net.SRV{
		{Target: "node1.example.com.", Port: 5000, Priority: 1, Weight: 1},
		{Target: "node2.example.com.", Port: 5000, Priority: 1, Weight: 1},
	}
	dns := NewDNSDiscovery(DNSConfig{
		Service:  "_raft",
		Proto:    "_udp",
		Domain:   "example.com",
		LookupFn: mockLookupSRV(records, nil),
	})

	peers, err := dns.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if peers[0].NodeID == peers[1].NodeID {
		t.Error("different SRV records should produce different NodeIDs")
	}
}

func TestDNSDiscovery_Name(t *testing.T) {
	dns := NewDNSDiscovery(DNSConfig{})
	if name := dns.Name(); name != "dns" {
		t.Errorf("Name() = %q, want %q", name, "dns")
	}
}

func TestDNSDiscovery_Stop(t *testing.T) {
	dns := NewDNSDiscovery(DNSConfig{})
	if err := dns.Stop(); err != nil {
		t.Errorf("Stop() returned unexpected error: %v", err)
	}
}

func TestDNSDiscovery_ImplementsMethod(t *testing.T) {
	var _ Method = (*DNSDiscovery)(nil)
}

func TestDNSDiscovery_NilLookupFnUsesDefault(t *testing.T) {
	dns := NewDNSDiscovery(DNSConfig{
		Service: "_raft",
		Proto:   "_udp",
		Domain:  "example.com",
	})
	// The internal lookupFn should not be nil.
	if dns.lookupFn == nil {
		t.Error("lookupFn should default to net.LookupSRV, not nil")
	}
}
