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
	"testing"
)

func TestStaticDiscovery_Discover(t *testing.T) {
	peers := []Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
		{NodeID: 2, Address: "10.0.0.2:5000"},
		{NodeID: 3, Address: "10.0.0.3:5000"},
	}
	sd := NewStaticDiscovery(StaticConfig{Peers: peers})

	result, err := sd.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 peers, got %d", len(result))
	}
	for i, p := range result {
		if p.NodeID != peers[i].NodeID || p.Address != peers[i].Address {
			t.Errorf("peer[%d] = %+v, want %+v", i, p, peers[i])
		}
	}
}

func TestStaticDiscovery_EmptyConfig(t *testing.T) {
	sd := NewStaticDiscovery(StaticConfig{})

	result, err := sd.Discover(context.Background())
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
	var npe *NoPeersError
	if !errors.As(err, &npe) {
		t.Errorf("expected NoPeersError, got %T: %v", err, err)
	}
}

func TestStaticDiscovery_DeepCopy(t *testing.T) {
	peers := []Peer{
		{NodeID: 1, Address: "10.0.0.1:5000"},
	}
	sd := NewStaticDiscovery(StaticConfig{Peers: peers})

	// Mutate the original slice.
	peers[0].Address = "mutated"

	result, err := sd.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result[0].Address == "mutated" {
		t.Error("internal peers should not be affected by external mutation")
	}
	if result[0].Address != "10.0.0.1:5000" {
		t.Errorf("expected original address, got %s", result[0].Address)
	}
}

func TestStaticDiscovery_DiscoverReturnsCopy(t *testing.T) {
	sd := NewStaticDiscovery(StaticConfig{
		Peers: []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
	})

	result1, err := sd.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mutate result.
	result1[0].Address = "mutated"

	result2, err := sd.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result2[0].Address == "mutated" {
		t.Error("second Discover() should return an independent copy")
	}
}

func TestStaticDiscovery_Name(t *testing.T) {
	sd := NewStaticDiscovery(StaticConfig{})
	if name := sd.Name(); name != "static" {
		t.Errorf("Name() = %q, want %q", name, "static")
	}
}

func TestStaticDiscovery_Stop(t *testing.T) {
	sd := NewStaticDiscovery(StaticConfig{})
	if err := sd.Stop(); err != nil {
		t.Errorf("Stop() returned unexpected error: %v", err)
	}
}

func TestStaticDiscovery_ImplementsMethod(t *testing.T) {
	var _ Method = (*StaticDiscovery)(nil)
}

// BenchmarkStaticDiscoveryDiscover measures the performance of static peer
// discovery with 5 peers. Each call returns a fresh slice copy.
func BenchmarkStaticDiscoveryDiscover(b *testing.B) {
	b.ReportAllocs()
	sd := NewStaticDiscovery(StaticConfig{
		Peers: []Peer{
			{NodeID: 1, Address: "10.0.0.1:5000"},
			{NodeID: 2, Address: "10.0.0.2:5000"},
			{NodeID: 3, Address: "10.0.0.3:5000"},
			{NodeID: 4, Address: "10.0.0.4:5000"},
			{NodeID: 5, Address: "10.0.0.5:5000"},
		},
	})
	for b.Loop() {
		_, _ = sd.Discover(context.Background())
	}
}

func TestStaticConfig_SharedSecret_DeepCopy(t *testing.T) {
	secret := []byte("my-cluster-secret")
	cfg := StaticConfig{
		Peers:        []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
		SharedSecret: secret,
	}
	sd := NewStaticDiscovery(cfg)

	// Mutate the original secret.
	secret[0] = 'X'

	if sd.sharedSecret[0] == 'X' {
		t.Error("internal sharedSecret should not be affected by external mutation")
	}
	if sd.sharedSecret[0] != 'm' {
		t.Errorf("sharedSecret[0] = %q, want %q", sd.sharedSecret[0], byte('m'))
	}
}

func TestStaticConfig_SharedSecret_NilHandled(t *testing.T) {
	sd := NewStaticDiscovery(StaticConfig{
		Peers:        []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
		SharedSecret: nil,
	})

	if sd.sharedSecret != nil {
		t.Errorf("sharedSecret should be nil, got %v", sd.sharedSecret)
	}

	// Ensure Discover still works with nil secret.
	result, err := sd.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(result))
	}
}

func TestStaticConfig_SharedSecret_EmptySliceHandled(t *testing.T) {
	sd := NewStaticDiscovery(StaticConfig{
		Peers:        []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
		SharedSecret: []byte{},
	})

	if sd.sharedSecret != nil {
		t.Errorf("sharedSecret should be nil for empty slice, got %v", sd.sharedSecret)
	}
}

func TestStaticConfig_SharedSecret_Preserved(t *testing.T) {
	secret := []byte("production-secret-key-256bit!!!!!")
	sd := NewStaticDiscovery(StaticConfig{
		Peers:        []Peer{{NodeID: 1, Address: "10.0.0.1:5000"}},
		SharedSecret: secret,
	})

	if string(sd.sharedSecret) != string(secret) {
		t.Errorf("sharedSecret = %q, want %q", sd.sharedSecret, secret)
	}
}

func TestStaticDiscovery_SinglePeer(t *testing.T) {
	sd := NewStaticDiscovery(StaticConfig{
		Peers: []Peer{{NodeID: 42, Address: "localhost:9000"}},
	})
	result, err := sd.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(result))
	}
	if result[0].NodeID != 42 {
		t.Errorf("NodeID = %d, want 42", result[0].NodeID)
	}
	if result[0].Address != "localhost:9000" {
		t.Errorf("Address = %q, want %q", result[0].Address, "localhost:9000")
	}
}
