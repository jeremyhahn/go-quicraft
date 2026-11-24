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

package transport

import (
	"errors"
	"testing"
)

// TestSetDefaultsAppliesProductionValues verifies that SetDefaults fills
// zero-valued fields with production defaults.
func TestSetDefaultsAppliesProductionValues(t *testing.T) {
	cfg := Config{
		ListenAddress: ":0",
	}
	cfg.SetDefaults()

	if cfg.StreamPoolSize != defaultStreamPoolSize {
		t.Fatalf("StreamPoolSize: want %d, got %d", defaultStreamPoolSize, cfg.StreamPoolSize)
	}
	if cfg.MaxStreamPoolSize != defaultMaxStreamPoolSize {
		t.Fatalf("MaxStreamPoolSize: want %d, got %d", defaultMaxStreamPoolSize, cfg.MaxStreamPoolSize)
	}
	if cfg.MaxSnapshotReceiveRate != defaultMaxSnapshotReceiveRate {
		t.Fatalf("MaxSnapshotReceiveRate: want %d, got %d", defaultMaxSnapshotReceiveRate, cfg.MaxSnapshotReceiveRate)
	}
	if cfg.MaxConcurrentSnapshotRecv != defaultMaxConcurrentSnapRecv {
		t.Fatalf("MaxConcurrentSnapshotRecv: want %d, got %d", defaultMaxConcurrentSnapRecv, cfg.MaxConcurrentSnapshotRecv)
	}
	if cfg.MaxSnapshotReceiveMemory != defaultMaxSnapshotRecvMemory {
		t.Fatalf("MaxSnapshotReceiveMemory: want %d, got %d", defaultMaxSnapshotRecvMemory, cfg.MaxSnapshotReceiveMemory)
	}
	if cfg.SendBatchMaxSize != defaultSendBatchMaxSize {
		t.Fatalf("SendBatchMaxSize: want %d, got %d", defaultSendBatchMaxSize, cfg.SendBatchMaxSize)
	}
	if cfg.MaxDecompressedSize != MaxFrameSize {
		t.Fatalf("MaxDecompressedSize: want %d, got %d", MaxFrameSize, cfg.MaxDecompressedSize)
	}
	if cfg.MaxIncomingConnections != defaultMaxIncomingConnections {
		t.Fatalf("MaxIncomingConnections: want %d, got %d", defaultMaxIncomingConnections, cfg.MaxIncomingConnections)
	}
	if cfg.MaxConnectionsPerIP != defaultMaxConnectionsPerIP {
		t.Fatalf("MaxConnectionsPerIP: want %d, got %d", defaultMaxConnectionsPerIP, cfg.MaxConnectionsPerIP)
	}
	if cfg.MaxStreamsPerConnection != defaultMaxStreamsPerConnection {
		t.Fatalf("MaxStreamsPerConnection: want %d, got %d", defaultMaxStreamsPerConnection, cfg.MaxStreamsPerConnection)
	}
}

// TestSetDefaultsPreservesExplicitValues verifies that SetDefaults does
// not overwrite explicitly set values.
func TestSetDefaultsPreservesExplicitValues(t *testing.T) {
	cfg := Config{
		ListenAddress:             ":0",
		StreamPoolSize:            8,
		MaxStreamPoolSize:         128,
		MaxSnapshotReceiveRate:    100,
		MaxConcurrentSnapshotRecv: 2,
		MaxSnapshotReceiveMemory:  500,
		SendBatchMaxSize:          32 * 1024,
		MaxDecompressedSize:       1024,
		MaxIncomingConnections:    64,
		MaxConnectionsPerIP:       4,
		MaxStreamsPerConnection:   32,
	}
	cfg.SetDefaults()

	if cfg.StreamPoolSize != 8 {
		t.Fatalf("StreamPoolSize should be preserved: want 8, got %d", cfg.StreamPoolSize)
	}
	if cfg.MaxStreamPoolSize != 128 {
		t.Fatalf("MaxStreamPoolSize should be preserved: want 128, got %d", cfg.MaxStreamPoolSize)
	}
	if cfg.MaxSnapshotReceiveRate != 100 {
		t.Fatalf("MaxSnapshotReceiveRate should be preserved: want 100, got %d", cfg.MaxSnapshotReceiveRate)
	}
	if cfg.MaxConcurrentSnapshotRecv != 2 {
		t.Fatalf("MaxConcurrentSnapshotRecv should be preserved: want 2, got %d", cfg.MaxConcurrentSnapshotRecv)
	}
	if cfg.MaxSnapshotReceiveMemory != 500 {
		t.Fatalf("MaxSnapshotReceiveMemory should be preserved: want 500, got %d", cfg.MaxSnapshotReceiveMemory)
	}
	if cfg.SendBatchMaxSize != 32*1024 {
		t.Fatalf("SendBatchMaxSize should be preserved: want %d, got %d", 32*1024, cfg.SendBatchMaxSize)
	}
	if cfg.MaxDecompressedSize != 1024 {
		t.Fatalf("MaxDecompressedSize should be preserved: want 1024, got %d", cfg.MaxDecompressedSize)
	}
	if cfg.MaxIncomingConnections != 64 {
		t.Fatalf("MaxIncomingConnections should be preserved: want 64, got %d", cfg.MaxIncomingConnections)
	}
	if cfg.MaxConnectionsPerIP != 4 {
		t.Fatalf("MaxConnectionsPerIP should be preserved: want 4, got %d", cfg.MaxConnectionsPerIP)
	}
	if cfg.MaxStreamsPerConnection != 32 {
		t.Fatalf("MaxStreamsPerConnection should be preserved: want 32, got %d", cfg.MaxStreamsPerConnection)
	}
}

// TestValidateRejectsInvalidConfig verifies that Validate rejects each
// out-of-range field of an otherwise valid, defaulted Config with the
// sentinel that names it.
func TestValidateRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*Config)
		expectedError error
	}{
		{"pool size zero", func(c *Config) { c.StreamPoolSize, c.MaxStreamPoolSize = 0, 256 }, ErrInvalidStreamPoolSize},
		{"pool size exceeds max", func(c *Config) { c.StreamPoolSize, c.MaxStreamPoolSize = 300, 256 }, ErrInvalidStreamPoolSize},
		{"zero max pool size", func(c *Config) { c.StreamPoolSize, c.MaxStreamPoolSize = 1, 0 }, ErrInvalidMaxStreamPoolSize},
		{"negative snapshot receive rate", func(c *Config) { c.MaxSnapshotReceiveRate = -1 }, ErrInvalidMaxSnapshotReceiveRate},
		{"zero concurrent snapshot recv", func(c *Config) { c.MaxConcurrentSnapshotRecv = 0 }, ErrInvalidMaxConcurrentSnapshotRecv},
		{"negative snapshot memory", func(c *Config) { c.MaxSnapshotReceiveMemory = -1 }, ErrInvalidMaxSnapshotReceiveMemory},
		{"zero batch max size", func(c *Config) { c.SendBatchMaxSize = 0 }, ErrInvalidSendBatchMaxSize},
		{"zero decompressed size", func(c *Config) { c.MaxDecompressedSize = 0 }, ErrInvalidMaxDecompressedSize},
		{"decompressed size exceeds max frame size", func(c *Config) { c.MaxDecompressedSize = MaxFrameSize + 1 }, ErrInvalidMaxDecompressedSize},
		// Below the frame size is the dangerous direction and the reason the
		// value is pinned. MaxFrameSize is a compile-time constant and is what
		// the SEND path admits against (messageFrameError/sendableGroup), so a
		// receiver configured lower rejects frames every peer in the cluster
		// considers valid. The sender treats that as a frame-level failure: it
		// drops the batch and KEEPS the connection (deliberately, so a
		// wire-version mismatch cannot become a dial storm), which leaves this
		// replica unable to replicate with nothing but a 10s-rate-limited
		// warning and a "peer_rejected" drop counter to show for it. Nothing is
		// bought in exchange — a peer that wants this node to buffer MaxFrameSize
		// bytes can just send an uncompressed frame of that size — so a lower
		// decompression ceiling is pure downside and must fail at startup.
		{"decompressed size below max frame size", func(c *Config) { c.MaxDecompressedSize = MaxFrameSize - 1 }, ErrInvalidMaxDecompressedSize},
		{"zero MaxIncomingConnections", func(c *Config) { c.MaxIncomingConnections = 0 }, ErrInvalidMaxIncomingConnections},
		{"negative MaxIncomingConnections", func(c *Config) { c.MaxIncomingConnections = -1 }, ErrInvalidMaxIncomingConnections},
		{"zero MaxConnectionsPerIP", func(c *Config) { c.MaxConnectionsPerIP = 0 }, ErrInvalidMaxConnectionsPerIP},
		{"negative MaxConnectionsPerIP", func(c *Config) { c.MaxConnectionsPerIP = -1 }, ErrInvalidMaxConnectionsPerIP},
		{"zero MaxStreamsPerConnection", func(c *Config) { c.MaxStreamsPerConnection = 0 }, ErrInvalidMaxStreamsPerConnection},
		{"negative MaxStreamsPerConnection", func(c *Config) { c.MaxStreamsPerConnection = -1 }, ErrInvalidMaxStreamsPerConnection},
		{"mTLS missing CA cert", func(c *Config) { c.MTLSConfig = &MTLSConfig{Cert: []byte("cert"), Key: []byte("key")} }, ErrIncompleteMTLSConfig},
		{"mTLS missing cert", func(c *Config) { c.MTLSConfig = &MTLSConfig{CACert: []byte("ca"), Key: []byte("key")} }, ErrIncompleteMTLSConfig},
		{"mTLS missing key", func(c *Config) { c.MTLSConfig = &MTLSConfig{CACert: []byte("ca"), Cert: []byte("cert")} }, ErrIncompleteMTLSConfig},
		{"mTLS all empty", func(c *Config) { c.MTLSConfig = &MTLSConfig{} }, ErrIncompleteMTLSConfig},
		// The 0-RTT check precedes the general MTLSConfig requirement for
		// defense-in-depth: even if the mTLS mandate were relaxed in the
		// future, 0-RTT would still require it.
		{"0-RTT without mTLS", func(c *Config) { c.Enable0RTT, c.MTLSConfig = true, nil }, ErrInsecure0RTT},
		{"zero MaxMessageReceiveMemory", func(c *Config) { c.MaxMessageReceiveMemory = 0 }, ErrInvalidMaxMessageReceiveMemory},
		{"negative MaxMessageReceiveMemory", func(c *Config) { c.MaxMessageReceiveMemory = -1 }, ErrInvalidMaxMessageReceiveMemory},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{ListenAddress: ":0", MTLSConfig: testMTLSConfig(t)}
			cfg.SetDefaults()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if !errors.Is(err, tc.expectedError) {
				t.Fatalf("expected %v, got %v", tc.expectedError, err)
			}
		})
	}
}

// TestValidateAcceptsBoundaryValues verifies that the minimum valid limits,
// and 0-RTT with complete mTLS material, pass validation.
func TestValidateAcceptsBoundaryValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"minimum connection limits", func(c *Config) { c.MaxIncomingConnections, c.MaxConnectionsPerIP = 1, 1 }},
		{"minimum MaxStreamsPerConnection", func(c *Config) { c.MaxStreamsPerConnection = 1 }},
		{"0-RTT with mTLS", func(c *Config) {
			c.Enable0RTT = true
			c.MTLSConfig = &MTLSConfig{CACert: []byte("ca"), Cert: []byte("cert"), Key: []byte("key")}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{ListenAddress: ":0", MTLSConfig: testMTLSConfig(t)}
			cfg.SetDefaults()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("%s should be valid, got: %v", tc.name, err)
			}
		})
	}
}
