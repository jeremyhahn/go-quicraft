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

// TestValidateAcceptsValidConfig verifies that a valid configuration passes.
func TestValidateAcceptsValidConfig(t *testing.T) {
	cfg := Config{
		ListenAddress: ":0",
		MTLSConfig:    testMTLSConfig(t),
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

// TestValidateRejectsEmptyListenAddress verifies missing listen address.
func TestValidateRejectsEmptyListenAddress(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	err := cfg.Validate()
	if !errors.Is(err, ErrEmptyListenAddress) {
		t.Fatalf("expected ErrEmptyListenAddress, got %v", err)
	}
}

// TestValidateRejectsInvalidStreamPoolSize verifies out-of-range pool sizes.
func TestValidateRejectsInvalidStreamPoolSize(t *testing.T) {
	tests := []struct {
		name          string
		poolSize      int
		maxPoolSize   int
		expectedError error
	}{
		{
			name:          "pool size zero",
			poolSize:      0,
			maxPoolSize:   256,
			expectedError: ErrInvalidStreamPoolSize,
		},
		{
			name:          "pool size exceeds max",
			poolSize:      300,
			maxPoolSize:   256,
			expectedError: ErrInvalidStreamPoolSize,
		},
		{
			name:          "negative max pool size",
			poolSize:      1,
			maxPoolSize:   0,
			expectedError: ErrInvalidMaxStreamPoolSize,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				ListenAddress: ":0",
				MTLSConfig:    testMTLSConfig(t),
			}
			cfg.SetDefaults()
			cfg.StreamPoolSize = tc.poolSize
			cfg.MaxStreamPoolSize = tc.maxPoolSize
			err := cfg.Validate()
			if !errors.Is(err, tc.expectedError) {
				t.Fatalf("expected %v, got %v", tc.expectedError, err)
			}
		})
	}
}

// TestValidateRejectsInvalidSnapshotConfig verifies snapshot config validation.
func TestValidateRejectsInvalidSnapshotConfig(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*Config)
		expectedError error
	}{
		{
			name: "negative snapshot receive rate",
			mutate: func(c *Config) {
				c.MaxSnapshotReceiveRate = -1
			},
			expectedError: ErrInvalidMaxSnapshotReceiveRate,
		},
		{
			name: "zero concurrent snapshot recv",
			mutate: func(c *Config) {
				c.MaxConcurrentSnapshotRecv = 0
			},
			expectedError: ErrInvalidMaxConcurrentSnapshotRecv,
		},
		{
			name: "negative snapshot memory",
			mutate: func(c *Config) {
				c.MaxSnapshotReceiveMemory = -1
			},
			expectedError: ErrInvalidMaxSnapshotReceiveMemory,
		},
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

// TestValidateRejectsInvalidBatchConfig verifies batch config validation.
func TestValidateRejectsInvalidBatchConfig(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*Config)
		expectedError error
	}{
		{
			name: "zero batch max size",
			mutate: func(c *Config) {
				c.SendBatchMaxSize = 0
			},
			expectedError: ErrInvalidSendBatchMaxSize,
		},
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

// TestValidateRejectsInvalidDecompressedSize verifies decompressed size bounds.
func TestValidateRejectsInvalidDecompressedSize(t *testing.T) {
	tests := []struct {
		name string
		size uint32
	}{
		{"zero", 0},
		{"exceeds max frame size", MaxFrameSize + 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{ListenAddress: ":0", MTLSConfig: testMTLSConfig(t)}
			cfg.SetDefaults()
			cfg.MaxDecompressedSize = tc.size
			err := cfg.Validate()
			if !errors.Is(err, ErrInvalidMaxDecompressedSize) {
				t.Fatalf("expected ErrInvalidMaxDecompressedSize, got %v", err)
			}
		})
	}
}

// TestValidateRejectsIncompleteMTLSConfig verifies that partial mTLS
// config is rejected.
func TestValidateRejectsIncompleteMTLSConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *MTLSConfig
	}{
		{
			name: "missing CA cert",
			cfg:  &MTLSConfig{Cert: []byte("cert"), Key: []byte("key")},
		},
		{
			name: "missing cert",
			cfg:  &MTLSConfig{CACert: []byte("ca"), Key: []byte("key")},
		},
		{
			name: "missing key",
			cfg:  &MTLSConfig{CACert: []byte("ca"), Cert: []byte("cert")},
		},
		{
			name: "all empty",
			cfg:  &MTLSConfig{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				ListenAddress: ":0",
				MTLSConfig:    tc.cfg,
			}
			cfg.SetDefaults()
			err := cfg.Validate()
			if !errors.Is(err, ErrIncompleteMTLSConfig) {
				t.Fatalf("expected ErrIncompleteMTLSConfig, got %v", err)
			}
		})
	}
}

// TestValidateRejectsNilMTLSConfig verifies that nil MTLSConfig is rejected
// since mTLS is now mandatory.
func TestValidateRejectsNilMTLSConfig(t *testing.T) {
	cfg := Config{
		ListenAddress: ":0",
		MTLSConfig:    nil,
	}
	cfg.SetDefaults()
	err := cfg.Validate()
	if !errors.Is(err, ErrMTLSConfigRequired) {
		t.Fatalf("expected ErrMTLSConfigRequired, got %v", err)
	}
}

// TestValidateRejectsInvalidConnectionLimits verifies that connection limit
// fields are validated.
func TestValidateRejectsInvalidConnectionLimits(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*Config)
		expectedError error
	}{
		{
			name: "zero MaxIncomingConnections",
			mutate: func(c *Config) {
				c.MaxIncomingConnections = 0
			},
			expectedError: ErrInvalidMaxIncomingConnections,
		},
		{
			name: "negative MaxIncomingConnections",
			mutate: func(c *Config) {
				c.MaxIncomingConnections = -1
			},
			expectedError: ErrInvalidMaxIncomingConnections,
		},
		{
			name: "zero MaxConnectionsPerIP",
			mutate: func(c *Config) {
				c.MaxConnectionsPerIP = 0
			},
			expectedError: ErrInvalidMaxConnectionsPerIP,
		},
		{
			name: "negative MaxConnectionsPerIP",
			mutate: func(c *Config) {
				c.MaxConnectionsPerIP = -1
			},
			expectedError: ErrInvalidMaxConnectionsPerIP,
		},
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

// TestValidateAcceptsMinimumConnectionLimits verifies that minimum valid
// connection limit values pass validation.
func TestValidateAcceptsMinimumConnectionLimits(t *testing.T) {
	cfg := Config{ListenAddress: ":0", MTLSConfig: testMTLSConfig(t)}
	cfg.SetDefaults()
	cfg.MaxIncomingConnections = 1
	cfg.MaxConnectionsPerIP = 1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("minimum connection limits should be valid, got: %v", err)
	}
}

// TestValidateRejects0RTTWithoutMTLS verifies that enabling 0-RTT without
// mTLS returns ErrInsecure0RTT. The 0-RTT check precedes the general
// MTLSConfig requirement for defense-in-depth: even if the mTLS mandate
// were relaxed in the future, 0-RTT would still require it.
func TestValidateRejects0RTTWithoutMTLS(t *testing.T) {
	cfg := Config{
		ListenAddress: ":0",
		Enable0RTT:    true,
		MTLSConfig:    nil,
	}
	cfg.SetDefaults()
	err := cfg.Validate()
	if !errors.Is(err, ErrInsecure0RTT) {
		t.Fatalf("expected ErrInsecure0RTT, got %v", err)
	}
}

// TestValidateAccepts0RTTWithMTLS verifies that enabling 0-RTT with a
// complete mTLS configuration passes validation.
func TestValidateAccepts0RTTWithMTLS(t *testing.T) {
	cfg := Config{
		ListenAddress: ":0",
		Enable0RTT:    true,
		MTLSConfig: &MTLSConfig{
			CACert: []byte("ca"),
			Cert:   []byte("cert"),
			Key:    []byte("key"),
		},
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("0-RTT with mTLS should be valid, got: %v", err)
	}
}

// TestValidateRejectsNo0RTTWithoutMTLS verifies that even with 0-RTT
// disabled, nil MTLSConfig is rejected since mTLS is now mandatory.
func TestValidateRejectsNo0RTTWithoutMTLS(t *testing.T) {
	cfg := Config{
		ListenAddress: ":0",
		Enable0RTT:    false,
		MTLSConfig:    nil,
	}
	cfg.SetDefaults()
	err := cfg.Validate()
	if !errors.Is(err, ErrMTLSConfigRequired) {
		t.Fatalf("expected ErrMTLSConfigRequired, got %v", err)
	}
}

// TestDefaultConstantValues verifies the default constant values are correct.
func TestDefaultConstantValues(t *testing.T) {
	if defaultStreamPoolSize != 16 {
		t.Fatalf("defaultStreamPoolSize: want 16, got %d", defaultStreamPoolSize)
	}
	if defaultMaxStreamPoolSize != 256 {
		t.Fatalf("defaultMaxStreamPoolSize: want 256, got %d", defaultMaxStreamPoolSize)
	}
	if defaultMaxSnapshotReceiveRate != 256*1024*1024 {
		t.Fatalf("defaultMaxSnapshotReceiveRate: want %d, got %d", 256*1024*1024, defaultMaxSnapshotReceiveRate)
	}
	if defaultMaxConcurrentSnapRecv != 4 {
		t.Fatalf("defaultMaxConcurrentSnapRecv: want 4, got %d", defaultMaxConcurrentSnapRecv)
	}
	if defaultMaxSnapshotRecvMemory != 1024*1024*1024 {
		t.Fatalf("defaultMaxSnapshotRecvMemory: want %d, got %d", 1024*1024*1024, defaultMaxSnapshotRecvMemory)
	}
	if defaultSendBatchMaxSize != 64*1024 {
		t.Fatalf("defaultSendBatchMaxSize: want %d, got %d", 64*1024, defaultSendBatchMaxSize)
	}
	if defaultMaxIncomingConnections != 256 {
		t.Fatalf("defaultMaxIncomingConnections: want 256, got %d", defaultMaxIncomingConnections)
	}
	if defaultMaxConnectionsPerIP != 16 {
		t.Fatalf("defaultMaxConnectionsPerIP: want 16, got %d", defaultMaxConnectionsPerIP)
	}
	if defaultMaxStreamsPerConnection != 256 {
		t.Fatalf("defaultMaxStreamsPerConnection: want 256, got %d", defaultMaxStreamsPerConnection)
	}
}

// ---------------------------------------------------------------------------
// S1: 0-RTT replay defense-in-depth
// ---------------------------------------------------------------------------

// TestValidate0RTTWithoutMTLSReturnsInsecure0RTT verifies that enabling 0-RTT
// without mTLS returns ErrInsecure0RTT (the specific 0-RTT error), not the
// general ErrMTLSConfigRequired. This is defense-in-depth: even if the mTLS
// mandate were relaxed, 0-RTT would still require mutual authentication.
func TestValidate0RTTWithoutMTLSReturnsInsecure0RTT(t *testing.T) {
	cfg := Config{
		ListenAddress: ":0",
		Enable0RTT:    true,
		MTLSConfig:    nil,
	}
	cfg.SetDefaults()
	err := cfg.Validate()
	if !errors.Is(err, ErrInsecure0RTT) {
		t.Fatalf("expected ErrInsecure0RTT, got %v", err)
	}
}

// TestValidate0RTTWithMTLSPasses verifies that enabling 0-RTT with a
// complete mTLS configuration passes the 0-RTT validation check.
func TestValidate0RTTWithMTLSPasses(t *testing.T) {
	cfg := Config{
		ListenAddress: ":0",
		Enable0RTT:    true,
		MTLSConfig:    testMTLSConfig(t),
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("0-RTT with mTLS should be valid, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// S4: MaxStreamsPerConnection config + validation
// ---------------------------------------------------------------------------

// TestConfigMaxStreamsPerConnectionDefault verifies that SetDefaults
// applies the default MaxStreamsPerConnection value.
func TestConfigMaxStreamsPerConnectionDefault(t *testing.T) {
	cfg := Config{ListenAddress: ":0"}
	cfg.SetDefaults()
	if cfg.MaxStreamsPerConnection != defaultMaxStreamsPerConnection {
		t.Fatalf("MaxStreamsPerConnection default: want %d, got %d",
			defaultMaxStreamsPerConnection, cfg.MaxStreamsPerConnection)
	}
}

// TestConfigMaxStreamsPerConnectionPreserved verifies that SetDefaults
// does not overwrite an explicitly set MaxStreamsPerConnection value.
func TestConfigMaxStreamsPerConnectionPreserved(t *testing.T) {
	cfg := Config{ListenAddress: ":0", MaxStreamsPerConnection: 64}
	cfg.SetDefaults()
	if cfg.MaxStreamsPerConnection != 64 {
		t.Fatalf("MaxStreamsPerConnection should be preserved: want 64, got %d",
			cfg.MaxStreamsPerConnection)
	}
}

// TestConfigMaxStreamsPerConnectionValidation verifies that
// MaxStreamsPerConnection < 1 returns ErrInvalidMaxStreamsPerConnection.
func TestConfigMaxStreamsPerConnectionValidation(t *testing.T) {
	tests := []struct {
		name  string
		value int
	}{
		{"zero", 0},
		{"negative", -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{ListenAddress: ":0", MTLSConfig: testMTLSConfig(t)}
			cfg.SetDefaults()
			cfg.MaxStreamsPerConnection = tc.value
			err := cfg.Validate()
			if !errors.Is(err, ErrInvalidMaxStreamsPerConnection) {
				t.Fatalf("expected ErrInvalidMaxStreamsPerConnection, got %v", err)
			}
		})
	}
}

// TestConfigMaxStreamsPerConnectionMinimumValid verifies that
// MaxStreamsPerConnection = 1 passes validation.
func TestConfigMaxStreamsPerConnectionMinimumValid(t *testing.T) {
	cfg := Config{ListenAddress: ":0", MTLSConfig: testMTLSConfig(t)}
	cfg.SetDefaults()
	cfg.MaxStreamsPerConnection = 1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("MaxStreamsPerConnection=1 should be valid, got: %v", err)
	}
}
