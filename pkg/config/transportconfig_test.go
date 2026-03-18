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

package config

import (
	"errors"
	"testing"
)

// validTransportConfig returns a TransportConfig with defaults applied and
// transport disabled so that numeric field validation tests do not require
// mTLS certificate material.
func validTransportConfig() TransportConfig {
	var tc TransportConfig
	tc.SetDefaults()
	tc.TransportDisabled = true
	return tc
}

// validTransportConfigWithMTLS returns a TransportConfig with defaults
// applied and a non-nil MTLSConfig for tests that exercise mTLS validation.
func validTransportConfigWithMTLS() TransportConfig {
	var tc TransportConfig
	tc.SetDefaults()
	tc.MTLSConfig = &MTLSConfig{
		CACert: []byte("ca"),
		Cert:   []byte("cert"),
		Key:    []byte("key"),
	}
	return tc
}

func TestTransportConfig_SetDefaults(t *testing.T) {
	t.Run("fills all zero fields", func(t *testing.T) {
		var tc TransportConfig
		tc.SetDefaults()

		if tc.StreamPoolSize != DefaultStreamPoolSize {
			t.Errorf("StreamPoolSize = %d, want %d", tc.StreamPoolSize, DefaultStreamPoolSize)
		}
		if tc.MaxStreamPoolSize != DefaultMaxStreamPoolSize {
			t.Errorf("MaxStreamPoolSize = %d, want %d", tc.MaxStreamPoolSize, DefaultMaxStreamPoolSize)
		}
		if tc.MaxSnapshotReceiveRate != DefaultMaxSnapshotReceiveRate {
			t.Errorf("MaxSnapshotReceiveRate = %d, want %d",
				tc.MaxSnapshotReceiveRate, DefaultMaxSnapshotReceiveRate)
		}
		if tc.MaxConcurrentSnapshotReceives != DefaultMaxConcurrentSnapshotRecv {
			t.Errorf("MaxConcurrentSnapshotReceives = %d, want %d",
				tc.MaxConcurrentSnapshotReceives, DefaultMaxConcurrentSnapshotRecv)
		}
		if tc.MaxSnapshotReceiveMemory != DefaultMaxSnapshotReceiveMemory {
			t.Errorf("MaxSnapshotReceiveMemory = %d, want %d",
				tc.MaxSnapshotReceiveMemory, DefaultMaxSnapshotReceiveMemory)
		}
		if tc.MaxDecompressedSize != DefaultMaxDecompressedSize {
			t.Errorf("MaxDecompressedSize = %d, want %d",
				tc.MaxDecompressedSize, DefaultMaxDecompressedSize)
		}
		if tc.MaxConnectionsPerIP != DefaultMaxConnectionsPerIP {
			t.Errorf("MaxConnectionsPerIP = %d, want %d",
				tc.MaxConnectionsPerIP, DefaultMaxConnectionsPerIP)
		}
		if tc.MaxIncomingConnections != DefaultMaxIncomingConnections {
			t.Errorf("MaxIncomingConnections = %d, want %d",
				tc.MaxIncomingConnections, DefaultMaxIncomingConnections)
		}
		if tc.SendBatchMaxSize != DefaultSendBatchMaxSize {
			t.Errorf("SendBatchMaxSize = %d, want %d", tc.SendBatchMaxSize, DefaultSendBatchMaxSize)
		}
		if tc.Enable0RTT {
			t.Error("Enable0RTT should default to false")
		}
	})

	t.Run("preserves non-zero fields", func(t *testing.T) {
		tc := TransportConfig{
			StreamPoolSize:                32,
			MaxStreamPoolSize:             512,
			MaxSnapshotReceiveRate:        100 * 1024 * 1024,
			MaxConcurrentSnapshotReceives: 8,
			MaxSnapshotReceiveMemory:      2 * 1024 * 1024 * 1024,
			MaxDecompressedSize:           128 * 1024 * 1024,
			MaxConnectionsPerIP:           32,
			MaxIncomingConnections:        512,
			SendBatchMaxSize:              128 * 1024,
		}
		tc.SetDefaults()

		if tc.StreamPoolSize != 32 {
			t.Errorf("StreamPoolSize = %d, want 32", tc.StreamPoolSize)
		}
		if tc.MaxStreamPoolSize != 512 {
			t.Errorf("MaxStreamPoolSize = %d, want 512", tc.MaxStreamPoolSize)
		}
		if tc.MaxSnapshotReceiveRate != 100*1024*1024 {
			t.Errorf("MaxSnapshotReceiveRate = %d, want %d",
				tc.MaxSnapshotReceiveRate, 100*1024*1024)
		}
		if tc.MaxConcurrentSnapshotReceives != 8 {
			t.Errorf("MaxConcurrentSnapshotReceives = %d, want 8",
				tc.MaxConcurrentSnapshotReceives)
		}
		if tc.MaxSnapshotReceiveMemory != 2*1024*1024*1024 {
			t.Errorf("MaxSnapshotReceiveMemory = %d, want %d",
				tc.MaxSnapshotReceiveMemory, 2*1024*1024*1024)
		}
		if tc.MaxDecompressedSize != 128*1024*1024 {
			t.Errorf("MaxDecompressedSize = %d, want %d",
				tc.MaxDecompressedSize, 128*1024*1024)
		}
		if tc.MaxConnectionsPerIP != 32 {
			t.Errorf("MaxConnectionsPerIP = %d, want 32", tc.MaxConnectionsPerIP)
		}
		if tc.MaxIncomingConnections != 512 {
			t.Errorf("MaxIncomingConnections = %d, want 512", tc.MaxIncomingConnections)
		}
		if tc.SendBatchMaxSize != 128*1024 {
			t.Errorf("SendBatchMaxSize = %d, want %d", tc.SendBatchMaxSize, 128*1024)
		}
	})
}

func TestTransportConfig_Validate_Valid(t *testing.T) {
	t.Run("valid defaults", func(t *testing.T) {
		tc := validTransportConfig()
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("valid with 0RTT enabled", func(t *testing.T) {
		tc := validTransportConfigWithMTLS()
		tc.Enable0RTT = true
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

}

func TestTransportConfig_Validate_StreamPoolSize(t *testing.T) {
	t.Run("zero StreamPoolSize", func(t *testing.T) {
		tc := validTransportConfig()
		tc.StreamPoolSize = 0
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.StreamPoolSize")
	})

	t.Run("negative StreamPoolSize", func(t *testing.T) {
		tc := validTransportConfig()
		tc.StreamPoolSize = -1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.StreamPoolSize")
	})

	t.Run("minimum valid", func(t *testing.T) {
		tc := validTransportConfig()
		tc.StreamPoolSize = 1
		tc.MaxStreamPoolSize = 1
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestTransportConfig_Validate_MaxStreamPoolSize(t *testing.T) {
	t.Run("less than StreamPoolSize", func(t *testing.T) {
		tc := validTransportConfig()
		tc.StreamPoolSize = 32
		tc.MaxStreamPoolSize = 16
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxStreamPoolSize")
	})

	t.Run("equal to StreamPoolSize", func(t *testing.T) {
		tc := validTransportConfig()
		tc.StreamPoolSize = 32
		tc.MaxStreamPoolSize = 32
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestTransportConfig_Validate_SnapshotReceiveRate(t *testing.T) {
	t.Run("negative rate", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxSnapshotReceiveRate = -1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxSnapshotReceiveRate")
	})

	t.Run("zero rate is valid (unlimited)", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxSnapshotReceiveRate = 0
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestTransportConfig_Validate_ConcurrentSnapshotReceives(t *testing.T) {
	t.Run("zero concurrent receives", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxConcurrentSnapshotReceives = 0
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxConcurrentSnapshotReceives")
	})

	t.Run("negative concurrent receives", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxConcurrentSnapshotReceives = -1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxConcurrentSnapshotReceives")
	})

	t.Run("minimum valid", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxConcurrentSnapshotReceives = 1
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestTransportConfig_Validate_SnapshotReceiveMemory(t *testing.T) {
	t.Run("negative memory", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxSnapshotReceiveMemory = -1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxSnapshotReceiveMemory")
	})

	t.Run("zero memory is valid (unlimited)", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxSnapshotReceiveMemory = 0
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestTransportConfig_Validate_MaxDecompressedSize(t *testing.T) {
	t.Run("zero MaxDecompressedSize", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxDecompressedSize = 0
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxDecompressedSize")
	})

	t.Run("minimum valid", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxDecompressedSize = 1
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestTransportConfig_Validate_MaxConnectionsPerIP(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxConnectionsPerIP = 0
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxConnectionsPerIP")
	})

	t.Run("negative", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxConnectionsPerIP = -1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxConnectionsPerIP")
	})

	t.Run("minimum valid", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxConnectionsPerIP = 1
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestTransportConfig_Validate_MaxIncomingConnections(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxIncomingConnections = 0
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxIncomingConnections")
	})

	t.Run("negative", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxIncomingConnections = -1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxIncomingConnections")
	})

	t.Run("minimum valid", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxIncomingConnections = 1
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestTransportConfig_Validate_SendBatchMaxSize(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		tc := validTransportConfig()
		tc.SendBatchMaxSize = 0
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.SendBatchMaxSize")
	})

	t.Run("negative", func(t *testing.T) {
		tc := validTransportConfig()
		tc.SendBatchMaxSize = -1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.SendBatchMaxSize")
	})

	t.Run("minimum valid", func(t *testing.T) {
		tc := validTransportConfig()
		tc.SendBatchMaxSize = 1
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestMTLSConfigFieldsPassthrough(t *testing.T) {
	caCert := []byte("-----BEGIN CERTIFICATE-----\nca-cert-data\n-----END CERTIFICATE-----")
	cert := []byte("-----BEGIN CERTIFICATE-----\nnode-cert-data\n-----END CERTIFICATE-----")
	key := []byte("-----BEGIN PRIVATE KEY-----\nnode-key-data\n-----END PRIVATE KEY-----")

	tc := validTransportConfig()
	tc.MTLSConfig = &MTLSConfig{
		CACert: caCert,
		Cert:   cert,
		Key:    key,
	}

	t.Run("CACert stored correctly", func(t *testing.T) {
		if string(tc.MTLSConfig.CACert) != string(caCert) {
			t.Errorf("CACert = %q, want %q", tc.MTLSConfig.CACert, caCert)
		}
	})

	t.Run("Cert stored correctly", func(t *testing.T) {
		if string(tc.MTLSConfig.Cert) != string(cert) {
			t.Errorf("Cert = %q, want %q", tc.MTLSConfig.Cert, cert)
		}
	})

	t.Run("Key stored correctly", func(t *testing.T) {
		if string(tc.MTLSConfig.Key) != string(key) {
			t.Errorf("Key = %q, want %q", tc.MTLSConfig.Key, key)
		}
	})
}

func TestValidateNilMTLSConfig(t *testing.T) {
	t.Run("nil MTLSConfig fails when transport enabled", func(t *testing.T) {
		tc := validTransportConfigWithMTLS()
		tc.MTLSConfig = nil // remove mTLS, keep transport enabled
		err := tc.Validate()
		if !errors.Is(err, ErrMTLSConfigRequired) {
			t.Errorf("Validate() = %v, want MTLSConfigRequiredError", err)
		}
	})

	t.Run("nil MTLSConfig passes when transport disabled", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MTLSConfig = nil
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil for disabled transport", err)
		}
	})

	t.Run("defaults with transport enabled require MTLSConfig", func(t *testing.T) {
		var tc TransportConfig
		tc.SetDefaults()
		// TransportDisabled defaults to false, MTLSConfig defaults to nil.
		err := tc.Validate()
		if !errors.Is(err, ErrMTLSConfigRequired) {
			t.Errorf("Validate() = %v, want MTLSConfigRequiredError", err)
		}
	})

	t.Run("non-nil MTLSConfig passes when transport enabled", func(t *testing.T) {
		tc := validTransportConfigWithMTLS()
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})
}

func TestMTLSConfigWithSetDefaults(t *testing.T) {
	t.Run("SetDefaults preserves non-nil MTLSConfig", func(t *testing.T) {
		mtls := &MTLSConfig{
			CACert: []byte("ca"),
			Cert:   []byte("cert"),
			Key:    []byte("key"),
		}
		tc := TransportConfig{
			MTLSConfig: mtls,
		}
		tc.SetDefaults()

		if tc.MTLSConfig != mtls {
			t.Error("SetDefaults overwrote non-nil MTLSConfig")
		}
		if string(tc.MTLSConfig.CACert) != "ca" {
			t.Errorf("CACert = %q, want %q", tc.MTLSConfig.CACert, "ca")
		}
		if string(tc.MTLSConfig.Cert) != "cert" {
			t.Errorf("Cert = %q, want %q", tc.MTLSConfig.Cert, "cert")
		}
		if string(tc.MTLSConfig.Key) != "key" {
			t.Errorf("Key = %q, want %q", tc.MTLSConfig.Key, "key")
		}
	})

	t.Run("SetDefaults leaves nil MTLSConfig as nil", func(t *testing.T) {
		var tc TransportConfig
		tc.SetDefaults()
		if tc.MTLSConfig != nil {
			t.Errorf("MTLSConfig = %v, want nil", tc.MTLSConfig)
		}
	})
}

// assertTransportValidationError checks that err is a
// *ValidationError with the expected field name.
func assertTransportValidationError(t *testing.T, err error, expectedField string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error for field %q, got nil", expectedField)
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if ve.Field != expectedField {
		t.Errorf("error field = %q, want %q", ve.Field, expectedField)
	}
}
