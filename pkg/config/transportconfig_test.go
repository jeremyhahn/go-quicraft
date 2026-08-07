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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"
)

// testMTLSMaterial generates a valid self-signed CA/cert/key PEM triple for
// tests. Validation now trial-parses the keypair and CA, so placeholder bytes
// no longer suffice.
func testMTLSMaterial() (caPEM, certPEM, keyPEM []byte) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Unix(1<<31-1, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		panic(err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, certPEM, keyPEM
}

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
	ca, cert, key := testMTLSMaterial()
	tc.MTLSConfig = &MTLSConfig{
		CACert: ca,
		Cert:   cert,
		Key:    key,
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
		if tc.MaxStreamsPerConnection != DefaultMaxStreamsPerConnection {
			t.Errorf("MaxStreamsPerConnection = %d, want %d",
				tc.MaxStreamsPerConnection, DefaultMaxStreamsPerConnection)
		}
		if tc.StreamReadIdleTimeout != DefaultStreamReadIdleTimeout {
			t.Errorf("StreamReadIdleTimeout = %v, want %v",
				tc.StreamReadIdleTimeout, DefaultStreamReadIdleTimeout)
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
			MaxStreamsPerConnection:       512,
			StreamReadIdleTimeout:         time.Minute,
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
		if tc.MaxStreamsPerConnection != 512 {
			t.Errorf("MaxStreamsPerConnection = %d, want 512", tc.MaxStreamsPerConnection)
		}
		if tc.StreamReadIdleTimeout != time.Minute {
			t.Errorf("StreamReadIdleTimeout = %v, want %v", tc.StreamReadIdleTimeout, time.Minute)
		}
	})
}

func TestTransportConfig_Validate_MaxStreamsPerConnection(t *testing.T) {
	tc := validTransportConfig()
	tc.MaxStreamsPerConnection = 0
	err := tc.Validate()
	assertTransportValidationError(t, err, "TransportConfig.MaxStreamsPerConnection")
}

func TestTransportConfig_Validate_StreamReadIdleTimeout(t *testing.T) {
	t.Run("negative rejected", func(t *testing.T) {
		tc := validTransportConfig()
		tc.StreamReadIdleTimeout = -time.Second
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.StreamReadIdleTimeout")
	})

	t.Run("zero accepted (uses default)", func(t *testing.T) {
		tc := validTransportConfig()
		tc.StreamReadIdleTimeout = 0
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
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

	t.Run("valid with pre-built Certificate (no PEM Cert/Key)", func(t *testing.T) {
		ca, cert, key := testMTLSMaterial()
		prebuilt, err := tls.X509KeyPair(cert, key)
		if err != nil {
			t.Fatalf("X509KeyPair: %v", err)
		}
		var tc TransportConfig
		tc.SetDefaults()
		tc.MTLSConfig = &MTLSConfig{CACert: ca, Certificate: &prebuilt}
		if err := tc.Validate(); err != nil {
			t.Errorf("Validate() with pre-built certificate = %v, want nil", err)
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

	t.Run("non-zero rate below floor rejected", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxSnapshotReceiveRate = MinMaxSnapshotReceiveRate - 1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxSnapshotReceiveRate")
	})

	t.Run("tiny rate (1 byte/s) rejected", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxSnapshotReceiveRate = 1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxSnapshotReceiveRate")
	})

	t.Run("rate at floor is valid", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxSnapshotReceiveRate = MinMaxSnapshotReceiveRate
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

	// A value below the frame size is the dangerous one. The send path admits
	// frames against the MaxFrameSize constant, so a replica configured lower
	// rejects frames every peer in the cluster considers valid. The sender
	// deliberately keeps the connection on a frame rejection (so a wire-version
	// mismatch cannot become a dial storm) and merely drops the batch, so the
	// misconfigured replica stops replicating with nothing but a rate-limited
	// warning to show for it. Nothing is gained in exchange: an attacker who
	// wants to make this node buffer 16 MB can simply send an uncompressed
	// 16 MB frame, so a decompression ceiling below the frame ceiling adds no
	// protection at all. It must be a startup error, not a silent stall.
	t.Run("below the frame size is rejected", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxDecompressedSize = 1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxDecompressedSize")
	})

	t.Run("above the frame size is rejected", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxDecompressedSize = DefaultMaxDecompressedSize + 1
		err := tc.Validate()
		assertTransportValidationError(t, err, "TransportConfig.MaxDecompressedSize")
	})

	t.Run("the frame size is valid", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxDecompressedSize = DefaultMaxDecompressedSize
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

// TestTransportConfig_Validate_RejectsEmptyMTLS is the regression test for M3:
// an enabled transport with present-but-empty mTLS material must fail at
// config validation, not at handshake time.
func TestTransportConfig_Validate_RejectsEmptyMTLS(t *testing.T) {
	ca, cert, key := testMTLSMaterial()

	// Each case must reach the branch it names: a placeholder CACert would
	// short-circuit at the CA pool check and leave the leaf-material branches
	// untested.
	cases := map[string]struct {
		mtls  *MTLSConfig
		field string
	}{
		"all empty": {
			mtls:  &MTLSConfig{},
			field: "TransportConfig.MTLSConfig.CACert",
		},
		"CACert not PEM": {
			mtls:  &MTLSConfig{CACert: []byte("not-a-pem-block"), Cert: cert, Key: key},
			field: "TransportConfig.MTLSConfig.CACert",
		},
		"empty cert": {
			mtls:  &MTLSConfig{CACert: ca, Key: key},
			field: "TransportConfig.MTLSConfig.Cert",
		},
		"empty key": {
			mtls:  &MTLSConfig{CACert: ca, Cert: cert},
			field: "TransportConfig.MTLSConfig.Key",
		},
		"bad keypair": {
			mtls:  &MTLSConfig{CACert: ca, Cert: []byte("notpem"), Key: []byte("notpem")},
			field: "TransportConfig.MTLSConfig",
		},
		"prebuilt certificate with no chain": {
			mtls:  &MTLSConfig{CACert: ca, Certificate: &tls.Certificate{}},
			field: "TransportConfig.MTLSConfig.Certificate",
		},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			var tc TransportConfig
			tc.SetDefaults()
			tc.MTLSConfig = tt.mtls
			err := tc.Validate()
			if err == nil {
				t.Fatal("expected validation error for empty/invalid mTLS material, got nil")
			}
			assertValidationError(t, err, tt.field)
		})
	}
}

// TestTransportConfig_Validate_AcceptsPrebuiltCertificate verifies the
// hardware-signer path: a pre-built tls.Certificate satisfies the leaf
// requirement without any PEM Cert/Key pair.
func TestTransportConfig_Validate_AcceptsPrebuiltCertificate(t *testing.T) {
	ca, cert, key := testMTLSMaterial()
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatalf("build keypair: %v", err)
	}

	var tc TransportConfig
	tc.SetDefaults()
	tc.MTLSConfig = &MTLSConfig{CACert: ca, Certificate: &pair}
	if err := tc.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil for a pre-built certificate", err)
	}
}

// TestTransportConfig_Validate_MaxMessageReceiveMemory covers the receive
// memory budget bound; a negative budget would make the accounting semaphore
// meaningless.
func TestTransportConfig_Validate_MaxMessageReceiveMemory(t *testing.T) {
	t.Run("negative rejected", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxMessageReceiveMemory = -1
		assertValidationError(t, tc.Validate(), "TransportConfig.MaxMessageReceiveMemory")
	})

	t.Run("zero accepted", func(t *testing.T) {
		tc := validTransportConfig()
		tc.MaxMessageReceiveMemory = 0
		if err := tc.Validate(); err != nil {
			t.Fatalf("Validate() = %v, want nil (0 means unlimited)", err)
		}
	})
}

// TestValidateRevocationConfig verifies RevocationConfig validation:
// unknown mode strings are rejected (a typo must not silently select the
// default), and modes that consult a source require that source.
func TestValidateRevocationConfig(t *testing.T) {
	base := func() TransportConfig {
		tc := TransportConfig{TransportDisabled: true}
		tc.SetDefaults()
		return tc
	}

	t.Run("invalid mode strings rejected", func(t *testing.T) {
		for _, mode := range []string{"CRL", "strict", "Any", "ocsp+crl"} {
			tc := base()
			tc.RevocationConfig = &RevocationConfig{Mode: mode, CRLPaths: []string{"/x"}, OCSPResponderURL: "http://r"}
			if err := tc.Validate(); err == nil {
				t.Fatalf("mode %q must be rejected", mode)
			}
		}
	})

	t.Run("valid modes accepted", func(t *testing.T) {
		for _, mode := range []string{"", "crl", "ocsp", "both", "any"} {
			tc := base()
			tc.RevocationConfig = &RevocationConfig{
				Mode:             mode,
				CRLPaths:         []string{"/etc/crl.pem"},
				OCSPResponderURL: "http://ocsp.example",
			}
			if err := tc.Validate(); err != nil {
				t.Fatalf("mode %q rejected: %v", mode, err)
			}
		}
	})

	t.Run("crl mode requires CRLPaths", func(t *testing.T) {
		tc := base()
		tc.RevocationConfig = &RevocationConfig{Mode: "crl"}
		if err := tc.Validate(); err == nil {
			t.Fatal("crl mode without CRLPaths must be rejected")
		}
	})

	t.Run("ocsp mode requires responder URL", func(t *testing.T) {
		tc := base()
		tc.RevocationConfig = &RevocationConfig{Mode: "ocsp"}
		if err := tc.Validate(); err == nil {
			t.Fatal("ocsp mode without OCSPResponderURL must be rejected")
		}
	})

	t.Run("both mode requires both sources", func(t *testing.T) {
		tc := base()
		tc.RevocationConfig = &RevocationConfig{Mode: "both", CRLPaths: []string{"/x"}}
		if err := tc.Validate(); err == nil {
			t.Fatal("both mode without OCSPResponderURL must be rejected")
		}
		tc.RevocationConfig = &RevocationConfig{Mode: "both", OCSPResponderURL: "http://r"}
		if err := tc.Validate(); err == nil {
			t.Fatal("both mode without CRLPaths must be rejected")
		}
	})

	// Negative durations/counts would otherwise reach the checker, where a
	// negative CheckInterval panics time.NewTicker and a negative cache TTL
	// expires every entry on insert.
	t.Run("negative tunables rejected", func(t *testing.T) {
		negatives := map[string]func(*RevocationConfig){
			"TransportConfig.RevocationConfig.OCSPCacheSeconds":   func(rc *RevocationConfig) { rc.OCSPCacheSeconds = -1 },
			"TransportConfig.RevocationConfig.OCSPTimeoutSeconds": func(rc *RevocationConfig) { rc.OCSPTimeoutSeconds = -1 },
			"TransportConfig.RevocationConfig.CheckInterval":      func(rc *RevocationConfig) { rc.CheckInterval = -time.Second },
		}
		for field, apply := range negatives {
			tc := base()
			rc := &RevocationConfig{Mode: "any"}
			apply(rc)
			tc.RevocationConfig = rc
			assertValidationError(t, tc.Validate(), field)
		}
	})
}
