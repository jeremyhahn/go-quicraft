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
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"
)

// requiredOnlyHostConfig returns a zero HostConfig with only the fields
// SetDefaults cannot supply, then SetDefaults applied. The transport stays
// enabled, so the mTLS material is one of those required fields.
func requiredOnlyHostConfig() HostConfig {
	ca, cert, key := testMTLSMaterial()
	hc := HostConfig{
		WALDir:        "/tmp/wal",
		NodeHostDir:   "/tmp/node",
		RaftAddress:   "127.0.0.1:26001",
		ListenAddress: "127.0.0.1:26001",
		DeploymentID:  1,
	}
	hc.TransportConfig.MTLSConfig = &MTLSConfig{CACert: ca, Cert: cert, Key: key}
	hc.SetDefaults()
	return hc
}

func TestHostConfig_Validate_RequiredFieldsOnlyPasses(t *testing.T) {
	hc := requiredOnlyHostConfig()
	if err := hc.Validate(); err != nil {
		t.Fatalf("Validate() after SetDefaults = %v, want nil", err)
	}
	// The key-rotation warning path (no MaxRetainedEpochs) is valid.
	hc.KeyRotationInterval = time.Second
	if err := hc.Validate(); err != nil {
		t.Fatalf("Validate() with KeyRotationInterval=1s = %v, want nil", err)
	}
}

// TestHostConfig_Validate_RejectsEveryOutOfRangeField walks every rejection
// branch of HostConfig.Validate and TransportConfig.Validate (reached through
// it): each row changes one field of a valid config and expects the named
// error type, whose text names the field.
func TestHostConfig_Validate_RejectsEveryOutOfRangeField(t *testing.T) {
	ca, cert, key := testMTLSMaterial()
	der, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatalf("build keypair: %v", err)
	}

	type kind int
	const (
		validation kind = iota
		logValidation
		keyRotation
		mtlsRequired
		receiveMemory
	)
	rev := func(rc RevocationConfig) func(*HostConfig) {
		return func(hc *HostConfig) { hc.TransportConfig.RevocationConfig = &rc }
	}
	mtls := func(m MTLSConfig) func(*HostConfig) {
		return func(hc *HostConfig) { hc.TransportConfig.MTLSConfig = &m }
	}

	cases := []struct {
		name   string
		mutate func(*HostConfig)
		kind   kind
		field  string
	}{
		// HostConfig.Validate.
		{"empty WALDir", func(hc *HostConfig) { hc.WALDir = "" }, validation, "WALDir"},
		{"empty NodeHostDir", func(hc *HostConfig) { hc.NodeHostDir = "" }, validation, "NodeHostDir"},
		{"empty RaftAddress", func(hc *HostConfig) { hc.RaftAddress = "" }, validation, "RaftAddress"},
		{"empty ListenAddress", func(hc *HostConfig) { hc.ListenAddress = "" }, validation, "ListenAddress"},
		{"zero RTTMillisecond", func(hc *HostConfig) { hc.RTTMillisecond = 0 }, validation, "RTTMillisecond"},
		{"RTTMillisecond above 1 minute", func(hc *HostConfig) { hc.RTTMillisecond = 60_001 }, validation, "RTTMillisecond"},
		{"zero DeploymentID", func(hc *HostConfig) { hc.DeploymentID = 0 }, validation, "DeploymentID"},
		{"zero NumWorkers", func(hc *HostConfig) { hc.NumWorkers = 0 }, validation, "NumWorkers"},
		{"negative MaxApplyWorkers", func(hc *HostConfig) { hc.MaxApplyWorkers = -1 }, validation, "MaxApplyWorkers"},
		{"negative CommitCBufferSize", func(hc *HostConfig) { hc.CommitCBufferSize = -1 }, validation, "CommitCBufferSize"},
		{"negative ShutdownTimeout", func(hc *HostConfig) { hc.ShutdownTimeout = -time.Second }, validation, "ShutdownTimeout"},
		{"unknown log format", func(hc *HostConfig) { hc.LogConfig.Format = "xml" }, logValidation, "LogConfig.Format"},
		{"unknown log level", func(hc *HostConfig) { hc.LogConfig.Level = "trace" }, logValidation, "LogConfig.Level"},
		{"KeyRotationInterval below 1s", func(hc *HostConfig) { hc.KeyRotationInterval = time.Millisecond }, keyRotation, "KeyRotationInterval"},

		// TransportConfig.validateLimits.
		{"zero StreamPoolSize", func(hc *HostConfig) { hc.TransportConfig.StreamPoolSize = 0 }, validation, "TransportConfig.StreamPoolSize"},
		{"MaxStreamPoolSize below StreamPoolSize", func(hc *HostConfig) {
			hc.TransportConfig.MaxStreamPoolSize = hc.TransportConfig.StreamPoolSize - 1
		}, validation, "TransportConfig.MaxStreamPoolSize"},
		{"negative MaxSnapshotReceiveRate", func(hc *HostConfig) { hc.TransportConfig.MaxSnapshotReceiveRate = -1 }, validation, "TransportConfig.MaxSnapshotReceiveRate"},
		{"MaxSnapshotReceiveRate below floor", func(hc *HostConfig) {
			hc.TransportConfig.MaxSnapshotReceiveRate = MinMaxSnapshotReceiveRate - 1
		}, validation, "TransportConfig.MaxSnapshotReceiveRate"},
		{"zero MaxConcurrentSnapshotReceives", func(hc *HostConfig) { hc.TransportConfig.MaxConcurrentSnapshotReceives = 0 }, validation, "TransportConfig.MaxConcurrentSnapshotReceives"},
		{"negative MaxSnapshotReceiveMemory", func(hc *HostConfig) { hc.TransportConfig.MaxSnapshotReceiveMemory = -1 }, validation, "TransportConfig.MaxSnapshotReceiveMemory"},
		{"MaxDecompressedSize off the frame size", func(hc *HostConfig) { hc.TransportConfig.MaxDecompressedSize = DefaultMaxDecompressedSize - 1 }, validation, "TransportConfig.MaxDecompressedSize"},
		{"negative MessageReceiveBudgetFrames", func(hc *HostConfig) { hc.TransportConfig.MessageReceiveBudgetFrames = -1 }, validation, "TransportConfig.MessageReceiveBudgetFrames"},
		{"negative MaxMessageReceiveMemory", func(hc *HostConfig) { hc.TransportConfig.MaxMessageReceiveMemory = -1 }, validation, "TransportConfig.MaxMessageReceiveMemory"},
		{"MaxMessageReceiveMemory below one frame", func(hc *HostConfig) { hc.TransportConfig.MaxMessageReceiveMemory = 1 }, receiveMemory, "TransportConfig.MaxMessageReceiveMemory"},
		{"zero MaxConnectionsPerIP", func(hc *HostConfig) { hc.TransportConfig.MaxConnectionsPerIP = 0 }, validation, "TransportConfig.MaxConnectionsPerIP"},
		{"zero MaxIncomingConnections", func(hc *HostConfig) { hc.TransportConfig.MaxIncomingConnections = 0 }, validation, "TransportConfig.MaxIncomingConnections"},
		{"zero SendBatchMaxSize", func(hc *HostConfig) { hc.TransportConfig.SendBatchMaxSize = 0 }, validation, "TransportConfig.SendBatchMaxSize"},
		{"zero MaxStreamsPerConnection", func(hc *HostConfig) { hc.TransportConfig.MaxStreamsPerConnection = 0 }, validation, "TransportConfig.MaxStreamsPerConnection"},
		{"negative StreamReadIdleTimeout", func(hc *HostConfig) { hc.TransportConfig.StreamReadIdleTimeout = -time.Second }, validation, "TransportConfig.StreamReadIdleTimeout"},

		// TransportConfig.validateMTLS.
		{"nil MTLSConfig", func(hc *HostConfig) { hc.TransportConfig.MTLSConfig = nil }, mtlsRequired, "MTLSConfig"},
		{"empty CACert", mtls(MTLSConfig{Certificate: &der}), validation, "TransportConfig.MTLSConfig.CACert"},
		{"CACert not PEM", mtls(MTLSConfig{CACert: []byte("not-pem"), Certificate: &der}), validation, "TransportConfig.MTLSConfig.CACert"},
		{"Certificate without chain", mtls(MTLSConfig{CACert: ca, Certificate: &tls.Certificate{}}), validation, "TransportConfig.MTLSConfig.Certificate"},
		{"Certificate without signer", mtls(MTLSConfig{CACert: ca, Certificate: &tls.Certificate{Certificate: der.Certificate}}), validation, "TransportConfig.MTLSConfig.Certificate"},
		{"empty Cert", mtls(MTLSConfig{CACert: ca, Key: key}), validation, "TransportConfig.MTLSConfig.Cert"},
		{"empty Key", mtls(MTLSConfig{CACert: ca, Cert: cert}), validation, "TransportConfig.MTLSConfig.Key"},
		{"Cert/Key not a keypair", mtls(MTLSConfig{CACert: ca, Cert: cert, Key: []byte("not-pem")}), validation, "TransportConfig.MTLSConfig"},

		// TransportConfig.validateRevocation.
		{"unknown revocation Mode", rev(RevocationConfig{Mode: "CRL", CRLPaths: []string{"/crl"}}), validation, "TransportConfig.RevocationConfig.Mode"},
		{"crl Mode without CRLPaths", rev(RevocationConfig{Mode: "crl"}), validation, "TransportConfig.RevocationConfig.CRLPaths"},
		{"ocsp Mode without responder", rev(RevocationConfig{Mode: "ocsp"}), validation, "TransportConfig.RevocationConfig.OCSPResponderURL"},
		{"negative OCSPCacheSeconds", rev(RevocationConfig{Mode: "crl", CRLPaths: []string{"/crl"}, OCSPCacheSeconds: -1}), validation, "TransportConfig.RevocationConfig.OCSPCacheSeconds"},
		{"negative OCSPTimeoutSeconds", rev(RevocationConfig{Mode: "crl", CRLPaths: []string{"/crl"}, OCSPTimeoutSeconds: -1}), validation, "TransportConfig.RevocationConfig.OCSPTimeoutSeconds"},
		{"negative CheckInterval", rev(RevocationConfig{Mode: "crl", CRLPaths: []string{"/crl"}, CheckInterval: -time.Second}), validation, "TransportConfig.RevocationConfig.CheckInterval"},
		{"enforced default Mode without a source", rev(RevocationConfig{}), validation, "TransportConfig.RevocationConfig"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			hc := requiredOnlyHostConfig()
			tt.mutate(&hc)
			err := hc.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want an error naming %q", tt.field)
			}
			switch tt.kind {
			case validation:
				var ve *ValidationError
				if !errors.As(err, &ve) {
					t.Fatalf("Validate() = %T %v, want *ValidationError", err, err)
				}
				if ve.Field != tt.field {
					t.Fatalf("ValidationError.Field = %q, want %q (%v)", ve.Field, tt.field, err)
				}
			case logValidation:
				var le *LogValidationError
				if !errors.As(err, &le) {
					t.Fatalf("Validate() = %T %v, want *LogValidationError", err, err)
				}
			case keyRotation:
				var ke *KeyRotationIntervalTooSmallError
				if !errors.As(err, &ke) {
					t.Fatalf("Validate() = %T %v, want *KeyRotationIntervalTooSmallError", err, err)
				}
			case mtlsRequired:
				var me *MTLSConfigRequiredError
				if !errors.As(err, &me) {
					t.Fatalf("Validate() = %T %v, want *MTLSConfigRequiredError", err, err)
				}
			case receiveMemory:
				var re *MaxMessageReceiveMemoryTooSmallError
				if !errors.As(err, &re) {
					t.Fatalf("Validate() = %T %v, want *MaxMessageReceiveMemoryTooSmallError", err, err)
				}
			}
			if !strings.Contains(err.Error(), tt.field) {
				t.Fatalf("error %q does not name the field %q", err, tt.field)
			}
		})
	}
}

// TestTransportConfig_Validate_RejectsThroughEachStage calls
// TransportConfig.Validate directly, once per stage it delegates to, and once
// on a valid config.
func TestTransportConfig_Validate_RejectsThroughEachStage(t *testing.T) {
	base := func() TransportConfig { return requiredOnlyHostConfig().TransportConfig }

	tc := base()
	if err := tc.Validate(); err != nil {
		t.Fatalf("Validate() of the defaults = %v, want nil", err)
	}

	tc = base()
	tc.SendBatchMaxSize = 0
	assertTransportValidationError(t, tc.Validate(), "TransportConfig.SendBatchMaxSize")

	tc = base()
	tc.MTLSConfig = nil
	if err := tc.Validate(); !errors.Is(err, ErrMTLSConfigRequired) {
		t.Fatalf("Validate() = %v, want ErrMTLSConfigRequired", err)
	}

	tc = base()
	tc.RevocationConfig = &RevocationConfig{Mode: "strict"}
	assertTransportValidationError(t, tc.Validate(), "TransportConfig.RevocationConfig.Mode")
}
