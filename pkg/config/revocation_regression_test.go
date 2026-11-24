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

import "testing"

// TestValidateRejectsEnforcedRevocationWithoutSource pins that fail-closed revocation with no source is rejected at configuration time.
func TestValidateRejectsEnforcedRevocationWithoutSource(t *testing.T) {
	for _, mode := range []string{"", "any"} {
		tc := TransportConfig{TransportDisabled: true}
		tc.SetDefaults()
		tc.RevocationConfig = &RevocationConfig{Mode: mode}
		assertValidationError(t, tc.Validate(), "TransportConfig.RevocationConfig")
	}
}

// TestValidateKeepsRevocationOptOutUsable pins that the check above does not
// break the soft-fail opt-out, nor an enforced configuration with a single
// source. A nil RevocationConfig is covered by TestTransportConfig_Validate_Valid.
func TestValidateKeepsRevocationOptOutUsable(t *testing.T) {
	cases := []struct {
		name string
		rc   *RevocationConfig
	}{
		{"soft-fail with no source", &RevocationConfig{Mode: "any", AllowSoftFail: true}},
		{"enforced with CRL", &RevocationConfig{Mode: "any", CRLPaths: []string{"/etc/crl.pem"}}},
		{"enforced with OCSP", &RevocationConfig{Mode: "any", OCSPResponderURL: "http://ocsp.example"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := TransportConfig{TransportDisabled: true}
			cfg.SetDefaults()
			cfg.RevocationConfig = tc.rc
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}
