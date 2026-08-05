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

// TestValidateRejectsEnforcedRevocationWithoutSource pins that enabling
// revocation with the fail-closed default and no source is rejected at
// configuration time. RevocationConfig{} selects the "any" mode, which names
// no specific source, so the per-mode source checks never fire: the checker
// consults nothing and admits every revoked certificate for the process
// lifetime with no error, no log and no metric.
func TestValidateRejectsEnforcedRevocationWithoutSource(t *testing.T) {
	for _, mode := range []string{"", "any"} {
		tc := TransportConfig{TransportDisabled: true}
		tc.SetDefaults()
		tc.RevocationConfig = &RevocationConfig{Mode: mode}
		assertValidationError(t, tc.Validate(), "TransportConfig.RevocationConfig")
	}
}

// TestValidateKeepsRevocationOptOutUsable pins that the check above does not
// break either documented way to run without revocation checking, nor a
// correctly-sourced enforced configuration.
func TestValidateKeepsRevocationOptOutUsable(t *testing.T) {
	cases := []struct {
		name string
		rc   *RevocationConfig
	}{
		{"disabled entirely", nil},
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
