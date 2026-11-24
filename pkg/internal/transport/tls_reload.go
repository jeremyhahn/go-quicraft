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
	"crypto/tls"
	"crypto/x509"
)

// currentTLS returns the TLS material in force: the pair NewQUICTransport
// built, or the last one ReloadMTLS accepted. It is nil only on a transport
// that was never constructed through NewQUICTransport.
func (t *QUICTransport) currentTLS() *tlsPair {
	return t.tlsState.Load()
}

// listenerTLS returns the listener's tls.Config, whose GetConfigForClient gives each handshake the current material.
func (t *QUICTransport) listenerTLS() *tls.Config {
	base := t.currentTLS().server.Clone()
	base.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return t.currentTLS().server, nil
	}
	return base
}

// ReloadMTLS validates and swaps in a new certificate, key and CA pool, requiring the same leaf public key and no coverage of peers; new handshakes use it, live connections are kept.
func (t *QUICTransport) ReloadMTLS(mtls *MTLSConfig, selfAddr string, peers []string) error {
	if err := validateMTLSConfig(mtls); err != nil {
		return &MTLSReloadError{Err: err}
	}

	t.reloadMu.Lock()
	defer t.reloadMu.Unlock()

	current := t.currentTLS()
	if current == nil || current.leaf == nil {
		return &MTLSReloadError{Err: ErrNoLocalCertificate}
	}

	// The issuer set is parsed first, as NewQUICTransport does, so a bundle
	// that cannot back revocation checking is refused before anything else.
	var issuers []*x509.Certificate
	if t.revocationChecker != nil {
		parsed, err := parseCACertificates(mtls.CACert)
		if err != nil {
			return &MTLSReloadError{Err: err}
		}
		issuers = parsed
	}

	pair, err := buildMTLSTLS(mtls, t.revocationChecker)
	if err != nil {
		return &MTLSReloadError{Err: err}
	}

	if was, now := NodeIdentityFromCertificate(current.leaf), NodeIdentityFromCertificate(pair.leaf); was != now {
		return &MTLSReloadError{Err: &NodeIdentityChangedError{Current: was, Reloaded: now}}
	}

	if len(peers) > 0 {
		if err := verifyCertificateExclusivity(pair.leaf, selfAddr, peers); err != nil {
			return &MTLSReloadError{Err: err}
		}
	}

	if t.revocationChecker != nil {
		t.revocationChecker.SetIssuers(issuers...)
	}
	t.tlsState.Store(pair)
	return nil
}
