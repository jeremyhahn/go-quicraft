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

// listenerTLS returns the tls.Config the QUIC listener is created with. It is
// the server config in force at Start, so every setting a handshake needs is
// present on it, plus a GetConfigForClient that hands each new handshake the
// server config CURRENT at that moment. That indirection is what makes a
// reload reach inbound handshakes: the listener is created once and keeps its
// config for life.
//
// Each pair's server config seals session tickets with its own ticket keys
// (the WrapSession/UnwrapSession hooks buildMTLSTLS installs), so a ticket
// issued before a reload does not decrypt after it and the peer falls back to
// a full handshake verified against the new pool. A session resumed across a
// CA drop would otherwise skip chain verification. The dial side gets the
// same property from the fresh ClientSessionCache every buildMTLSTLS call
// creates.
func (t *QUICTransport) listenerTLS() *tls.Config {
	base := t.currentTLS().server.Clone()
	base.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return t.currentTLS().server, nil
	}
	return base
}

// ReloadMTLS replaces this node's certificate, key and CA pool without a
// restart. The new material is validated exactly as NewQUICTransport
// validates it at start — presence, leaf parse and key match, a non-empty CA
// pool, and the CA bundle's issuers when revocation checking is enabled — and
// in addition:
//
//   - the leaf must carry the same public key as the one in force, because
//     the node identity peers committed is the SHA-256 of that key
//     (NodeIdentityChangedError);
//   - when peers is non-empty, the leaf must not cover any of their hosts,
//     the check StartShard runs through VerifyPeerAddressExclusivity
//     (SharedPeerCertificateError).
//
// Only when every check passes is the material swapped, together with the
// revocation checker's issuer set; on any failure nothing changes and a
// *MTLSReloadError wrapping the failed check is returned.
//
// A reload reaches every handshake that starts after it returns, inbound and
// outbound. Connections already established keep the session they were
// authenticated with until they close and reconnect: a reload never tears
// down a live connection. TLS 1.3, the ALPN, the key-exchange groups, the
// revocation callback and the peer-identity binding are unchanged.
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
