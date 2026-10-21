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

package quicraft

import (
	"errors"
	"testing"
)

// TestHostReloadMTLS covers the Host surface of a live mTLS reload: a valid
// reload is accepted and keeps the node identity, an invalid one is refused
// with a typed error, and a host with no transport or a closed host refuses.
func TestHostReloadMTLS(t *testing.T) {
	cfg := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	identity := h.NodeIdentity()

	if err := h.ReloadMTLS(cfg.TransportConfig.MTLSConfig); err != nil {
		t.Fatalf("ReloadMTLS(same material) = %v", err)
	}
	if got := h.NodeIdentity(); got != identity {
		t.Fatalf("NodeIdentity after reload = %q, want %q", got, identity)
	}

	other := sanHostConfig(t, "node-a.raft.test:5000", "node-a.raft.test")
	err = h.ReloadMTLS(other.TransportConfig.MTLSConfig)
	var reloadErr *MTLSReloadError
	if !errors.As(err, &reloadErr) || !errors.Is(err, ErrNodeIdentityChanged) {
		t.Fatalf("ReloadMTLS(new keypair) = %v, want an MTLSReloadError for ErrNodeIdentityChanged", err)
	}
	if err := h.ReloadMTLS(nil); !errors.Is(err, ErrMTLSReload) {
		t.Fatalf("ReloadMTLS(nil) = %v, want ErrMTLSReload", err)
	}
	if got := h.NodeIdentity(); got != identity {
		t.Fatal("a refused reload changed the node identity")
	}

	h.Close()
	if err := h.ReloadMTLS(cfg.TransportConfig.MTLSConfig); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReloadMTLS on a closed host = %v, want ErrClosed", err)
	}

	noTransport, err := NewHost(cfg, WithMemoryLogDB(), WithoutTransport(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost(WithoutTransport) failed: %v", err)
	}
	defer noTransport.Close()
	if err := noTransport.ReloadMTLS(cfg.TransportConfig.MTLSConfig); !errors.Is(err, ErrNoLocalCertificate) {
		t.Fatalf("ReloadMTLS without a transport = %v, want ErrNoLocalCertificate", err)
	}
}
