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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
)

const reloadRefusalAddr = "node-a.raft.test:5000"

// newReloadRefusalHost starts a Host on mtls and returns it with its
// NodeIdentity. The Host is closed when the test ends.
func newReloadRefusalHost(ctx context.Context, t *testing.T, mtls *config.MTLSConfig) (*Host, string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	cfg := sanHostConfig(t, reloadRefusalAddr, "node-a.raft.test")
	cfg.TransportConfig.MTLSConfig = mtls
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost failed: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	id := h.NodeIdentity()
	if id == "" {
		t.Fatal("NodeIdentity is empty")
	}
	return h, id
}

// newReloadRefusalKey returns a fresh P-256 key.
func newReloadRefusalKey(ctx context.Context, t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("context: %v", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

func TestHostReloadMTLSRefusals(t *testing.T) {
	ctx := context.Background()
	key := newReloadRefusalKey(ctx, t)
	original := selfSignedMTLS(t, key, 1, "node-a.raft.test")
	h, identity := newReloadRefusalHost(ctx, t, original)

	assertIdentity := func(step string) {
		t.Helper()
		if got := h.NodeIdentity(); got != identity {
			t.Fatalf("%s: NodeIdentity = %q, want %q", step, got, identity)
		}
	}

	t.Run("new keypair names both identities", func(t *testing.T) {
		other := selfSignedMTLS(t, newReloadRefusalKey(ctx, t), 2, "node-a.raft.test")
		_, otherIdentity := newReloadRefusalHost(ctx, t, other)
		if otherIdentity == identity {
			t.Fatal("two keypairs gave the same NodeIdentity")
		}
		err := h.ReloadMTLS(other)
		if !errors.Is(err, ErrMTLSReload) {
			t.Fatalf("ReloadMTLS(new keypair) = %v, want ErrMTLSReload", err)
		}
		var changed *NodeIdentityChangedError
		if !errors.As(err, &changed) {
			t.Fatalf("ReloadMTLS(new keypair) = %v, want a *NodeIdentityChangedError", err)
		}
		if changed.Current != identity {
			t.Errorf("Current = %q, want %q", changed.Current, identity)
		}
		if changed.Reloaded != otherIdentity {
			t.Errorf("Reloaded = %q, want %q", changed.Reloaded, otherIdentity)
		}
		assertIdentity("after new keypair")
	})

	t.Run("mismatched key is refused", func(t *testing.T) {
		other := selfSignedMTLS(t, newReloadRefusalKey(ctx, t), 3, "node-a.raft.test")
		mismatched := &config.MTLSConfig{
			CACert: original.CACert,
			Cert:   original.Cert,
			Key:    other.Key,
		}
		if err := h.ReloadMTLS(mismatched); !errors.Is(err, ErrMTLSReload) {
			t.Fatalf("ReloadMTLS(mismatched key) = %v, want ErrMTLSReload", err)
		}
		assertIdentity("after mismatched key")
	})

	t.Run("empty CA pool is refused", func(t *testing.T) {
		// A PEM bundle with no CERTIFICATE block yields an empty pool.
		empty := &config.MTLSConfig{
			CACert: original.Key,
			Cert:   original.Cert,
			Key:    original.Key,
		}
		if err := h.ReloadMTLS(empty); !errors.Is(err, ErrMTLSReload) {
			t.Fatalf("ReloadMTLS(empty CA pool) = %v, want ErrMTLSReload", err)
		}
		assertIdentity("after empty CA pool")
	})

	t.Run("renewed leaf on the same key is accepted", func(t *testing.T) {
		renewed := selfSignedMTLS(t, key, 4, "node-a.raft.test")
		if string(renewed.Cert) == string(original.Cert) {
			t.Fatal("renewed leaf is byte-identical to the original")
		}
		if err := h.ReloadMTLS(renewed); err != nil {
			t.Fatalf("ReloadMTLS(renewed leaf) = %v, want nil", err)
		}
		assertIdentity("after renewed leaf")
	})
}
