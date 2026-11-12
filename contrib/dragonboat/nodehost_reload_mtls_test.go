// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dragonboat_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	dragonboat "github.com/jeremyhahn/go-quicraft/contrib/dragonboat"
	"github.com/jeremyhahn/go-quicraft/contrib/dragonboat/config"
	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	qcconfig "github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// reloadFixture is a NodeHost serving mutual TLS from files on disk, with
// the original PEM material kept so a subtest can restore it.
type reloadFixture struct {
	nh       *dragonboat.NodeHost
	ca       *testpki.CA
	files    testpki.Files
	origCert []byte
	origKey  []byte
	identity string
}

func newReloadFixture(t *testing.T) *reloadFixture {
	t.Helper()
	dir := t.TempDir()
	ca := testpki.NewCA(t)
	files := ca.LeafFiles(t, t.TempDir())
	nh, err := dragonboat.NewNodeHost(config.NodeHostConfig{
		DeploymentID:   1,
		WALDir:         filepath.Join(dir, "wal"),
		NodeHostDir:    filepath.Join(dir, "data"),
		RaftAddress:    "127.0.0.1:0",
		ListenAddress:  "127.0.0.1:0",
		RTTMillisecond: 200,
		MutualTLS:      true,
		CAFile:         files.CAFile,
		CertFile:       files.CertFile,
		KeyFile:        files.KeyFile,
	})
	if err != nil {
		t.Fatalf("NewNodeHost: %v", err)
	}
	t.Cleanup(nh.Close)
	f := &reloadFixture{
		nh:       nh,
		ca:       ca,
		files:    files,
		origCert: readFile(t, files.CertFile),
		origKey:  readFile(t, files.KeyFile),
		identity: nh.NodeIdentity(),
	}
	if f.identity == "" {
		t.Fatal("NodeIdentity is empty")
	}
	return f
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func (f *reloadFixture) restore(t *testing.T) {
	t.Helper()
	writeFile(t, f.files.CertFile, f.origCert)
	writeFile(t, f.files.KeyFile, f.origKey)
}

func (f *reloadFixture) requireIdentity(t *testing.T) {
	t.Helper()
	if got := f.nh.NodeIdentity(); got != f.identity {
		t.Fatalf("NodeIdentity = %q, want %q", got, f.identity)
	}
}

// TestNodeHost_ReloadMutualTLS_ReissueSameKey: a certificate re-issued on
// the same keypair (new serial) reloads and keeps the node identity.
func TestNodeHost_ReloadMutualTLS_ReissueSameKey(t *testing.T) {
	f := newReloadFixture(t)
	reissued := f.ca.Reissue(t, &qcconfig.MTLSConfig{Key: f.origKey})
	if string(reissued.Cert) == string(f.origCert) {
		t.Fatal("Reissue returned the original certificate")
	}
	writeFile(t, f.files.CertFile, reissued.Cert)
	if err := f.nh.ReloadMutualTLS(); err != nil {
		t.Fatalf("ReloadMutualTLS after reissue: %v", err)
	}
	f.requireIdentity(t)
}

// TestNodeHost_ReloadMutualTLS_ChangedIdentityRefused: a fresh keypair
// changes the node identity and is refused; nothing changes, and the
// original files reload afterwards.
func TestNodeHost_ReloadMutualTLS_ChangedIdentityRefused(t *testing.T) {
	f := newReloadFixture(t)
	fresh := f.ca.Leaf(t)
	writeFile(t, f.files.CertFile, fresh.Cert)
	writeFile(t, f.files.KeyFile, fresh.Key)
	err := f.nh.ReloadMutualTLS()
	if err == nil {
		t.Fatal("ReloadMutualTLS with a new keypair returned nil")
	}
	if !errors.Is(err, quicraft.ErrNodeIdentityChanged) {
		t.Errorf("errors.Is(err, ErrNodeIdentityChanged) = false: %v", err)
	}
	if !errors.Is(err, quicraft.ErrMTLSReload) {
		t.Errorf("errors.Is(err, ErrMTLSReload) = false: %v", err)
	}
	var reloadErr *quicraft.MTLSReloadError
	if !errors.As(err, &reloadErr) {
		t.Errorf("err is not a *MTLSReloadError: %T", err)
	}
	f.requireIdentity(t)

	f.restore(t)
	if err := f.nh.ReloadMutualTLS(); err != nil {
		t.Fatalf("ReloadMutualTLS after restoring the files: %v", err)
	}
	f.requireIdentity(t)
}

// TestNodeHost_ReloadMutualTLS_NotPEMRefused: a CertFile that is not PEM is
// refused and the host keeps serving its previous material.
func TestNodeHost_ReloadMutualTLS_NotPEMRefused(t *testing.T) {
	f := newReloadFixture(t)
	writeFile(t, f.files.CertFile, []byte("not a certificate"))
	if err := f.nh.ReloadMutualTLS(); err == nil {
		t.Fatal("ReloadMutualTLS with a non-PEM CertFile returned nil")
	}
	f.requireIdentity(t)
	if f.nh.RaftAddress() == "" {
		t.Error("RaftAddress is empty after a refused reload")
	}

	f.restore(t)
	if err := f.nh.ReloadMutualTLS(); err != nil {
		t.Fatalf("ReloadMutualTLS after restoring the file: %v", err)
	}
	f.requireIdentity(t)
}

// TestNodeHost_ReloadMutualTLS_AfterClose: a closed NodeHost returns
// ErrClosed (dragonboat.ErrClosed aliases quicraft.ErrClosed).
func TestNodeHost_ReloadMutualTLS_AfterClose(t *testing.T) {
	f := newReloadFixture(t)
	f.nh.Close()
	err := f.nh.ReloadMutualTLS()
	if !errors.Is(err, dragonboat.ErrClosed) {
		t.Fatalf("ReloadMutualTLS after Close = %v, want ErrClosed", err)
	}
}
