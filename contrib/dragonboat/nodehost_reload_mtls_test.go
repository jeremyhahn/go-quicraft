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
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// freeUDPAddr reserves a loopback UDP port and releases it, so a NodeHost
// can bind an address its peers can dial (127.0.0.1:0 is not dialable).
func freeUDPAddr(ctx context.Context, t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	pc, err := lc.ListenPacket(ctx, "udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve UDP port: %v", err)
	}
	addr := pc.LocalAddr().String()
	if err := pc.Close(); err != nil {
		t.Fatalf("release UDP port: %v", err)
	}
	return addr
}

// newMTLSNodeHost starts a NodeHost serving mutual TLS from files on addr.
func newMTLSNodeHost(t *testing.T, files testpki.Files, addr string) *dragonboat.NodeHost {
	t.Helper()
	dir := t.TempDir()
	nh, err := dragonboat.NewNodeHost(config.NodeHostConfig{
		DeploymentID:   1,
		WALDir:         filepath.Join(dir, "wal"),
		NodeHostDir:    filepath.Join(dir, "data"),
		RaftAddress:    addr,
		ListenAddress:  addr,
		RTTMillisecond: 20,
		MutualTLS:      true,
		CAFile:         files.CAFile,
		CertFile:       files.CertFile,
		KeyFile:        files.KeyFile,
	})
	if err != nil {
		t.Fatalf("NewNodeHost: %v", err)
	}
	t.Cleanup(nh.Close)
	return nh
}

func newReloadFixture(t *testing.T) *reloadFixture {
	t.Helper()
	ca := testpki.NewCA(t)
	files := ca.LeafFiles(t, t.TempDir())
	nh := newMTLSNodeHost(t, files, freeUDPAddr(context.Background(), t))
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

// requireServes proves the fixture's listener still accepts authenticated
// connections: a second NodeHost with a fresh leaf from the same CA forms a
// two-member shard with the fixture and a proposal commits, which needs
// both hosts to complete new mutual-TLS handshakes with each other.
func (f *reloadFixture) requireServes(ctx context.Context, t *testing.T) {
	t.Helper()
	peer := newMTLSNodeHost(t, f.ca.LeafFiles(t, t.TempDir()), freeUDPAddr(ctx, t))
	const shardID = 1
	members := map[uint64]dragonboat.Target{
		1: f.nh.RaftAddress(),
		2: peer.RaftAddress(),
	}
	identities := map[uint64]string{
		1: f.nh.NodeIdentity(),
		2: peer.NodeIdentity(),
	}
	for replicaID, nh := range map[uint64]*dragonboat.NodeHost{1: f.nh, 2: peer} {
		if err := nh.StartReplica(members, false, newKVStore, config.Config{
			ShardID:          shardID,
			ReplicaID:        replicaID,
			MemberIdentities: identities,
			ElectionRTT:      10,
			HeartbeatRTT:     1,
			CheckQuorum:      true,
		}); err != nil {
			t.Fatalf("StartReplica %d: %v", replicaID, err)
		}
	}
	session := f.nh.GetNoOPSession(shardID)
	for {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		_, err := f.nh.SyncPropose(attempt, session, []byte("k=v"))
		cancel()
		if err == nil {
			return
		}
		if !dragonboat.IsTempError(err) || ctx.Err() != nil {
			t.Fatalf("SyncPropose on a shard with a same-CA peer: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f.requireServes(ctx, t)

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
