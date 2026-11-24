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

//go:build integration

package host_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// TestHostAPI_CARollover_ThreeNode runs the CA-rollover runbook
// (docs/quic-transport/ca-rollover.md) against a live 3-node shard: trust
// old+new, re-issue every leaf from the new CA on the same key, drop the old
// CA. No node restarts, and a proposer running throughout must never go a
// proposal timeout without a commit. After each step a fresh handshake to
// every node proves what it now presents and accepts.
func TestHostAPI_CARollover_ThreeNode(t *testing.T) {
	cluster := newTestCluster(t, 3)
	t.Cleanup(func() { cluster.close() })

	const shardID uint64 = 1
	trackers := cluster.startShard(shardID)
	cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	proposeAndWait(t, cluster, trackers, shardID, "warmup", "ok")

	oldCA := cluster.ca
	newCA := testpki.NewCA(t)
	bundle := append(append([]byte{}, oldCA.PEM()...), newCA.PEM()...)
	oldProbe := oldCA.Leaf(t)
	newProbe := newCA.Leaf(t)

	// The proposer is the quorum check: every proposal must commit within
	// its timeout while the material changes under the cluster.
	var (
		stop      = make(chan struct{})
		done      sync.WaitGroup
		committed atomic.Int64
		failures  atomic.Int64
		lastErr   atomic.Value
	)
	done.Add(1)
	go func() {
		defer done.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			cmd := encodeKVPut(fmt.Sprintf("rollover-%05d", i), "v")
			if _, err := tryProposeToLeader(cluster, shardID, cmd, defaultTimeout); err != nil {
				failures.Add(1)
				lastErr.Store(err.Error())
				continue
			}
			committed.Add(1)
		}
	}()

	reloadAll := func(step string, material func(i int) *config.MTLSConfig) {
		t.Helper()
		before := committed.Load()
		for i, h := range cluster.hosts {
			if err := h.ReloadMTLS(material(i)); err != nil {
				t.Fatalf("%s: node[%d] ReloadMTLS: %v", step, i, err)
			}
		}
		waitForCondition(t, defaultTimeout, step+": proposals commit after the reload", func() bool {
			return committed.Load() > before+5
		})
		proposeAndWait(t, cluster, trackers, shardID, step, "done")
	}

	// Step 1: every node trusts both CAs. Leaves are unchanged.
	reloadAll("trust-both", func(i int) *config.MTLSConfig {
		m := *cluster.leaves[i]
		m.CACert = bundle
		return &m
	})
	for i, addr := range cluster.addrs {
		requireProbe(t, addr, newProbe, bundle, true, fmt.Sprintf("trust-both: node[%d] accepts a new-CA peer", i))
		requireProbe(t, addr, oldProbe, oldCA.PEM(), true, fmt.Sprintf("trust-both: node[%d] still presents an old-CA leaf", i))
	}

	// Step 2: every node presents a new-CA leaf on its existing key, still
	// trusting both.
	reissued := make([]*config.MTLSConfig, len(cluster.hosts))
	reloadAll("rotate-leaves", func(i int) *config.MTLSConfig {
		reissued[i] = newCA.Reissue(t, cluster.leaves[i])
		m := *reissued[i]
		m.CACert = bundle
		return &m
	})
	for i, addr := range cluster.addrs {
		requireProbe(t, addr, newProbe, newCA.PEM(), true, fmt.Sprintf("rotate-leaves: node[%d] presents a new-CA leaf", i))
		requireProbe(t, addr, oldProbe, bundle, true, fmt.Sprintf("rotate-leaves: node[%d] still accepts an old-CA peer", i))
	}
	if ids := nodeIdentities(cluster.hosts); fmt.Sprint(ids) != fmt.Sprint(cluster.ids) {
		t.Fatalf("re-issued leaves changed node identities: %v, want %v", ids, cluster.ids)
	}

	// Step 3: the old CA is dropped everywhere.
	reloadAll("drop-old", func(i int) *config.MTLSConfig {
		return reissued[i]
	})
	for i, addr := range cluster.addrs {
		requireProbe(t, addr, newProbe, newCA.PEM(), true, fmt.Sprintf("drop-old: node[%d] accepts a new-CA peer", i))
		requireProbe(t, addr, oldProbe, bundle, false, fmt.Sprintf("drop-old: node[%d] refuses an old-CA peer", i))
	}

	close(stop)
	done.Wait()
	if n := failures.Load(); n != 0 {
		t.Fatalf("%d proposals failed during the rollover (last: %v): quorum was lost", n, lastErr.Load())
	}
	t.Logf("CA rollover complete: %d proposals committed, none failed", committed.Load())
}

// requireProbe dials addr as a peer presenting probe's leaf and trusting
// trust, and requires the TLS handshake to be accepted or refused. Only the
// handshake is judged: a refusal is a TLS alert (a QUIC crypto error), while
// the transport's own post-handshake checks close an accepted probe — which
// is no member — with an application error.
func requireProbe(t *testing.T, addr string, probe *config.MTLSConfig, trust []byte, accept bool, what string) {
	t.Helper()
	cert, err := tls.X509KeyPair(probe.Cert, probe.Key)
	if err != nil {
		t.Fatalf("%s: probe keypair: %v", what, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(trust) {
		t.Fatalf("%s: probe trust bundle", what)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   "127.0.0.1",
		NextProtos:   []string{"quicraft"},
		MinVersion:   tls.VersionTLS13,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, addr, cfg, &quic.Config{HandshakeIdleTimeout: 3 * time.Second})
	if err == nil {
		defer func() { _ = conn.CloseWithError(0, "") }()
		select {
		case <-conn.Context().Done():
			err = context.Cause(conn.Context())
		case <-time.After(500 * time.Millisecond):
		}
	}
	var te *quic.TransportError
	refused := errors.As(err, &te) && te.ErrorCode.IsCryptoError()
	if err != nil && !refused {
		var ae *quic.ApplicationError
		if !errors.As(err, &ae) {
			t.Fatalf("%s: probe failed outside the handshake: %v", what, err)
		}
	}
	if accept && refused {
		t.Fatalf("%s: handshake refused: %v", what, err)
	}
	if !accept && !refused {
		t.Fatalf("%s: handshake accepted, want a TLS refusal", what)
	}
}
