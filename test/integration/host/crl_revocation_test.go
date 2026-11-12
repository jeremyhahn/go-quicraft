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
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// crlCheckInterval is the CRL reload period of every host in the CRL test:
// short, so a CRL written to disk takes effect within the test's budget.
const crlCheckInterval = 200 * time.Millisecond

// revokedSerials records the serials one host's OnCertRevoked reports.
type revokedSerials struct {
	mu      sync.Mutex
	serials []string
}

func (r *revokedSerials) record(serial string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.serials = append(r.serials, serial)
}

func (r *revokedSerials) has(serial string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.serials, serial)
}

// TestHostAPI_CRLRevocation_ThreeNode drives the documented CRL revocation
// (config.RevocationConfig: CRLPaths, CheckInterval, Mode "crl",
// OnCertRevoked) across three live hosts. Every host starts from an empty
// CRL and a shard commits. Host 3 stops; a CRL revoking its leaf is written to
// every host's CRL file and the test waits past one CheckInterval. Host 3
// then restarts on its directories, address and leaf: hosts 1 or 2 must
// report its serial (lowercase hex) through OnCertRevoked, host 3 must never
// apply a write made after its restart, and a fresh handshake presenting its
// leaf must be refused by hosts 1 and 2. A proposer running from before the
// stop to the end proves hosts 1 and 2 keep committing throughout.
func TestHostAPI_CRLRevocation_ThreeNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const (
		numNodes        = 3
		shardID  uint64 = 1
		victim          = 2 // host 3's index
	)
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, numNodes)
	// OnCertRevoked reports a serial as lowercase hex.
	block, _ := pem.Decode(leaves[victim].Cert)
	if block == nil {
		t.Fatal("host 3 leaf: no PEM certificate")
	}
	victimCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("host 3 leaf: parse: %v", err)
	}
	victimSerial := victimCert.SerialNumber.Text(16)

	// (a) Three hosts, one CRL file each, starting from an empty CRL.
	crlPaths := make([]string, numNodes)
	for i := range crlPaths {
		crlPaths[i] = filepath.Join(t.TempDir(), fmt.Sprintf("host-%d.crl", i+1))
	}
	writeCRLFiles(ctx, t, crlPaths, ca.CRL(t))

	reports := make([]*revokedSerials, numNodes)
	hostConfig := func(i int, nc nodeConfig) config.HostConfig {
		hc := hostConfigAt(nc.walDir, nc.nodeHostDir, nc.addr, leaves[i])
		hc.TransportConfig.RevocationConfig = &config.RevocationConfig{
			CRLPaths:      []string{crlPaths[i]},
			CheckInterval: crlCheckInterval,
			Mode:          "crl",
			OnCertRevoked: reports[i].record,
		}
		return hc
	}

	hosts := make([]*quicraft.Host, numNodes)
	addrs := make([]string, numNodes)
	configs := make([]nodeConfig, numNodes)
	nodeHostDirs := make([]string, numNodes)
	for i := range numNodes {
		reports[i] = &revokedSerials{}
		configs[i] = nodeConfig{walDir: t.TempDir(), nodeHostDir: t.TempDir(), addr: kernelAddr}
		h, err := quicraft.NewHost(hostConfig(i, configs[i]), quicraft.WithoutEncryption())
		if err != nil {
			for j := range i {
				hosts[j].Close()
			}
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		hosts[i] = h
		addrs[i] = h.RaftAddress()
		configs[i].addr = addrs[i]
		nodeHostDirs[i] = configs[i].nodeHostDir
	}
	cluster := &testCluster{
		hosts:        hosts,
		addrs:        addrs,
		stopped:      make([]atomic.Bool, numNodes),
		nodeHostDirs: nodeHostDirs,
		ca:           ca,
		leaves:       leaves,
		ids:          nodeIdentities(hosts),
		t:            t,
	}
	t.Cleanup(func() { cluster.close() })

	trackers := cluster.startShard(shardID)
	leader := cluster.waitForClusterLeader(shardID, leaderElectionTimeout)
	proposeAndWait(t, cluster, trackers, shardID, "crl-warmup", "ok")
	if leader == victim {
		transferLeadershipTo(t, cluster, shardID, 0, leaderElectionTimeout)
	}
	for i, r := range reports {
		if r.has(victimSerial) {
			t.Fatalf("host %d reported serial %s revoked under an empty CRL", i+1, victimSerial)
		}
	}

	// (d) The proposer runs from before the stop to the end: every proposal
	// to hosts 1-2 must commit.
	var (
		done      sync.WaitGroup
		committed atomic.Int64
		failures  atomic.Int64
		lastErr   atomic.Value
		lastKey   atomic.Value
	)
	proposerCtx, stopProposer := context.WithCancel(ctx)
	defer stopProposer()
	done.Add(1)
	go func() {
		defer done.Done()
		for i := 0; proposerCtx.Err() == nil; i++ {
			key := fmt.Sprintf("crl-%05d", i)
			if _, err := tryProposeToLeader(cluster, shardID, encodeKVPut(key, "v"), defaultTimeout); err != nil {
				failures.Add(1)
				lastErr.Store(err.Error())
				continue
			}
			lastKey.Store(key)
			committed.Add(1)
		}
	}()
	waitCommits := func(step string) {
		t.Helper()
		before := committed.Load()
		waitForCondition(t, defaultTimeout, step+": hosts 1-2 keep committing", func() bool {
			return committed.Load() > before+5
		})
	}
	waitCommits("before the stop")

	// (b) Host 3 stops; a CRL revoking its leaf reaches every CRL file; wait
	// past one CheckInterval so every running host has reloaded it.
	cluster.stopNode(victim)
	waitCommits("host 3 stopped")
	writeCRLFiles(ctx, t, crlPaths, ca.CRL(t, leaves[victim]))
	time.Sleep(2 * crlCheckInterval)
	waitCommits("CRL revoking host 3 written")

	// (c) Host 3 restarts on the same directories, address and leaf.
	h, err := quicraft.NewHost(hostConfig(victim, configs[victim]), quicraft.WithoutEncryption())
	if err != nil {
		t.Fatalf("restart host 3: NewHost: %v", err)
	}
	members := make(map[uint64]string, numNodes)
	for i, addr := range addrs {
		members[uint64(i+1)] = addr
	}
	createFn, victimTracker := kvCreateFunc()
	scfg := testShardConfig(shardID, victim+1)
	scfg.MemberIdentities = cluster.identities()
	if err := h.StartShard(members, false, createFn, scfg); err != nil {
		h.Close()
		t.Fatalf("restart host 3: StartShard: %v", err)
	}
	cluster.hostsMu.Lock()
	cluster.hosts[victim] = h
	cluster.stopped[victim].Store(false)
	cluster.hostsMu.Unlock()

	waitForCondition(t, defaultTimeout, "host 1 or 2 reports host 3's serial through OnCertRevoked", func() bool {
		return reports[0].has(victimSerial) || reports[1].has(victimSerial)
	})
	waitCommits("host 3 restarted")
	afterRestart, _ := lastKey.Load().(string)
	waitCommits("host 3 refused")
	if afterRestart == "" {
		t.Fatal("no write committed after host 3 restarted")
	}
	sm := victimTracker.Get(shardID, victim+1)
	if sm == nil {
		t.Fatalf("restarted host 3: no state machine for shard %d replica %d", shardID, victim+1)
	}
	if _, ok := sm.Get(afterRestart); ok {
		t.Fatalf("revoked host 3 applied %q, a write committed after its restart", afterRestart)
	}
	for i := range victim {
		requireProbe(t, addrs[i], leaves[victim], ca.PEM(), false,
			fmt.Sprintf("host %d refuses a handshake presenting host 3's revoked leaf", i+1))
		requireProbe(t, addrs[i], leaves[1-i], ca.PEM(), true,
			fmt.Sprintf("host %d accepts a handshake presenting an unrevoked leaf", i+1))
	}

	stopProposer()
	done.Wait()
	if n := failures.Load(); n != 0 {
		t.Fatalf("%d proposals to hosts 1-2 failed (last: %v)", n, lastErr.Load())
	}
	t.Logf("CRL revocation: %d proposals committed, none failed; serial %s refused", committed.Load(), victimSerial)
}

// writeCRLFiles writes crl to every path, each by a rename so a checker never
// reads a half-written file.
func writeCRLFiles(ctx context.Context, t *testing.T, paths []string, crl []byte) {
	t.Helper()
	for _, p := range paths {
		if err := ctx.Err(); err != nil {
			t.Fatalf("write CRL %s: %v", p, err)
		}
		tmp := p + ".tmp"
		if err := os.WriteFile(tmp, crl, 0o600); err != nil {
			t.Fatalf("write CRL %s: %v", tmp, err)
		}
		if err := os.Rename(tmp, p); err != nil {
			t.Fatalf("rename CRL %s: %v", p, err)
		}
	}
}
