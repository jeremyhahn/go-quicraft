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
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/test/netport"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// TestNewHost_InboundSnapshotRejectionIsDispatched proves, end to end, that
// NewHost installs the host's listener dispatcher on the transport.
//
// The snapshot receiver fires OnSnapshotRejected from the goroutine that owns
// the inbound stream. Without the dispatcher that call is inline, so a listener
// that blocks — or calls a synchronous Host API — holds the receive goroutine,
// its snapshot receive slot and the transport's shutdown with it. The receiver
// falls back to inline delivery when no dispatcher is installed, so nothing
// short of a real host receiving a real transfer shows the wiring is there.
//
// Host A runs shard 1 with B as a member, which gives A the route and identity
// binding to send B a snapshot. B never starts shard 1, so it refuses the
// transfer from its header ("shard not loaded") and reports the rejection. The
// listener blocks until the test releases it; while it is blocked, B's
// transport must be able to stop, which it does only once every receive
// goroutine has returned.
func TestNewHost_InboundSnapshotRejectionIsDispatched(t *testing.T) {
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 2)
	addrs := []string{netport.MustReserveUDP(t.Fatalf), netport.MustReserveUDP(t.Fatalf)}

	hostConfig := func(i int) config.HostConfig {
		cfg := testHostConfig(t)
		cfg.RaftAddress = addrs[i]
		cfg.ListenAddress = addrs[i]
		cfg.TransportConfig.MTLSConfig = leaves[i]
		return cfg
	}

	rejected := make(chan config.SnapshotInfo, 8)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }

	cfgB := hostConfig(1)
	cfgB.EventListener = &config.EventListener{
		OnSnapshotRejected: func(info config.SnapshotInfo) {
			rejected <- info
			<-release
		},
	}
	b, err := NewHost(cfgB, WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost(B): %v", err)
	}
	// Registered first so it runs last: the listener must be free before B's
	// Close drains the dispatcher, including when an assertion below fails.
	t.Cleanup(func() { _ = b.Close() })
	t.Cleanup(unblock)

	a, err := NewHost(hostConfig(0), WithMemoryLogDB(), WithoutEncryption())
	if err != nil {
		t.Fatalf("NewHost(A): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	members := map[uint64]string{1: addrs[0], 2: addrs[1]}
	scfg := testShardConfig(1, 1)
	scfg.MemberIdentities = map[uint64]string{1: a.NodeIdentity(), 2: b.NodeIdentity()}
	if startErr := a.StartShard(members, false, testCreateFunc, scfg); startErr != nil {
		t.Fatalf("StartShard(A): %v", startErr)
	}

	chunks := []proto.SnapshotChunk{{
		BinVer:       proto.WireVersion,
		DeploymentID: 1,
		ShardID:      1,
		ReplicaID:    2,
		From:         1,
		Index:        10,
		Term:         1,
		ChunkCount:   1,
		ChunkSize:    3,
		Data:         []byte("abc"),
	}}
	// The receiver refuses from the header, so the send may or may not see
	// the stream torn down before its chunk is written; only the receiver's
	// side is under test.
	_ = a.transport.SendSnapshot(chunks)

	select {
	case info := <-rejected:
		if info.ShardID != 1 || info.ReplicaID != 2 || info.Index != 10 {
			t.Fatalf("OnSnapshotRejected reported %+v, want shard 1 replica 2 index 10", info)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnSnapshotRejected never fired for a transfer to a shard B does not run")
	}

	// The listener is still blocked. Stop returns only after every receive
	// goroutine has exited; an inline listener would be holding one of them.
	stopped := make(chan struct{})
	go func() {
		_ = b.transport.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		unblock()
		<-stopped
		t.Fatal("B's transport could not stop while OnSnapshotRejected was blocked: " +
			"the listener ran on the receive goroutine, so NewHost did not install " +
			"the listener dispatcher on the transport")
	}
}
