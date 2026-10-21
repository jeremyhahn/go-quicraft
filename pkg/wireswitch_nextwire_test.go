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

//go:build quicraft_nextwire

package quicraft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// ---------------------------------------------------------------------------
// Two builds on one wire: a rolling upgrade to the next wire version.
//
// The next build is this build with its window moved one version ahead
// (current = WireVersion+1, previous = WireVersion), which is exactly the
// window the release that bumps the wire version will ship. Three real hosts
// on QUIC start on this build's window and are restarted one at a time onto
// the next build's, on their own directories, address and leaf. While any
// replica runs the old build the shard must stay on the previous version;
// once all three run the new build the leader must commit the switch, and
// the shard must keep committing afterwards.
// ---------------------------------------------------------------------------

const nextWireShard uint64 = 1

func nextBuildWire() transport.WireWindow {
	return transport.WireWindow{
		Proto:         proto.WireWindow{Current: proto.WireVersion + 1, Previous: proto.WireVersion},
		FrameCurrent:  transport.WireVersion + 1,
		FramePrevious: transport.WireVersion,
	}
}

type nextWireNode struct {
	walDir, nodeHostDir, addr string
	leaf                      *config.MTLSConfig
	host                      *Host
}

type nextWireCluster struct {
	t     *testing.T
	nodes []*nextWireNode
	ids   map[uint64]string
}

func nextWireHostConfig(n *nextWireNode) config.HostConfig {
	return config.HostConfig{
		WALDir:         n.walDir,
		NodeHostDir:    n.nodeHostDir,
		RaftAddress:    n.addr,
		ListenAddress:  n.addr,
		RTTMillisecond: 5,
		DeploymentID:   1,
		NoSyncWAL:      true,
		TransportConfig: config.TransportConfig{
			MTLSConfig: n.leaf,
		},
	}
}

func newNextWireCluster(t *testing.T) *nextWireCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &nextWireCluster{t: t, ids: make(map[uint64]string, 3)}
	for i := range 3 {
		n := &nextWireNode{walDir: t.TempDir(), nodeHostDir: t.TempDir(), addr: "127.0.0.1:0", leaf: leaves[i]}
		h, err := NewHost(nextWireHostConfig(n), WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		n.addr, n.host = h.RaftAddress(), h
		c.ids[uint64(i+1)] = h.NodeIdentity()
		c.nodes = append(c.nodes, n)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			if n.host != nil {
				n.host.Close()
			}
		}
	})
	return c
}

func (c *nextWireCluster) members() map[uint64]string {
	m := make(map[uint64]string, len(c.nodes))
	for i, n := range c.nodes {
		m[uint64(i+1)] = n.addr
	}
	return m
}

func (c *nextWireCluster) startShard(i int) {
	c.t.Helper()
	scfg := testShardConfig(nextWireShard, uint64(i+1))
	scfg.MemberIdentities = c.ids
	if err := c.nodes[i].host.StartShard(c.members(), false, testCreateFunc, scfg); err != nil {
		c.t.Fatalf("StartShard[%d]: %v", i, err)
	}
}

// upgrade restarts node i on the next build's window.
func (c *nextWireCluster) upgrade(i int) {
	c.t.Helper()
	n := c.nodes[i]
	n.host.Close()
	n.host = nil
	h, err := NewHost(nextWireHostConfig(n), WithoutEncryption(), withWireWindow(nextBuildWire()))
	if err != nil {
		c.t.Fatalf("upgrade NewHost[%d]: %v", i, err)
	}
	n.host = h
	c.startShard(i)
}

// committed is the WireVersion every replica has applied, or false while the
// replicas disagree.
func (c *nextWireCluster) committed() (uint64, bool) {
	var v uint64
	for i, n := range c.nodes {
		eng := n.host.engine.GetNode(nextWireShard)
		if eng == nil || eng.Peer() == nil {
			return 0, false
		}
		got := eng.Peer().WireVersion()
		if i > 0 && got != v {
			return 0, false
		}
		v = got
	}
	return v, true
}

// propose commits one entry through whichever replica leads, retrying across
// an election.
func (c *nextWireCluster) propose(tag string) {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			info, err := n.host.ShardInfo(nextWireShard)
			if err != nil || !info.IsLeader {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, lastErr = n.host.SyncPropose(ctx, nextWireShard, []byte(tag))
			cancel()
			if lastErr == nil {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("proposal %q never committed: %v", tag, lastErr)
}

func TestNextWire_RollingUpgradeSwitchesOnlyOnceEveryReplicaRunsIt(t *testing.T) {
	c := newNextWireCluster(t)
	for i := range c.nodes {
		c.startShard(i)
	}
	c.propose("before")

	previous := proto.WireVersion
	for i := range 2 {
		c.upgrade(i)
		c.propose(fmt.Sprintf("mixed-%d", i))
		// Give an upgraded leader time to see every advertisement and, if it
		// wrongly would, propose: a switch while a replica runs the old build
		// would cut it off.
		time.Sleep(3 * wireSwitchRetryInterval)
		for j, n := range c.nodes {
			if v := n.host.engine.GetNode(nextWireShard).Peer().WireVersion(); v != previous {
				t.Fatalf("after upgrading %d of 3, replica %d committed WireVersion %d, want %d",
					i+1, j+1, v, previous)
			}
		}
	}

	c.upgrade(2)
	target := proto.WireVersion + 1
	deadline := time.Now().Add(30 * time.Second)
	for {
		if v, ok := c.committed(); ok && v == target {
			break
		}
		if time.Now().After(deadline) {
			v, _ := c.committed()
			t.Fatalf("every replica runs the next build, yet the shard never switched to %d (at %d)", target, v)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for i := range 3 {
		c.propose(fmt.Sprintf("after-%d", i))
	}
}
