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
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// ---------------------------------------------------------------------------
// Two real hosts on QUIC, each on a wire window. On one window they elect
// and replicate. When one host emulates a build whose window excludes the
// other's, neither side's traffic is admitted, no leader is elected, and the
// Raft layer refuses the peer's messages with a *WireVersionMismatchError
// naming both versions.
// ---------------------------------------------------------------------------

// farBuildWire is a build two wire versions ahead: its window shares no
// member with this build's.
func farBuildWire() transport.WireWindow {
	return transport.WireWindow{
		Proto:         proto.WireWindow{Current: proto.WireVersion + 2, Previous: proto.WireVersion + 1},
		FrameCurrent:  transport.WireVersion + 2,
		FramePrevious: transport.WireVersion + 1,
	}
}

const wwShard uint64 = 1

type wwNode struct {
	addr string
	host *Host
}

type wwCluster struct {
	t     *testing.T
	nodes []*wwNode
}

// newWireWindowPair starts one host per window (the zero window is this
// build's), each on its own directories, address and leaf, and starts the
// shard on every one of them.
func newWireWindowPair(t *testing.T, wires ...transport.WireWindow) *wwCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, len(wires))
	c := &wwCluster{t: t}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.host.Close()
		}
	})
	ids := make(map[uint64]string, len(wires))
	for i, w := range wires {
		cfg := config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  5,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}
		opts := []HostOption{WithoutEncryption()}
		if w != (transport.WireWindow{}) {
			opts = append(opts, withWireWindow(w))
		}
		h, err := NewHost(cfg, opts...)
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		c.nodes = append(c.nodes, &wwNode{addr: h.RaftAddress(), host: h})
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	members := make(map[uint64]string, len(c.nodes))
	for i, n := range c.nodes {
		members[uint64(i+1)] = n.addr
	}
	for i, n := range c.nodes {
		scfg := testShardConfig(wwShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := n.host.StartShard(members, false, testCreateFunc, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

// propose commits one entry through whichever replica leads, retrying across
// an election.
func (c *wwCluster) propose(tag string) {
	c.t.Helper()
	var lastErr error
	if waitUntil(20*time.Second, func() bool {
		for _, n := range c.nodes {
			info, err := n.host.ShardInfo(wwShard)
			if err != nil || !info.IsLeader {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, lastErr = n.host.SyncPropose(ctx, wwShard, []byte(tag))
			cancel()
			if lastErr == nil {
				return true
			}
		}
		return false
	}) {
		return
	}
	c.t.Fatalf("proposal %q never committed: %v", tag, lastErr)
}

func TestWireWindow_SameWindowElectsAndReplicates(t *testing.T) {
	c := newWireWindowPair(t, transport.WireWindow{}, transport.WireWindow{})
	if got := c.nodes[0].host.WireWindow(); got != proto.DefaultWireWindow() {
		t.Fatalf("Host.WireWindow() = %+v, want %+v", got, proto.DefaultWireWindow())
	}
	for i := range 3 {
		c.propose("same-window-" + strconv.Itoa(i))
	}
	for i, n := range c.nodes {
		info, err := n.host.ShardInfo(wwShard)
		if err != nil || info.LeaderID == 0 {
			t.Fatalf("host %d knows no leader after replicating (info %+v, err %v)", i+1, info, err)
		}
	}
}

func TestWireWindow_PeerOutsideWindowIsRefused(t *testing.T) {
	far := farBuildWire()
	c := newWireWindowPair(t, transport.WireWindow{}, far)
	b := c.nodes[1].host
	if got := b.WireWindow(); got != far.Proto {
		t.Fatalf("emulating host's WireWindow() = %+v, want %+v", got, far.Proto)
	}

	// Kept: the absence of a leader over this span is the behaviour under
	// test. At a 5 ms RTT many election rounds pass in it.
	time.Sleep(2 * time.Second)
	for i, n := range c.nodes {
		if info, err := n.host.ShardInfo(wwShard); err == nil && info.LeaderID != 0 {
			t.Fatalf("host %d elected leader %d across windows that share no version", i+1, info.LeaderID)
		}
	}

	// Each host's INSTALLED handler, fed only by the traffic its transport
	// actually received from the other, must have recorded a refusal that
	// names the peer's version and this host's own window. Nothing is
	// injected: the far build's frames are refused on the receive path.
	windows := []transport.WireWindow{transport.DefaultWireWindow(), far}
	for i, n := range c.nodes {
		local, peer := windows[i], windows[1-i]
		h := n.host.msgHandler
		if h == nil {
			t.Fatalf("host %d has no installed message handler", i+1)
		}
		var refusal *WireVersionMismatchError
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
			if refusal = h.receiveWireRefusal(); refusal != nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if refusal == nil {
			t.Fatalf("host %d recorded no wire-version refusal from the traffic it received", i+1)
		}
		var err error = refusal
		if !errors.Is(err, proto.ErrUnsupportedVersion) {
			t.Fatalf("host %d: errors.Is(%v, ErrUnsupportedVersion) = false", i+1, err)
		}
		var want []uint64
		if refusal.Frame {
			if !peer.AcceptsFrame(refusal.PeerFrameVersion) {
				t.Fatalf("host %d: refused frame version %d is not the peer's (%+v)",
					i+1, refusal.PeerFrameVersion, peer)
			}
			if refusal.LocalFrameCurrent != local.FrameCurrent || refusal.LocalFramePrevious != local.FramePrevious {
				t.Fatalf("host %d: refusal names frame window {%d, %d}, want {%d, %d}", i+1,
					refusal.LocalFrameCurrent, refusal.LocalFramePrevious, local.FrameCurrent, local.FramePrevious)
			}
			want = []uint64{uint64(refusal.PeerFrameVersion), uint64(local.FrameCurrent), uint64(local.FramePrevious)}
		} else {
			if !peer.Proto.Accepts(refusal.PeerBinVer) {
				t.Fatalf("host %d: refused batch version %d is not the peer's (%+v)", i+1, refusal.PeerBinVer, peer.Proto)
			}
			if refusal.Local != local.Proto {
				t.Fatalf("host %d: refusal names window %+v, want %+v", i+1, refusal.Local, local.Proto)
			}
			want = []uint64{refusal.PeerBinVer, local.Proto.Current, local.Proto.Previous}
		}
		msg := err.Error()
		for _, v := range want {
			if !strings.Contains(msg, strconv.FormatUint(v, 10)) {
				t.Fatalf("host %d: refusal %q does not name version %d", i+1, msg, v)
			}
		}
		if refusal.Source == "" {
			t.Fatalf("host %d: refusal %q names no source peer", i+1, msg)
		}
	}
}
