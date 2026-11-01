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
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/transport"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// TestMembersAdvertise pins the leader's switch precondition: every member
// other than itself — voter, non-voting member and witness alike — last
// advertised at least the target, and a member never heard from counts as
// not upgraded.
func TestMembersAdvertise(t *testing.T) {
	target := proto.WireVersion + 1
	m := proto.Membership{
		Addresses: map[uint64]string{1: "a1", 2: "a2"},
		Observers: map[uint64]string{3: "a3"},
		Witnesses: map[uint64]string{4: "a4"},
	}
	ns := &nodeState{replicaID: 1, peerWire: map[uint64]uint64{2: target, 3: target}}
	if ns.membersAdvertise(m, target) {
		t.Fatal("the witness was never heard from, yet the precondition held")
	}

	ns.peerWire[4] = target
	if !ns.membersAdvertise(m, target) {
		t.Fatal("every other member advertised the target, yet the precondition failed")
	}

	// The latest value counts: a member rolled back stops counting.
	ns.peerWire[2] = target - 1
	if ns.membersAdvertise(m, target) {
		t.Fatal("a member rolled back to the previous build still counted as upgraded")
	}
}

// TestWireSwitchDisabledOnSingleVersionWindow pins that a build whose window
// holds one version — every build until the next wire version exists —
// neither records advertisements nor proposes.
func TestWireSwitchDisabledOnSingleVersionWindow(t *testing.T) {
	h := &Host{}
	h.wire.Proto = proto.DefaultWireWindow()
	if h.wireSwitchEnabled() && proto.WireVersion == proto.PreviousWireVersion {
		t.Fatal("a single-version window enabled the wire-version switch")
	}
}

// TestWithWireWindow_ReachesHost pins the test-only option that gives
// a host the next build's window: the host holds it and enables the switch.
func TestWithWireWindow_ReachesHost(t *testing.T) {
	next := transport.WireWindow{
		Proto:         proto.WireWindow{Current: proto.WireVersion + 1, Previous: proto.WireVersion},
		FrameCurrent:  transport.WireVersion + 1,
		FramePrevious: transport.WireVersion,
	}
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption(),
		withWireWindow(next))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	if h.wire != next || !h.wireSwitchEnabled() {
		t.Fatalf("host window = %+v, enabled=%v; want %+v, enabled", h.wire, h.wireSwitchEnabled(), next)
	}
}

// TestWireSwitch_SingleMemberShardSwitches pins that a shard with no other
// member, which never receives an advertisement, still switches: its leader
// proposes once it is ready.
func TestWireSwitch_SingleMemberShardSwitches(t *testing.T) {
	next := transport.WireWindow{
		Proto:         proto.WireWindow{Current: proto.WireVersion + 1, Previous: proto.WireVersion},
		FrameCurrent:  transport.WireVersion + 1,
		FramePrevious: transport.WireVersion,
	}
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption(),
		withWireWindow(next))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	// No waitForLeader: it overwrites the host's cached term with 0, which
	// makes the leader-ready mark, and so this trigger, refuse the real pair.
	deadline := time.Now().Add(10 * time.Second)
	for h.engine.GetNode(1).Peer().WireVersion() != next.Proto.Current {
		if time.Now().After(deadline) {
			t.Fatalf("single-member shard at WireVersion %d, never switched to %d",
				h.engine.GetNode(1).Peer().WireVersion(), next.Proto.Current)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// withWireSwitchFault refuses each wire-version switch proposal with the
// error fault returns, before it reaches the shard.
func withWireSwitchFault(fault func() error) HostOption {
	return func(o *hostOptions) {
		o.wireSwitchFault = fault
	}
}

// startSingleMemberNextWire starts a one-member shard on a host one wire
// version ahead of this build, with fault refusing switch proposals.
func startSingleMemberNextWire(t *testing.T, fault func() error) (*Host, uint64) {
	t.Helper()
	next := transport.WireWindow{
		Proto:         proto.WireWindow{Current: proto.WireVersion + 1, Previous: proto.WireVersion},
		FrameCurrent:  transport.WireVersion + 1,
		FramePrevious: transport.WireVersion,
	}
	h, err := NewHost(testHostConfig(t), WithMemoryLogDB(), WithoutTransport(), WithoutEncryption(),
		withWireWindow(next), withWireSwitchFault(fault))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { h.Close() })
	if err := h.StartShard(map[uint64]string{1: "127.0.0.1:5000"}, false, testCreateFunc, testShardConfig(1, 1)); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	return h, next.Proto.Current
}

// TestWireSwitch_SingleMemberRetriesAfterRefusal pins that a one-member
// shard, which no peer batch ever prompts, still switches when its leader's
// first proposal is refused.
func TestWireSwitch_SingleMemberRetriesAfterRefusal(t *testing.T) {
	var attempts atomic.Int32
	h, next := startSingleMemberNextWire(t, func() error {
		if attempts.Add(1) == 1 {
			return ErrSystemBusy
		}
		return nil
	})
	deadline := time.Now().Add(10 * time.Second)
	for h.engine.GetNode(1).Peer().WireVersion() != next {
		if time.Now().After(deadline) {
			t.Fatalf("single-member shard at WireVersion %d after %d attempts, never switched to %d",
				h.engine.GetNode(1).Peer().WireVersion(), attempts.Load(), next)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := attempts.Load(); n < 2 {
		t.Fatalf("switched after %d attempts, want a retry after the refusal", n)
	}
}

// TestWireSwitch_RetryStopsOnLeadershipLossAndClose pins the retry's bounds:
// it does not run for a term this replica no longer leads in, and Close
// joins it, after which no attempt is made.
func TestWireSwitch_RetryStopsOnLeadershipLossAndClose(t *testing.T) {
	var attempts atomic.Int32
	h, _ := startSingleMemberNextWire(t, func() error {
		attempts.Add(1)
		return ErrSystemBusy
	})
	deadline := time.Now().Add(10 * time.Second)
	for attempts.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the leader never attempted the switch")
		}
		time.Sleep(5 * time.Millisecond)
	}

	ns, err := h.getNode(1)
	if err != nil {
		t.Fatalf("getNode: %v", err)
	}
	done := make(chan struct{})
	h.wireSwitchWG.Add(1)
	go func() {
		h.retryWireSwitch(ns, ns.term.Load()+1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("retry kept running for a term this replica does not lead")
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	after := attempts.Load()
	time.Sleep(wireSwitchRetryInterval + 200*time.Millisecond)
	if n := attempts.Load(); n != after {
		t.Fatalf("%d switch attempts after Close returned", n-after)
	}
}
