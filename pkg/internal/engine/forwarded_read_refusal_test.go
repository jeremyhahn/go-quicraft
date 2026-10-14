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

package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// refusalWatchdog bounds how long a refused read may take to fail. A refusal
// arrives within a step cycle or one network hop; anything near this bound is
// the hang the tests below exist to catch.
const refusalWatchdog = 3 * time.Second

// newCallbackCluster starts a three-replica cluster whose replicas each
// report to their own testCallback.
func newCallbackCluster(t *testing.T, shardID uint64) (*testCluster, map[uint64]*testCallback) {
	t.Helper()
	cbs := map[uint64]*testCallback{1: newTestCallback(), 2: newTestCallback(), 3: newTestCallback()}
	c := newTestCluster(t, shardID, 3, withCallback(func(replicaID uint64) Callback {
		return cbs[replicaID]
	}))
	return c, cbs
}

// awaitReadFailure waits for key to be failed on cb, and fails the test if
// it is answered as a successful read or not answered at all.
func awaitReadFailure(t *testing.T, cb *testCallback, key uint64) error {
	t.Helper()
	deadline := time.Now().Add(refusalWatchdog)
	for time.Now().Before(deadline) {
		for _, call := range cb.getReadIndexFailedCalls() {
			for _, k := range call.keys {
				if k == key {
					return call.err
				}
			}
		}
		cb.mu.Lock()
		for _, call := range cb.readyToReadKeys {
			for _, rtr := range call.readyToRead {
				if rtr.Key == key {
					cb.mu.Unlock()
					t.Fatalf("read %d was served at index %d by a leader that refused it", key, rtr.Index)
				}
			}
		}
		cb.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("read %d was neither served nor failed within %v: the caller would block forever", key, refusalWatchdog)
	return nil
}

// TestForwardedRead_NewLeaderNotReady_FailsFast holds a freshly elected
// leader before it commits its first entry of the term (every Replicate is
// dropped; heartbeats still flow, so it stays leader) and issues a read on a
// follower. The read cannot be served until that commit, and nothing ends
// the wait unless the refusal reaches the caller: it must fail with
// ErrReadIndexNotReady, not hang.
func TestForwardedRead_NewLeaderNotReady_FailsFast(t *testing.T) {
	const shardID = 41
	c, cbs := newCallbackCluster(t, shardID)
	c.router.setDrop(func(m proto.Message) bool { return m.Type == proto.Replicate })

	leader := c.waitForLeader(t, 10*time.Second)
	if leader == 0 {
		t.Fatal("no leader elected")
	}
	follower := leader%3 + 1
	fnode := c.replicas[follower].node
	deadline := time.Now().Add(10 * time.Second)
	for {
		if l, _ := fnode.LeaderAndTerm(); l == leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower %d never learned leader %d", follower, leader)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.replicas[leader].node.CommittedIndex(); got != 0 {
		t.Fatalf("fixture: the leader committed index %d with every Replicate dropped", got)
	}

	const key = 0x51b
	if !fnode.DeliverReadIndex(key) {
		t.Fatal("DeliverReadIndex refused")
	}
	c.replicas[follower].engine.NotifyWork(shardID)

	if err := awaitReadFailure(t, cbs[follower], key); !errors.Is(err, raft.ErrReadIndexNotReady) {
		t.Fatalf("read failed with %v, want ErrReadIndexNotReady", err)
	}
}

// TestForwardedRead_LeaderRefusalIsAnswered covers the refusal a read-ready
// follower cannot predict: the leader is mid leadership transfer (to a
// replica that is down, so the transfer aborts and the leader stays leader).
// The leader must answer the forwarded read with a reject, and the follower
// must fail the read with ErrReadIndexNotReady rather than wait for an
// answer that never comes.
func TestForwardedRead_LeaderRefusalIsAnswered(t *testing.T) {
	const shardID = 42
	c, cbs := newCallbackCluster(t, shardID)

	leader := c.waitForLeader(t, 10*time.Second)
	if leader == 0 {
		t.Fatal("no leader elected")
	}
	follower := leader%3 + 1
	target := follower%3 + 1
	lnode := c.replicas[leader].node
	deadline := time.Now().Add(10 * time.Second)
	for lnode.CommittedIndex() == 0 || !lnode.Peer().IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("leader never committed its first entry")
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, term := lnode.LeaderAndTerm()

	c.router.kill(target)
	// The transfer and the forwarded read land in the same inbox batch, so
	// the read is stepped while the transfer is in progress.
	if !lnode.DeliverLocal(proto.Message{Type: proto.LeaderTransfer, From: leader, Hint: target}) {
		t.Fatal("DeliverLocal(LeaderTransfer) refused")
	}
	const key = 0x51c
	if !c.replicas[leader].engine.DeliverMessage(shardID, proto.Message{
		Type:    proto.ReadIndex,
		ShardID: shardID,
		From:    follower,
		To:      leader,
		Term:    term,
		Hint:    key,
	}) {
		t.Fatal("DeliverMessage(ReadIndex) refused")
	}
	c.replicas[leader].engine.NotifyWork(shardID)

	if err := awaitReadFailure(t, cbs[follower], key); !errors.Is(err, raft.ErrReadIndexNotReady) {
		t.Fatalf("read failed with %v, want ErrReadIndexNotReady", err)
	}
}
