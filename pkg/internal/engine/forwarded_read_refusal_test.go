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
	"fmt"
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
	var failure error
	msg := fmt.Sprintf("read %d was neither served nor failed within %v: the caller would block forever", key, refusalWatchdog)
	waitFor(t, refusalWatchdog, 5*time.Millisecond, msg, func() bool {
		for _, call := range cb.getReadIndexFailedCalls() {
			for _, k := range call.keys {
				if k == key {
					failure = call.err
					return true
				}
			}
		}
		cb.mu.Lock()
		defer cb.mu.Unlock()
		for _, call := range cb.readyToReadKeys {
			for _, rtr := range call.readyToRead {
				if rtr.Key == key {
					t.Fatalf("read %d was served at index %d by a leader that refused it", key, rtr.Index)
				}
			}
		}
		return false
	})
	return failure
}

// TestForwardedRead_NewLeaderNotReady_FailsFast verifies a follower read fails with ErrReadIndexNotReady before the new leader's first commit.
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
	waitFor(t, 10*time.Second, 5*time.Millisecond, fmt.Sprintf("follower %d never learned leader %d", follower, leader), func() bool {
		l, _ := fnode.LeaderAndTerm()
		return l == leader
	})
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

// TestForwardedRead_RefusedUnderCommitPending_IsAnswered verifies the leader answers a refused forwarded read while commitPending is set.
func TestForwardedRead_RefusedUnderCommitPending_IsAnswered(t *testing.T) {
	const shardID = 43
	c, cbs := newCallbackCluster(t, shardID)
	c.router.setDrop(func(m proto.Message) bool { return m.Type == proto.Replicate })

	leader := c.waitForLeader(t, 10*time.Second)
	if leader == 0 {
		t.Fatal("no leader elected")
	}
	follower := leader%3 + 1
	fnode := c.replicas[follower].node
	lnode := c.replicas[leader].node
	waitFor(t, 10*time.Second, 5*time.Millisecond, fmt.Sprintf("follower %d never learned leader %d", follower, leader), func() bool {
		l, _ := fnode.LeaderAndTerm()
		return l == leader
	})

	// Hold the leader in commitPending, claiming the flag only from an unowned state and retrying when another owner lowers it.
	t.Cleanup(func() {
		lnode.commitPending.Store(false)
		c.replicas[leader].engine.NotifyWork(shardID)
	})
	const baseKey = 0x376
	answered := false
	for attempt := range uint64(20) {
		waitFor(t, 10*time.Second, time.Millisecond, "fixture: the leader's hand-offs never left commitPending unowned", func() bool {
			return lnode.commitPending.CompareAndSwap(false, true)
		})
		// Kept: a window in which an in-flight hand-off usually completes, so
		// most attempts that do not own the flag are discarded before a read.
		time.Sleep(50 * time.Millisecond)
		if !lnode.commitPending.Load() {
			continue
		}
		if got := lnode.CommittedIndex(); got != 0 {
			t.Fatalf("fixture: the leader committed index %d with every Replicate dropped", got)
		}

		// Delivered to the leader as the transport would: a follower that has
		// not seen the term commit refuses its own reads locally, so the
		// leader's refusal is reached by a read relayed across a leader change.
		_, term := lnode.LeaderAndTerm()
		key := baseKey + attempt
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
		if lnode.commitPending.Load() {
			answered = true
			break
		}
		// The flag was lowered by a hand-off this attempt did not own; the
		// reject may have gone out through processReady. Try again.
	}
	if !answered {
		t.Fatal("fixture: no attempt held the leader in commitPending until the refusal arrived")
	}
	if got := lnode.CommittedIndex(); got != 0 {
		t.Fatalf("the leader committed index %d while held; the refusal proves nothing", got)
	}
}

// TestForwardedRead_LeaderRefusalIsAnswered verifies a leader mid-transfer rejects a forwarded read and the follower fails it with ErrReadIndexNotReady.
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
	waitFor(t, 10*time.Second, 5*time.Millisecond, "leader never committed its first entry", func() bool {
		return lnode.CommittedIndex() != 0 && lnode.Peer().IsLeader()
	})
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
