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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// ---------------------------------------------------------------------------
// FollowerRead on a real follower (#924).
//
// Three real hosts on the QUIC transport run one shard of the in-memory KV
// state machine. FollowerRead is documented to wait until the local state
// machine has applied up to this replica's committed index as of the start
// of the read, so a read never reflects less than what the replica already
// knows is committed. A follower learns a commit on the next append or
// heartbeat, so each assertion first waits until the follower's committed
// index covers the acknowledged write, then reads.
// ---------------------------------------------------------------------------

const followerReadShard uint64 = 1

type followerReadCluster struct {
	hosts []*Host
}

func newFollowerReadCluster(t *testing.T) *followerReadCluster {
	t.Helper()
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, 3)
	c := &followerReadCluster{}
	addrs := make(map[uint64]string, 3)
	ids := make(map[uint64]string, 3)
	t.Cleanup(func() {
		for _, h := range c.hosts {
			h.Close()
		}
	})
	for i := range 3 {
		h, err := NewHost(config.HostConfig{
			WALDir:          t.TempDir(),
			NodeHostDir:     t.TempDir(),
			RaftAddress:     "127.0.0.1:0",
			ListenAddress:   "127.0.0.1:0",
			RTTMillisecond:  5,
			DeploymentID:    1,
			NoSyncWAL:       true,
			TransportConfig: config.TransportConfig{MTLSConfig: leaves[i]},
		}, WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		c.hosts = append(c.hosts, h)
		addrs[uint64(i+1)] = h.RaftAddress()
		ids[uint64(i+1)] = h.NodeIdentity()
	}
	for i, h := range c.hosts {
		scfg := testShardConfig(followerReadShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := h.StartShard(addrs, false, kv.NewMemoryCreateFunc(), scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	return c
}

// leader returns the index into hosts of a replica reporting itself leader,
// or -1 when none does.
func (c *followerReadCluster) leader() int {
	for i, h := range c.hosts {
		if info, err := h.ShardInfo(followerReadShard); err == nil && info.IsLeader {
			return i
		}
	}
	return -1
}

// committed returns the commit index replica i knows of.
func (c *followerReadCluster) committed(i int) uint64 {
	n := c.hosts[i].engine.GetNode(followerReadShard)
	if n == nil {
		return 0
	}
	return n.CommittedIndex()
}

// tryPut writes key=value once through the current leader. On success it
// returns that leader and its commit index after the acknowledgement, which
// covers the write.
func (c *followerReadCluster) tryPut(key, value string) (int, uint64, error) {
	cmd, err := kv.EncodePut([]byte(key), []byte(value))
	if err != nil {
		return -1, 0, err
	}
	li := c.leader()
	if li < 0 {
		return -1, 0, ErrNotLeader
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.hosts[li].SyncPropose(ctx, followerReadShard, cmd); err != nil {
		return -1, 0, err
	}
	return li, c.committed(li), nil
}

// put writes key=value, retrying across an election, and fails the test if
// it never commits.
func (c *followerReadCluster) put(t *testing.T, key, value string) (int, uint64) {
	t.Helper()
	var li int
	var index uint64
	var lastErr error
	if waitUntil(20*time.Second, func() bool {
		li, index, lastErr = c.tryPut(key, value)
		return lastErr == nil
	}) {
		return li, index
	}
	t.Fatalf("put %q=%q never committed: %v", key, value, lastErr)
	return -1, 0
}

// awaitCommitted waits until replica i knows index is committed.
func (c *followerReadCluster) awaitCommitted(i int, index uint64) bool {
	return waitUntil(10*time.Second, func() bool { return c.committed(i) >= index })
}

// read runs FollowerRead for key on replica i and returns the value.
func (c *followerReadCluster) read(i int, key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	v, err := c.hosts[i].FollowerRead(ctx, followerReadShard, []byte(key))
	if err != nil {
		return "", err
	}
	b, ok := v.([]byte)
	if !ok && v != nil {
		return "", fmt.Errorf("FollowerRead returned %T, want []byte", v)
	}
	return string(b), nil
}

// TestFollowerRead_RealFollowerReturnsEveryWriteCommittedBeforeTheRead
// checks the documented FollowerRead contract on a running three-replica
// shard: on a follower and on the leader the result reflects every write
// the replica knows is committed when the read starts, and a cancelled
// context fails with ErrCanceled.
func TestFollowerRead_RealFollowerReturnsEveryWriteCommittedBeforeTheRead(t *testing.T) {
	c := newFollowerReadCluster(t)
	c.put(t, "warmup", "1")

	t.Run("each put is visible on every follower once it knows the commit", func(t *testing.T) {
		for n := range 50 {
			key, want := fmt.Sprintf("seq-%02d", n), fmt.Sprintf("value-%02d", n)
			li, index := c.put(t, key, want)
			for i := range c.hosts {
				if i == li {
					continue
				}
				if !c.awaitCommitted(i, index) {
					t.Fatalf("put %d: replica %d never learned commit index %d", n, i+1, index)
				}
				got, err := c.read(i, key)
				if err != nil {
					t.Fatalf("put %d: FollowerRead on replica %d: %v", n, i+1, err)
				}
				if got != want {
					t.Fatalf("put %d: FollowerRead on replica %d = %q, want %q (commit index %d known)",
						n, i+1, got, want, index)
				}
			}
		}
	})

	t.Run("reads during writes never return a value older than a known commit", func(t *testing.T) {
		const key = "counter"
		type acked struct {
			value int
			index uint64
		}
		var (
			mu   sync.Mutex
			last acked
		)
		snapshot := func() acked {
			mu.Lock()
			defer mu.Unlock()
			return last
		}

		li, _ := c.put(t, "followers", "1")
		followers := make([]int, 0, 2)
		for i := range c.hosts {
			if i != li {
				followers = append(followers, i)
			}
		}
		if len(followers) != 2 {
			t.Fatalf("expected two followers, have %v (leader %d)", followers, li)
		}

		stop := make(chan struct{})
		var writers, readers sync.WaitGroup
		writers.Add(1)
		go func() {
			defer writers.Done()
			for v := 1; ; {
				select {
				case <-stop:
					return
				default:
				}
				if _, index, err := c.tryPut(key, strconv.Itoa(v)); err == nil {
					mu.Lock()
					last = acked{value: v, index: index}
					mu.Unlock()
					v++
				} else {
					time.Sleep(time.Millisecond)
				}
			}
		}()

		reads := make([]int, len(followers))
		failures := make([][]string, len(followers))
		for r, fi := range followers {
			readers.Add(1)
			go func() {
				defer readers.Done()
				prev := 0
				fail := func(format string, args ...any) {
					failures[r] = append(failures[r], fmt.Sprintf("replica %d: ", fi+1)+fmt.Sprintf(format, args...))
				}
				for len(failures[r]) < 5 {
					select {
					case <-stop:
						return
					default:
					}
					snap := snapshot()
					if snap.value == 0 {
						time.Sleep(time.Millisecond)
						continue
					}
					if !c.awaitCommitted(fi, snap.index) {
						fail("never learned commit index %d of value %d", snap.index, snap.value)
						continue
					}
					got, err := c.read(fi, key)
					if err != nil {
						fail("FollowerRead: %v", err)
						continue
					}
					v, err := strconv.Atoi(got)
					if err != nil {
						fail("FollowerRead = %q, not a written value", got)
						continue
					}
					if v < snap.value {
						fail("FollowerRead = %d, older than %d acknowledged at commit index %d before the read",
							v, snap.value, snap.index)
					}
					if v < prev {
						fail("FollowerRead = %d after an earlier read returned %d", v, prev)
					}
					prev = v
					reads[r]++
				}
			}()
		}

		enough := waitUntil(30*time.Second, func() bool {
			return snapshot().value >= 200
		})
		close(stop)
		writers.Wait()
		readers.Wait()
		for r := range followers {
			for _, f := range failures[r] {
				t.Error(f)
			}
			if reads[r] < 20 {
				t.Errorf("replica %d completed %d checked reads, want at least 20", followers[r]+1, reads[r])
			}
		}
		if !enough {
			t.Errorf("the writer acknowledged only %d writes in 30s", snapshot().value)
		}
		t.Logf("%d writes acknowledged; checked reads per follower %v", snapshot().value, reads)
	})

	t.Run("FollowerRead on the leader returns the latest value", func(t *testing.T) {
		for n := range 20 {
			want := fmt.Sprintf("latest-%02d", n)
			li, _ := c.put(t, "latest", want)
			got, err := c.read(li, "latest")
			if err != nil {
				t.Fatalf("write %d: FollowerRead on leader replica %d: %v", n, li+1, err)
			}
			if got != want {
				t.Fatalf("write %d: FollowerRead on leader replica %d = %q, want %q", n, li+1, got, want)
			}
		}
	})

	t.Run("a cancelled context fails with ErrCanceled", func(t *testing.T) {
		li, _ := c.put(t, "cancel", "1")
		fi := (li + 1) % len(c.hosts)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		v, err := c.hosts[fi].FollowerRead(ctx, followerReadShard, []byte("cancel"))
		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("FollowerRead with a cancelled context on replica %d = (%v, %v), want ErrCanceled", fi+1, v, err)
		}
	})
}
