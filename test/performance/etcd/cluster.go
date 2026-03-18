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

package etcd_perf

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// clusterTickInterval is the tick interval for cluster node tickers.
// Matches QuicRaft's RTTMillisecond.
const clusterTickInterval = 1 * time.Millisecond

// clusterLeaderTimeout is the maximum time to wait for leader election
// in a multi-node cluster.
const clusterLeaderTimeout = 30 * time.Second

// clusterConfChangeTimeout is the maximum time to wait for initial
// configuration changes to be applied across all nodes.
const clusterConfChangeTimeout = 10 * time.Second

// etcdClusterNode bundles a single node in a multi-node etcd/raft cluster.
// Each node has its own transport, WAL, state machine, and event loop
// goroutine that processes Ready structs and delivers messages via the
// TCP+TLS transport layer.
type etcdClusterNode struct {
	id           uint64
	node         raft.Node
	storage      *raft.MemoryStorage
	wal          *walWriter
	sm           *benchStateMachineEtcd
	transport    *raftTransport
	confDone     *confChangeTracker
	stopCh       chan struct{}
	loopDone     sync.WaitGroup
	tickDone     chan struct{}
	totalApplied atomic.Int64
	stopped      atomic.Bool
}

// EtcdCluster holds a multi-node etcd/raft cluster for benchmarks.
// Nodes communicate via TCP+TLS transport with length-prefixed protobuf
// framing.
type EtcdCluster struct {
	Nodes []*etcdClusterNode
}

// ErrNoLeader indicates that no node in the cluster currently reports
// itself as the raft leader.
type ErrNoLeader struct{}

func (e ErrNoLeader) Error() string {
	return "no leader available in cluster"
}

// clusterElectionTick returns the appropriate ElectionTick value scaled
// by cluster size. Larger clusters need more headroom to avoid spurious
// elections under load, because the leader must complete 2 fsyncs plus
// N-1 serial sends within the election timeout window.
func clusterElectionTick(numNodes int) int {
	switch {
	case numNodes >= 5:
		return 100
	case numNodes >= 3:
		return 50
	default:
		return 10
	}
}

// CurrentLeaderNode polls all nodes' Status() and returns the node
// that currently reports itself as the raft leader. Returns nil and
// ErrNoLeader if no node is currently the leader.
func (c *EtcdCluster) CurrentLeaderNode() (*etcdClusterNode, error) {
	for _, cn := range c.Nodes {
		status := cn.node.Status()
		if status.Lead == cn.id && status.RaftState == raft.StateLeader {
			return cn, nil
		}
	}
	return nil, ErrNoLeader{}
}

// waitForLeader polls all nodes until one reports itself as the current
// leader, returning that node. This is used for dynamic leader
// re-discovery during benchmarks when leadership changes occur.
func (c *EtcdCluster) waitForLeader(timeout time.Duration) (*etcdClusterNode, error) {
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		for _, cn := range c.Nodes {
			status := cn.node.Status()
			if status.Lead == cn.id && status.RaftState == raft.StateLeader {
				return cn, nil
			}
		}
		select {
		case <-deadline.C:
			return nil, ErrNoLeader{}
		case <-ticker.C:
		}
	}
}

// freeTCPPort binds and immediately releases a TCP port on localhost,
// returning the address string. This is used to allocate unique ports
// for each cluster node's transport listener.
func freeTCPPort(b *testing.B) string {
	b.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to find free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// SetupEtcdCluster creates an N-node etcd/raft cluster with TCP+TLS
// transport, WAL persistence, and state machines. It waits for initial
// configuration changes to be applied on all nodes, then waits for
// natural leader election (no Campaign shortcut). A warmup proposal is
// sent to ensure all connections are fully established before returning.
func SetupEtcdCluster(b *testing.B, numNodes int) *EtcdCluster {
	b.Helper()

	// Generate a single shared TLS configuration for all cluster nodes.
	tlsCfg, err := generateSharedTLS()
	if err != nil {
		b.Fatalf("generateSharedTLS failed: %v", err)
	}

	// Allocate unique ports for each node.
	addrs := make(map[uint64]string, numNodes)
	for i := 1; i <= numNodes; i++ {
		addrs[uint64(i)] = freeTCPPort(b)
	}

	electionTick := clusterElectionTick(numNodes)
	nodes := make([]*etcdClusterNode, numNodes)

	for i := 0; i < numNodes; i++ {
		nodeID := uint64(i + 1)

		storage := raft.NewMemoryStorage()
		cfg := &raft.Config{
			ID:              nodeID,
			ElectionTick:    electionTick,
			HeartbeatTick:   1,
			Storage:         storage,
			MaxSizePerMsg:   1024 * 1024,
			MaxInflightMsgs: 256,
			CheckQuorum:     true,
			PreVote:         true,
		}

		peers := make([]raft.Peer, numNodes)
		for j := 0; j < numNodes; j++ {
			peers[j] = raft.Peer{ID: uint64(j + 1)}
		}

		node := raft.StartNode(cfg, peers)

		walDir := b.TempDir()
		wal, walErr := newWALWriter(walDir)
		if walErr != nil {
			node.Stop()
			b.Fatalf("newWALWriter for node %d: %v", nodeID, walErr)
		}

		cn := &etcdClusterNode{
			id:       nodeID,
			node:     node,
			storage:  storage,
			wal:      wal,
			sm:       newBenchStateMachineEtcd(),
			confDone: newConfChangeTrackerWithCount(numNodes),
			stopCh:   make(chan struct{}),
		}

		// Build peer address map excluding self.
		peerAddrs := make(map[uint64]string, numNodes-1)
		for id, addr := range addrs {
			if id != nodeID {
				peerAddrs[id] = addr
			}
		}

		// Create transport with message delivery into the raft node.
		// Capture node in closure to avoid data race.
		localNode := node
		transport, transportErr := newRaftTransport(
			nodeID,
			addrs[nodeID],
			peerAddrs,
			tlsCfg,
			func(msg raftpb.Message) {
				// Deliver received message into the raft node.
				// Errors from Step are expected during shutdown.
				localNode.Step(context.Background(), msg)
			},
		)
		if transportErr != nil {
			node.Stop()
			wal.close()
			b.Fatalf("newRaftTransport for node %d: %v", nodeID, transportErr)
		}
		cn.transport = transport

		// Start event loop that processes Ready structs and sends
		// messages via the transport.
		cn.loopDone.Add(1)
		go func() {
			defer cn.loopDone.Done()
			cn.clusterEventLoop()
		}()

		// Start ticker goroutine that drives raft ticks at 1ms intervals.
		cn.tickDone = make(chan struct{})
		go func() {
			defer close(cn.tickDone)
			ticker := time.NewTicker(clusterTickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					cn.node.Tick()
				case <-cn.stopCh:
					return
				}
			}
		}()

		nodes[i] = cn
	}

	cluster := &EtcdCluster{Nodes: nodes}

	// Wait for all initial ConfChange entries to be applied on every node.
	for _, cn := range nodes {
		if !cn.confDone.wait(clusterConfChangeTimeout) {
			cluster.Close()
			b.Fatalf("initial ConfChange not applied for node %d", cn.id)
		}
	}

	// Wait for leader election via natural randomized timeout.
	_, leaderErr := waitForEtcdClusterLeader(nodes, clusterLeaderTimeout)
	if leaderErr != nil {
		cluster.Close()
		b.Fatalf("cluster leader election failed: %v", leaderErr)
	}

	// Warmup proposal to prime TLS connections and the full pipeline.
	leader, findErr := cluster.CurrentLeaderNode()
	if findErr != nil {
		cluster.Close()
		b.Fatalf("warmup: %v", findErr)
	}

	warmupPayload := make([]byte, 128)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if propErr := leader.node.Propose(ctx, warmupPayload); propErr != nil {
		cancel()
		cluster.Close()
		b.Fatalf("warmup propose failed: %v", propErr)
	}
	cancel()

	if !leader.waitForApplied(1, 10*time.Second) {
		cluster.Close()
		b.Fatalf("warmup apply did not complete")
	}

	return cluster
}

// clusterEventLoop processes Ready structs from the raft node, persisting
// entries and hard state to the WAL, appending to MemoryStorage, sending
// messages to peers via the transport, and applying committed entries
// through the state machine.
func (cn *etcdClusterNode) clusterEventLoop() {
	for {
		select {
		case rd, ok := <-cn.node.Ready():
			if !ok {
				return
			}

			// Persist entries and hard state to WAL with a single fsync.
			hasEntries := len(rd.Entries) > 0
			hasHardState := !raft.IsEmptyHardState(rd.HardState)
			if cn.wal != nil && (hasEntries || hasHardState) {
				if err := cn.wal.persistReady(rd.Entries, rd.HardState); err != nil {
					return
				}
			}

			// Persist hard state to MemoryStorage.
			if hasHardState {
				cn.storage.SetHardState(rd.HardState)
			}

			// Append entries to MemoryStorage for raft internals.
			if hasEntries {
				cn.storage.Append(rd.Entries)
			}

			// Send messages to peers via TCP+TLS transport.
			if len(rd.Messages) > 0 {
				cn.transport.send(rd.Messages)
			}

			// Apply committed entries through the state machine.
			if len(rd.CommittedEntries) > 0 {
				applied := int64(0)
				for _, entry := range rd.CommittedEntries {
					switch entry.Type {
					case raftpb.EntryConfChange:
						var cc raftpb.ConfChange
						if unmarshalErr := cc.Unmarshal(entry.Data); unmarshalErr == nil {
							cn.node.ApplyConfChange(cc)
						}
						cn.confDone.signal()
					case raftpb.EntryNormal:
						if len(entry.Data) > 0 {
							applied++
						}
					}
				}

				// Batch-apply all normal entries through state machine.
				cn.sm.apply(rd.CommittedEntries)

				if applied > 0 {
					cn.totalApplied.Add(applied)
				}
			}

			cn.node.Advance()

		case <-cn.stopCh:
			return
		}
	}
}

// waitForApplied polls the atomic totalApplied counter until it reaches
// or exceeds the target count, or the timeout expires. Returns true if
// the target was reached. Uses tight polling with runtime.Gosched to
// avoid channel signal drops under sustained load.
func (cn *etcdClusterNode) waitForApplied(target int64, timeout time.Duration) bool {
	ticker := time.NewTicker(50 * time.Microsecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if cn.totalApplied.Load() >= target {
			return true
		}
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}

// stop shuts down the cluster node: stops the raft node, signals the
// event loop and ticker to exit, waits for goroutine cleanup, closes
// the transport, and closes the WAL file.
func (cn *etcdClusterNode) stop() {
	if !cn.stopped.CompareAndSwap(false, true) {
		return
	}
	cn.node.Stop()
	close(cn.stopCh)
	cn.loopDone.Wait()
	<-cn.tickDone
	cn.transport.close()
	if cn.wal != nil {
		cn.wal.close()
	}
}

// Close shuts down all nodes in the cluster.
func (c *EtcdCluster) Close() {
	for _, cn := range c.Nodes {
		cn.stop()
	}
}

// waitForEtcdClusterLeader polls all nodes until one reports itself as
// leader, returning its index in the nodes slice. Returns an error if
// no leader is elected within the timeout.
func waitForEtcdClusterLeader(nodes []*etcdClusterNode, timeout time.Duration) (int, error) {
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		for i, cn := range nodes {
			status := cn.node.Status()
			if status.Lead == cn.id && status.RaftState == raft.StateLeader {
				return i, nil
			}
		}
		select {
		case <-deadline.C:
			return -1, fmt.Errorf("no leader elected within %v", timeout)
		case <-ticker.C:
		}
	}
}
