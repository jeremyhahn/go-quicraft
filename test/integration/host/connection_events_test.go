//go:build integration

package host_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/test/testpki"
)

// connEventShard is the shard the connection-event test runs.
const connEventShard uint64 = 908

// connEventLostTimeout bounds how long a survivor may take to report a
// closed peer through OnConnectionFailed.
const connEventLostTimeout = 30 * time.Second

// connLog records every ConnectionInfo one host's EventListener delivers.
type connLog struct {
	mu          sync.Mutex
	established []config.ConnectionInfo
	failed      []config.ConnectionInfo
}

func (l *connLog) listener() *config.EventListener {
	return &config.EventListener{
		OnConnectionEstablished: func(info config.ConnectionInfo) {
			l.mu.Lock()
			l.established = append(l.established, info)
			l.mu.Unlock()
		},
		OnConnectionFailed: func(info config.ConnectionInfo) {
			l.mu.Lock()
			l.failed = append(l.failed, info)
			l.mu.Unlock()
		},
	}
}

// sawEstablished reports whether OnConnectionEstablished fired for addr.
func (l *connLog) sawEstablished(addr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, info := range l.established {
		if info.Address == addr {
			return true
		}
	}
	return false
}

// sawFailed reports whether OnConnectionFailed fired for addr.
func (l *connLog) sawFailed(addr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, info := range l.failed {
		if info.Address == addr {
			return true
		}
	}
	return false
}

// selfReports returns every recorded event whose Address is self.
func (l *connLog) selfReports(self string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, info := range l.established {
		if info.Address == self {
			out = append(out, "established "+info.Address)
		}
	}
	for _, info := range l.failed {
		if info.Address == self {
			out = append(out, fmt.Sprintf("failed %s (%v)", info.Address, info.Err))
		}
	}
	return out
}

func (l *connLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return fmt.Sprintf("established=%v failed=%v", l.established, l.failed)
}

// TestConnectionEvents_PeerAddress pins the documented connection events on
// the real QUIC transport: each host reports OnConnectionEstablished for its
// peer's raft address once a two-voter shard has elected a leader, the
// survivor reports OnConnectionFailed for a peer that closes, and no event
// ever names the host's own address.
func TestConnectionEvents_PeerAddress(t *testing.T) {
	assertApplyOrder(t)
	const n = 2
	ca := testpki.NewCA(t)
	leaves := ca.Leaves(t, n)

	logs := make([]*connLog, n)
	hosts := make([]*quicraft.Host, n)
	addrs := make([]string, n)
	closed := make([]bool, n)
	t.Cleanup(func() {
		for i, h := range hosts {
			if h != nil && !closed[i] {
				h.Close()
			}
		}
	})
	for i := range n {
		logs[i] = &connLog{}
		cfg := testHostConfig(t, kernelAddr, leaves[i])
		cfg.EventListener = logs[i].listener()
		h, err := quicraft.NewHost(cfg, quicraft.WithoutEncryption())
		if err != nil {
			t.Fatalf("NewHost[%d]: %v", i, err)
		}
		hosts[i] = h
		addrs[i] = h.RaftAddress()
	}

	members := make(map[uint64]string, n)
	ids := make(map[uint64]string, n)
	for i := range n {
		members[uint64(i+1)] = addrs[i]
		ids[uint64(i+1)] = hosts[i].NodeIdentity()
	}
	for i, h := range hosts {
		createFn, _ := kvCreateFunc()
		scfg := testShardConfig(connEventShard, uint64(i+1))
		scfg.MemberIdentities = ids
		if err := h.StartShard(members, false, createFn, scfg); err != nil {
			t.Fatalf("StartShard[%d]: %v", i, err)
		}
	}
	for _, h := range hosts {
		waitForLeader(t, h, connEventShard, leaderElectionTimeout)
	}

	// (a) Each host has seen its peer's raft address established.
	for i := range n {
		peer := addrs[1-i]
		waitForConditionDiag(t, defaultTimeout,
			fmt.Sprintf("host %d sees OnConnectionEstablished for %s", i, peer),
			func() bool { return logs[i].sawEstablished(peer) },
			func() string { return logs[i].String() })
	}

	// (b) Close host 1; the survivor reports its address failed.
	const gone, survivor = 1, 0
	hosts[gone].Close()
	closed[gone] = true
	waitForConditionDiag(t, connEventLostTimeout,
		fmt.Sprintf("survivor sees OnConnectionFailed for %s", addrs[gone]),
		func() bool { return logs[survivor].sawFailed(addrs[gone]) },
		func() string { return logs[survivor].String() })

	// (c) No callback on either host named that host's own address.
	for i := range n {
		if self := logs[i].selfReports(addrs[i]); len(self) > 0 {
			t.Errorf("host %d reported its own address %s: %v", i, addrs[i], self)
		}
	}
}
