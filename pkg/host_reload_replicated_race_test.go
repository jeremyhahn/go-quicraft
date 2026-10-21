package quicraft

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// conflictReports counts the records reporting a committed address that the
// installed certificate covers.
func (c *hostLogs) conflictReports() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.records {
		if strings.HasPrefix(r.Message, "quicraft: a committed member address is covered by this host's certificate") {
			n++
		}
	}
	return n
}

// TestHostReloadMTLS_SerializedWithReplicatedPublication races a certificate
// reload against the apply of a committed AddNonVoting entry that publishes
// a peer address the new certificate covers. The committed entry always
// applies. Either the reload saw the address and was refused, or it was
// installed first and the apply, checking under the same lock, reported the
// conflict. The apply used to register the address with no lock and no
// check, so a reload could read the registry before it, install a covering
// certificate after it, and leave the conflict silent.
func TestHostReloadMTLS_SerializedWithReplicatedPublication(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	const self = "node-a.raft.test:5000"
	base := selfSignedMTLS(t, key, 1, "node-a.raft.test")
	cfg := sanHostConfig(t, self, "node-a.raft.test")
	cfg.TransportConfig.MTLSConfig = base

	logs := &hostLogs{}
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption(), WithLogger(slog.New(logs)))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	ns := testNodeState(h)
	defer h.nodes.Delete(uint64(1))
	cb := &hostEngineCallback{h: h}

	const iterations = 40
	reloadsLost := 0
	for i := range iterations {
		host := fmt.Sprintf("node-r%d.raft.test", i)
		addr := host + ":5000"
		replicaID := uint64(200 + i)
		covering := selfSignedMTLS(t, key, int64(2+i), "node-a.raft.test", host)
		cc := proto.ConfigChange{Type: proto.AddNonVoting, ReplicaID: replicaID, Address: addr}
		cmd := make([]byte, cc.Size())
		if _, err := cc.MarshalTo(cmd); err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}
		reports := logs.conflictReports()

		var wg sync.WaitGroup
		var reloadErr error
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			reloadErr = h.ReloadMTLS(covering)
		}()
		go func() {
			defer wg.Done()
			<-start
			cb.applyMembershipRouting(ns, 1,
				[]proto.Entry{{Index: uint64(10 + i), Term: 1, Type: proto.EntryConfigChange, Cmd: cmd}})
		}()
		close(start)
		wg.Wait()

		if got, err := h.registry.Resolve(1, replicaID); err != nil || got != addr {
			t.Fatalf("iteration %d: the committed address is not registered: %q, %v", i, got, err)
		}
		if reloadErr != nil {
			if !errors.Is(reloadErr, ErrSharedPeerCertificate) {
				t.Fatalf("iteration %d: reload after the apply = %v, want ErrSharedPeerCertificate", i, reloadErr)
			}
			reloadsLost++
			continue
		}
		if logs.conflictReports() == reports {
			t.Fatalf("iteration %d: a certificate covering %s was installed and the committed address published "+
				"with no conflict reported", i, host)
		}
		if err := h.ReloadMTLS(base); err != nil {
			t.Fatalf("iteration %d: reload back to the per-node certificate: %v", i, err)
		}
	}
	t.Logf("reloads lost %d of %d", reloadsLost, iterations)
}
