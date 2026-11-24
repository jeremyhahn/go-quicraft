package quicraft

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"log/slog"
	"testing"
)

// newCoveringCertHost returns a host whose installed certificate covers
// node-b.raft.test, a peer host no registered member uses yet, as a reload
// that admitted no member there leaves it.
func newCoveringCertHost(t *testing.T, logs *hostLogs) *Host {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	const self = "node-a.raft.test:5000"
	cfg := sanHostConfig(t, self, "node-a.raft.test")
	cfg.TransportConfig.MTLSConfig = selfSignedMTLS(t, key, 1, "node-a.raft.test")
	h, err := NewHost(cfg, WithMemoryLogDB(), WithoutEncryption(), WithLogger(slog.New(logs)))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if err := h.ReloadMTLS(selfSignedMTLS(t, key, 2, "node-a.raft.test", "node-b.raft.test")); err != nil {
		t.Fatalf("ReloadMTLS onto a certificate covering node-b: %v", err)
	}
	return h
}

// TestLearnSourceAddress_RefusesAddressTheCertificateCovers pins that an
// address learned from an inbound message's source is published under peer
// admission and refused when the installed certificate covers it. It used to
// go straight into the registry, so a peer at a host this certificate covers
// became a registered member the exclusivity check had never seen.
func TestLearnSourceAddress_RefusesAddressTheCertificateCovers(t *testing.T) {
	const covered = "node-b.raft.test:5000"
	h := newCoveringCertHost(t, &hostLogs{})
	handler := &hostMessageHandler{host: h}

	// First contact from a sender outside the configuration.
	handler.learnSourceAddress(1, 7, "", covered, false)
	if addr, err := h.registry.Resolve(1, 7); err == nil {
		t.Fatalf("first contact published %q, an address the installed certificate covers", addr)
	}

	// A configured member moving to a covered address.
	const established = "node-c.raft.test:5000"
	h.registry.Register(1, 8, established)
	handler.learnSourceAddress(1, 8, established, covered, true)
	if addr, err := h.registry.Resolve(1, 8); err != nil || addr != established {
		t.Fatalf("Resolve(1, 8) = (%q, %v): a covered address replaced %q", addr, err, established)
	}

	// An address the certificate does not cover is still learned.
	const uncovered = "node-d.raft.test:5000"
	handler.learnSourceAddress(1, 9, "", uncovered, false)
	if addr, err := h.registry.Resolve(1, 9); err != nil || addr != uncovered {
		t.Fatalf("Resolve(1, 9) = (%q, %v), want the uncovered address learned", addr, err)
	}
}

// TestHandleSnapshot_JoiningRouteIsCommittedPublication pins that the
// leader address a joining replica takes from its bootstrap snapshot is
// published as committed membership: under peer admission and checked
// against the installed certificate. It is registered even when covered,
// because the snapshot's membership is committed and its install reconciles
// the same address anyway, and the conflict is reported. The write used to
// skip the lock and the check, so a covered leader address was published
// silently.
func TestHandleSnapshot_JoiningRouteIsCommittedPublication(t *testing.T) {
	const leaderAddr = "node-b.raft.test:7001"
	const leaderID = "spki:1111111111111111111111111111111111111111111111111111111111111111"

	logs := &hostLogs{}
	certHost := newCoveringCertHost(t, logs)
	f := newSnapshotReceiveFixture(t, nil)
	f.host.transport = certHost.transport
	f.host.logger = certHost.logger
	node := newJoiningEngineNode(t, 1, 2)
	f.engine.LoadNode(node)
	t.Cleanup(func() { f.engine.UnloadNode(1) })
	f.host.markShardJoining(1)
	f.host.setShardAnchor(1, map[uint64]string{1: leaderID})

	addrs := map[uint64]string{1: leaderAddr, 2: "127.0.0.1:7002"}
	chunks := snapshotChunkFor(1, 2, 1, 500, 10, addrs, []byte("bootstrap-membership"))
	for i := range chunks {
		chunks[i].Membership.Identities = map[uint64]string{1: leaderID, 2: f.host.NodeIdentity()}
	}
	if err := f.handler.HandleSnapshot(chunks, certCoveringAs(leaderID, leaderAddr)); err != nil {
		t.Fatalf("joining replica rejected its bootstrap snapshot: %v", err)
	}
	if addr, err := f.host.registry.Resolve(1, 1); err != nil || addr != leaderAddr {
		t.Fatalf("Resolve(1, 1) = (%q, %v), want the committed leader address", addr, err)
	}
	if logs.conflictReports() == 0 {
		t.Fatal("a committed leader address the installed certificate covers was published with no conflict reported")
	}
}
