package raft

import (
	"errors"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// newIdentityHolderPeer builds a peer whose committed membership binds
// "spki:one" to replica 1 and "spki:two" to replica 2, with replica 5 a
// member whose identity is empty.
func newIdentityHolderPeer(t *testing.T) *Peer {
	t.Helper()
	ms := proto.Membership{
		Addresses: map[uint64]string{1: "a1", 2: "a2", 5: "a5"},
		Identities: map[uint64]string{
			1: "spki:one",
			2: "spki:two",
			5: "",
		},
	}
	return newTestPeer(t, testConfig(), proto.State{}, ms)
}

// applyCommittedChange folds one committed config change through the
// production apply path and publishes the result, as processConfigChanges does.
func applyCommittedChange(t *testing.T, p *Peer, index uint64, cc proto.ConfigChange) bool {
	t.Helper()
	e := proto.Entry{Index: index, Term: 1, Type: proto.EntryConfigChange, Cmd: marshalConfigChange(t, cc)}
	changed, err := p.applyMembershipEntry(e)
	if err != nil {
		t.Fatalf("applyMembershipEntry(%+v): %v", cc, err)
	}
	if changed {
		p.publishMembership()
	}
	return changed
}

func wantHolder(t *testing.T, p *Peer, identity string, wantID uint64, wantOK bool) {
	t.Helper()
	id, ok := p.IdentityHolder(identity)
	if id != wantID || ok != wantOK {
		t.Fatalf("IdentityHolder(%q) = (%d, %v), want (%d, %v)", identity, id, ok, wantID, wantOK)
	}
}

func TestPeerIdentityHolder(t *testing.T) {
	t.Run("committed identities name their replicas", func(t *testing.T) {
		p := newIdentityHolderPeer(t)
		wantHolder(t, p, "spki:one", 1, true)
		wantHolder(t, p, "spki:two", 2, true)
	})

	t.Run("unknown and empty identities are held by nobody", func(t *testing.T) {
		p := newIdentityHolderPeer(t)
		wantHolder(t, p, "spki:unknown", 0, false)
		wantHolder(t, p, "", 0, false)
	})

	t.Run("a replica with an empty identity makes no empty holder", func(t *testing.T) {
		p := newIdentityHolderPeer(t)
		// Replica 5 is a member with an empty identity; "" must still be
		// unheld, otherwise an unauthenticated sender would match it.
		if !p.membershipSnap.Load().isMember(5) {
			t.Fatal("replica 5 should be a member")
		}
		wantHolder(t, p, "", 0, false)
	})

	t.Run("a committed add binds the new identity", func(t *testing.T) {
		p := newIdentityHolderPeer(t)
		if !applyCommittedChange(t, p, 10, proto.ConfigChange{
			Type: proto.AddNode, ReplicaID: 3, Address: "a3", NodeIdentity: "spki:three",
		}) {
			t.Fatal("committed AddNode for replica 3 reported no change")
		}
		wantHolder(t, p, "spki:three", 3, true)
		wantHolder(t, p, "spki:one", 1, true)
	})

	t.Run("a committed removal unbinds the identity and tombstones it", func(t *testing.T) {
		p := newIdentityHolderPeer(t)
		if !applyCommittedChange(t, p, 10, proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 2}) {
			t.Fatal("committed RemoveNode for replica 2 reported no change")
		}
		wantHolder(t, p, "spki:two", 0, false)
		if !p.IsRemovedIdentity("spki:two") {
			t.Fatal("IsRemovedIdentity(spki:two) = false after its replica was removed")
		}
		wantHolder(t, p, "spki:one", 1, true)
	})

	t.Run("an add reusing a bound identity is refused", func(t *testing.T) {
		p := newIdentityHolderPeer(t)
		// The duplicate refusal is validate-only (validateMemberIdentity):
		// the apply path never refuses a committed entry, so the propose-time
		// gate is what keeps a second binding out of the log.
		cc := proto.ConfigChange{Type: proto.AddNode, ReplicaID: 4, Address: "a4", NodeIdentity: "spki:one"}
		if err := p.membership.validate(cc); !errors.Is(err, ErrDuplicateMemberIdentity) {
			t.Fatalf("validate duplicate identity: got %v, want ErrDuplicateMemberIdentity", err)
		}
		wantHolder(t, p, "spki:one", 1, true)
	})

	t.Run("concurrent readers while entries apply", func(t *testing.T) {
		p := newIdentityHolderPeer(t)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if id, ok := p.IdentityHolder("spki:one"); id != 1 || !ok {
						t.Errorf("IdentityHolder(spki:one) = (%d, %v) during apply", id, ok)
						return
					}
					if _, ok := p.IdentityHolder(""); ok {
						t.Error("IdentityHolder(\"\") reported a holder during apply")
						return
					}
					p.IdentityHolder("spki:three")
					p.IsRemovedIdentity("spki:two")
				}
			}()
		}
		applyCommittedChange(t, p, 10, proto.ConfigChange{
			Type: proto.AddNode, ReplicaID: 3, Address: "a3", NodeIdentity: "spki:three",
		})
		applyCommittedChange(t, p, 11, proto.ConfigChange{Type: proto.RemoveNode, ReplicaID: 2})
		close(stop)
		wg.Wait()
		wantHolder(t, p, "spki:three", 3, true)
		wantHolder(t, p, "spki:two", 0, false)
	})
}
