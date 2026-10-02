package seal

import (
	"context"
	"testing"
)

// TestStampSealedStateMACLetsAnEmbedderRewrapTheRootKey covers the case the
// method exists for: an embedder re-wraps the root key with its own strategy,
// assembles the new blob outside the barrier, and needs it to survive its own
// next Unseal.
//
// Without the stamp, the blob carries no StateMAC and Unseal refuses it -- the
// same shape as a stripped MAC, which is why it fails closed.
func TestStampSealedStateMACLetsAnEmbedderRewrapTheRootKey(t *testing.T) {
	ctx := context.Background()
	b, strategy := testInitializedBarrier(t)

	// What an embedder's re-wrap produces: the previous metadata carried
	// forward onto a blob whose ciphertext and strategy fields are new, and
	// whose MAC is therefore absent.
	existing := b.SealedRootKeyData()
	if existing == nil {
		t.Fatal("an initialized barrier must expose its sealed state")
	}
	rewrapped := cloneSealedRootKey(existing)
	rewrapped.StateMAC = nil

	if err := b.StampSealedStateMAC(context.Background(), rewrapped); err != nil {
		t.Fatalf("StampSealedStateMAC: %v", err)
	}
	if len(rewrapped.StateMAC) == 0 {
		t.Fatal("the blob must carry a MAC after stamping")
	}

	if err := b.SetSealedRootKey(rewrapped); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Unseal(ctx, strategy, Credentials{}); err != nil {
		t.Fatalf("Unseal after a stamped re-wrap must succeed: %v", err)
	}
}

// TestUnsealRefusesAnUnstampedRewrap is the other half: the same flow WITHOUT
// the stamp must fail, or the method above would be proving nothing.
func TestUnsealRefusesAnUnstampedRewrap(t *testing.T) {
	ctx := context.Background()
	b, strategy := testInitializedBarrier(t)

	unstamped := cloneSealedRootKey(b.SealedRootKeyData())
	unstamped.StateMAC = nil

	if err := b.SetSealedRootKey(unstamped); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Unseal(ctx, strategy, Credentials{}); err == nil {
		t.Fatal("Unseal must refuse a blob carrying no metadata MAC")
	}
}

// TestStampSealedStateMACCoversTheMetadata pins that the MAC authenticates the
// fields it claims to: a blob stamped and then edited must not verify, which
// is what stops Epoch and KnownEpochs from being attacker-controlled.
func TestStampSealedStateMACCoversTheMetadata(t *testing.T) {
	ctx := context.Background()
	b, strategy := testInitializedBarrier(t)

	edited := cloneSealedRootKey(b.SealedRootKeyData())
	edited.StateMAC = nil
	if err := b.StampSealedStateMAC(context.Background(), edited); err != nil {
		t.Fatalf("StampSealedStateMAC: %v", err)
	}

	// Editing a covered field after stamping is exactly the tamper the MAC
	// exists to catch.
	edited.KnownEpochs = append(edited.KnownEpochs, edited.Epoch)

	if err := b.SetSealedRootKey(edited); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Unseal(ctx, strategy, Credentials{}); err == nil {
		t.Fatal("Unseal must refuse a blob whose metadata changed after stamping")
	}
}

// TestStampSealedStateMACRequiresAnUnsealedBarrier covers the precondition:
// the root key is the MAC key, so a sealed barrier has nothing to stamp with.
func TestStampSealedStateMACRequiresAnUnsealedBarrier(t *testing.T) {
	b, _ := testInitializedBarrier(t)
	sealed := cloneSealedRootKey(b.SealedRootKeyData())

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.StampSealedStateMAC(context.Background(), sealed); err != ErrBarrierSealed {
		t.Fatalf("StampSealedStateMAC on a sealed barrier: got %v, want %v", err, ErrBarrierSealed)
	}
}

// TestStampSealedStateMACRejectsNil keeps a missing blob a sentinel error
// rather than a nil dereference.
func TestStampSealedStateMACRejectsNil(t *testing.T) {
	b, _ := testInitializedBarrier(t)
	if err := b.StampSealedStateMAC(context.Background(), nil); err != ErrNilSealedData {
		t.Fatalf("StampSealedStateMAC(nil): got %v, want %v", err, ErrNilSealedData)
	}
}

// TestStampSealedStateMACWithRootKeyStampsForALaterUnseal covers the import
// case: a blob assembled and stamped with a key in hand, with no barrier
// unsealed at the time, must authenticate when a barrier opens it later.
func TestStampSealedStateMACWithRootKeyStampsForALaterUnseal(t *testing.T) {
	ctx := context.Background()
	b, strategy := testInitializedBarrier(t)

	rootKey, err := b.RootKey(context.Background())
	if err != nil {
		t.Fatalf("RootKey: %v", err)
	}

	blob := cloneSealedRootKey(b.SealedRootKeyData())
	blob.StateMAC = nil
	if err := StampSealedStateMACWithRootKey(rootKey, b.config.DeploymentID, blob); err != nil {
		t.Fatalf("StampSealedStateMACWithRootKey: %v", err)
	}

	if err := b.SetSealedRootKey(blob); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Unseal(ctx, strategy, Credentials{}); err != nil {
		t.Fatalf("Unseal of a blob stamped from a bare root key: %v", err)
	}
}

// TestStampSealedStateMACWithRootKeyBindsTheDeployment pins the property the
// doc comment claims: a blob stamped for one deployment must not authenticate
// under another, which is what stops it being replayed into a different
// cluster.
func TestStampSealedStateMACWithRootKeyBindsTheDeployment(t *testing.T) {
	ctx := context.Background()
	b, strategy := testInitializedBarrier(t)

	rootKey, err := b.RootKey(context.Background())
	if err != nil {
		t.Fatalf("RootKey: %v", err)
	}

	blob := cloneSealedRootKey(b.SealedRootKeyData())
	blob.StateMAC = nil
	if err := StampSealedStateMACWithRootKey(rootKey, b.config.DeploymentID+1, blob); err != nil {
		t.Fatalf("StampSealedStateMACWithRootKey: %v", err)
	}

	if err := b.SetSealedRootKey(blob); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Unseal(ctx, strategy, Credentials{}); err == nil {
		t.Fatal("Unseal must refuse a blob stamped for a different deployment")
	}
}

// TestStampSealedStateMACWithRootKeyRejectsBadInput keeps the two unusable
// inputs sentinel errors rather than a nil dereference or a MAC keyed by
// nothing.
func TestStampSealedStateMACWithRootKeyRejectsBadInput(t *testing.T) {
	if err := StampSealedStateMACWithRootKey([]byte("key"), 1, nil); err != ErrNilSealedData {
		t.Fatalf("nil blob: got %v, want %v", err, ErrNilSealedData)
	}
	if err := StampSealedStateMACWithRootKey(nil, 1, &SealedRootKey{}); err != ErrNoRootKeyAvailable {
		t.Fatalf("empty root key: got %v, want %v", err, ErrNoRootKeyAvailable)
	}
}
