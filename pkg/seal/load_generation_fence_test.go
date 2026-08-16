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

package seal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
)

// fenceStore is a StatePersister backed by an in-memory blob, standing in for
// the embedder's sealed-state file. It round-trips through JSON so the test
// exercises the same encode/decode path a real deployment uses — which is also
// what proves a newly added blob field is actually persisted rather than
// dropped by a field-by-field copy somewhere.
type fenceStore struct {
	mu    sync.Mutex
	blob  []byte
	saves int
}

func (s *fenceStore) save(t *testing.T, sealed *SealedRootKey) {
	t.Helper()
	encoded, err := json.Marshal(sealed)
	if err != nil {
		t.Fatalf("marshal sealed state: %v", err)
	}
	s.mu.Lock()
	s.blob = encoded
	s.saves++
	s.mu.Unlock()
}

// persister returns the StatePersister to install on a barrier.
func (s *fenceStore) persister(t *testing.T) StatePersister {
	return func(sealed *SealedRootKey) error {
		s.save(t, sealed)
		return nil
	}
}

// read decodes the current on-disk state, as a starting process would.
func (s *fenceStore) read(t *testing.T) *SealedRootKey {
	t.Helper()
	s.mu.Lock()
	encoded := append([]byte(nil), s.blob...)
	s.mu.Unlock()
	var sealed SealedRootKey
	if err := json.Unmarshal(encoded, &sealed); err != nil {
		t.Fatalf("unmarshal sealed state: %v", err)
	}
	return &sealed
}

// fenceBarrier builds a barrier wired to store and loads the current on-disk
// state into it, without unsealing.
func fenceBarrier(t *testing.T, deploymentID uint64, store *fenceStore) *Barrier {
	t.Helper()
	cfg := DefaultBarrierConfig(deploymentID)
	cfg.StatePersister = store.persister(t)
	b := MustNewBarrier(cfg)
	if err := b.SetSealedRootKey(store.read(t)); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	return b
}

// bootstrapFenced initializes a fresh deployment and returns its store.
func bootstrapFenced(t *testing.T, deploymentID uint64) *fenceStore {
	t.Helper()
	cfg := DefaultBarrierConfig(deploymentID)
	store := &fenceStore{}
	cfg.StatePersister = store.persister(t)
	src := MustNewBarrier(cfg)
	if err := src.Initialize(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	// Initialize does not call the persister; the embedder writes the bootstrap
	// blob itself, exactly as BarrierConfig.StatePersister documents.
	store.save(t, src.SealedRootKeyData())
	return store
}

// TestLoadGenerationFenceSupersedesEarlierLoader is the fence's reason to
// exist. A second process loads the sealed state, claims it, and writes the
// claim back. When the first barrier is handed that newer state it must refuse
// to encrypt from then on — turning a silently shared epoch DEK, which two
// loaders of one blob unavoidably derive, into a loud, attributable failure.
func TestLoadGenerationFenceSupersedesEarlierLoader(t *testing.T) {
	logs := captureLogs(t)
	store := bootstrapFenced(t, 101)

	// Process A: loads, claims, and is happily encrypting.
	first := fenceBarrier(t, 101, store)
	if err := first.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("first Unseal: %v", err)
	}
	claimA := first.LoadGeneration()
	if _, err := first.Encrypt(nil, []byte("A writes")); err != nil {
		t.Fatalf("first barrier could not encrypt after claiming the blob: %v", err)
	}

	// Process B: starts later, reads the state A wrote, and claims it.
	second := fenceBarrier(t, 101, store)
	if err := second.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("second Unseal: %v", err)
	}
	claimB := second.LoadGeneration()
	if claimB <= claimA {
		t.Fatalf("second loader claimed generation %d, want strictly above the first's %d: "+
			"the claim is not monotonic and the fence can never fire", claimB, claimA)
	}

	// A is handed the current on-disk state — the moment it can learn it lost.
	err := first.SetSealedRootKey(store.read(t))
	if !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("SetSealedRootKey on the superseded barrier returned %v, want ErrBarrierSuperseded", err)
	}
	var superseded *SupersededError
	if !errors.As(err, &superseded) {
		t.Fatalf("error %v does not carry *SupersededError; an embedder cannot tell a lost "+
			"claim from an I/O failure", err)
	}
	if superseded.Observed != claimB || superseded.Claimed != claimA {
		t.Fatalf("SupersededError{Observed:%d Claimed:%d}, want {Observed:%d Claimed:%d}",
			superseded.Observed, superseded.Claimed, claimB, claimA)
	}
	if !first.IsSuperseded() {
		t.Fatal("the barrier must MARK itself superseded, not merely return an error: a caller " +
			"that ignores the error must still not be left encrypting")
	}
	if !logs.find(slog.LevelError, "claimed by another loader") {
		t.Fatal("losing the claim must be reported at error level")
	}

	// THE POINT: A may no longer add to the DEK it shares with B.
	if _, err := first.Encrypt(nil, []byte("A must not write")); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("a superseded barrier ENCRYPTED (err=%v); two writers are now sharing one "+
			"epoch DEK with two independent nonce sequences", err)
	}
	// Every Encrypt entry point, not just the simple one.
	if _, _, err := first.EncryptWithEpoch(nil, []byte("x")); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("EncryptWithEpoch on a superseded barrier returned %v", err)
	}
	if _, _, err := first.EncryptWithEpochPinned(nil, []byte("x")); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("EncryptWithEpochPinned on a superseded barrier returned %v", err)
	}
	if _, err := first.EncryptWithContext(nil, []byte("x"), []byte("ctx")); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("EncryptWithContext on a superseded barrier returned %v", err)
	}
	// Writing the blob is refused too: rotating or purging would stamp this
	// instance's stale state over the winner's claim.
	if _, err := first.Rotate(); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("Rotate on a superseded barrier returned %v, want ErrBarrierSuperseded", err)
	}
	if _, err := first.PurgeEpochsBefore(first.CurrentEpoch()); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("PurgeEpochsBefore on a superseded barrier returned %v, want ErrBarrierSuperseded", err)
	}

	// The winner is untouched.
	if second.IsSuperseded() {
		t.Fatal("the loader that holds the newest claim must not be superseded")
	}
	if _, err := second.Encrypt(nil, []byte("B writes")); err != nil {
		t.Fatalf("the current owner could not encrypt: %v", err)
	}
}

// TestSupersededBarrierStillDecrypts pins the deliberate asymmetry: losing the
// claim stops this instance from ADDING to a shared key, not from reading data
// it already holds. A node that cannot serve reads because another process
// touched its sealed blob would be a worse outage than the hazard.
func TestSupersededBarrierStillDecrypts(t *testing.T) {
	captureLogs(t)
	store := bootstrapFenced(t, 102)

	first := fenceBarrier(t, 102, store)
	if err := first.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("first Unseal: %v", err)
	}
	plaintext := []byte("data written before the claim was lost")
	ct, err := first.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	second := fenceBarrier(t, 102, store)
	if err := second.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("second Unseal: %v", err)
	}
	if err := first.SetSealedRootKey(store.read(t)); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("expected supersession, got %v", err)
	}

	pt, err := first.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("a superseded barrier must still DECRYPT, got %v", err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatal("decrypted plaintext does not match")
	}
}

// TestSupersededBarrierCannotReclaimByResealing pins that supersession cannot
// be laundered. Sealing and unsealing again would otherwise mint a fresh claim
// and leave two instances stealing the blob back from each other in a loop —
// the automatic reclamation that was explicitly not wanted, because which
// instance should own the state is an operator decision.
func TestSupersededBarrierCannotReclaimByResealing(t *testing.T) {
	captureLogs(t)
	store := bootstrapFenced(t, 103)

	first := fenceBarrier(t, 103, store)
	if err := first.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("first Unseal: %v", err)
	}
	second := fenceBarrier(t, 103, store)
	if err := second.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("second Unseal: %v", err)
	}
	if err := first.SetSealedRootKey(store.read(t)); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("expected supersession, got %v", err)
	}

	if err := first.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := first.Unseal(context.Background(), testStrategy(t), Credentials{}); !errors.Is(err, ErrBarrierSuperseded) {
		t.Fatalf("a superseded barrier re-unsealed successfully (%v); it would re-claim the "+
			"blob and supersede the current owner in turn", err)
	}
	if second.IsSuperseded() {
		t.Fatal("the current owner must not have been superseded by the loser's re-unseal")
	}
}

// TestSingleWriterUnaffectedByFence is the other half of the requirement: a
// deployment nobody else touches must behave exactly as it did before the
// fence. One process bootstraps, restarts twice, rotates, purges and encrypts
// throughout — with no refusal, no supersession, and a claim that only ever
// moves forward.
func TestSingleWriterUnaffectedByFence(t *testing.T) {
	logs := captureLogs(t)
	store := bootstrapFenced(t, 104)

	var lastClaim, lastEpoch uint64
	var lastCiphertext []byte
	for restart := range 3 {
		b := fenceBarrier(t, 104, store)
		if err := b.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
			t.Fatalf("restart %d: Unseal: %v", restart, err)
		}
		if b.IsSuperseded() {
			t.Fatalf("restart %d: a sole writer was superseded by its own previous run", restart)
		}
		claim := b.LoadGeneration()
		if claim <= lastClaim {
			t.Fatalf("restart %d: claim %d did not advance past %d", restart, claim, lastClaim)
		}
		lastClaim = claim

		// Data written by the previous run must still be readable: the fence
		// must not disturb epoch retention across restarts.
		if lastCiphertext != nil {
			if _, err := b.DecryptForEpoch(nil, lastCiphertext, lastEpoch); err != nil {
				t.Fatalf("restart %d: data from the previous run (epoch %d) is unreadable: %v",
					restart, lastEpoch, err)
			}
		}

		ct, epoch, err := b.EncryptWithEpoch(nil, []byte("sole writer"))
		if err != nil {
			t.Fatalf("restart %d: EncryptWithEpoch: %v", restart, err)
		}
		lastCiphertext, lastEpoch = ct, epoch

		if _, err := b.Rotate(); err != nil {
			t.Fatalf("restart %d: Rotate: %v", restart, err)
		}
		if _, err := b.Encrypt(nil, []byte("after rotate")); err != nil {
			t.Fatalf("restart %d: Encrypt after Rotate: %v", restart, err)
		}
		// A realistic retention floor: retire everything strictly below the
		// frame this run must keep readable.
		if _, err := b.PurgeEpochsBefore(epoch); err != nil {
			t.Fatalf("restart %d: PurgeEpochsBefore: %v", restart, err)
		}
		// Re-reading its OWN persisted state must be a no-op, not a
		// self-supersession: the barrier wrote that generation itself.
		if err := b.SetSealedRootKey(store.read(t)); err != nil {
			t.Fatalf("restart %d: reloading its own state failed: %v", restart, err)
		}
		if b.IsSuperseded() {
			t.Fatalf("restart %d: the barrier superseded ITSELF by re-reading its own claim", restart)
		}
		if err := b.Seal(); err != nil {
			t.Fatalf("restart %d: Seal: %v", restart, err)
		}
	}

	if logs.find(slog.LevelError, "claimed by another loader") {
		t.Fatal("a single-writer deployment must never report a lost claim")
	}
}

// TestLoadGenerationSurvivesPersistenceRoundTrip is the field-by-field-copy
// guard. LoadGeneration is carried through a JSON encode/decode, through
// cloneSealedRootKey (which copies field by field and silently drops anything
// not listed), and through the rotate and purge persists — a field that
// vanishes in any one of those compiles cleanly and disables the fence without
// a single test failing anywhere else.
func TestLoadGenerationSurvivesPersistenceRoundTrip(t *testing.T) {
	captureLogs(t)
	store := bootstrapFenced(t, 105)

	// Initialize must stamp the first claim into the bootstrap blob.
	if got := store.read(t).LoadGeneration; got != firstLoadGeneration {
		t.Fatalf("bootstrap blob LoadGeneration = %d, want %d", got, firstLoadGeneration)
	}

	b := fenceBarrier(t, 105, store)
	if err := b.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	claim := b.LoadGeneration()
	if claim != firstLoadGeneration+1 {
		t.Fatalf("claim after one load = %d, want %d", claim, firstLoadGeneration+1)
	}
	if got := store.read(t).LoadGeneration; got != claim {
		t.Fatalf("persisted LoadGeneration = %d, want the claim %d: the claim never reached "+
			"storage and the next loader will hand out the same number", got, claim)
	}

	// cloneSealedRootKey feeds every persist; a rotation and a purge must both
	// carry the claim forward rather than reset it.
	if _, err := b.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if got := store.read(t).LoadGeneration; got != claim {
		t.Fatalf("LoadGeneration after Rotate = %d, want %d (the claim was dropped by the "+
			"rotate persist)", got, claim)
	}
	if _, err := b.PurgeEpochsBefore(b.CurrentEpoch()); err != nil {
		t.Fatalf("PurgeEpochsBefore: %v", err)
	}
	if got := store.read(t).LoadGeneration; got != claim {
		t.Fatalf("LoadGeneration after PurgeEpochsBefore = %d, want %d", got, claim)
	}

	// SealedRootKeyData is what an embedder persists directly; it must expose
	// the claim too.
	if got := b.SealedRootKeyData().LoadGeneration; got != claim {
		t.Fatalf("SealedRootKeyData().LoadGeneration = %d, want %d", got, claim)
	}

	// cloneSealedRootKey itself, directly — the copy that silently drops fields.
	src := &SealedRootKey{Epoch: 9, LoadGeneration: 1234}
	if got := cloneSealedRootKey(src).LoadGeneration; got != 1234 {
		t.Fatalf("cloneSealedRootKey dropped LoadGeneration (%d)", got)
	}
}

// TestLoadGenerationIsAuthenticated pins that the fence's number is covered by
// the metadata MAC. The barrier refuses to encrypt on the strength of it, so an
// unauthenticated LoadGeneration would be a durable, passphrase-free way for
// anyone with write access to the blob to stop a node from writing — a denial
// of service introduced by the very mechanism meant to protect it.
func TestLoadGenerationIsAuthenticated(t *testing.T) {
	captureLogs(t)
	store := bootstrapFenced(t, 106)

	b := fenceBarrier(t, 106, store)
	if err := b.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Forge a higher generation, leaving the genuine MAC in place.
	forged := store.read(t)
	forged.LoadGeneration += 100

	victim := MustNewBarrier(func() BarrierConfig {
		cfg := DefaultBarrierConfig(106)
		cfg.StatePersister = store.persister(t)
		return cfg
	}())
	if err := victim.SetSealedRootKey(forged); err != nil {
		t.Fatalf("a fresh barrier has no claim to be superseded from: %v", err)
	}
	err := victim.Unseal(context.Background(), testStrategy(t), Credentials{})
	if !errors.Is(err, ErrSealedStateUnauthenticated) {
		t.Fatalf("Unseal of a blob with a forged LoadGeneration returned %v, want "+
			"ErrSealedStateUnauthenticated: the fence's number is not MAC-covered", err)
	}
}

// TestFenceInertWithoutStatePersister pins the documented dead spot rather than
// leaving it to be discovered. A deployment with no persister cannot write a
// claim, so the fence cannot fire; the barrier must say so at WARN and must not
// invent a claim it never made durable.
func TestFenceInertWithoutStatePersister(t *testing.T) {
	logs := captureLogs(t)
	store := bootstrapFenced(t, 107)

	cfg := DefaultBarrierConfig(107) // deliberately no StatePersister
	b := MustNewBarrier(cfg)
	if err := b.SetSealedRootKey(store.read(t)); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	if err := b.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if got := b.LoadGeneration(); got != firstLoadGeneration {
		t.Fatalf("claim without a persister = %d, want the loaded value %d unchanged: a claim "+
			"that was never written down is not a claim", got, firstLoadGeneration)
	}
	if !logs.find(slog.LevelWarn, "load-generation fence is inert") {
		t.Fatal("a deployment that cannot claim must be told the fence is inert")
	}
}

// TestFenceClaimFailureAbortsUnseal pins the ordering the fence depends on: the
// claim must be durable BEFORE the barrier can encrypt. A persister that fails
// must leave the barrier sealed rather than unsealed-and-unclaimed, which would
// be a writer with no claim on disk at all.
func TestFenceClaimFailureAbortsUnseal(t *testing.T) {
	captureLogs(t)
	store := bootstrapFenced(t, 108)

	cfg := DefaultBarrierConfig(108)
	failure := errors.New("disk full")
	cfg.StatePersister = func(*SealedRootKey) error { return failure }
	b := MustNewBarrier(cfg)
	if err := b.SetSealedRootKey(store.read(t)); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}

	err := b.Unseal(context.Background(), testStrategy(t), Credentials{})
	if !errors.Is(err, failure) {
		t.Fatalf("Unseal with a failing persister returned %v, want the persist error", err)
	}
	if !b.IsSealed() {
		t.Fatal("a barrier whose claim could not be persisted must stay SEALED; unsealing " +
			"anyway would let it encrypt with no claim on disk")
	}
	if _, err := b.Encrypt(nil, []byte("x")); !errors.Is(err, ErrBarrierSealed) {
		t.Fatalf("Encrypt after an aborted unseal returned %v, want ErrBarrierSealed", err)
	}
}

// TestPreFenceBlobStillLoads pins the migration path for a sealed blob written
// before the load-generation fence existed.
//
// Such a blob carries a PRESENT metadata tag computed over the v1 field set, so
// it fails the v2 comparison while being entirely authentic. The
// AllowUnauthenticatedSealedState hatch does not rescue it — that hatch is for a
// blob with NO tag at all — so without an explicit v1 path a deployment that
// upgraded into the fence could never load its own sealed state again. That is
// data loss, delivered by a security fix.
func TestPreFenceBlobStillLoads(t *testing.T) {
	store := bootstrapFenced(t, 909)

	// Rewrite the stored blob as a pre-fence build would have left it: no
	// LoadGeneration, and a tag over the v1 covered set.
	blob := store.read(t)
	blob.LoadGeneration = 0
	legacy := legacyStateMAC(t, 909, blob)
	blob.StateMAC = legacy
	store.save(t, blob)

	b := MustNewBarrier(func() BarrierConfig {
		cfg := DefaultBarrierConfig(909)
		cfg.StatePersister = store.persister(t)
		return cfg
	}())
	if err := b.SetSealedRootKey(store.read(t)); err != nil {
		t.Fatalf("SetSealedRootKey on a pre-fence blob: %v", err)
	}
	if err := b.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("a pre-fence blob failed to unseal: %v; the deployment cannot "+
			"reach its own data after upgrading", err)
	}
	if _, err := b.Encrypt(nil, []byte("after migration")); err != nil {
		t.Fatalf("Encrypt after migrating a pre-fence blob: %v", err)
	}
	// The claim must have advanced off zero, so the blob is now fenced.
	if got := b.LoadGeneration(); got == 0 {
		t.Error("a migrated blob was left unfenced: LoadGeneration is still 0")
	}
}

// TestTamperedPreFenceBlobIsStillRefused is the other half: accepting a valid v1
// tag must not become a way to bypass authentication. An attacker with write
// access to the blob and no root key cannot produce a v1 tag any more than a v2
// one, so a blob whose metadata has been edited must still be refused.
func TestTamperedPreFenceBlobIsStillRefused(t *testing.T) {
	store := bootstrapFenced(t, 910)

	blob := store.read(t)
	blob.LoadGeneration = 0
	blob.StateMAC = legacyStateMAC(t, 910, blob)
	// Now tamper, AFTER the tag was computed. The salt is chosen deliberately:
	// it is covered by the tag and feeds DEK derivation, but unlike the epoch it
	// passes the structural checks that run before authentication — so this
	// reaches the MAC comparison, which is the path under test.
	if len(blob.Salt) == 0 {
		t.Fatal("fixture: the blob has no salt to tamper with")
	}
	blob.Salt = append([]byte(nil), blob.Salt...)
	blob.Salt[0] ^= 0xFF
	store.save(t, blob)

	cfg := DefaultBarrierConfig(910)
	cfg.StatePersister = store.persister(t)
	b := MustNewBarrier(cfg)
	if err := b.SetSealedRootKey(store.read(t)); err != nil {
		t.Fatalf("SetSealedRootKey: %v", err)
	}
	err := b.Unseal(context.Background(), testStrategy(t), Credentials{})
	if err == nil {
		t.Fatal("a tampered pre-fence blob unsealed; the v1 acceptance path is " +
			"an authentication bypass")
	}
	if !errors.Is(err, ErrSealedStateUnauthenticated) {
		t.Fatalf("Unseal of a tampered pre-fence blob = %v, want "+
			"ErrSealedStateUnauthenticated", err)
	}
}

// legacyStateMAC computes the v1 metadata tag the way a pre-fence build would
// have, by recovering the root key through the same strategy the barrier uses.
func legacyStateMAC(t *testing.T, deploymentID uint64, s *SealedRootKey) []byte {
	t.Helper()
	rootKey, err := testStrategy(t).UnsealRootKey(context.Background(), s, Credentials{})
	if err != nil {
		t.Fatalf("UnsealRootKey for the legacy tag: %v", err)
	}
	mac, err := sealedStateMACAt(1, rootKey, deploymentID, s)
	if err != nil {
		t.Fatalf("sealedStateMACAt(1): %v", err)
	}
	return mac
}
