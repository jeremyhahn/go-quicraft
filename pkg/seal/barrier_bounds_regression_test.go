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
	"errors"
	"log/slog"
	"testing"
)

// --- in-flight epoch pin accounting ---

// TestDeregisterInFlightEpochReportsUnbalancedRelease pins the DECIDABLE half
// of the in-flight pin contract.
//
// What is decidable: a release for an epoch holding no reference is a caller
// bug, and the barrier can say so. What is NOT decidable, and this test does
// not pretend otherwise: a duplicate release that arrives while the count is
// still positive is byte-for-byte indistinguishable from the legitimate one, so
// no reference-counted API can refuse it. Preventing that needs a single-use
// release handle, which is a public API change and not made here.
//
// The old body decremented unconditionally and deleted the entry at `<= 0`, so
// the underflow it produced was erased in the same critical section and the
// caller bug left no trace at all — the failure surfaced later, somewhere else,
// as an artifact whose DEK had been purged.
func TestDeregisterInFlightEpochReportsUnbalancedRelease(t *testing.T) {
	logs := captureLogs(t)
	b, _ := testInitializedBarrier(t)

	const epoch uint64 = 1
	b.RegisterInFlightEpoch(epoch)
	b.DeregisterInFlightEpoch(epoch) // the matched release
	if logs.find(slog.LevelError, "unbalanced DeregisterInFlightEpoch") {
		t.Fatal("a matched release must not be reported as unbalanced")
	}

	b.DeregisterInFlightEpoch(epoch) // one release too many
	if !logs.find(slog.LevelError, "unbalanced DeregisterInFlightEpoch") {
		t.Fatal("a release for an epoch holding no reference must be reported at error " +
			"level; absorbing it silently is what left the caller bug to surface later " +
			"as an undecryptable artifact")
	}
}

// TestDeregisterInFlightEpochNeverUnderflows pins that stray releases leave no
// buried negative count behind. PurgeEpochsBefore protects an epoch only while
// its count is > 0, so a count sitting at -2 would mean the next two legitimate
// pins protect nothing at all.
func TestDeregisterInFlightEpochNeverUnderflows(t *testing.T) {
	captureLogs(t)
	b, _ := testInitializedBarrier(t)

	const epoch uint64 = 42
	for range 3 {
		b.DeregisterInFlightEpoch(epoch)
	}

	b.inFlightMu.Lock()
	n, present := b.inFlightEpochs[epoch]
	b.inFlightMu.Unlock()
	if present && n < 0 {
		t.Fatalf("pin count for a never-registered epoch = %d; the counter underflowed", n)
	}

	// A single Register must be enough to protect the epoch again, and the
	// protection must be real: a purge has to skip it.
	b.RegisterInFlightEpoch(epoch)
	b.inFlightMu.Lock()
	n, present = b.inFlightEpochs[epoch]
	b.inFlightMu.Unlock()
	if !present || n != 1 {
		t.Fatalf("pin count after Register following stray releases = %d (present=%v), want 1",
			n, present)
	}
}

// TestPinnedEpochSurvivesPurgeAfterStrayReleases is the end-to-end statement of
// the same invariant: stray releases before a pin is taken must not leave the
// epoch unprotected once it IS taken, so a frame sealed under it stays
// decryptable across a rotate + purge.
func TestPinnedEpochSurvivesPurgeAfterStrayReleases(t *testing.T) {
	captureLogs(t)
	b, _ := testInitializedBarrier(t)

	epoch := b.CurrentEpoch()
	// Stray releases arrive first, before anything holds the epoch.
	b.DeregisterInFlightEpoch(epoch)
	b.DeregisterInFlightEpoch(epoch)

	plaintext := []byte("frame whose DEK must survive a purge")
	ct, sealedEpoch, err := b.EncryptWithEpochPinned(nil, plaintext)
	if err != nil {
		t.Fatalf("EncryptWithEpochPinned: %v", err)
	}
	defer b.DeregisterInFlightEpoch(sealedEpoch)

	if _, err := b.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if _, err := b.PurgeEpochsBefore(context.Background(), b.CurrentEpoch()); err != nil {
		t.Fatalf("PurgeEpochsBefore: %v", err)
	}

	pt, err := b.DecryptForEpoch(nil, ct, sealedEpoch)
	if err != nil {
		t.Fatalf("frame sealed under pinned epoch %d is UNDECRYPTABLE after a purge (%v): "+
			"earlier stray releases left the pin unable to protect it", sealedEpoch, err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatal("decrypted frame does not match the original plaintext")
	}
}

// --- KnownEpochs structural bounds ---

// TestSetSealedRootKeyRejectsZeroKnownEpoch pins that epoch 0 cannot enter the
// derivation set. Epochs start at 1 and only increment, so no data was ever
// sealed under epoch 0. Accepting it was not inert: Unseal derived and
// installed a full epoch-0 DEK, AEAD and mlock'd enclave, the forced rotation
// re-persisted it, and no purge floor can retire it (PurgeEpochsBefore drops
// epochs strictly BELOW its floor, and nothing is below 0). It also left
// b.ciphers[0] populated — the one epoch a zero-filled ciphertext header
// decodes to.
func TestSetSealedRootKeyRejectsZeroKnownEpoch(t *testing.T) {
	src := MustNewBarrier(testBarrierConfig(31))
	if err := src.Initialize(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	blob := cloneSealedRootKey(src.SealedRootKeyData())
	blob.KnownEpochs = []uint64{0, blob.Epoch}
	// Stamped so the metadata MAC is valid: the structural gate must stand on
	// its own, not lean on authentication that a root-key holder can satisfy.
	if err := src.StampSealedStateMAC(context.Background(), blob); err != nil {
		t.Fatalf("StampSealedStateMAC: %v", err)
	}

	b := MustNewBarrier(testBarrierConfig(31))
	err := b.SetSealedRootKey(blob)
	if !errors.Is(err, ErrInvalidSealedEpoch) {
		t.Fatalf("SetSealedRootKey with KnownEpochs={0,%d} returned %v, want ErrInvalidSealedEpoch",
			blob.Epoch, err)
	}

	// Rejection must be total: nothing from the refused blob may be retained.
	if _, ok := b.knownEpochs[0]; ok {
		t.Fatal("epoch 0 entered the known-epoch set despite the rejection")
	}
}

// TestSetSealedRootKeyAcceptsValidKnownEpochs pins that the new gate is
// specific: a legitimate historical set still loads.
func TestSetSealedRootKeyAcceptsValidKnownEpochs(t *testing.T) {
	src := MustNewBarrier(testBarrierConfig(32))
	if err := src.Initialize(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if _, err := src.Rotate(context.Background()); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	blob := src.SealedRootKeyData()
	if len(blob.KnownEpochs) != 2 {
		t.Fatalf("expected two known epochs after one rotation, got %v", blob.KnownEpochs)
	}

	b := MustNewBarrier(testBarrierConfig(32))
	if err := b.SetSealedRootKey(blob); err != nil {
		t.Fatalf("a valid two-epoch blob was rejected: %v", err)
	}
}

// --- per-invocation message bounds ---

// TestMaxPlaintextSizePerAlgorithm pins the ceilings against the values the
// underlying implementations actually enforce. These are not policy numbers:
// crypto/cipher panics above (2^32-2)*16 for GCM and x/crypto panics above
// 2^38-64 for both ChaCha20-Poly1305 constructions, so a wrong value here is
// either a needless rejection or a process crash.
func TestMaxPlaintextSizePerAlgorithm(t *testing.T) {
	for _, tc := range knownAlgorithms {
		if got := tc.alg.MaxPlaintextSize(); got != tc.maxPlaintext {
			t.Errorf("%s MaxPlaintextSize() = %d, want %d", tc.alg, got, tc.maxPlaintext)
		}
	}
	// An algorithm this build does not register has no ceiling to report, which
	// is the same answer as for an identifier nobody defined; see
	// TestUnregisteredAlgorithmsAreRefused.
	for _, alg := range unregisteredAlgorithms {
		if got := alg.MaxPlaintextSize(); got != 0 {
			t.Errorf("unregistered %#x MaxPlaintextSize() = %d, want 0", byte(alg), got)
		}
	}
	if got := SymmetricAlgorithm(0x7f).MaxPlaintextSize(); got != 0 {
		t.Errorf("unknown algorithm MaxPlaintextSize() = %d, want 0", got)
	}
}

// TestEncryptRejectsOversizedPlaintext pins that the barrier refuses a frame
// its cipher would PANIC on. Neither crypto/cipher nor x/crypto returns an
// error for an over-long message, so without this bound an oversized frame
// takes the embedding process down from inside Seal — a library killing its
// host over an input it could have rejected.
//
// The barrier's ceiling is lowered for the test because the real one is ~64 GiB
// (AES-GCM); the bound under test is the comparison in encryptLocked, not the
// constant, which TestMaxPlaintextSizePerAlgorithm pins separately.
func TestEncryptRejectsOversizedPlaintext(t *testing.T) {
	b, _ := testInitializedBarrier(t)
	b.maxPlaintext = 64

	if _, err := b.Encrypt(nil, bytes.Repeat([]byte("a"), 64)); err != nil {
		t.Fatalf("plaintext exactly at the ceiling was rejected: %v", err)
	}

	oversized := bytes.Repeat([]byte("a"), 65)
	if _, err := b.Encrypt(nil, oversized); !errors.Is(err, ErrPlaintextTooLarge) {
		t.Fatalf("Encrypt of an over-ceiling plaintext returned %v, want ErrPlaintextTooLarge", err)
	}
	if _, _, err := b.EncryptWithEpoch(nil, oversized); !errors.Is(err, ErrPlaintextTooLarge) {
		t.Fatalf("EncryptWithEpoch returned %v, want ErrPlaintextTooLarge", err)
	}
	if _, _, err := b.EncryptWithEpochPinned(nil, oversized); !errors.Is(err, ErrPlaintextTooLarge) {
		t.Fatalf("EncryptWithEpochPinned returned %v, want ErrPlaintextTooLarge", err)
	}
	if _, err := b.EncryptWithContext(nil, oversized, []byte("ctx")); !errors.Is(err, ErrPlaintextTooLarge) {
		t.Fatalf("EncryptWithContext returned %v, want ErrPlaintextTooLarge", err)
	}

	// A rejected frame must not consume AEAD budget: the check runs before the
	// nonce is drawn, recorded, or counted.
	before := b.invocations.Count()
	_, _ = b.Encrypt(nil, oversized)
	if after := b.invocations.Count(); after != before {
		t.Fatalf("a rejected oversized frame consumed invocation budget (%d -> %d)", before, after)
	}
}

// TestDecryptRejectsOversizedCiphertext pins the same bound on the read path,
// where it matters more: the input comes off disk or the wire, and x/crypto's
// ChaCha20-Poly1305 Open PANICS on an over-long ciphertext rather than failing
// authentication. The rejection uses the generic ErrDecryptionFailed so the
// length is not an oracle.
func TestDecryptRejectsOversizedCiphertext(t *testing.T) {
	b, _ := testInitializedBarrier(t)

	ct, err := b.Encrypt(nil, []byte("small frame"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := b.Decrypt(nil, ct); err != nil {
		t.Fatalf("round trip failed before the bound was applied: %v", err)
	}

	// Lower the ceiling below this frame's payload and re-present it: the
	// pre-Open gate must reject it rather than hand it to the AEAD.
	b.maxPlaintext = 1
	if _, err := b.Decrypt(nil, ct); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("Decrypt of an over-ceiling payload returned %v, want ErrDecryptionFailed", err)
	}
	if _, err := b.DecryptForEpoch(nil, ct, b.CurrentEpoch()); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("DecryptForEpoch of an over-ceiling payload returned %v, want ErrDecryptionFailed", err)
	}
}

// --- duplicate-writer hazard (documented, not defended) ---

// TestDoubleLoadedBlobStillSharesTheEpochDEK pins the HAZARD the load-generation
// fence exists to make visible, and the exact boundary of the fence's reach.
//
// Two barriers loaded from one blob still derive the SAME epoch DEK — the fence
// does not and cannot prevent that. Both rotate on unseal (the loaded epoch's
// spent invocation budget is unknowable) to the same epoch+1 from the same root
// key, salt and deployment ID, each with a zeroed invocation counter and an
// empty nonce tracker; given the same nonce they produce byte-identical
// ciphertext. What the fence changes is that the second loader leaves a claim
// behind, so the condition becomes DETECTABLE at the next load instead of
// silent — see TestLoadGenerationFenceSupersedesEarlierLoader.
//
// Keeping this test is deliberate. If someone later believes the fence prevents
// DEK sharing rather than exposing it, this is the test that says otherwise.
func TestDoubleLoadedBlobStillSharesTheEpochDEK(t *testing.T) {
	captureLogs(t)
	src := MustNewBarrier(testBarrierConfig(77))
	if err := src.Initialize(context.Background(), testStrategy(t), Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	// Both loaders read the SAME on-disk state, before either writes its claim
	// — the one interleaving the fence cannot separate.
	blob := src.SealedRootKeyData()

	load := func() *Barrier {
		b := MustNewBarrier(testBarrierConfig(77))
		if err := b.SetSealedRootKey(cloneSealedRootKey(blob)); err != nil {
			t.Fatalf("SetSealedRootKey: %v", err)
		}
		if err := b.Unseal(context.Background(), testStrategy(t), Credentials{}); err != nil {
			t.Fatalf("Unseal: %v", err)
		}
		return b
	}
	first, second := load(), load()

	if first.CurrentEpoch() != second.CurrentEpoch() {
		t.Fatalf("two loads of one blob landed on epochs %d and %d; the recorded "+
			"behaviour (both rotate to loaded+1) has changed and this test must be revisited",
			first.CurrentEpoch(), second.CurrentEpoch())
	}
	if first.LoadGeneration() != second.LoadGeneration() {
		t.Fatalf("two loaders that read the same generation claimed %d and %d; the fence "+
			"cannot distinguish them and both must claim the same number",
			first.LoadGeneration(), second.LoadGeneration())
	}
	if first.IsSuperseded() || second.IsSuperseded() {
		t.Fatal("neither loader can detect the other from a simultaneous read; the fence " +
			"must not claim a detection it did not make")
	}

	// Same key: each barrier opens the other's frames. That is the shared-DEK
	// condition the invocation cap and the nonce tracker exist to bound, and
	// exactly what the fence exposes rather than prevents — two writers each
	// spending a full SP 800-38D budget against one key, neither able to see
	// the other's nonces.
	//
	// It cannot be shown by pinning both barriers' randReader to a constant
	// and comparing ciphertexts byte for byte: the AES-GCM nonce is generated
	// inside the FIPS module boundary
	// (cipher.NewGCMWithRandomNonce), so no test can force two encryptions to
	// collide. Cross-decryption proves the same shared DEK without needing to.
	a, err := first.Encrypt(nil, []byte("duplicate writer"))
	if err != nil {
		t.Fatalf("first Encrypt: %v", err)
	}
	c, err := second.Encrypt(nil, []byte("duplicate writer"))
	if err != nil {
		t.Fatalf("second Encrypt: %v", err)
	}
	if bytes.Equal(a, c) {
		t.Fatal("two independent encryptions produced identical frames; the " +
			"module-generated nonce is not varying")
	}

	fromSecond, err := first.Decrypt(nil, c)
	if err != nil {
		t.Fatalf("two barriers loaded from one blob no longer share an epoch DEK "+
			"(first could not open second's frame); if that is now prevented "+
			"rather than merely detected, rewrite this test: %v", err)
	}
	fromFirst, err := second.Decrypt(nil, a)
	if err != nil {
		t.Fatalf("second could not open first's frame: %v", err)
	}
	if !bytes.Equal(fromFirst, []byte("duplicate writer")) || !bytes.Equal(fromSecond, []byte("duplicate writer")) {
		t.Fatalf("cross-decrypt mismatch: %q / %q", fromFirst, fromSecond)
	}
}
