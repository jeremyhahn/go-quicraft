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
	"errors"
	"testing"
)

// TestBarrier_ContextAADRoundTrip verifies that the additive context-AAD API
// binds a caller context into the AEAD AAD alongside the epoch: a matching
// context authenticates, a mismatched context fails closed, and an epoch-only
// ciphertext still decrypts with a nil context (backward compatibility).
func TestBarrier_ContextAADRoundTrip(t *testing.T) {
	b, _ := testInitializedBarrier(t)

	plaintext := []byte("positionally-bound at-rest frame")
	// Distinct positional contexts, e.g. a snapshot's SnapshotIndex||ChunkSeq.
	ctxA := []byte("snapshot=7;chunk=0")
	ctxB := []byte("snapshot=7;chunk=1")

	// Same context round-trips.
	ct, err := b.EncryptWithContext(nil, plaintext, ctxA)
	if err != nil {
		t.Fatalf("EncryptWithContext failed: %v", err)
	}
	got, err := b.DecryptWithContext(nil, ct, ctxA)
	if err != nil {
		t.Fatalf("DecryptWithContext with matching context failed: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, plaintext)
	}

	// Different context fails authentication (reorder/splice protection).
	if _, err := b.DecryptWithContext(nil, ct, ctxB); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("DecryptWithContext with wrong context: got %v want ErrDecryptionFailed", err)
	}

	// A nil context against a context-bound ciphertext also fails: the caller
	// must reproduce the exact context.
	if _, err := b.DecryptWithContext(nil, ct, nil); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("DecryptWithContext with nil context on bound ciphertext: got %v want ErrDecryptionFailed", err)
	}
	if _, err := b.Decrypt(nil, ct); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("Decrypt (epoch-only) on bound ciphertext: got %v want ErrDecryptionFailed", err)
	}
}

// TestBarrier_ContextAADNilIsEpochOnly verifies the additive guarantee: a
// nil/empty context produces epoch-only AAD, so context ciphertext and the
// existing Encrypt/Decrypt path are fully interoperable.
func TestBarrier_ContextAADNilIsEpochOnly(t *testing.T) {
	b, _ := testInitializedBarrier(t)

	plaintext := []byte("epoch-only compatible frame")

	// Epoch-only ciphertext from Encrypt decrypts with a nil-context call.
	ct, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}
	got, err := b.DecryptWithContext(nil, ct, nil)
	if err != nil {
		t.Fatalf("DecryptWithContext(nil) on epoch-only ciphertext failed: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("mismatch: got %q want %q", got, plaintext)
	}

	// Ciphertext sealed with a nil context is decryptable by the plain Decrypt.
	ct2, err := b.EncryptWithContext(nil, plaintext, nil)
	if err != nil {
		t.Fatalf("EncryptWithContext(nil) failed: %v", err)
	}
	got2, err := b.Decrypt(nil, ct2)
	if err != nil {
		t.Fatalf("Decrypt of EncryptWithContext(nil) ciphertext failed: %v", err)
	}
	if !bytes.Equal(got2, plaintext) {
		t.Fatalf("mismatch: got %q want %q", got2, plaintext)
	}
	// An empty (non-nil) context is treated identically to nil.
	if _, err := b.DecryptWithContext(nil, ct2, []byte{}); err != nil {
		t.Fatalf("DecryptWithContext(empty) on nil-context ciphertext failed: %v", err)
	}
}

// TestBarrier_ContextAADEpochVariants exercises the epoch-returning and
// epoch-pinned context variants used by the WAL and snapshot call sites.
func TestBarrier_ContextAADEpochVariants(t *testing.T) {
	b, _ := testInitializedBarrier(t)

	plaintext := []byte("wal record payload")
	// e.g. a WAL segment's SegmentID||RecordSeq.
	segCtx := []byte("seg=3;rec=42")

	ct, epoch, err := b.EncryptWithEpochContext(nil, plaintext, segCtx)
	if err != nil {
		t.Fatalf("EncryptWithEpochContext failed: %v", err)
	}
	if epoch != b.CurrentEpoch() {
		t.Fatalf("EncryptWithEpochContext epoch=%d want %d", epoch, b.CurrentEpoch())
	}
	got, err := b.DecryptForEpochContext(nil, ct, epoch, segCtx)
	if err != nil {
		t.Fatalf("DecryptForEpochContext failed: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("mismatch: got %q want %q", got, plaintext)
	}
	// Wrong context under the correct epoch still fails.
	if _, err := b.DecryptForEpochContext(nil, ct, epoch, []byte("seg=3;rec=43")); !errors.Is(err, ErrDecryptionFailed) {
		t.Fatalf("DecryptForEpochContext wrong context: got %v want ErrDecryptionFailed", err)
	}

	// Pinned variant seals, pins the epoch in-flight, and round-trips.
	snapCtx := []byte("snap=100;chunk=5")
	ct2, epoch2, err := b.EncryptWithEpochPinnedContext(nil, plaintext, snapCtx)
	if err != nil {
		t.Fatalf("EncryptWithEpochPinnedContext failed: %v", err)
	}
	b.DeregisterInFlightEpoch(epoch2)
	got2, err := b.DecryptWithContext(nil, ct2, snapCtx)
	if err != nil {
		t.Fatalf("DecryptWithContext (pinned) failed: %v", err)
	}
	if !bytes.Equal(got2, plaintext) {
		t.Fatalf("mismatch: got %q want %q", got2, plaintext)
	}
}

// TestBarrier_SetSealedRootKey_ReloadResetsKnownEpochs pins that a second
// SetSealedRootKey makes the loaded blob authoritative and does NOT resurrect
// epochs a prior (reduced) blob had purged. Merging the epoch set into the
// existing map would let the union {1,2,3} survive reloading a reduced {1,3}.
func TestBarrier_SetSealedRootKey_ReloadResetsKnownEpochs(t *testing.T) {
	b := MustNewBarrier(DefaultBarrierConfig(1))

	// First load names epochs {1,2,3}.
	if err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategyPassphrase,
		Epoch:       3,
		Salt:        make([]byte, 32),
		HKDFVersion: HKDFV2,
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{1, 2, 3},
	}); err != nil {
		t.Fatalf("first SetSealedRootKey: %v", err)
	}
	if len(b.knownEpochs) != 3 {
		t.Fatalf("after first load: knownEpochs=%v want {1,2,3}", b.knownEpochs)
	}

	// A PurgeEpochsBefore reduced and persisted the set to {1,3}; reloading that
	// blob must yield exactly {1,3}, not the union with the earlier {1,2,3}.
	if err := b.SetSealedRootKey(&SealedRootKey{
		Strategy:    StrategyPassphrase,
		Epoch:       3,
		Salt:        make([]byte, 32),
		HKDFVersion: HKDFV2,
		Ciphertext:  []byte("dummy"),
		KnownEpochs: []uint64{1, 3},
	}); err != nil {
		t.Fatalf("second SetSealedRootKey: %v", err)
	}
	if len(b.knownEpochs) != 2 {
		t.Fatalf("after reduced reload: knownEpochs=%v want {1,3} (no resurrection)", b.knownEpochs)
	}
	if _, resurrected := b.knownEpochs[2]; resurrected {
		t.Fatalf("purged epoch 2 was resurrected on reload: knownEpochs=%v", b.knownEpochs)
	}
	for _, ep := range []uint64{1, 3} {
		if _, ok := b.knownEpochs[ep]; !ok {
			t.Fatalf("epoch %d missing after reload: knownEpochs=%v", ep, b.knownEpochs)
		}
	}
}
