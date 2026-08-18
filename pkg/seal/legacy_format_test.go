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
	"encoding/hex"
	"testing"
)

// This file is the compatibility proof for the migration from caller-supplied
// AES-GCM IVs to cipher.NewGCMWithRandomNonce (see newAESGCM).
//
// Every vector below was produced by the PRE-MIGRATION code — the version that
// drew a nonce from a crypto/rand reader and wrote it into the blob by hand —
// and is checked in as bytes. Regenerating them from the current code would
// make these tests tautological: the point is that data written by a build that
// no longer exists still reads.
//
// The vectors are also why this package needs no envelope version
// discriminator. cipher.NewGCMWithRandomNonce emits nonce ‖ ciphertext ‖ tag,
// which is byte-for-byte the layout the hand-rolled framing already used, in
// the same positions, with the same total length. v1 and v2 are the same
// format; only the party that generates the nonce changed, and that party is
// not recorded on disk because it never was. A discriminator would have to
// distinguish two identical byte strings.
//
// These tests deliberately execute NO non-approved primitive: they only OPEN
// legacy bytes, using the approved module-nonce AEAD. That is what lets them
// run unchanged under GOFIPS140=latest GODEBUG=fips140=only, which is where
// this compatibility claim actually needs to hold.

// Legacy sealed root key, written by SoftwareStrategy.SealRootKey before the
// migration: [KDFSalt:32 | Nonce:12 | EncryptedRootKey:32 | Tag:16], wrapped
// under PBKDF2-SHA256 at the production floor iteration count.
const (
	legacyRootKeyPassphrase = "quicraft-legacy-golden-passphrase"
	legacyRootKeyHex        = "00070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9"
	legacySealedRootKeyHex  = "0dc45974f70c14fefe23c4f2c26bedaff6fc7dd9127a8dc73d56f49679c785e3" +
		"e191f83088546d992c55a6c3affd771b23cab2e3c2a95d04071c41013b78c580" +
		"35ed6db40b6fa6bb1ed39d1d299a2820d53477d21e07c59be86fd530"
)

// Legacy barrier frames, written by Barrier.encryptLocked before the migration:
// [epoch:8][algID:1][nonce:12][ciphertext+tag]. legacyFrameDEKHex is the epoch
// DEK they were sealed under; both frames name epoch 7 in their header, which
// is what their AAD is bound to.
const (
	legacyFrameEpoch  = 7
	legacyFrameDEKHex = "fffcf9f6f3f0edeae7e4e1dedbd8d5d2cfccc9c6c3c0bdbab7b4b1aeaba8a5a2"

	legacyFramePlaintext = "legacy frame written before the FIPS random-nonce migration"
	legacyFrameHex       = "000000000000000703ea7e7145cde30cc7af03fde0e6860f29dd2ddbf181e083" +
		"b2244b97251ef988db848b5f450be113982d0ab0b75c0626b03a9a3cbe204292" +
		"01c45b4a37d3fccc2f019361d6f264bbd1c9208d673039ffe6e1258f6efea7ba"

	legacyContextFrameContext   = "wal/segment/000042"
	legacyContextFramePlaintext = "legacy frame with caller context AAD"
	legacyContextFrameHex       = "000000000000000703b878b0fba54b480dfe245863efc8e84b21dfd796932316" +
		"a4f7f1d050bfb2c966460b48be3febcf8562e99e5cfe364587461a046b7d3a84" +
		"e278eeba2ba8602970"
)

// mustHex decodes a checked-in golden vector.
func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad golden vector: %v", err)
	}
	return b
}

// TestLegacyFormat_UnsealRootKey proves that a root key sealed by the
// pre-migration wrapping path still unseals. This is the migration's load-bearing
// claim: SoftwareStrategy.SealRootKey is what every consumer's barrier is
// bootstrapped from, so a blob that stopped opening would lock a deployment out
// of all of its data at once.
func TestLegacyFormat_UnsealRootKey(t *testing.T) {
	// The production PBKDF2 floor, so this test does not depend on the
	// work-factor bypass and exercises the same path a real deployment does.
	s, err := NewSoftwareStrategyFIPS([]byte(legacyRootKeyPassphrase), minPBKDF2Iter)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyFIPS: %v", err)
	}
	defer func() { _ = s.Close() }()

	sealed := &SealedRootKey{
		Strategy:   StrategyPassphrase,
		Ciphertext: mustHex(t, legacySealedRootKeyHex),
		Metadata:   []byte{byte(KDFPbkdf2SHA256)},
	}

	got, err := s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("a root key sealed before the random-nonce migration no longer "+
			"unseals; the wrapping format is not backward compatible: %v", err)
	}
	if want := mustHex(t, legacyRootKeyHex); !bytes.Equal(got, want) {
		t.Fatalf("unsealed root key mismatch:\n got:  %x\n want: %x", got, want)
	}
}

// TestLegacyFormat_UnsealRootKeyRejectsTamper guards against the compatibility
// claim above being satisfied by an AEAD that stopped authenticating. A single
// flipped bit anywhere in the legacy blob — salt, nonce, ciphertext or tag —
// must still fail.
func TestLegacyFormat_UnsealRootKeyRejectsTamper(t *testing.T) {
	golden := mustHex(t, legacySealedRootKeyHex)
	offsets := map[string]int{
		"kdf salt":   0,
		"nonce":      kdfSaltLen,
		"ciphertext": kdfSaltLen + wrappingGCMNonceSize,
		"tag":        len(golden) - 1,
	}

	for name, off := range offsets {
		t.Run(name, func(t *testing.T) {
			s, err := NewSoftwareStrategyFIPS([]byte(legacyRootKeyPassphrase), minPBKDF2Iter)
			if err != nil {
				t.Fatalf("NewSoftwareStrategyFIPS: %v", err)
			}
			defer func() { _ = s.Close() }()

			corrupt := bytes.Clone(golden)
			corrupt[off] ^= 0x01

			sealed := &SealedRootKey{
				Strategy:   StrategyPassphrase,
				Ciphertext: corrupt,
				Metadata:   []byte{byte(KDFPbkdf2SHA256)},
			}
			if _, err := s.UnsealRootKey(context.Background(), sealed, Credentials{}); err == nil {
				t.Fatalf("a legacy blob with a flipped bit in the %s unsealed successfully", name)
			}
		})
	}
}

// TestLegacyFormat_BarrierDecryptsFrame proves that data-path frames written by
// the pre-migration barrier still decrypt. The DEK is installed directly
// because the frames were sealed under a fixed key rather than one this test
// can re-derive; everything after that is the ordinary public read path,
// including the length gates and the algID cross-check.
func TestLegacyFormat_BarrierDecryptsFrame(t *testing.T) {
	b, strategy := testInitializedBarrier(t)
	defer func() { _ = strategy.Close() }()

	aead, err := newAEADForAlgorithm(AlgAES256GCM, mustHex(t, legacyFrameDEKHex))
	if err != nil {
		t.Fatalf("newAEADForAlgorithm: %v", err)
	}
	b.mu.Lock()
	b.ciphers[legacyFrameEpoch] = aead
	b.mu.Unlock()

	t.Run("epoch-only AAD", func(t *testing.T) {
		got, err := b.DecryptForEpoch(nil, mustHex(t, legacyFrameHex), legacyFrameEpoch)
		if err != nil {
			t.Fatalf("a frame written before the random-nonce migration no longer "+
				"decrypts; the wire format is not backward compatible: %v", err)
		}
		if string(got) != legacyFramePlaintext {
			t.Fatalf("plaintext mismatch:\n got:  %q\n want: %q", got, legacyFramePlaintext)
		}
	})

	t.Run("context AAD", func(t *testing.T) {
		got, err := b.DecryptForEpochContext(nil,
			mustHex(t, legacyContextFrameHex), legacyFrameEpoch, []byte(legacyContextFrameContext))
		if err != nil {
			t.Fatalf("a legacy frame with a caller context AAD no longer decrypts: %v", err)
		}
		if string(got) != legacyContextFramePlaintext {
			t.Fatalf("plaintext mismatch:\n got:  %q\n want: %q", got, legacyContextFramePlaintext)
		}
	})

	t.Run("wrong context AAD still fails", func(t *testing.T) {
		if _, err := b.DecryptForEpochContext(nil,
			mustHex(t, legacyContextFrameHex), legacyFrameEpoch, []byte("wal/segment/000043")); err == nil {
			t.Fatal("a legacy frame decrypted under the wrong context AAD; the " +
				"binding is not being authenticated")
		}
	})

	t.Run("tampered frame still fails", func(t *testing.T) {
		corrupt := mustHex(t, legacyFrameHex)
		corrupt[len(corrupt)-1] ^= 0x01
		if _, err := b.DecryptForEpoch(nil, corrupt, legacyFrameEpoch); err == nil {
			t.Fatal("a legacy frame with a flipped tag bit decrypted successfully")
		}
	})
}

// TestFrameLayoutUnchangedByMigration asserts the other direction of
// compatibility: a frame written NOW occupies exactly the bytes the legacy
// framing did, so a reader that has not taken this change — an older node in a
// mixed-version cluster, or a forensic tool — parses it correctly.
//
// It is a length-and-position assertion rather than a byte comparison because
// the nonce is random by construction; the golden vectors above cover the
// value-level claim in the other direction.
func TestFrameLayoutUnchangedByMigration(t *testing.T) {
	b, strategy := testInitializedBarrier(t)
	defer func() { _ = strategy.Close() }()

	plaintext := []byte("frame written after the migration")
	frame, err := b.Encrypt(nil, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	const legacyNonceLen, legacyTagLen = 12, 16
	want := epochSize + algIDSize + legacyNonceLen + len(plaintext) + legacyTagLen
	if len(frame) != want {
		t.Fatalf("frame is %d bytes, want %d ([epoch:8][algID:1][nonce:12][ct+tag]); "+
			"the on-disk layout has changed and every stored blob is now a "+
			"different size than its reader expects", len(frame), want)
	}
	if got := SymmetricAlgorithm(frame[epochSize]); got != AlgAES256GCM {
		t.Fatalf("frame algID = %#x, want %#x", byte(got), byte(AlgAES256GCM))
	}
	if got := SymmetricAlgorithm(frame[epochSize]).NonceSize(); got != legacyNonceLen {
		t.Fatalf("SymmetricAlgorithm.NonceSize() = %d, want %d: the wire nonce "+
			"width must stay meaningful even though the AEAD no longer takes "+
			"a caller nonce", got, legacyNonceLen)
	}
}
