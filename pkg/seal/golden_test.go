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
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/golden"
)

// The fixed golden inputs. None of them is a secret: they exist so the
// encrypted layouts can be reproduced byte for byte.
var (
	goldenDEK        = bytes.Repeat([]byte{0x5a}, 32)
	goldenRootKey    = bytes.Repeat([]byte{0x3c}, 32)
	goldenSalt       = bytes.Repeat([]byte{0xa5}, kdfSaltLen)
	goldenNonce      = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	goldenPassphrase = []byte("golden fixture passphrase")
	goldenContextAAD = []byte("wal/segment/000042")
)

const (
	goldenEpoch        = 9
	goldenDeploymentID = 0xfeedface
	goldenPlaintext    = "golden barrier frame plaintext"
)

// fixedNonceAEAD is the test-only seam the encrypted fixtures are written
// through. Production AES-GCM draws its nonce inside the FIPS module
// (cipher.NewGCMWithRandomNonce), reports NonceSize() == 0 and prepends the
// nonce to its Seal output. This wrapper has that exact shape but prepends a
// fixed nonce, so the bytes it writes are the bytes production writes, minus
// the randomness.
type fixedNonceAEAD struct {
	gcm   cipher.AEAD
	nonce []byte
}

// newFixedNonceAEAD returns the seam, or skips t when the build refuses a
// caller-supplied GCM nonce (FIPS 140-only mode). Decoding never needs it.
func newFixedNonceAEAD(t *testing.T, key []byte) cipher.AEAD {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("aes.NewCipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Skipf("re-encoding needs a caller-nonce GCM, which this mode refuses: %v", err)
	}
	return fixedNonceAEAD{gcm: gcm, nonce: goldenNonce}
}

func (a fixedNonceAEAD) NonceSize() int { return 0 }
func (a fixedNonceAEAD) Overhead() int  { return len(a.nonce) + a.gcm.Overhead() }

func (a fixedNonceAEAD) Seal(dst, _, plaintext, aad []byte) []byte {
	dst = append(dst, a.nonce...)
	return a.gcm.Seal(dst, a.nonce, plaintext, aad)
}

func (a fixedNonceAEAD) Open(dst, _, ciphertext, aad []byte) ([]byte, error) {
	n := len(a.nonce)
	return a.gcm.Open(dst, ciphertext[:n], ciphertext[n:], aad)
}

func checkGoldenReencoded(t *testing.T, name string, got []byte) {
	t.Helper()
	if raw := golden.Load(t, name); !bytes.Equal(got, raw) {
		t.Fatalf("re-encoding the decoded %s changed its bytes:\n got  %x\n want %x", name, got, raw)
	}
}

func encodeGoldenFrame(t *testing.T, key, nonce, plaintext []byte) []byte {
	t.Helper()
	b := MustNewBarrier(DefaultBarrierConfig(goldenDeploymentID))
	aead := newFixedNonceAEAD(t, key).(fixedNonceAEAD)
	aead.nonce = nonce
	out, err := b.encryptLocked(nil, plaintext, goldenEpoch, aead, goldenContextAAD)
	if err != nil {
		t.Fatalf("encryptLocked: %v", err)
	}
	return out
}

// TestGoldenBarrierFrame pins the barrier ciphertext frame: big-endian epoch,
// algorithm byte, nonce, ciphertext and tag, authenticated under the epoch
// and the caller's context.
func TestGoldenBarrierFrame(t *testing.T) {
	if golden.Updating() {
		golden.Check(t, "barrier-frame.bin", encodeGoldenFrame(t, goldenDEK, goldenNonce, []byte(goldenPlaintext)))
	}

	raw := golden.Load(t, "barrier-frame.bin")
	b := MustNewBarrier(DefaultBarrierConfig(goldenDeploymentID))
	// Decoding goes through the production module-nonce AEAD, which works in
	// every mode; only re-encoding needs the fixed-nonce seam.
	prod, err := newAESGCM(goldenDEK)
	if err != nil {
		t.Fatalf("newAESGCM: %v", err)
	}
	alg := SymmetricAlgorithm(raw[epochSize])
	if alg != AlgAES256GCM {
		t.Fatalf("algorithm byte = %#x, want %#x", byte(alg), byte(AlgAES256GCM))
	}
	pt, err := b.decryptLocked(nil, raw, prod, alg, goldenContextAAD)
	if err != nil {
		t.Fatalf("decryptLocked: %v", err)
	}
	if string(pt) != goldenPlaintext {
		t.Fatalf("plaintext = %q, want %q", pt, goldenPlaintext)
	}
	if epoch := binary.BigEndian.Uint64(raw[:epochSize]); epoch != goldenEpoch {
		t.Fatalf("frame epoch = %d, want %d", epoch, goldenEpoch)
	}
	nonce := raw[epochSize+algIDSize : epochSize+algIDSize+len(goldenNonce)]
	if !bytes.Equal(nonce, goldenNonce) {
		t.Fatalf("nonce = %x, want %x", nonce, goldenNonce)
	}
	checkGoldenReencoded(t, "barrier-frame.bin", encodeGoldenFrame(t, goldenDEK, nonce, pt))
}

func goldenStrategy(t *testing.T) *SoftwareStrategy {
	t.Helper()
	s, err := NewSoftwareStrategyFIPS(context.Background(), goldenPassphrase, minPBKDF2Iter)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyFIPS: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func encodeGoldenSealedRootKey(t *testing.T, s *SoftwareStrategy, salt, nonce, rootKey []byte) []byte {
	t.Helper()
	key, err := DeriveKey(goldenPassphrase, salt, s.KDFVersion(), s.KDFParams())
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	aead := newFixedNonceAEAD(t, key).(fixedNonceAEAD)
	aead.nonce = nonce
	return s.sealedFromCipher(aead, salt, rootKey).Ciphertext
}

// TestGoldenSealedRootKey pins the passphrase strategy's sealed root key
// ciphertext: KDF salt, nonce, wrapped key and tag.
func TestGoldenSealedRootKey(t *testing.T) {
	s := goldenStrategy(t)
	if golden.Updating() {
		golden.Check(t, "sealed-root-key.bin", encodeGoldenSealedRootKey(t, s, goldenSalt, goldenNonce, goldenRootKey))
	}

	raw := golden.Load(t, "sealed-root-key.bin")
	sealed := &SealedRootKey{
		Strategy:   StrategyPassphrase,
		Ciphertext: raw,
		Metadata:   []byte{byte(KDFPbkdf2SHA256)},
	}
	got, err := s.UnsealRootKey(context.Background(), sealed, Credentials{})
	if err != nil {
		t.Fatalf("UnsealRootKey: %v", err)
	}
	if !bytes.Equal(got, goldenRootKey) {
		t.Fatalf("root key = %x, want %x", got, goldenRootKey)
	}
	salt := raw[:kdfSaltLen]
	nonce := raw[kdfSaltLen : kdfSaltLen+wrappingGCMNonceSize]
	if !bytes.Equal(salt, goldenSalt) || !bytes.Equal(nonce, goldenNonce) {
		t.Fatalf("salt/nonce = %x/%x, want %x/%x", salt, nonce, goldenSalt, goldenNonce)
	}
	if len(raw) != kdfSaltLen+wrappingGCMNonceSize+len(goldenRootKey)+wrappingGCMTagSize {
		t.Fatalf("fixture is %d bytes", len(raw))
	}
	checkGoldenReencoded(t, "sealed-root-key.bin", encodeGoldenSealedRootKey(t, s, salt, nonce, got))
}

func goldenSealedState() *SealedRootKey {
	return &SealedRootKey{
		Strategy: StrategyPassphrase, Version: 1, Epoch: goldenEpoch, LoadGeneration: 4,
		Salt:        bytes.Repeat([]byte{0x11}, 32),
		KnownEpochs: []uint64{7, 8, 9},
		HKDFVersion: 1, Algorithm: AlgAES256GCM,
		Ciphertext:      bytes.Repeat([]byte{0x22}, 92),
		Metadata:        []byte{byte(KDFPbkdf2SHA256)},
		Nonce:           goldenNonce,
		HardwarePayload: []byte("hw"),
	}
}

// TestGoldenSealedStateMAC pins the version-2 sealed-state MAC: HMAC-SHA256,
// under a key HKDF-derived from the root key, over every authenticated field
// of the sealed root key.
func TestGoldenSealedStateMAC(t *testing.T) {
	in := goldenSealedState()
	mac, err := sealedStateMAC(goldenRootKey, goldenDeploymentID, in)
	if err != nil {
		t.Fatalf("sealedStateMAC: %v", err)
	}
	golden.Check(t, "sealed-state-mac.bin", mac)

	raw := golden.Load(t, "sealed-state-mac.bin")
	b := MustNewBarrier(DefaultBarrierConfig(goldenDeploymentID))
	b.sealed = goldenSealedState()
	b.sealed.StateMAC = raw
	if err := b.verifySealedStateMAC(goldenRootKey); err != nil {
		t.Fatalf("verifySealedStateMAC: %v", err)
	}
	b.sealed.LoadGeneration++
	if err := b.verifySealedStateMAC(goldenRootKey); err == nil {
		t.Fatal("verifySealedStateMAC accepted the fixture over a changed load generation")
	}
	again, err := sealedStateMAC(goldenRootKey, goldenDeploymentID, goldenSealedState())
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	checkGoldenReencoded(t, "sealed-state-mac.bin", again)
}
