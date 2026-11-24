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

import "testing"

// TestEpochAAD_IsActuallyInTheAAD fails if and only if the epoch stops being
// folded into the AEAD's additional authenticated data.
//
// The four existing tests named after this property could not detect its
// removal: deleting the epoch from aadForEpoch left the entire pkg/seal suite
// green. Cross-epoch splicing is ALREADY prevented by key separation — a frame
// sealed under DEK_3 will not open under DEK_5 whatever the AAD is — so a test
// that observes ErrDecryptionFailed cannot tell "the AAD bound the epoch" from
// "the key was different". The two tests that call epochAAD directly keep
// passing while nothing on the data path calls it.
//
// This test attacks the AAD directly instead: it opens the ciphertext with the
// SAME epoch's AEAD and varies only the AAD, so key separation cannot satisfy
// it. The epoch AAD is defence-in-depth today, layered over per-epoch DEKs —
// which is exactly why its silent loss would go unnoticed until something else
// made two epochs share key material.
func TestEpochAAD_IsActuallyInTheAAD(t *testing.T) {
	b, s := testInitializedBarrier(t)
	defer func() { _ = s.Close() }()

	ct, err := b.Encrypt(nil, []byte("frame"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	b.mu.RLock()
	epoch := b.epoch
	aead := b.ciphers[epoch]
	b.mu.RUnlock()
	if aead == nil {
		t.Fatal("no AEAD for the current epoch; the fixture is wrong")
	}

	nonceStart := epochSize + algIDSize
	nonce := ct[nonceStart : nonceStart+aead.NonceSize()]
	body := ct[nonceStart+aead.NonceSize():]

	// The declared AAD must open it — otherwise the test proves nothing below.
	if _, err := aead.Open(nil, nonce, body, aadForEpoch(epoch, nil)); err != nil {
		t.Fatalf("ciphertext does not open under aadForEpoch(epoch): %v", err)
	}
	// An EMPTY AAD must NOT, which is true only if the epoch is really bound.
	if _, err := aead.Open(nil, nonce, body, nil); err == nil {
		t.Fatal("the ciphertext opened under an EMPTY AAD: the epoch is not " +
			"in the AAD at all, and every test named after this property " +
			"passes anyway because per-epoch key separation hides it")
	}
	// Nor may a neighbouring epoch's AAD.
	if _, err := aead.Open(nil, nonce, body, aadForEpoch(epoch+1, nil)); err == nil {
		t.Fatal("the ciphertext opened under a DIFFERENT epoch's AAD")
	}
}

// TestBarrier_EncryptNoncesAreFreshAndRandom asserts the FIRST line of defence
// — the nonce bytes themselves — with the tracker OFF.
//
// Forcing the nonce to a constant fails 11 tests today, but every one of them
// fails via ErrNonceReuse from InMemoryNonceTracker: the suite catches a broken
// nonce only through the second-line detector, never through an assertion about
// the generator. NonceTracking is a documented, supported opt-out
// (BarrierConfig.NonceTracking), and the configuration guidance steers
// high-throughput deployments toward disabling it — so in exactly those
// deployments the regression would be invisible.
//
// A repeated (key, nonce) pair under AES-GCM is catastrophic: it breaks
// confidentiality and leaks the authentication key.
func TestBarrier_EncryptNoncesAreFreshAndRandom(t *testing.T) {
	cfg := testBarrierConfig(1)
	cfg.NonceTracking = BoolPtr(false)
	b := MustNewBarrier(cfg)
	s := testStrategy(t)
	defer func() { _ = s.Close() }()
	if err := b.Initialize(t.Context(), s, Credentials{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if b.NonceTracker() != nil {
		t.Fatal("this test must run with the tracker OFF, or it proves only " +
			"that the detector works")
	}

	const n = 2048
	nonceStart := epochSize + algIDSize
	seen := make(map[string]struct{}, n)
	var ored, anded byte = 0x00, 0xff

	for i := 0; i < n; i++ {
		ct, err := b.Encrypt(nil, []byte("frame"))
		if err != nil {
			t.Fatalf("Encrypt %d: %v", i, err)
		}
		nonce := ct[nonceStart : nonceStart+12]
		if _, dup := seen[string(nonce)]; dup {
			t.Fatalf("encrypt %d repeated nonce %x under one key: with GCM "+
				"this leaks the authentication key", i, nonce)
		}
		seen[string(nonce)] = struct{}{}
		for _, c := range nonce {
			ored |= c
			anded &= c
		}
	}

	// A constant, zeroed or otherwise degenerate nonce leaves these stuck.
	// Over 2048 random draws the chance of a genuine source failing either is
	// about 2^-2048.
	if ored != 0xff || anded != 0x00 {
		t.Fatalf("nonce bytes are not varying across 2048 draws: OR=%02x "+
			"AND=%02x — the generator is degenerate", ored, anded)
	}
}
