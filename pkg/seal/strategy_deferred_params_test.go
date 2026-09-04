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
	"context"
	"errors"
	"os"
	"testing"
)

// TestKDFParams_RoundTripsEveryConstructor pins that KDFParams() reports what
// the strategy was actually built with, for every constructor that exists.
//
// This is the read half of go-qrdb #156. The work factor is not recoverable any
// other way: a sealed blob records the KDF VERSION byte and nothing else, so the
// live strategy object is the only place the number exists.
func TestKDFParams_RoundTripsEveryConstructor(t *testing.T) {
	restore := AllowWeakKDFParams()
	defer restore()

	passphrase := []byte("a-passphrase-long-enough-to-not-warn")

	t.Run("NewSoftwareStrategy uses the defaults for the selected version", func(t *testing.T) {
		s, err := NewSoftwareStrategy(passphrase)
		if err != nil {
			t.Fatalf("NewSoftwareStrategy: %v", err)
		}
		defer s.Close()

		want := DefaultKDFParams()
		if s.KDFVersion() == KDFPbkdf2SHA256 {
			want = DefaultFIPSKDFParams()
		}
		if s.KDFParams() != want {
			t.Fatalf("KDFParams() = %+v, want %+v", s.KDFParams(), want)
		}
	})

	t.Run("NewSoftwareStrategyFIPS reports its iteration count", func(t *testing.T) {
		const iterations = 123_456
		s, err := NewSoftwareStrategyFIPS(passphrase, iterations)
		if err != nil {
			t.Fatalf("NewSoftwareStrategyFIPS: %v", err)
		}
		defer s.Close()

		if got := s.KDFParams().Iterations; got != iterations {
			t.Fatalf("Iterations = %d, want %d", got, iterations)
		}
		if s.KDFVersion() != KDFPbkdf2SHA256 {
			t.Fatalf("KDFVersion = %v, want PBKDF2", s.KDFVersion())
		}
	})

	t.Run("NewSoftwareStrategyWithParams reports its Argon2id factors", func(t *testing.T) {
		s, err := NewSoftwareStrategyWithParams(passphrase, 7, 96*1024, 3)
		if err != nil {
			// A -tags fips build has no Argon2id deriver at all. That refusal is
			// the documented behaviour, not a failure of this accessor.
			if errors.Is(err, ErrKDFUnsupportedVersion) || errors.Is(err, ErrKDFNotFIPSApproved) {
				t.Skipf("Argon2id unavailable in this build/mode: %v", err)
			}
			t.Fatalf("NewSoftwareStrategyWithParams: %v", err)
		}
		defer s.Close()

		got := s.KDFParams()
		if got.Time != 7 || got.Memory != 96*1024 || got.Threads != 3 {
			t.Fatalf("KDFParams() = %+v, want Time=7 Memory=98304 Threads=3", got)
		}
	})

	t.Run("NewDeferredSoftwareStrategy still reports the defaults", func(t *testing.T) {
		s := NewDeferredSoftwareStrategy()
		defer s.Close()

		want := DefaultKDFParams()
		if s.KDFVersion() == KDFPbkdf2SHA256 {
			want = DefaultFIPSKDFParams()
		}
		if s.KDFParams() != want {
			t.Fatalf("KDFParams() = %+v, want %+v", s.KDFParams(), want)
		}
	})

	t.Run("NewDeferredSoftwareStrategyWithParams reports what it was given", func(t *testing.T) {
		params := KDFParams{Iterations: 222_222, KeyLen: 32, SaltLen: 32}
		s, err := NewDeferredSoftwareStrategyWithParams(KDFPbkdf2SHA256, params)
		if err != nil {
			t.Fatalf("NewDeferredSoftwareStrategyWithParams: %v", err)
		}
		defer s.Close()

		if s.KDFParams() != params {
			t.Fatalf("KDFParams() = %+v, want %+v", s.KDFParams(), params)
		}
		if !s.DeferredPassphrase() {
			t.Fatal("the strategy is not deferred, which is the whole point of it")
		}
	})
}

// TestNewDeferredSoftwareStrategyWithParams_ComposesWithKDFParams is the shape
// go-qrdb's ChangePassword actually needs: read the factors off the strategy in
// hand, carry them into the deferred replacement, without branching on the
// algorithm. Taking (version, params) together is what makes that one line.
func TestNewDeferredSoftwareStrategyWithParams_ComposesWithKDFParams(t *testing.T) {
	restore := AllowWeakKDFParams()
	defer restore()

	old, err := NewSoftwareStrategyFIPS([]byte("the-original-passphrase-value"), 150_000)
	if err != nil {
		t.Fatalf("NewSoftwareStrategyFIPS: %v", err)
	}
	defer old.Close()

	replacement, err := NewDeferredSoftwareStrategyWithParams(old.KDFVersion(), old.KDFParams())
	if err != nil {
		t.Fatalf("carrying the work factor into the replacement: %v", err)
	}
	defer replacement.Close()

	if replacement.KDFVersion() != old.KDFVersion() || replacement.KDFParams() != old.KDFParams() {
		t.Fatalf("the replacement did not preserve the work factor: %v/%+v vs %v/%+v",
			replacement.KDFVersion(), replacement.KDFParams(),
			old.KDFVersion(), old.KDFParams())
	}
	if !replacement.DeferredPassphrase() {
		t.Fatal("the replacement is bound, so the barrier would stop authenticating its unsealer")
	}
}

// TestDeferredStrategy_NonDefaultWorkFactorIsPreservedAcrossSealUnseal is the
// assertion the issue asks for by name, and the one a defaults-only fixture
// cannot make: default-in/default-out passes whether params are preserved or
// reset, so it proves nothing.
//
// A blob sealed at a NON-default work factor must unseal through a strategy
// carrying that same factor, and must FAIL through one built at the defaults.
// The second half is what distinguishes "preserved" from "reset".
func TestDeferredStrategy_NonDefaultWorkFactorIsPreservedAcrossSealUnseal(t *testing.T) {
	restore := AllowWeakKDFParams()
	defer restore()

	// Deliberately not DefaultFIPSKDFParams().Iterations. Kept small because the
	// bypass above permits it and PBKDF2 at production counts would make this
	// test slow for no extra signal.
	tuned := KDFParams{Iterations: 4_321, KeyLen: 32, SaltLen: 32}
	defaults := DefaultFIPSKDFParams()
	if tuned.Iterations == defaults.Iterations {
		t.Fatal("the fixture must differ from the defaults or it proves nothing")
	}

	creds := mustCreds(t, "the-deferred-passphrase-value")
	rootKey := make([]byte, rootKeySize)
	for i := range rootKey {
		rootKey[i] = byte(i + 1)
	}

	sealer, err := NewDeferredSoftwareStrategyWithParams(KDFPbkdf2SHA256, tuned)
	if err != nil {
		t.Fatalf("tuned deferred strategy: %v", err)
	}
	defer sealer.Close()

	sealed, err := sealer.SealRootKey(context.Background(), rootKey, creds)
	if err != nil {
		t.Fatalf("SealRootKey at the tuned factor: %v", err)
	}

	// Same factor: must succeed.
	unsealer, err := NewDeferredSoftwareStrategyWithParams(KDFPbkdf2SHA256, tuned)
	if err != nil {
		t.Fatalf("second tuned strategy: %v", err)
	}
	defer unsealer.Close()

	got, err := unsealer.UnsealRootKey(context.Background(), sealed, creds)
	if err != nil {
		t.Fatalf("UnsealRootKey with the preserved work factor: %v", err)
	}
	if len(got) != rootKeySize {
		t.Fatalf("unsealed %d bytes, want %d", len(got), rootKeySize)
	}

	// Defaults: must FAIL. This is the half that catches a reset work factor —
	// the blob's metadata records only the KDF version, so a strategy at the
	// wrong factor derives a different wrapping key and AES-GCM refuses it.
	reset := NewDeferredSoftwareStrategy()
	defer reset.Close()
	if reset.KDFParams() == tuned {
		t.Fatal("the default strategy somehow carries the tuned factor")
	}

	if _, err := reset.UnsealRootKey(context.Background(), sealed, creds); err == nil {
		t.Fatal("a strategy built at the DEFAULT work factor unsealed a blob " +
			"sealed at a tuned one: the work factor is not actually being used, " +
			"so preserving it would be meaningless")
	}
}

// TestNewDeferredSoftwareStrategyWithParams_FIPS pins the asymmetry the issue
// asked to be explicit about, so a consumer preserving a work factor cannot
// compile everywhere and fail on the FIPS leg only.
//
// The two FIPS mechanisms give different errors, and both are the bound
// constructors' behaviour exactly: a -tags fips BUILD has no Argon2id deriver
// (ErrKDFUnsupportedVersion), while runtime FIPS in a build that has one refuses
// it by policy (ErrKDFNotFIPSApproved).
func TestNewDeferredSoftwareStrategyWithParams_FIPS(t *testing.T) {
	restore := AllowWeakKDFParams()
	defer restore()

	t.Run("PBKDF2 is accepted with FIPS active", func(t *testing.T) {
		original := os.Getenv(envGOFIPS140)
		t.Cleanup(func() {
			if original == "" {
				os.Unsetenv(envGOFIPS140)
			} else {
				os.Setenv(envGOFIPS140, original)
			}
		})
		os.Setenv(envGOFIPS140, "1")

		s, err := NewDeferredSoftwareStrategyWithParams(KDFPbkdf2SHA256, DefaultFIPSKDFParams())
		if err != nil {
			t.Fatalf("PBKDF2 must be accepted under FIPS: %v", err)
		}
		defer s.Close()
	})

	t.Run("Argon2id is refused with FIPS active", func(t *testing.T) {
		original := os.Getenv(envGOFIPS140)
		t.Cleanup(func() {
			if original == "" {
				os.Unsetenv(envGOFIPS140)
			} else {
				os.Setenv(envGOFIPS140, original)
			}
		})
		os.Setenv(envGOFIPS140, "1")

		_, err := NewDeferredSoftwareStrategyWithParams(KDFArgon2id, DefaultKDFParams())
		if err == nil {
			t.Fatal("Argon2id must be refused while FIPS is active")
		}
		// Either error is correct, and which one says which mechanism refused:
		// absence of the deriver in a -tags fips build, or the runtime policy.
		if !errors.Is(err, ErrKDFNotFIPSApproved) && !errors.Is(err, ErrKDFUnsupportedVersion) {
			t.Fatalf("error = %v, want ErrKDFNotFIPSApproved or ErrKDFUnsupportedVersion", err)
		}
	})

	t.Run("an unknown version is refused", func(t *testing.T) {
		_, err := NewDeferredSoftwareStrategyWithParams(KDFVersion(200), DefaultFIPSKDFParams())
		if !errors.Is(err, ErrKDFUnsupportedVersion) {
			t.Fatalf("error = %v, want ErrKDFUnsupportedVersion", err)
		}
	})
}
