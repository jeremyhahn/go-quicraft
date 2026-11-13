package seal

import (
	"bytes"
	"context"
	"errors"
	"testing"

	enclave "github.com/jeremyhahn/go-enclave"
)

// rootKeyProtectedEquals opens the barrier's root key through RootKeyProtected
// and fails the test unless it holds exactly want. The enclave is destroyed
// before return.
func rootKeyProtectedEquals(ctx context.Context, t *testing.T, b *Barrier, want []byte) {
	t.Helper()
	got, err := b.RootKeyProtected(ctx)
	if err != nil {
		t.Fatalf("RootKeyProtected: %v", err)
	}
	defer func() { _ = got.Destroy() }()
	eq, err := got.EqualTo(want)
	if err != nil {
		t.Fatalf("EqualTo: %v", err)
	}
	if !eq {
		t.Fatal("RootKeyProtected does not hold the barrier's root key")
	}
}

// plainRootKey returns a heap copy of the barrier's root key via RootKey.
func plainRootKey(ctx context.Context, t *testing.T, b *Barrier) []byte {
	t.Helper()
	k, err := b.RootKey(ctx)
	if err != nil {
		t.Fatalf("RootKey: %v", err)
	}
	if len(k) == 0 {
		t.Fatal("RootKey returned an empty key")
	}
	return k
}

// TestRootKeyProtected_EqualsRootKey pins (a): on an unsealed barrier the
// protected root key holds the same bytes RootKey returns.
func TestRootKeyProtected_EqualsRootKey(t *testing.T) {
	ctx := context.Background()
	b, s := testInitializedBarrier(t)
	defer s.Close()

	want := plainRootKey(ctx, t, b)
	defer enclave.Wipe(want)
	rootKeyProtectedEquals(ctx, t, b, want)
}

// TestRootKeyProtected_IndependentCopies pins (b): each call hands back its own
// enclave, so destroying one leaves the other readable and the barrier's own
// root key, and so its encryption, untouched.
func TestRootKeyProtected_IndependentCopies(t *testing.T) {
	ctx := context.Background()
	b, s := testInitializedBarrier(t)
	defer s.Close()

	want := plainRootKey(ctx, t, b)
	defer enclave.Wipe(want)

	first, err := b.RootKeyProtected(ctx)
	if err != nil {
		t.Fatalf("first RootKeyProtected: %v", err)
	}
	second, err := b.RootKeyProtected(ctx)
	if err != nil {
		_ = first.Destroy()
		t.Fatalf("second RootKeyProtected: %v", err)
	}
	defer func() { _ = second.Destroy() }()

	ct, err := b.Encrypt(nil, []byte("before destroy"))
	if err != nil {
		_ = first.Destroy()
		t.Fatalf("Encrypt before destroy: %v", err)
	}

	if err := first.Destroy(); err != nil && !errors.Is(err, enclave.ErrReleasedWithoutWipe) {
		t.Fatalf("Destroy: %v", err)
	}
	if _, err := first.EqualTo(want); !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("EqualTo on the destroyed copy = %v, want ErrEnclaveDestroyed", err)
	}

	eq, err := second.EqualTo(want)
	if err != nil {
		t.Fatalf("EqualTo on the surviving copy: %v", err)
	}
	if !eq {
		t.Fatal("destroying one copy changed the other")
	}

	// The barrier's own key is untouched: a fresh call still holds it, old
	// ciphertext still decrypts, and new ciphertext round-trips.
	rootKeyProtectedEquals(ctx, t, b, want)
	pt, err := b.Decrypt(nil, ct)
	if err != nil {
		t.Fatalf("Decrypt after destroy: %v", err)
	}
	if !bytes.Equal(pt, []byte("before destroy")) {
		t.Fatal("Decrypt after destroy returned the wrong plaintext")
	}
	ct2, err := b.Encrypt(nil, []byte("after destroy"))
	if err != nil {
		t.Fatalf("Encrypt after destroy: %v", err)
	}
	pt2, err := b.Decrypt(nil, ct2)
	if err != nil {
		t.Fatalf("Decrypt of new ciphertext: %v", err)
	}
	if !bytes.Equal(pt2, []byte("after destroy")) {
		t.Fatal("round trip after destroy returned the wrong plaintext")
	}
}

// TestRootKeyProtected_Sealed pins (c): before Initialize and after Seal the
// barrier holds no root key to hand out and says so with ErrBarrierSealed.
func TestRootKeyProtected_Sealed(t *testing.T) {
	ctx := context.Background()

	fresh := MustNewBarrier(testBarrierConfig(1))
	if got, err := fresh.RootKeyProtected(ctx); !errors.Is(err, ErrBarrierSealed) || got != nil {
		t.Fatalf("RootKeyProtected before Initialize = (%v, %v), want (nil, ErrBarrierSealed)", got, err)
	}

	b, s := testInitializedBarrier(t)
	defer s.Close()
	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if got, err := b.RootKeyProtected(ctx); !errors.Is(err, ErrBarrierSealed) || got != nil {
		t.Fatalf("RootKeyProtected after Seal = (%v, %v), want (nil, ErrBarrierSealed)", got, err)
	}
}

// TestRootKeyProtected_NoEncryption pins (d): a no-encryption barrier keeps no
// root key and returns ErrNoRootKeyAvailable rather than an empty enclave.
func TestRootKeyProtected_NoEncryption(t *testing.T) {
	ctx := context.Background()
	s := NewNoneStrategy()
	defer s.Close()

	b := MustNewBarrier(DefaultBarrierConfig(1))
	if err := b.Initialize(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Initialize (none): %v", err)
	}
	if b.IsSealed() {
		t.Fatal("no-encryption barrier is sealed after Initialize")
	}
	if got, err := b.RootKeyProtected(ctx); !errors.Is(err, ErrNoRootKeyAvailable) || got != nil {
		t.Fatalf("RootKeyProtected (none) = (%v, %v), want (nil, ErrNoRootKeyAvailable)", got, err)
	}
}

// TestRootKeyProtected_AfterUnseal pins (e): Seal then Unseal restores the same
// root key, so RootKeyProtected hands back the bytes it held before the seal.
func TestRootKeyProtected_AfterUnseal(t *testing.T) {
	ctx := context.Background()
	b, s := testInitializedBarrier(t)
	defer s.Close()

	want := plainRootKey(ctx, t, b)
	defer enclave.Wipe(want)
	rootKeyProtectedEquals(ctx, t, b, want)

	if err := b.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := b.Unseal(ctx, s, Credentials{}); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	rootKeyProtectedEquals(ctx, t, b, want)
}
