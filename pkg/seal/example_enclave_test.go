package seal_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// allZero reports whether every byte of b is zero.
func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// Example_enclaveNewSealed is the guide's "Hold a key for the life of a
// service" flow (docs/memory-enclave/user.md): NewSealed consumes the slice
// it is handed, and Open returns the same bytes in a new enclave the caller
// destroys.
func Example_enclaveNewSealed() {
	ctx := context.Background()

	keyBytes := bytes.Repeat([]byte{0xA5}, 32)
	want := bytes.Clone(keyBytes)

	key, err := enclave.NewSealed(ctx, keyBytes)
	if err != nil {
		fmt.Println("seal:", err)
		return
	}
	defer func() { _ = key.Destroy() }()
	fmt.Println("input zeroed:", allZero(keyBytes))

	buf, err := key.Open(ctx)
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	defer func() { _ = buf.Destroy() }()

	var same bool
	if err := buf.WithBytes(func(k []byte) error {
		same = bytes.Equal(k, want)
		return nil
	}); err != nil {
		fmt.Println("borrow:", err)
		return
	}
	fmt.Println("opens to the original bytes:", same)
	// Output:
	// input zeroed: true
	// opens to the original bytes: true
}

// Example_enclaveWithBytes is the guide's "Use the key" and "Retire a key"
// flows: the key is read only inside WithBytes, where an AES-GCM AEAD is built,
// used and dropped; once the key is destroyed, Open reports
// ErrEnclaveDestroyed.
func Example_enclaveWithBytes() {
	ctx := context.Background()

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		fmt.Println("rand:", err)
		return
	}
	key, err := enclave.NewSealed(ctx, raw)
	if err != nil {
		fmt.Println("seal:", err)
		return
	}

	plaintext, aad := []byte("hello"), []byte("header")

	// gcm runs fn with an AES-GCM AEAD (module-generated nonces, FIPS 140-only safe) keyed from a fresh Open of key, built
	// inside the borrow and dropped with it.
	gcm := func(fn func(cipher.AEAD) error) error {
		buf, err := key.Open(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = buf.Destroy() }()
		return buf.WithBytes(func(k []byte) error {
			block, err := aes.NewCipher(k)
			if err != nil {
				return err
			}
			aead, err := cipher.NewGCMWithRandomNonce(block)
			if err != nil {
				return err
			}
			return fn(aead)
		})
	}

	var ciphertext []byte
	if err := gcm(func(aead cipher.AEAD) error {
		ciphertext = aead.Seal(nil, nil, plaintext, aad)
		return nil
	}); err != nil {
		fmt.Println("encrypt:", err)
		return
	}
	var opened []byte
	if err := gcm(func(aead cipher.AEAD) error {
		var err error
		opened, err = aead.Open(nil, nil, ciphertext, aad)
		return err
	}); err != nil {
		fmt.Println("decrypt:", err)
		return
	}
	fmt.Println("round trip:", string(opened))

	_ = key.Destroy()
	_ = key.Destroy() // a second Destroy is fine
	_, err = key.Open(ctx)
	fmt.Println("open after destroy:", errors.Is(err, enclave.ErrEnclaveDestroyed))
	// Output:
	// round trip: hello
	// open after destroy: true
}

// Example_enclaveNewMemorySized is the guide's "Build a key in place, then
// freeze it" flow: a sized enclave is filled where it lies, then frozen
// through the Freezer capability reached with enclave.As. A capability the
// handle lacks is refused by As and reported as a *CapabilityError.
func Example_enclaveNewMemorySized() {
	buf, err := enclave.NewMemorySized(32)
	if err != nil {
		fmt.Println("alloc:", err)
		return
	}
	defer func() { _ = buf.Destroy() }()

	if err := buf.WithBytes(func(dst []byte) error {
		_, err := rand.Read(dst)
		return err
	}); err != nil {
		fmt.Println("fill:", err)
		return
	}

	var f enclave.Freezer
	if !enclave.As(buf, &f) {
		fmt.Println("no freezer")
		return
	}
	fmt.Println("frozen:", f.Freeze() == nil)
	fmt.Println("freeze again:", f.Freeze() == nil)

	// A locked-memory enclave is not an AEAD: the caller reports that as the
	// guide does for a missing Freezer.
	var a enclave.AEAD
	if enclave.As(buf, &a) {
		fmt.Println("unexpected AEAD")
		return
	}
	err = &enclave.CapabilityError{Backend: buf.Backend().Name(), Capability: enclave.CapabilityAEAD}
	var ce *enclave.CapabilityError
	fmt.Println("capability error:", errors.As(err, &ce) && ce.Capability == enclave.CapabilityAEAD)
	fmt.Println("matches ErrCapabilityUnsupported:", errors.Is(err, enclave.ErrCapabilityUnsupported))
	// Output:
	// frozen: true
	// freeze again: true
	// capability error: true
	// matches ErrCapabilityUnsupported: true
}

// Example_enclaveWipe is the guide's "Clean up a slice you could not avoid"
// flow: Wipe zeroes a transient copy in place.
func Example_enclaveWipe() {
	transient := []byte("a copy a third-party API forced on us")
	enclave.Wipe(transient)
	fmt.Println("wiped:", allZero(transient), len(transient) > 0)
	// Output:
	// wiped: true true
}

// Example_enclaveNewCredentials is the guide's "Supply a passphrase to
// go-quicraft's seal layer" flow: NewCredentials consumes the passphrase into
// a Sealed the caller owns and destroys.
func Example_enclaveNewCredentials() {
	ctx := context.Background()

	passphrase := []byte("correct horse battery staple")
	want := bytes.Clone(passphrase)

	creds, err := seal.NewCredentials(ctx, passphrase)
	if err != nil {
		fmt.Println("credentials:", err)
		return
	}
	fmt.Println("passphrase zeroed:", allZero(passphrase))

	buf, err := creds.Secret.Open(ctx)
	if err != nil {
		fmt.Println("open:", err)
		return
	}
	same, err := buf.EqualTo(want)
	_ = buf.Destroy()
	if err != nil {
		fmt.Println("compare:", err)
		return
	}
	fmt.Println("secret holds the passphrase:", same)

	fmt.Println("destroy:", creds.Secret.Destroy())
	_, err = creds.Secret.Open(ctx)
	fmt.Println("open after destroy:", errors.Is(err, enclave.ErrEnclaveDestroyed))

	_, err = seal.NewCredentials(ctx, nil)
	fmt.Println("empty passphrase:", errors.Is(err, seal.ErrInvalidPassphrase))
	// Output:
	// passphrase zeroed: true
	// secret holds the passphrase: true
	// destroy: <nil>
	// open after destroy: true
	// empty passphrase: true
}
