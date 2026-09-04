package seal

import (
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// encBytes is the test-side replacement for the MemoryEnclave.Bytes accessor
// this repository's former pkg/enclave used to own. go-enclave's Open reports a
// destroyed enclave as an ERROR where the old accessor reported it as a nil
// slice; these tests were written against the nil, so the error is folded back
// into one here rather than rewritten at every assertion site. Production code
// handles the error.
func encBytes(e *enclave.MemoryEnclave) []byte {
	if e == nil {
		return nil
	}
	b, err := e.Open()
	if err != nil {
		return nil
	}
	return b
}

// mustSized allocates n bytes of locked, writable memory, failing the test if
// the kernel refuses the lock. It replaces the bare-pointer constructor this
// repository's former pkg/enclave used to own: go-enclave reports a refused
// mlock rather than hiding it, and a test that ignored the refusal would assert
// against a nil enclave for reasons unrelated to what it is testing.
func mustSized(t testing.TB, n int) *enclave.MemoryEnclave {
	t.Helper()
	e, err := enclave.NewMemoryEnclaveSized(n)
	if err != nil {
		t.Fatalf("NewMemoryEnclaveSized(%d): %v", n, err)
	}
	return e
}

// mustFromBytes moves data into a locked, FROZEN enclave, failing the test if
// the kernel refuses the lock.
func mustFromBytes(t testing.TB, data []byte) *enclave.MemoryEnclave {
	t.Helper()
	e, err := enclave.NewMemoryEnclave(data)
	if err != nil {
		t.Fatalf("NewMemoryEnclave: %v", err)
	}
	return e
}

// mustCreds builds Credentials over a passphrase, failing the test if the
// sealing is refused. It exists because #41 retyped Credentials.Secret onto
// protected memory, so the `Credentials{Secret: []byte("pw")}` literal that
// every deferred-strategy test used no longer compiles — and because
// NewCredentials CONSUMES its argument, so each call site needs its own copy of
// the string rather than a shared slice.
func mustCreds(t testing.TB, passphrase string) Credentials {
	t.Helper()
	c, err := NewCredentials([]byte(passphrase))
	if err != nil {
		t.Fatalf("NewCredentials(%q): %v", passphrase, err)
	}
	return c
}
