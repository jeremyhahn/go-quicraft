package seal

import (
	"context"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// encCopy returns a heap copy of an enclave's bytes, or nil if it is nil or
// destroyed. The copy is taken inside the borrow, so no view of the locked
// pages outlives it; these tests assert on a nil slice, so a destroyed enclave
// is folded into one here rather than handled at every assertion site.
// Production code never copies protected material out like this.
func encCopy(e *enclave.Memory) []byte {
	var out []byte
	if err := e.WithBytes(func(b []byte) error {
		out = append([]byte(nil), b...)
		return nil
	}); err != nil {
		return nil
	}
	return out
}

// mustSized allocates n bytes of locked, writable memory, failing the test if
// the kernel refuses the lock. go-enclave reports a refused mlock rather than
// hiding it, and a test that ignored the refusal would assert
// against a nil enclave for reasons unrelated to what it is testing.
func mustSized(t testing.TB, n int) *enclave.Memory {
	t.Helper()
	e, err := enclave.NewMemorySized(n)
	if err != nil {
		t.Fatalf("NewMemorySized(%d): %v", n, err)
	}
	return e
}

// destroyOnCleanup releases e when the test ends and fails the test if the
// release is refused. go-enclave's Destroy reports a refused release rather than
// hiding it, and a test environment that cannot release locked memory is one
// whose later allocations would fail for reasons unrelated to the test.
func destroyOnCleanup(t testing.TB, e *enclave.Memory) {
	t.Helper()
	t.Cleanup(func() {
		if err := e.Destroy(); err != nil {
			t.Errorf("Memory.Destroy: %v", err)
		}
	})
}

// mustFromBytes moves data into a locked, FROZEN enclave, failing the test if
// the kernel refuses the lock.
func mustFromBytes(t testing.TB, data []byte) *enclave.Memory {
	t.Helper()
	e, err := enclave.NewMemory(data)
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	return e
}

// mustCreds builds Credentials over a passphrase, failing the test if the
// sealing is refused. It exists because Credentials.Secret is protected
// memory, so a `Credentials{Secret: []byte("pw")}` literal does not compile —
// and because NewCredentials CONSUMES its argument, so each call site needs its own copy of
// the string rather than a shared slice.
func mustCreds(t testing.TB, passphrase string) Credentials {
	t.Helper()
	c, err := NewCredentials(context.Background(), []byte(passphrase))
	if err != nil {
		t.Fatalf("NewCredentials(%q): %v", passphrase, err)
	}
	return c
}
