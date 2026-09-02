package seal

import (
	"testing"

	goenclave "github.com/jeremyhahn/go-enclave"
)

// encBytes is the test-side replacement for the MemoryEnclave.Bytes accessor
// pkg/enclave used to own. go-enclave's Open reports a destroyed enclave as an
// error where the old accessor reported it as a nil slice; these tests were
// written against the nil, so the error is folded back into one here rather than
// rewritten at every assertion site. Production code handles the error.
func encBytes(e *goenclave.MemoryEnclave) []byte {
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
// the kernel refuses the lock. It replaces the bare-pointer constructor
// pkg/enclave used to own: go-enclave reports a refused mlock rather than
// hiding it, and a test that ignored the refusal would assert against a nil
// enclave for reasons unrelated to what it is testing.
func mustSized(t testing.TB, n int) *goenclave.MemoryEnclave {
	t.Helper()
	e, err := goenclave.NewMemoryEnclaveSized(n)
	if err != nil {
		t.Fatalf("NewMemoryEnclaveSized(%d): %v", n, err)
	}
	return e
}

// mustFromBytes moves data into a locked, FROZEN enclave, failing the test if
// the kernel refuses the lock.
func mustFromBytes(t testing.TB, data []byte) *goenclave.MemoryEnclave {
	t.Helper()
	e, err := goenclave.NewMemoryEnclave(data)
	if err != nil {
		t.Fatalf("NewMemoryEnclave: %v", err)
	}
	return e
}
