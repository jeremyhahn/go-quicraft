package shamir

import (
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// encBytes returns an enclave's bytes, or nil if it is nil or destroyed.
// go-enclave's Open reports a destroyed enclave as an error; these tests
// assert on a nil slice, so the error is folded into one here rather than
// handled at ~30 assertion sites. Production code handles the error.
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

// mustFromBytes moves data into a locked, frozen enclave, failing the test if the
// kernel refuses the lock; go-enclave reports a refused mlock instead of hiding
// it.
func mustFromBytes(t testing.TB, data []byte) *enclave.MemoryEnclave {
	t.Helper()
	e, err := enclave.NewMemoryEnclave(data)
	if err != nil {
		t.Fatalf("NewMemoryEnclave: %v", err)
	}
	return e
}

var _ = enclave.ErrEnclaveDestroyed
