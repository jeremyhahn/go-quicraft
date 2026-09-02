package shamir

import (
	"testing"

	goenclave "github.com/jeremyhahn/go-enclave"
	"github.com/jeremyhahn/go-quicraft/pkg/enclave"
)

// encBytes is the test-side replacement for the MemoryEnclave.Bytes accessor
// this package used to own. go-enclave's Open reports a destroyed enclave as an
// error where the old accessor reported it as a nil slice; these tests were
// written against the nil, so the error is folded back into one here rather than
// rewritten at ~30 assertion sites. Production code handles the error.
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

// mustFromBytes moves data into a locked, frozen enclave, failing the test if the
// kernel refuses the lock. It replaces the bare-pointer constructor this package
// used to own; go-enclave reports a refused mlock instead of hiding it.
func mustFromBytes(t testing.TB, data []byte) *goenclave.MemoryEnclave {
	t.Helper()
	e, err := goenclave.NewMemoryEnclave(data)
	if err != nil {
		t.Fatalf("NewMemoryEnclave: %v", err)
	}
	return e
}

var _ = enclave.ErrEnclaveDestroyed
