package shamir

import (
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// encCopy returns a heap copy of an enclave's bytes, or nil if it is nil or
// destroyed. The copy is taken inside the borrow, so no view of the locked
// pages outlives it; these tests assert on a nil slice, so a destroyed enclave
// is folded into one here rather than handled at ~30 assertion sites.
// Production code never copies protected material out like this.
func encCopy(e enclave.ByteAccess) []byte {
	var out []byte
	if err := e.WithBytes(func(b []byte) error {
		out = append([]byte(nil), b...)
		return nil
	}); err != nil {
		return nil
	}
	return out
}

// valueCopy is encCopy for a share's value, taken through WithValue: a heap
// copy of the base64 value, or nil if the share is nil or destroyed.
func valueCopy(s *ProtectedShare) []byte {
	var out []byte
	if err := s.WithValue(func(v []byte) error {
		out = append([]byte(nil), v...)
		return nil
	}); err != nil {
		return nil
	}
	return out
}

// mustFromBytes moves data into a locked, frozen enclave, failing the test if the
// kernel refuses the lock; go-enclave reports a refused mlock instead of hiding
// it.
func mustFromBytes(t testing.TB, data []byte) enclave.ByteAccess {
	t.Helper()
	e, err := enclave.NewMemory(data)
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	return e
}

var _ = enclave.ErrEnclaveDestroyed
