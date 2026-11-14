package seal

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
)

// faultyAADEncryptor is an AADSymmetricEncrypter double over an honest AEAD
// that injects one fault: a failing EncryptWithAAD, a failing matching-AAD
// DecryptWithAAD, or a matching-AAD DecryptWithAAD that returns other bytes.
// "Matching" means the aad passed to the last EncryptWithAAD call.
type faultyAADEncryptor struct {
	*honestAADEncryptor
	encryptErr      error
	matchDecryptErr error
	corruptMatch    bool
	sealedAAD       []byte
}

func newFaultyAADEncryptor(ctx context.Context, t *testing.T) *faultyAADEncryptor {
	t.Helper()
	_ = ctx
	return &faultyAADEncryptor{honestAADEncryptor: newHonestAADEncryptor(t)}
}

func (e *faultyAADEncryptor) EncryptWithAAD(plaintext, aad []byte) ([]byte, error) {
	if e.encryptErr != nil {
		return nil, e.encryptErr
	}
	e.sealedAAD = append([]byte(nil), aad...)
	return e.honestAADEncryptor.EncryptWithAAD(plaintext, aad)
}

func (e *faultyAADEncryptor) DecryptWithAAD(ciphertext, aad []byte) ([]byte, error) {
	matching := bytes.Equal(aad, e.sealedAAD)
	if matching && e.matchDecryptErr != nil {
		return nil, e.matchDecryptErr
	}
	got, err := e.honestAADEncryptor.DecryptWithAAD(ciphertext, aad)
	if err != nil || !matching || !e.corruptMatch {
		return got, err
	}
	other := append([]byte(nil), got...)
	other[0] ^= 0xff
	return other, nil
}

// failAfterReader yields n random bytes, then fails every read with err.
type failAfterReader struct {
	n   int
	err error
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, r.err
	}
	if len(p) > r.n {
		p = p[:r.n]
	}
	k, err := rand.Reader.Read(p)
	r.n -= k
	return k, err
}

// requireAADProbeStage asserts that err is an *AADBindingProbeError at stage
// want whose chain reaches cause.
func requireAADProbeStage(ctx context.Context, t *testing.T, err error, want string, cause error) {
	t.Helper()
	_ = ctx
	var probeErr *AADBindingProbeError
	if !errors.As(err, &probeErr) {
		t.Fatalf("probe error = %#v, want *AADBindingProbeError", err)
	}
	if probeErr.Stage != want {
		t.Fatalf("probe stage = %q (%v), want %q", probeErr.Stage, err, want)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("probe error = %v, want it to wrap %v", err, cause)
	}
}

func TestAADProbe_ProbeEntropyStage(t *testing.T) {
	ctx := context.Background()
	errEntropy := errors.New("entropy source failed")
	// 0 fails the probe read; aadProbeSize the first aad read; twice that the
	// second aad read. Every one is the probe-entropy stage.
	for _, n := range []int{0, aadProbeSize, 2 * aadProbeSize} {
		enc := newFaultyAADEncryptor(ctx, t)
		err := verifyAADBinding(enc, &failAfterReader{n: n, err: errEntropy})
		requireAADProbeStage(ctx, t, err, "probe-entropy", errEntropy)
		if enc.sealedAAD != nil {
			t.Fatalf("reader failing after %d bytes: the probe encrypted before its entropy was complete", n)
		}
	}
}

func TestAADProbe_EncryptStage(t *testing.T) {
	ctx := context.Background()
	errEncrypt := errors.New("encrypt failed")
	enc := newFaultyAADEncryptor(ctx, t)
	enc.encryptErr = errEncrypt
	err := verifyAADBinding(enc, rand.Reader)
	requireAADProbeStage(ctx, t, err, "encrypt", errEncrypt)
}

func TestAADProbe_DecryptMatchingAADStage(t *testing.T) {
	ctx := context.Background()
	errDecrypt := errors.New("decrypt failed")
	enc := newFaultyAADEncryptor(ctx, t)
	enc.matchDecryptErr = errDecrypt
	err := verifyAADBinding(enc, rand.Reader)
	requireAADProbeStage(ctx, t, err, "decrypt-matching-aad", errDecrypt)
	if errors.Is(err, ErrAADProbeNotBound) {
		t.Fatalf("probe error = %v: a failing matching-AAD decrypt is not an unbound AAD", err)
	}
}

func TestAADProbe_RoundTripMismatch(t *testing.T) {
	ctx := context.Background()
	enc := newFaultyAADEncryptor(ctx, t)
	enc.corruptMatch = true
	err := verifyAADBinding(enc, rand.Reader)
	requireAADProbeStage(ctx, t, err, "decrypt-matching-aad", ErrAADProbeRoundTrip)
	if errors.Is(err, ErrAADProbeNotBound) {
		t.Fatalf("probe error = %v: a round-trip mismatch must not report ErrAADProbeNotBound", err)
	}
}

// TestAADProbe_HonestDoublePasses pins that the double without a fault passes
// the probe, so each failure above comes from its injected fault alone.
func TestAADProbe_HonestDoublePasses(t *testing.T) {
	ctx := context.Background()
	enc := newFaultyAADEncryptor(ctx, t)
	if err := verifyAADBinding(enc, rand.Reader); err != nil {
		t.Fatalf("fault-free double failed the probe: %v", err)
	}
	if _, err := io.ReadFull(&failAfterReader{n: 4, err: io.ErrUnexpectedEOF}, make([]byte, 4)); err != nil {
		t.Fatalf("failAfterReader yielded an error inside its byte budget: %v", err)
	}
}
