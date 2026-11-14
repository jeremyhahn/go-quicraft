package seal_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// ExampleBarrier_SetSealedRootKey is the persist and restart lifecycle of
// the "Minimal correct setup" block in docs/seal-barrier/user.md: the first
// process initializes the barrier and writes SealedRootKeyData as JSON; a
// later process reads the JSON back, hands it to SetSealedRootKey and
// unseals with a strategy built from the same passphrase.
func ExampleBarrier_SetSealedRootKey() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "seal-example-")
	if err != nil {
		fmt.Println("tempdir:", err)
		return
	}
	defer os.RemoveAll(dir)
	blobPath := filepath.Join(dir, "sealed-root-key.json")

	// The guide's persister also fsyncs the file and its directory; that
	// durability is not what this example checks.
	persist := func(s *seal.SealedRootKey) error {
		blob, err := json.Marshal(s)
		if err != nil {
			return err
		}
		return os.WriteFile(blobPath, blob, 0o600)
	}
	newBarrier := func() (*seal.Barrier, error) {
		cfg := seal.DefaultBarrierConfig(1)
		cfg.StatePersister = persist
		return seal.NewBarrier(cfg)
	}

	// First process: initialize, persist, encrypt, seal.
	ct, err := firstBoot(ctx, newBarrier, persist)
	if err != nil {
		fmt.Println("first boot:", err)
		return
	}

	// Second process: read the blob back and unseal with the same passphrase.
	pt, err := restart(ctx, newBarrier, blobPath, "correct horse battery staple", ct)
	if err != nil {
		fmt.Println("restart:", err)
		return
	}
	fmt.Println("decrypted after restart:", string(pt))

	// A strategy built from a different passphrase cannot unwrap the root
	// key: Unseal returns an *UnsealError and the barrier stays sealed.
	_, err = restart(ctx, newBarrier, blobPath, "wrong passphrase", ct)
	var ue *seal.UnsealError
	fmt.Println("wrong passphrase refused:", errors.As(err, &ue))
	// Output:
	// blob written: true
	// sealed: true
	// decrypted after restart: hello
	// still sealed: true
	// wrong passphrase refused: true
}

// firstBoot initializes a new barrier, persists its sealed root key,
// encrypts a value and seals the barrier. It returns the ciphertext.
func firstBoot(
	ctx context.Context,
	newBarrier func() (*seal.Barrier, error),
	persist func(*seal.SealedRootKey) error,
) ([]byte, error) {
	strategy, err := seal.NewSoftwareStrategy(ctx, []byte("correct horse battery staple"))
	if err != nil {
		return nil, err
	}
	defer strategy.Close()

	b, err := newBarrier()
	if err != nil {
		return nil, err
	}
	// Seal on every return path so the keys are erased before the deferred
	// strategy.Close runs.
	defer func() { _ = b.Seal() }()

	// Initialize does not call the persister; the embedder stores
	// SealedRootKeyData itself.
	if err := b.Initialize(ctx, strategy, seal.Credentials{}); err != nil {
		return nil, err
	}
	if err := persist(b.SealedRootKeyData()); err != nil {
		return nil, err
	}
	fmt.Println("blob written: true")

	ct, err := b.Encrypt(nil, []byte("hello"))
	if err != nil {
		return nil, err
	}
	if err := b.Seal(); err != nil {
		return nil, err
	}
	fmt.Println("sealed:", b.IsSealed())
	return ct, nil
}

// restart builds a new barrier from the persisted JSON, unseals it with a
// strategy from passphrase and decrypts ct.
func restart(
	ctx context.Context,
	newBarrier func() (*seal.Barrier, error),
	blobPath, passphrase string,
	ct []byte,
) ([]byte, error) {
	strategy, err := seal.NewSoftwareStrategy(ctx, []byte(passphrase))
	if err != nil {
		return nil, err
	}
	defer strategy.Close()

	blob, err := os.ReadFile(blobPath)
	if err != nil {
		return nil, err
	}
	sealed := new(seal.SealedRootKey)
	if err := json.Unmarshal(blob, sealed); err != nil {
		return nil, err
	}

	b, err := newBarrier()
	if err != nil {
		return nil, err
	}
	defer func() { _ = b.Seal() }()

	if err := b.SetSealedRootKey(sealed); err != nil {
		return nil, err
	}
	if err := b.Unseal(ctx, strategy, seal.Credentials{}); err != nil {
		fmt.Println("still sealed:", b.IsSealed())
		return nil, err
	}
	return b.Decrypt(nil, ct)
}
