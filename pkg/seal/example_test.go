// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package seal_test

import (
	"context"
	"fmt"

	"github.com/jeremyhahn/go-quicraft/pkg/seal"
)

// ExampleBarrier seals and unseals a value with the passphrase-based
// SoftwareStrategy, then rotates the data key.
func ExampleBarrier() {
	ctx := context.Background()

	strategy, err := seal.NewSoftwareStrategy(ctx, []byte("correct horse battery staple"))
	if err != nil {
		fmt.Println("strategy:", err)
		return
	}
	defer strategy.Close()

	// The barrier owns no storage: the persister durably records each new
	// sealed state. Rotate refuses to run without one.
	var persisted *seal.SealedRootKey
	cfg := seal.DefaultBarrierConfig(1)
	cfg.StatePersister = func(s *seal.SealedRootKey) error {
		persisted = s
		return nil
	}
	b, err := seal.NewBarrier(cfg)
	if err != nil {
		fmt.Println("barrier:", err)
		return
	}

	// First boot: generate and seal the root key. Initialize does not call
	// the persister; the embedder stores SealedRootKeyData itself. The
	// software strategy needs no credentials, so they are left empty.
	if err := b.Initialize(ctx, strategy, seal.Credentials{}); err != nil {
		fmt.Println("initialize:", err)
		return
	}
	persisted = b.SealedRootKeyData()

	ct, err := b.Encrypt(nil, []byte("hello"))
	if err != nil {
		fmt.Println("encrypt:", err)
		return
	}

	// Seal drops the keys from memory; Unseal recovers them.
	if err := b.Seal(); err != nil {
		fmt.Println("seal:", err)
		return
	}
	fmt.Println("sealed:", b.IsSealed())
	if err := b.Unseal(ctx, strategy, seal.Credentials{}); err != nil {
		fmt.Println("unseal:", err)
		return
	}

	pt, err := b.Decrypt(nil, ct)
	if err != nil {
		fmt.Println("decrypt:", err)
		return
	}
	fmt.Println("decrypted:", string(pt))

	// Rotate moves new writes to a fresh epoch; old ciphertext still opens.
	before := b.CurrentEpoch()
	after, err := b.Rotate(ctx)
	if err != nil {
		fmt.Println("rotate:", err)
		return
	}
	fmt.Println("epoch advanced:", after > before)
	fmt.Println("persisted epoch:", persisted.Epoch == after)

	pt, err = b.Decrypt(nil, ct)
	if err != nil {
		fmt.Println("decrypt after rotate:", err)
		return
	}
	fmt.Println("old ciphertext:", string(pt))
	// Output:
	// sealed: true
	// decrypted: hello
	// epoch advanced: true
	// persisted epoch: true
	// old ciphertext: hello
}
