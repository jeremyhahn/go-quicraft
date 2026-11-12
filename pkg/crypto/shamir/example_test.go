// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package shamir_test

import (
	"fmt"

	"github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"
)

// ExampleSplitBytes splits a secret into five shares, any three of which
// reconstruct it, then combines three of them.
func ExampleSplitBytes() {
	secret := []byte("root key material")

	shares, err := shamir.SplitBytes(secret, 3, 5)
	if err != nil {
		fmt.Println("split:", err)
		return
	}
	// Share values are secret material: wipe them once distributed.
	defer func() {
		for _, s := range shares {
			clear(s)
		}
	}()
	fmt.Println("shares:", len(shares))

	// Any threshold-sized subset works; here shares 1, 3 and 5.
	subset := [][]byte{shares[0], shares[2], shares[4]}
	recovered, err := shamir.CombineBytes(subset, 3)
	if err != nil {
		fmt.Println("combine:", err)
		return
	}
	defer clear(recovered)

	// CombineBytes returns a CombineError when given fewer values than the
	// threshold argument, but it does not verify the result: corrupted or
	// mismatched shares, or a threshold below the one used to split, yield
	// garbage, not an error. Callers authenticate the secret out of band;
	// this example only prints it.
	fmt.Println("recovered:", string(recovered))
	// Output:
	// shares: 5
	// recovered: root key material
}
