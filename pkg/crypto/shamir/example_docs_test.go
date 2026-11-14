// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package shamir_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"
)

// These examples compile the protected-share snippets of
// docs/shamir-primitive/user.md, so a renamed or re-signatured function fails
// the build instead of leaving the guide wrong.

// exampleRootKey is a fixed 32-byte key the examples split and recover.
func exampleRootKey() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i + 1)
	}
	return k
}

// secretEquals reports whether the protected secret holds want, comparing it
// inside its borrow so no copy of the secret leaves protected memory.
func secretEquals(secret enclave.ByteAccess, want []byte) bool {
	match := false
	if err := secret.WithBytes(func(b []byte) error {
		match = bytes.Equal(b, want)
		return nil
	}); err != nil {
		return false
	}
	return match
}

// ExampleSplitProtected follows the guide's "Run a ceremony in protected
// memory" snippet: split 3-of-5, hand each share out through WithValue, and
// Destroy every share.
func ExampleSplitProtected() {
	rootKey := exampleRootKey()

	shares, err := shamir.SplitProtected(rootKey, 3, 5)
	if err != nil {
		fmt.Println("split:", err)
		return
	}
	// Destroy as each is consumed: locked memory is a finite kernel resource.
	defer func() {
		for _, s := range shares {
			s.Destroy()
		}
	}()

	deliverToCustodian := func(index int, v []byte) error {
		fmt.Printf("share %d: %d value bytes\n", index, len(v))
		return nil
	}
	for _, s := range shares {
		// WithValue holds the share open for the callback, so a concurrent
		// Destroy blocks until it returns. Do not keep the slice past it.
		if err := s.WithValue(func(v []byte) error {
			return deliverToCustodian(s.Index, v)
		}); err != nil {
			fmt.Println("deliver:", err)
			return
		}
	}
	// Output:
	// share 1: 48 value bytes
	// share 2: 48 value bytes
	// share 3: 48 value bytes
	// share 4: 48 value bytes
	// share 5: 48 value bytes
}

// ExampleUnmarshalShareSet follows the guide's recovery snippet: a share set
// submitted from outside the process as JSON is decoded with
// UnmarshalShareSet, the submitted buffer is wiped, and CombineProtected
// reconstructs the key.
func ExampleUnmarshalShareSet() {
	rootKey := exampleRootKey()

	split, err := shamir.SplitProtected(rootKey, 3, 5)
	if err != nil {
		fmt.Println("split:", err)
		return
	}
	defer func() {
		for _, s := range split {
			s.Destroy()
		}
	}()
	// The submitted set: three custodians' shares, as JSON of the share set.
	submitted, err := json.Marshal(split[1:4])
	if err != nil {
		fmt.Println("marshal:", err)
		return
	}

	// Shares that arrived from outside the process: bounded decode, never
	// json.Unmarshal into a []*ProtectedShare.
	shares, err := shamir.UnmarshalShareSet(submitted)
	enclave.Wipe(submitted) // the decoded copies are protected; wipe your buffer
	if err != nil {
		fmt.Println("unmarshal:", err)
		return
	}
	defer func() {
		for _, s := range shares {
			s.Destroy()
		}
	}()

	secret, err := shamir.CombineProtected(shares)
	if err != nil {
		fmt.Println("combine:", err)
		return
	}
	defer secret.Destroy()

	fmt.Println("decoded shares:", len(shares))
	fmt.Println("submitted buffer wiped:", bytes.Count(submitted, []byte{0}) == len(submitted))
	fmt.Println("secret matches:", secretEquals(secret, rootKey))
	// Output:
	// decoded shares: 3
	// submitted buffer wiped: true
	// secret matches: true
}

// ExampleCombineProtected combines three of five shares, which reconstructs
// the key, and two of five, which CombineProtected refuses: each share carries
// the threshold it was split under, and the count is checked against it.
func ExampleCombineProtected() {
	rootKey := exampleRootKey()

	shares, err := shamir.SplitProtected(rootKey, 3, 5)
	if err != nil {
		fmt.Println("split:", err)
		return
	}
	defer func() {
		for _, s := range shares {
			s.Destroy()
		}
	}()

	// Any three of the five: here shares 1, 3 and 5.
	secret, err := shamir.CombineProtected([]*shamir.ProtectedShare{shares[0], shares[2], shares[4]})
	if err != nil {
		fmt.Println("combine 3 of 5:", err)
		return
	}
	defer secret.Destroy()
	fmt.Println("3 of 5 matches:", secretEquals(secret, rootKey))

	// Below the threshold.
	short, err := shamir.CombineProtected(shares[:2])
	var ce *shamir.CombineError
	switch {
	case errors.As(err, &ce):
		fmt.Println("2 of 5 refused:", ce.Reason)
	case err != nil:
		fmt.Println("2 of 5 unexpected error:", err)
	default:
		defer short.Destroy()
		fmt.Println("2 of 5 matches:", secretEquals(short, rootKey))
	}
	// Output:
	// 3 of 5 matches: true
	// 2 of 5 refused: need at least 3 shares, got 2
}
