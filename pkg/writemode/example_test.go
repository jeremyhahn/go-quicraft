// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package writemode_test

import (
	"errors"
	"fmt"

	"github.com/jeremyhahn/go-quicraft/pkg/writemode"
)

// ExampleFromString is "Carry a durability mode in configuration" in
// docs/write-batching/user.md: a configured name parses to a Mode that
// round-trips through String, the durability predicates answer as the guide
// states (Batched requires quorum), and both an unknown name and an unknown
// Mode are refused with *InvalidModeError.
func ExampleFromString() {
	for _, name := range []string{"sync", "wal_durable", "batched"} {
		m, err := writemode.FromString(name)
		if err != nil {
			fmt.Println(name, "error:", err)
			continue
		}
		fmt.Printf("%s: string=%s sync=%t async=%t quorum=%t valid=%v\n",
			name, m.String(), m.IsSynchronous(), m.IsAsync(), m.RequiresQuorum(), m.Validate())
	}

	var ime *writemode.InvalidModeError
	_, err := writemode.FromString("eventual")
	fmt.Println("unknown name:", errors.As(err, &ime), ime.Value)

	unknown := writemode.Mode(99)
	err = unknown.Validate()
	fmt.Println("unknown mode:", errors.As(err, &ime), ime.Value)
	fmt.Printf("unknown mode predicates: sync=%t async=%t quorum=%t\n",
		unknown.IsSynchronous(), unknown.IsAsync(), unknown.RequiresQuorum())

	// Output:
	// sync: string=sync sync=true async=false quorum=true valid=<nil>
	// wal_durable: string=wal_durable sync=false async=true quorum=false valid=<nil>
	// batched: string=batched sync=true async=false quorum=true valid=<nil>
	// unknown name: true eventual
	// unknown mode: true 99
	// unknown mode predicates: sync=false async=false quorum=false
}
