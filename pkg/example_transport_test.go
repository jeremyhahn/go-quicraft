// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package quicraft_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
)

// ExampleHost_NodeIdentity shows the quic-transport guide's no-transport
// mode (docs/quic-transport/user.md, "Task: run without a transport"): a
// Host built with WithoutTransport has no key to derive an identity from,
// so NodeIdentity is empty and its shard starts without member identities.
func ExampleHost_NodeIdentity() {
	dir, err := os.MkdirTemp("", "quicraft-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h, err := startExampleShard(ctx, dir)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("identity=%q\n", h.NodeIdentity())

	if err := h.Close(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// identity=""
}
