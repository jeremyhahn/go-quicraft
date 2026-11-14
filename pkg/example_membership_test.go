// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quicraft_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// exampleObserverID, exampleObserverAddr and exampleObserverIdentity name the
// observer the membership examples add. The identity is the form a host
// reports from NodeIdentity: "spki:" and a 64-digit hex key hash.
const (
	exampleObserverID       = 2
	exampleObserverAddr     = "127.0.0.1:63101"
	exampleObserverIdentity = "spki:00000000000000000000000000000000000000000000000000000000000000c0"
)

// runMembershipExample starts the single-node example shard in a fresh
// directory and runs fn against it with a bounded context.
func runMembershipExample(ctx context.Context, fn func(ctx context.Context, h *quicraft.Host)) {
	dir, err := os.MkdirTemp("", "quicraft-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	h, err := startExampleShard(ctx, dir)
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()

	fn(ctx, h)
}

// addExampleObserver adds the example observer with its identity, guarded by
// the ConfigChangeID of a linearizable membership read.
func addExampleObserver(ctx context.Context, h *quicraft.Host) {
	m, err := h.SyncGetShardMembership(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}
	if err := h.SyncRequestAddObserverWithIdentity(ctx, 1, exampleObserverID,
		exampleObserverAddr, exampleObserverIdentity, m.ConfigChangeID); err != nil {
		log.Fatal(err)
	}
}

// This example reads who is in the shard. The Sync read is linearizable, so
// its ConfigChangeID is the one the next config change must pass. A shard
// that has had no config change since it was formed reports 0, which the
// first change passes; every applied change then advances it.
func ExampleHost_SyncGetShardMembership() {
	runMembershipExample(context.Background(), func(ctx context.Context, h *quicraft.Host) {
		m, err := h.SyncGetShardMembership(ctx, 1)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("voters:", len(m.Nodes))
		fmt.Println("replica 1 is a voter:", m.Nodes[1] != "")
		fmt.Println("observers:", len(m.Observers))
		fmt.Println("config change ID before any change:", m.ConfigChangeID)
	})
	// Output:
	// voters: 1
	// replica 1 is a voter: true
	// observers: 0
	// config change ID before any change: 0
}

// This example adds a non-voting observer together with the identity the
// shard commits for it. The membership read after the add lists the observer
// and its identity, and its ConfigChangeID has moved past the old one.
func ExampleHost_SyncRequestAddObserverWithIdentity() {
	runMembershipExample(context.Background(), func(ctx context.Context, h *quicraft.Host) {
		before, err := h.SyncGetShardMembership(ctx, 1)
		if err != nil {
			log.Fatal(err)
		}
		addExampleObserver(ctx, h)

		m, err := h.SyncGetShardMembership(ctx, 1)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("voters:", len(m.Nodes))
		fmt.Println("observer address:", m.Observers[exampleObserverID])
		fmt.Println("observer identity committed:", m.Identities[exampleObserverID] == exampleObserverIdentity)
		fmt.Println("config change ID advanced:", m.ConfigChangeID > before.ConfigChangeID)
		fmt.Println("config change ID now non-zero:", m.ConfigChangeID != 0)
	})
	// Output:
	// voters: 1
	// observer address: 127.0.0.1:63101
	// observer identity committed: true
	// config change ID advanced: true
	// config change ID now non-zero: true
}

// This example removes the observer the previous example added. Removal is a
// consensus operation; afterwards the membership no longer lists the replica
// and records its ID as removed.
func ExampleHost_SyncRequestRemoveNode() {
	runMembershipExample(context.Background(), func(ctx context.Context, h *quicraft.Host) {
		addExampleObserver(ctx, h)

		m, err := h.SyncGetShardMembership(ctx, 1)
		if err != nil {
			log.Fatal(err)
		}
		if err := h.SyncRequestRemoveNode(ctx, 1, exampleObserverID, m.ConfigChangeID); err != nil {
			log.Fatal(err)
		}

		m, err = h.SyncGetShardMembership(ctx, 1)
		if err != nil {
			log.Fatal(err)
		}
		_, listed := m.Observers[exampleObserverID]
		_, removed := m.Removed[exampleObserverID]
		fmt.Println("voters:", len(m.Nodes))
		fmt.Println("observer listed:", listed)
		fmt.Println("observer removed:", removed)
	})
	// Output:
	// voters: 1
	// observer listed: false
	// observer removed: true
}
