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
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
	"github.com/jeremyhahn/go-quicraft/pkg/config"
	"github.com/jeremyhahn/go-quicraft/pkg/sm/kv"
)

// ExampleNotLeader shows the redirect check from
// docs/raft-consensus/user.md: NotLeader finds the leader hint through any
// wrapping, and also matches the bare ErrNotLeader sentinel, which carries no
// hint.
func ExampleNotLeader() {
	hinted := fmt.Errorf("propose: %w", &quicraft.NotLeaderError{
		ShardID:       1,
		LeaderID:      3,
		LeaderAddress: "replica-3.example:63100",
	})
	if leaderID, ok := quicraft.NotLeader(hinted); ok {
		fmt.Println("redirect to replica", leaderID)
	}
	var nle *quicraft.NotLeaderError
	if errors.As(hinted, &nle) {
		fmt.Println("leader address:", nle.LeaderAddress)
	}

	bare := fmt.Errorf("propose: %w", quicraft.ErrNotLeader)
	leaderID, ok := quicraft.NotLeader(bare)
	fmt.Println("sentinel:", ok, "leader:", leaderID)

	_, ok = quicraft.NotLeader(quicraft.ErrSystemBusy)
	fmt.Println("busy is not-leader:", ok)
	// Output:
	// redirect to replica 3
	// leader address: replica-3.example:63100
	// sentinel: true leader: 0
	// busy is not-leader: false
}

// ExampleMaybeApplied shows the duplicate-write check from
// docs/raft-consensus/user.md. A proposal whose wait timed out after it was
// handed to Raft is wrapped in *MaybeAppliedError and may still apply; a
// definite rejection, and a read's timeout (reads append nothing), may not.
func ExampleMaybeApplied() {
	submittedTimeout := fmt.Errorf("sync propose: %w",
		&quicraft.MaybeAppliedError{Err: quicraft.ErrTimeout})
	fmt.Println("submitted timeout may apply:", quicraft.MaybeApplied(submittedTimeout))
	fmt.Println("still matches ErrTimeout:", errors.Is(submittedTimeout, quicraft.ErrTimeout))

	fmt.Println("busy rejection may apply:", quicraft.MaybeApplied(quicraft.ErrSystemBusy))
	fmt.Println("read timeout may apply:", quicraft.MaybeApplied(quicraft.ErrTimeout))
	fmt.Println("nil may apply:", quicraft.MaybeApplied(nil))
	// Output:
	// submitted timeout may apply: true
	// still matches ErrTimeout: true
	// busy rejection may apply: false
	// read timeout may apply: false
	// nil may apply: false
}

// ExampleHost_SyncReadBuf shows the pooled linearizable read from
// docs/raft-consensus/user.md on a running single-node shard. The release
// func is always non-nil and must be called once the buffer is done with.
func ExampleHost_SyncReadBuf() {
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
	defer h.Close()

	cmd, err := kv.EncodePut([]byte("k"), []byte("v"))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := h.SyncPropose(ctx, 1, cmd); err != nil {
		log.Fatal(err)
	}

	buf, release, err := h.SyncReadBuf(ctx, 1, []byte("k"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("k = %s\n", buf)
	release()
	fmt.Println("released")
	// Output:
	// k = v
	// released
}

// ExampleHost_StartShard_leaseRead shows the LeaseRead rules from
// docs/raft-consensus/user.md, which config.Config.Validate (run by
// StartShard) enforces: LeaseRead needs CheckQuorum, and ElectionRTT has a
// minimum of 5.
func ExampleHost_StartShard_leaseRead() {
	cfg := config.Config{ShardID: 1, ReplicaID: 1}
	cfg.SetDefaults()
	cfg.LeaseRead = true
	cfg.CheckQuorum = nil // nil means enabled
	cfg.ElectionRTT = 10
	fmt.Println("lease read with check quorum:", cfg.Validate())

	cfg.CheckQuorum = config.BoolPtr(false)
	err := cfg.Validate()
	var lrErr *config.LeaseReadRequiresCheckQuorumError
	fmt.Println("without check quorum:", errors.As(err, &lrErr),
		errors.Is(err, config.ErrLeaseReadRequiresCheckQuorum))

	cfg.CheckQuorum = nil
	cfg.ElectionRTT = 4
	err = cfg.Validate()
	var vErr *config.ValidationError
	if errors.As(err, &vErr) {
		fmt.Println("ElectionRTT 4 refused, field:", vErr.Field)
	}
	// Output:
	// lease read with check quorum: <nil>
	// without check quorum: true true
	// ElectionRTT 4 refused, field: ElectionRTT
}
