// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0

package batch_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jeremyhahn/go-quicraft/pkg/batch"
)

// ExampleDecodeBatch is the decode loop of docs/write-batching/user.md
// ("Decode batches in your state machine"): one committed entry carries a
// whole batch, DecodeBatch splits it back into the submitted commands in
// order, and an empty entry decodes to nil with no error.
func ExampleDecodeBatch() {
	entry, err := batch.EncodeBatch([][]byte{
		[]byte("set a=1"),
		[]byte("set b=2"),
		[]byte("del c"),
	})
	if err != nil {
		fmt.Println("encode:", err)
		return
	}

	cmds, err := batch.DecodeBatch(entry)
	if err != nil {
		fmt.Println("decode:", err)
		return
	}
	fmt.Println("commands:", len(cmds))
	for i, cmd := range cmds {
		fmt.Printf("%d: %s\n", i, cmd)
	}

	empty, err := batch.DecodeBatch(nil)
	fmt.Println("empty entry:", empty == nil, err)

	// Output:
	// commands: 3
	// 0: set a=1
	// 1: set b=2
	// 2: del c
	// empty entry: true <nil>
}

// failingProposer is a Proposer whose every propose fails with err.
type failingProposer struct {
	err error
}

func (p failingProposer) SyncPropose(_ context.Context, _ uint64, _ []byte) error {
	return p.err
}

// classify is the switch of docs/write-batching/user.md ("Handle errors").
func classify(err error) string {
	var fe *batch.FlushError
	switch {
	case err == nil:
		return "applied"
	case errors.As(err, &fe) && fe.Indeterminate:
		return fmt.Sprintf("indeterminate on shard %d", fe.ShardID)
	case errors.Is(err, batch.ErrQueueFull):
		return "queue full: back off and retry"
	default:
		return "definite failure: not applied"
	}
}

// submitOnce submits one command through an Aggregator over proposer,
// prints how the guide's switch classifies the result, and closes the
// Aggregator.
func submitOnce(ctx context.Context, proposer batch.Proposer, shardID uint64) {
	cfg := batch.DefaultConfig()
	cfg.FlushInterval = time.Millisecond
	agg, err := batch.NewAggregator(proposer,
		batch.WithConfig(cfg),
		batch.WithRegistry(prometheus.NewRegistry()),
		batch.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		fmt.Println("new aggregator:", err)
		return
	}

	err = agg.Submit(ctx, shardID, []byte("set a=1"))
	var fe *batch.FlushError
	if errors.As(err, &fe) {
		fmt.Printf("FlushError: shard=%d indeterminate=%t\n", fe.ShardID, fe.Indeterminate)
	}
	fmt.Println(classify(err))
	fmt.Println("close:", agg.Close())
}

// ExampleFlushError shows the error a Submit caller sees when the batch's
// propose fails. A plain rejection is a definite failure; a propose that
// times out is indeterminate, because the batch may still commit. Both carry
// the shard ID, and Close returns nil.
func ExampleFlushError() {
	ctx := context.Background()

	submitOnce(ctx, failingProposer{err: errors.New("not leader")}, 7)
	submitOnce(ctx, failingProposer{err: context.DeadlineExceeded}, 8)

	// Output:
	// FlushError: shard=7 indeterminate=false
	// definite failure: not applied
	// close: <nil>
	// FlushError: shard=8 indeterminate=true
	// indeterminate on shard 8
	// close: <nil>
}
