package batch

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	quicraft "github.com/jeremyhahn/go-quicraft/pkg"
)

// TestSubmit_MaybeAppliedFlushIsIndeterminate checks that a proposer failure
// reported after hand-off to Raft (*quicraft.MaybeAppliedError) is flagged
// Indeterminate even though it wraps no context error, while the same
// sentinel unwrapped stays a definite rejection (#1224).
func TestSubmit_MaybeAppliedFlushIsIndeterminate(t *testing.T) {
	cases := []struct {
		name          string
		err           error
		indeterminate bool
	}{
		{"maybe applied", &quicraft.MaybeAppliedError{Err: quicraft.ErrShardNotReady}, true},
		{"plain shard not ready", quicraft.ErrShardNotReady, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			agg, err := NewAggregator(rejectingProposer{err: tc.err},
				WithConfig(Config{MaxBatchSize: 1, FlushInterval: 5 * time.Millisecond, QueueSize: 10}),
				WithLogger(slog.Default()),
				WithRegistry(newTestRegistry(t)),
			)
			if err != nil {
				t.Fatalf("NewAggregator: %v", err)
			}
			defer func() { _ = agg.Close() }()

			submitErr := agg.Submit(context.Background(), 1, []byte("write"))
			var flushErr *FlushError
			if !errors.As(submitErr, &flushErr) {
				t.Fatalf("error = %T (%v), want *FlushError", submitErr, submitErr)
			}
			if flushErr.ShardID != 1 {
				t.Errorf("FlushError.ShardID = %d, want 1", flushErr.ShardID)
			}
			if flushErr.Indeterminate != tc.indeterminate {
				t.Errorf("Indeterminate = %v, want %v", flushErr.Indeterminate, tc.indeterminate)
			}
			if !errors.Is(submitErr, quicraft.ErrShardNotReady) {
				t.Error("errors.Is(err, quicraft.ErrShardNotReady) = false, want true")
			}
			if got := quicraft.MaybeApplied(submitErr); got != tc.indeterminate {
				t.Errorf("quicraft.MaybeApplied(err) = %v, want %v", got, tc.indeterminate)
			}
			if !errors.Is(submitErr, ErrFlush) {
				t.Error("errors.Is(err, ErrFlush) = false, want true")
			}
		})
	}
}
