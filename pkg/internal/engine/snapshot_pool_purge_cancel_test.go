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

package engine

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

// blockingPurgeBarrier is a testBarrier whose PurgeEpochsBefore blocks until
// the pool's lifetime context ends, as a purge waiting on a sealing provider
// would. It then fails with failure, or — when failure is nil — with the shape
// go-enclave gives an open whose caller stopped waiting.
type blockingPurgeBarrier struct {
	*testBarrier
	failure error
}

func (b *blockingPurgeBarrier) PurgeEpochsBefore(ctx context.Context, minEpoch uint64) (int, error) {
	<-ctx.Done()
	if b.failure != nil {
		return 0, b.failure
	}
	return 0, errors.Join(enclave.ErrOpenFailed, ctx.Err())
}

// capturePurgeLogs routes the default logger into a buffer at DEBUG for the
// duration of the test.
func capturePurgeLogs(t *testing.T) *logCapture {
	t.Helper()
	buf := &logCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// TestApplyPurge_DuringStop pins how a purge that ends while the pool is
// stopping is logged. One the pool's own stop cut short is logged at DEBUG, not
// reported as a failure; the carve-out is keyed to the error's cause, so a
// persister failure that surfaces while the pool is stopping is still the WARN
// failure line.
func TestApplyPurge_DuringStop(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
		want    []string
		notWant string
	}{
		{
			name:    "lifetime cancel abandons at debug",
			want:    []string{"level=DEBUG", "epoch purge abandoned"},
			notWant: "epoch purge failed",
		},
		{
			name:    "real failure logs warn",
			failure: errors.New("quicraft/seal: no state persister configured"),
			want:    []string{"level=WARN", "epoch purge failed"},
			notWant: "epoch purge abandoned",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := capturePurgeLogs(t)
			stopC := make(chan struct{})
			barrier := &blockingPurgeBarrier{testBarrier: newTestBarrier(5), failure: tc.failure}
			p := newSnapshotPool(1, nil, barrier, nil, nil, stopC, nil)
			close(stopC)

			p.applyPurge(barrier, 3, 1, 1)

			out := logs.String()
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Fatalf("want %q in the purge log, got:\n%s", w, out)
				}
			}
			if strings.Contains(out, tc.notWant) {
				t.Fatalf("the purge log carries %q, the other outcome's line:\n%s", tc.notWant, out)
			}
		})
	}
}
