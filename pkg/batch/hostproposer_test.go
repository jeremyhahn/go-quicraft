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

package batch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

type fakeSyncProposer struct {
	gotShard uint64
	gotCmd   []byte
	result   sm.Result
	err      error
}

func (f *fakeSyncProposer) SyncPropose(_ context.Context, shardID uint64, cmd []byte) (sm.Result, error) {
	f.gotShard = shardID
	f.gotCmd = append([]byte(nil), cmd...)
	return f.result, f.err
}

// TestHostProposer_AdaptsHostSignature pins the adapter that makes the
// documented construction compile.
//
// README showed batch.NewAggregator(host, ...), but *Host.SyncPropose returns
// (sm.Result, error) while Proposer requires error alone — so *Host never
// satisfied Proposer, no adapter existed anywhere, and the package had zero
// importers. Every behaviour it claims was verified against mocks only.
func TestHostProposer_AdaptsHostSignature(t *testing.T) {
	fake := &fakeSyncProposer{result: sm.Result{Value: 7}}
	var p Proposer = HostProposer{Host: fake}

	if err := p.SyncPropose(context.Background(), 3, []byte("cmd")); err != nil {
		t.Fatalf("SyncPropose = %v, want nil", err)
	}
	if fake.gotShard != 3 || string(fake.gotCmd) != "cmd" {
		t.Fatalf("adapter altered the call: shard=%d cmd=%q",
			fake.gotShard, fake.gotCmd)
	}
}

// TestHostProposer_PropagatesError pins that the error — the only channel a
// batched submitter has — is not swallowed along with the result.
func TestHostProposer_PropagatesError(t *testing.T) {
	want := errors.New("propose failed")
	p := HostProposer{Host: &fakeSyncProposer{err: want}}
	if err := p.SyncPropose(context.Background(), 1, []byte("x")); !errors.Is(err, want) {
		t.Fatalf("SyncPropose = %v, want %v", err, want)
	}
}

// TestHostProposer_DrivesAnAggregator is the integration the package never
// had: the adapter actually used to construct and flush an Aggregator, rather
// than a mock shaped like whatever the test needed.
func TestHostProposer_DrivesAnAggregator(t *testing.T) {
	fake := &fakeSyncProposer{}
	cfg := DefaultConfig()
	cfg.MaxBatchSize = 4
	cfg.FlushInterval = 5 * time.Millisecond
	agg, err := NewAggregator(HostProposer{Host: fake}, WithConfig(cfg))
	if err != nil {
		t.Fatalf("NewAggregator: %v", err)
	}
	t.Cleanup(func() { _ = agg.Close() })

	if err := agg.Submit(context.Background(), 9, []byte("hello")); err != nil {
		t.Fatalf("Submit through the adapter failed: %v", err)
	}
	if fake.gotShard != 9 {
		t.Fatalf("the aggregator proposed to shard %d, want 9", fake.gotShard)
	}
	if len(fake.gotCmd) == 0 {
		t.Fatal("the aggregator proposed an empty batch")
	}
}
