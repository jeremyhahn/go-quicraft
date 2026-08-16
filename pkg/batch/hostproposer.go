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

	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// SyncProposer is the subset of *quicraft.Host that HostProposer adapts.
//
// It is declared structurally rather than importing the host package, which
// would be an import cycle: the host is the thing this package is batched on
// behalf of.
type SyncProposer interface {
	SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error)
}

// HostProposer adapts a Host to the Proposer interface.
//
// It exists because the documented way to construct an Aggregator did not
// compile: README showed batch.NewAggregator(host, ...), but *Host's
// SyncPropose returns (sm.Result, error) while Proposer requires error alone,
// so *Host never satisfied Proposer. No adapter existed anywhere in this
// repository or downstream, and `git grep quicraft/pkg/batch` found no
// importers — so every behaviour this package claims (ordering, indeterminate
// flush handling, backpressure) was verified only against mocks.
//
// The discarded sm.Result is not an oversight but the limitation it names: a
// batched proposal carries commands from mutually unaware callers, and the
// aggregate result cannot be attributed to any individual one. See Proposer.
type HostProposer struct {
	Host SyncProposer
}

// SyncPropose submits cmd and discards the state machine's aggregate result.
//
// The error is what reaches the submitting caller. A caller that needs the
// state machine's per-command result must use Host.SyncPropose directly:
// batching trades that away, and Submit's documentation says so.
func (p HostProposer) SyncPropose(ctx context.Context, shardID uint64, cmd []byte) error {
	_, err := p.Host.SyncPropose(ctx, shardID, cmd)
	return err
}

// Compile-time proof that the adapter satisfies the interface it exists for.
// Without this, a signature drift on either side would only be caught by a
// downstream build.
var _ Proposer = HostProposer{}
