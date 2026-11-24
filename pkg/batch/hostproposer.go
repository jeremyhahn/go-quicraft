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

// SyncProposer is the subset of *quicraft.Host that HostProposer adapts, declared structurally to avoid an import cycle.
type SyncProposer interface {
	// SyncPropose proposes cmd to the shard and blocks until it is applied.
	SyncPropose(ctx context.Context, shardID uint64, cmd []byte) (sm.Result, error)
}

// HostProposer adapts a Host to Proposer, discarding the aggregate sm.Result: batch.NewAggregator(batch.HostProposer{Host: host}, ...).
type HostProposer struct {
	// Host is the proposer the adapter forwards to, normally a *quicraft.Host.
	Host SyncProposer
}

// SyncPropose submits cmd and discards the aggregate result; use Host.SyncPropose for per-command results.
func (p HostProposer) SyncPropose(ctx context.Context, shardID uint64, cmd []byte) error {
	_, err := p.Host.SyncPropose(ctx, shardID, cmd)
	return err
}

// Compile-time proof that the adapter satisfies the interface it exists for.
// Without this, a signature drift on either side would only be caught by a
// downstream build.
var _ Proposer = HostProposer{}
