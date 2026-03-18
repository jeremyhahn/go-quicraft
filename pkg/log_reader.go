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

package quicraft

import "github.com/jeremyhahn/go-quicraft/pkg/logdb"

// ReadonlyLogReader provides read-only access to a shard's Raft log state.
// It exposes a minimal surface area for inspecting log range and node state
// without permitting mutations.
type ReadonlyLogReader interface {
	// GetRange returns the first and last index of the Raft log for this shard.
	GetRange() (firstIndex uint64, lastIndex uint64)
	// NodeState returns the last log index and current term.
	NodeState() (lastIndex uint64, term uint64)
}

// logReaderAdapter wraps a logdb.LogReader to expose only read-only methods.
type logReaderAdapter struct {
	reader *logdb.LogReader
}

// GetRange returns the first and last index of the Raft log.
func (a *logReaderAdapter) GetRange() (uint64, uint64) {
	return a.reader.IndexRange()
}

// NodeState returns the last log index and current term.
func (a *logReaderAdapter) NodeState() (uint64, uint64) {
	_, lastIndex := a.reader.IndexRange()
	state, _ := a.reader.NodeState()
	return lastIndex, state.Term
}
