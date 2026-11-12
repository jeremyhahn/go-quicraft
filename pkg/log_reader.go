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
	// currentTerm reports the live Raft term; nil reports the log-derived
	// term only.
	currentTerm func() uint64
}

// GetRange returns the first and last index of the Raft log.
func (a *logReaderAdapter) GetRange() (uint64, uint64) {
	return a.reader.IndexRange()
}

// NodeState returns the last log index and current term.
//
// The current term is read from the live Raft peer through currentTerm: a
// term can advance without a log append (a follower with a term-2 log that
// accepts a term-3 heartbeat is at term 3), so neither the log tail nor the
// reader's persisted state, recorded at bootstrap and restart only, can be
// relied on alone. The larger of the peer's term and the log-derived term
// (the larger of the persisted term and the last entry's term) is reported;
// with no currentTerm the log-derived term is reported.
func (a *logReaderAdapter) NodeState() (uint64, uint64) {
	_, lastIndex := a.reader.IndexRange()
	state, _ := a.reader.NodeState()
	term := state.Term
	if lastIndex > 0 {
		if lastTerm, err := a.reader.Term(lastIndex); err == nil && lastTerm > term {
			term = lastTerm
		}
	}
	if a.currentTerm != nil {
		if current := a.currentTerm(); current > term {
			term = current
		}
	}
	return lastIndex, term
}
