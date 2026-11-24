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

import (
	"testing"

	"github.com/jeremyhahn/go-quicraft/pkg/internal/raft"
	"github.com/jeremyhahn/go-quicraft/pkg/logdb"
	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// bootstrapLogEntry returns the committed EntryInitialMembership that every
// shard this code base bootstraps lays down at raft.BootstrapMembershipIndex,
// as a logdb.Entry a fixture can seed straight into a LogDB.
//
// A restart fixture that puts an ordinary entry at that index is not modelling
// any shard this code can create; it is modelling a DAMAGED log, which
// reconstructShardMembership now refuses (*BootstrapEntryMissingError) rather
// than re-seeding membership from the caller's members map.
//
// term is the fixture's own term for index 1. Reconstruction reads only the
// entry's type and payload, but keeping the term consistent with the rest of
// the fixture's entries keeps the log a legal one.
func bootstrapLogEntry(t *testing.T, term uint64, addresses map[uint64]string) logdb.Entry {
	t.Helper()
	e, err := raft.BuildInitialMembershipEntry(proto.Membership{Addresses: addresses})
	if err != nil {
		t.Fatalf("BuildInitialMembershipEntry: %v", err)
	}
	return logdb.Entry{Index: e.Index, Term: term, Type: e.Type, Cmd: e.Cmd}
}
