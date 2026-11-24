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

package waldb

import "github.com/jeremyhahn/go-quicraft/pkg/logdb"

// copySnapshot returns a deep copy of ss safe to retain.
func copySnapshot(ss logdb.Snapshot) logdb.Snapshot {
	ss.Membership = copyMembership(ss.Membership)
	return ss
}

// copyMembership returns a deep copy of m; a nil map stays nil.
func copyMembership(m logdb.Membership) logdb.Membership {
	m.Addresses = copyAddrMap(m.Addresses)
	m.Observers = copyAddrMap(m.Observers)
	m.Witnesses = copyAddrMap(m.Witnesses)
	m.Identities = copyAddrMap(m.Identities)
	m.RemovedIdentities = copyAddrMap(m.RemovedIdentities)
	if m.Removed != nil {
		removed := make(map[uint64]bool, len(m.Removed))
		for k, v := range m.Removed {
			removed[k] = v
		}
		m.Removed = removed
	}
	return m
}

// copyAddrMap clones a replica-address map, preserving nil.
func copyAddrMap(src map[uint64]string) map[uint64]string {
	if src == nil {
		return nil
	}
	dst := make(map[uint64]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// copyBootstrap returns a deep copy of bs. Its Addresses map is retained in
// waldb's in-memory bootstrap record and served from GetBootstrap, so the same
// ownership rule as copySnapshot applies in both directions.
func copyBootstrap(bs logdb.Bootstrap) logdb.Bootstrap {
	bs.Addresses = copyAddrMap(bs.Addresses)
	return bs
}
