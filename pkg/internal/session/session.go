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

package session

import (
	"github.com/jeremyhahn/go-quicraft/pkg/sm"
)

// clientSession tracks one client's deduplication state: the highest completed series ID and recent results.
type clientSession struct {
	clientID uint64
	// ownerID is the principal the session was registered under, kept for removal from the per-owner count.
	ownerID uint64
	// registeredIndex is the log index of the Register entry that created this incarnation of the clientID.
	registeredIndex uint64
	respondedTo     uint64               // highest completed series
	responses       map[uint64]sm.Result // seriesID -> cached result
	lastActiveIndex uint64               // log index of last activity
}

// noOwnerID is the OwnerID meaning no owner was declared; such sessions are not counted against a per-owner cap.
const noOwnerID uint64 = 0

// newClientSession creates a new client session with the given client ID,
// owner and initial log index.
func newClientSession(clientID, ownerID, index uint64) *clientSession {
	return &clientSession{
		clientID:        clientID,
		ownerID:         ownerID,
		registeredIndex: index,
		responses:       make(map[uint64]sm.Result),
		lastActiveIndex: index,
	}
}

// isExpired returns true if the session has been inactive for more than
// inactivityThreshold entries relative to the current log index.
func (s *clientSession) isExpired(currentIndex, inactivityThreshold uint64) bool {
	if inactivityThreshold == 0 {
		return false
	}
	if currentIndex <= s.lastActiveIndex {
		return false
	}
	return currentIndex-s.lastActiveIndex > inactivityThreshold
}

// touchIndex updates the session's last active index.
func (s *clientSession) touchIndex(index uint64) {
	if index > s.lastActiveIndex {
		s.lastActiveIndex = index
	}
}

// evictUpTo removes all cached responses with seriesID <= respondedTo.
// This is called when the client advances respondedTo, indicating that
// all responses up to that point have been received.
func (s *clientSession) evictUpTo(respondedTo uint64) {
	for sid := range s.responses {
		if sid <= respondedTo {
			delete(s.responses, sid)
		}
	}
}
