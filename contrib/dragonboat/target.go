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

// Package dragonboat provides a compatibility adapter that lets existing
// dragonboat users switch to quicraft with minimal code changes. Error
// sentinels, configuration types, and API surfaces are mapped to their
// quicraft equivalents.
//
// # Known incompatibilities
//
// Empty proposals are rejected. Dragonboat's proposalShard.propose accepts
// a zero-length command and proposes it as a normal ApplicationEntry, so a
// zero-length proposal is a usable commit barrier. Quicraft's Host.Propose
// rejects len(cmd)==0 with ErrInvalidOperation, and the adapter cannot
// override that check. Applications that use empty proposals as barriers
// must switch to a one-byte sentinel command before migrating.
//
// Client sessions expose a different shape. Dragonboat's client.Session
// exports ShardID, ClientID, SeriesID and RespondedTo as struct FIELDS.
// This adapter's client.Session exports only ShardID as a field; ClientID,
// SeriesID and RespondedTo are METHODS. Code that reads those fields fails
// to compile (loudly, never silently) and must be changed to method calls.
//
// On-disk state machines ignore quicraft's managed snapshot directory.
// Dragonboat's IOnDiskStateMachine.Open(stopc) takes no directory, so the
// adapter discards the initialDir quicraft passes; an on-disk state machine
// keeps managing its own storage paths exactly as it did on dragonboat.
//
// Membership changes are pre-validated. Quicraft rejects a change that is
// invalid against the current membership synchronously, with a nil
// *RequestState and a non-nil error, where dragonboat would return a
// RequestState that later resolves as Rejected.
//
// Gossip is not supported. NodeHostConfig.Validate rejects a non-empty
// GossipConfig rather than silently dropping it, because a gossip
// deployment whose discovery is dropped never forms a cluster.
package dragonboat

// Target identifies a NodeHost instance by its Raft address string.
type Target = string
