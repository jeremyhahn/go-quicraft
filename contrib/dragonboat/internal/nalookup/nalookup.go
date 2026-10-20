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

// Package nalookup carries a NodeHost.NAReadLocalNode query from the
// dragonboat NodeHost to the statemachine adapters.
//
// The query travels through the host's generic local-read path, so it is
// served under the same state machine read guard as Lookup. It is not sent
// as a bare []byte: the host answers a []byte query through its own
// byte-slice fast path, which would bypass the adapter, and a []byte query
// from ReadLocalNode must keep reaching the state machine's Lookup.
package nalookup

// Query is a NAReadLocalNode query. The adapters answer it with the state
// machine's IExtended.NALookup, or with statemachine.ErrNotImplemented when
// the state machine does not implement IExtended.
type Query struct {
	// Key is the byte-slice query passed to NAReadLocalNode.
	Key []byte
}
