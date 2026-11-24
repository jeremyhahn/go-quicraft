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

// Package nalookup wraps a NAReadLocalNode query so it reaches the adapter's Lookup through the host's local-read path rather than its []byte fast path.
package nalookup

// Query is a NAReadLocalNode query. The adapters answer it with the state
// machine's IExtended.NALookup, or with statemachine.ErrNotImplemented when
// the state machine does not implement IExtended.
type Query struct {
	// Key is the byte-slice query passed to NAReadLocalNode.
	Key []byte
}
