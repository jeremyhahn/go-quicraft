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

import "testing"

// ---------------------------------------------------------------------------
// NewMembership
// ---------------------------------------------------------------------------

func TestNewMembership_MapsInitialized(t *testing.T) {
	m := NewMembership()
	if m.Nodes == nil {
		t.Error("Nodes map is nil")
	}
	if m.Observers == nil {
		t.Error("Observers map is nil")
	}
	if m.Witnesses == nil {
		t.Error("Witnesses map is nil")
	}
	if m.Removed == nil {
		t.Error("Removed map is nil")
	}
}
