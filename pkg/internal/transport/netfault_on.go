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

//go:build quicraft_netfault

package transport

import "github.com/jeremyhahn/go-quicraft/test/netfault"

// netFaultEnabled gates the send-path check. See netfault_off.go for why this
// is a constant rather than a variable.
const netFaultEnabled = true

// netFaultDrop forwards to the injector.
//
// The state lives in test/netfault rather than here because the integration
// suites that drive it sit outside pkg/ and so cannot import an internal
// package. This file is the only thing that couples the two, and it exists
// only under the build tag, so a normal build never pulls test/netfault into
// the dependency graph.
func netFaultDrop(shardID, from, to uint64) bool {
	return netfault.Drop(shardID, from, to)
}
