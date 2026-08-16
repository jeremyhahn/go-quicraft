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

//go:build !quicraft_netfault

package transport

// netFaultEnabled is a compile-time constant so that every
//
//	if netFaultEnabled && netFaultDrop(...)
//
// site in the send path folds away entirely in a normal build: the compiler
// proves the condition false, drops the call, and the shipped binary is
// identical to one written without the hook. This is the same discipline the
// quicraft_errfs and quicraft_fuzz suites use — the injection point costs
// production nothing, and the test build pays for it alone.
const netFaultEnabled = false

// netFaultDrop is the no-op stand-in. It is never called in this build; it
// exists so both variants type-check against the same call sites.
func netFaultDrop(_, _, _ uint64) bool { return false }
