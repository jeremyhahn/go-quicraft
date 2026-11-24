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

package lockedmem

import "unsafe"

// addressOf reports the address of b's first byte WITHOUT reading it.
//
// It is the one unsafe operation in this package and is isolated here so the
// reason is stated once: the whole point is to inspect a region whose read and
// write permissions are what the test is asking about, so the address has to be
// obtained without a dereference. unsafe.SliceData does exactly that — it
// returns the pointer, and nothing loads through it.
func addressOf(b []byte) uint64 {
	return uint64(uintptr(unsafe.Pointer(unsafe.SliceData(b)))) // #nosec G103 -- address only; never dereferenced
}
