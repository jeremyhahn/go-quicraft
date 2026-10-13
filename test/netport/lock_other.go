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

//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package netport

// crossProcessClaims reports whether lockPort can claim a port across
// processes on this platform. It cannot here, so a port is guarded by the
// in-process record and the probe alone.
const crossProcessClaims = false

// lockPort claims nothing on this platform and never reports a port busy.
func lockPort(string) (unlock func(), busy bool) {
	return nil, false
}
