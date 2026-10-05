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

package transport

import (
	"testing"

	"golang.org/x/sys/windows"
)

// errAddrInUse is the error a bind to an address another socket holds fails
// with.
const errAddrInUse = windows.WSAEADDRINUSE

// TestStart_AppliesListenerSockopts verifies that the socket Start binds
// carries at least the configured SO_RCVBUF and SO_SNDBUF and does NOT carry
// SO_REUSEADDR, which on Windows would let another socket bind over the live
// listener (see sockopt_windows.go).
func TestStart_AppliesListenerSockopts(t *testing.T) {
	const bufSize = 64 * 1024
	got := listenerSockopts(t, bufSize, map[string]int{
		"SO_REUSEADDR": windows.SO_REUSEADDR,
		"SO_RCVBUF":    windows.SO_RCVBUF,
		"SO_SNDBUF":    windows.SO_SNDBUF,
	}, func(fd uintptr, opt int) (int, error) {
		return windows.GetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, opt)
	})

	if got["SO_REUSEADDR"] != 0 {
		t.Error("SO_REUSEADDR is set on the listener socket")
	}
	if got["SO_RCVBUF"] < bufSize {
		t.Errorf("SO_RCVBUF = %d, want >= %d", got["SO_RCVBUF"], bufSize)
	}
	if got["SO_SNDBUF"] < bufSize {
		t.Errorf("SO_SNDBUF = %d, want >= %d", got["SO_SNDBUF"], bufSize)
	}
}
