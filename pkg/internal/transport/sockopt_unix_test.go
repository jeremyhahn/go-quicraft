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

//go:build unix

package transport

import (
	"testing"

	"golang.org/x/sys/unix"
)

// errAddrInUse is the error a bind to an address another socket holds fails
// with.
const errAddrInUse = unix.EADDRINUSE

// TestStart_AppliesListenerSockopts verifies that the socket Start binds
// carries at least the configured SO_RCVBUF and SO_SNDBUF and does NOT carry
// SO_REUSEADDR, which on Linux would let another socket that sets it share the
// listener's address (see sockopt_unix.go). The kernel may round a buffer size
// up (Linux doubles it) and quic-go may raise the receive buffer further, so
// the buffers are checked as floors.
func TestStart_AppliesListenerSockopts(t *testing.T) {
	const bufSize = 64 * 1024
	got := listenerSockopts(t, bufSize, map[string]int{
		"SO_REUSEADDR": unix.SO_REUSEADDR,
		"SO_RCVBUF":    unix.SO_RCVBUF,
		"SO_SNDBUF":    unix.SO_SNDBUF,
	}, func(fd uintptr, opt int) (int, error) {
		return unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, opt)
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
