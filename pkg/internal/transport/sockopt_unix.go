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
	"log/slog"

	"golang.org/x/sys/unix"
)

// setListenerSockopts configures the UDP listener socket fd before it is
// bound: SO_RCVBUF/SO_SNDBUF when recvBuf/sendBuf are positive. A failure is
// logged rather than returned: the socket still works with the kernel's
// defaults, and Linux caps a buffer request at net.core.rmem_max/wmem_max.
//
// SO_REUSEADDR is deliberately not set. UDP has no TIME_WAIT, so it buys
// nothing on restart, and on Linux it lets two UDP sockets that both set it
// share one address and silently split its datagrams. Without it a second bind
// of a live listener's address fails with EADDRINUSE.
func setListenerSockopts(fd uintptr, recvBuf, sendBuf int) {
	if recvBuf > 0 {
		if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF, recvBuf); err != nil {
			slog.Warn("SO_RCVBUF setsockopt failed; increase net.core.rmem_max",
				"requested", recvBuf, "error", err)
		}
	}
	if sendBuf > 0 {
		if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, sendBuf); err != nil {
			slog.Warn("SO_SNDBUF setsockopt failed; increase net.core.wmem_max",
				"requested", sendBuf, "error", err)
		}
	}
}
