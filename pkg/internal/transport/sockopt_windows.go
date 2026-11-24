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
	"log/slog"

	"golang.org/x/sys/windows"
)

// setListenerSockopts configures the UDP listener socket fd before it is
// bound: SO_RCVBUF/SO_SNDBUF when recvBuf/sendBuf are positive. A failure is
// logged rather than returned: the socket still works with the system's
// defaults.
//
// SO_REUSEADDR is deliberately not set. On Windows it lets a socket bind an
// address that a live socket already holds, whether or not the holder set it,
// so another process could take over the listener's port. The net package
// leaves it unset on Windows for the same reason.
func setListenerSockopts(fd uintptr, recvBuf, sendBuf int) {
	if recvBuf > 0 {
		if err := windows.SetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, windows.SO_RCVBUF, recvBuf); err != nil {
			slog.Warn("SO_RCVBUF setsockopt failed", "requested", recvBuf, "error", err)
		}
	}
	if sendBuf > 0 {
		if err := windows.SetsockoptInt(windows.Handle(fd), windows.SOL_SOCKET, windows.SO_SNDBUF, sendBuf); err != nil {
			slog.Warn("SO_SNDBUF setsockopt failed", "requested", sendBuf, "error", err)
		}
	}
}
