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

// setListenerSockopts sets SO_RCVBUF/SO_SNDBUF before bind when positive, logging failures; SO_REUSEADDR is not set because on Windows it allows port takeover.
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
