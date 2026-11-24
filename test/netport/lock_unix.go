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

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package netport

import (
	"errors"
	"os"

	"syscall"
)

// crossProcessClaims reports whether lockPort can claim a port across
// processes on this platform.
const crossProcessClaims = true

// lockPort takes an exclusive non-blocking flock on path, returning the unlock function; busy is true only when another process holds it. The file is never removed.
func lockPort(path string) (unlock func(), busy bool) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, false
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, errors.Is(err, syscall.EWOULDBLOCK)
	}
	return func() { _ = f.Close() }, false
}
