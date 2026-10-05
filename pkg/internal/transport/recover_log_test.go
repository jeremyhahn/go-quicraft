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
	"time"
)

// TestLogRecovered_HostileHandler: logRecovered, the diagnostic every
// transport recovery emits (handleConnection's among them), returns to its
// caller whatever the slog handler does, and the record still reaches it.
func TestLogRecovered_HostileHandler(t *testing.T) {
	for exitName, exit := range hostileLogExits {
		t.Run(exitName, func(t *testing.T) {
			logged := installHostileRecords(t, "recovered in test", exit)
			done := make(chan struct{})
			go func() {
				defer close(done)
				logRecovered("recovered in test", "boom", nil, "remote", "127.0.0.1:1")
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("logRecovered did not return")
			}
			awaitCount(t, logged, 1, "the diagnostic never reached the handler")
		})
	}
}
