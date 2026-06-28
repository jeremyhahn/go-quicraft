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

package seal

import (
	"os"
	"testing"
)

// TestMain enables the test-only KDF work-factor bypass for the whole
// seal test binary so unit tests can derive keys with the cheap Argon2id
// / PBKDF2 parameters they rely on (Time=1, Iterations=1000) without
// tripping the production floor enforced by DeriveKey. The floor itself
// is verified directly via kdfParamsBelowFloor in
// TestKDFParamsBelowFloor, independent of this bypass.
func TestMain(m *testing.M) {
	allowWeakKDFParams = true
	os.Exit(m.Run())
}
