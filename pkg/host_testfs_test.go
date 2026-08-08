// Copyright 2026 Jeremy Hahn
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quicraft

// withSnapshotFS routes the host's snapshot persistence I/O through fsys.
//
// It lives in a _test.go file rather than beside the other HostOptions
// because it has no production caller: fault injection is a testing facility
// and must not appear in the public option set. Keeping it in host.go made it
// genuinely unused there, which the linter reported correctly — the fix is to
// put the helper where its only callers are, not to suppress the report.
//
// Being in the same package, it still reaches hostOptions' unexported fields.
func withSnapshotFS(fsys snapshotFS) HostOption {
	return func(o *hostOptions) {
		o.fsys = fsys
	}
}
