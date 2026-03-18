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

package perfresult

// ErrNoResults indicates that no benchmark results were collected.
type ErrNoResults struct{}

func (e *ErrNoResults) Error() string {
	return "no benchmark results collected"
}

// ErrParseFailure indicates that JSON parsing of benchmark output failed.
type ErrParseFailure struct {
	Detail string
}

func (e *ErrParseFailure) Error() string {
	return "failed to parse benchmark output: " + e.Detail
}

// ErrNoSentinel indicates that the sentinel markers were not found
// in the benchmark output.
type ErrNoSentinel struct{}

func (e *ErrNoSentinel) Error() string {
	return "sentinel markers not found in benchmark output"
}

// ErrSystemUnknown indicates that the specified benchmark system
// is not registered.
type ErrSystemUnknown struct {
	System string
}

func (e *ErrSystemUnknown) Error() string {
	return "unknown benchmark system: " + e.System
}
