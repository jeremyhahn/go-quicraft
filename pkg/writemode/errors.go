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

package writemode

import "fmt"

// InvalidModeError is returned when a write mode string cannot be parsed
// or when a Mode value is outside the valid range.
type InvalidModeError struct {
	Value string
}

// Error returns a human-readable description of the invalid mode.
func (e *InvalidModeError) Error() string {
	return fmt.Sprintf("quicraft/writemode: invalid write mode: %q", e.Value)
}
