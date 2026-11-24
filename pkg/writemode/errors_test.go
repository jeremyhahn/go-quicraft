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

import "testing"

func TestInvalidModeError_Error(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "unknown string value",
			value: "turbo",
			want:  `quicraft/writemode: invalid write mode: "turbo"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &InvalidModeError{Value: tt.value}
			if got := err.Error(); got != tt.want {
				t.Errorf("InvalidModeError.Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Compile-time check that *InvalidModeError implements error.
var _ error = (*InvalidModeError)(nil)
