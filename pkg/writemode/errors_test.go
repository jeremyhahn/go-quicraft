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

import (
	"errors"
	"testing"
)

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
		{
			name:  "numeric overflow value",
			value: "99",
			want:  `quicraft/writemode: invalid write mode: "99"`,
		},
		{
			name:  "empty value",
			value: "",
			want:  `quicraft/writemode: invalid write mode: ""`,
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

// Compile-time proof that *InvalidModeError satisfies error. This replaces a
// TestInvalidModeError_ImplementsError that did `var err error =
// &InvalidModeError{...}` and then asserted `err != nil`: a non-nil pointer in
// an interface is never nil, so the runtime half could not fail and the
// assignment was the only real check. That check belongs here, and everything
// else the test could have proved is already covered — the error message by
// TestInvalidModeError_Error, recovery through the error interface by
// TestInvalidModeError_ErrorsAs, and the production call sites that return it
// by TestFromString_InvalidMode and TestMode_Validate_Invalid in mode_test.go.
var _ error = (*InvalidModeError)(nil)

func TestInvalidModeError_ErrorsAs(t *testing.T) {
	err := &InvalidModeError{Value: "unknown"}
	var target *InvalidModeError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should match *InvalidModeError")
	}
	if target.Value != "unknown" {
		t.Errorf("errors.As target.Value = %q, want %q", target.Value, "unknown")
	}
}
