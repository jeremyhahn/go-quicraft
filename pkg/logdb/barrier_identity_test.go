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

package logdb

import "testing"

type pointerBarrier struct{ id int }

type otherPointerBarrier struct{ id int }

// sliceBarrier is not comparable: == on two of them panics.
type sliceBarrier struct{ labels []string }

func TestSameBarrier(t *testing.T) {
	p := &pointerBarrier{id: 1}
	q := &pointerBarrier{id: 1}
	o := &otherPointerBarrier{id: 1}
	v := sliceBarrier{labels: []string{"a"}}
	var typedNil *pointerBarrier
	cases := []struct {
		name string
		a, b any
		want bool
	}{
		{name: "same pointer", a: p, b: p, want: true},
		{name: "equal contents, different pointers", a: p, b: q, want: false},
		{name: "different pointer types", a: p, b: o, want: false},
		{name: "non-comparable value and its copy", a: v, b: v, want: false},
		{name: "non-comparable value and a pointer", a: v, b: p, want: false},
		{name: "comparable equal values", a: pointerBarrier{id: 1}, b: pointerBarrier{id: 1}, want: false},
		{name: "nil and pointer", a: nil, b: p, want: false},
		{name: "both nil", a: nil, b: nil, want: false},
		{name: "typed nil pointer and its copy", a: typedNil, b: typedNil, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SameBarrier(tc.a, tc.b); got != tc.want {
				t.Fatalf("SameBarrier = %v, want %v", got, tc.want)
			}
		})
	}
}
