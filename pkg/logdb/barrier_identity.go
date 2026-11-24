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

import "reflect"

// SameBarrier reports whether a and b are the same barrier by pointer identity; non-pointer or nil handles match nothing. It never panics; never compare handles with == or use them as map keys.
func SameBarrier(a, b any) bool {
	pa, ok := barrierPointer(a)
	if !ok {
		return false
	}
	pb, ok := barrierPointer(b)
	return ok && pa.Type() == pb.Type() && pa.Pointer() == pb.Pointer()
}

// barrierPointer returns b as a reflect.Value and true when b's dynamic value
// is a non-nil pointer.
func barrierPointer(b any) (reflect.Value, bool) {
	if b == nil {
		return reflect.Value{}, false
	}
	v := reflect.ValueOf(b)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return reflect.Value{}, false
	}
	return v, true
}
