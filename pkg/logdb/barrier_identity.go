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

// SameBarrier reports whether a and b are the same barrier. It is the one
// identity rule for barrier handles, used by the host, the snapshot pool and
// waldb, and the rule every implementation of PerBarrierEpochAwareLogDB and
// BarrierShardEnumerator must apply to its barrier argument.
//
// Identity is pointer identity. A handle whose dynamic value is a non-nil
// pointer is the same barrier as another handle exactly when both hold the
// same pointer of the same type. A handle of any other kind (nil, a typed nil
// pointer, or a non-pointer value) matches nothing, not even a copy of itself,
// so SameBarrier(b, b) reports whether b has an identity at all.
//
// No barrier interface requires a comparable dynamic value, so a handle must
// never be compared with == or used as a map key: either panics when the value
// holds a slice, map or func. SameBarrier never panics, whatever the dynamic
// types.
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
