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

package proto_test

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/jeremyhahn/go-quicraft/pkg/proto"
)

// ExampleEntry_MarshalTo is the codec round trip docs/wire-protocol/api.md
// shows under "encode a value" and "decode a value": a buffer of exactly
// Size() bytes holds the entry, MarshalTo and UnmarshalFrom both report that
// length, and a buffer one byte short is refused with ErrBufferTooSmall.
func ExampleEntry_MarshalTo() {
	entry := proto.Entry{
		Term:  3,
		Index: 42,
		Type:  proto.EntryNormal,
		Cmd:   []byte("set k=v"),
	}

	buf := make([]byte, entry.Size())
	n, err := entry.MarshalTo(buf)
	fmt.Println("size:", entry.Size(), "written:", n, "err:", err)

	var decoded proto.Entry
	m, err := decoded.UnmarshalFrom(buf[:n])
	fmt.Println("consumed:", m, "err:", err)
	fmt.Println("term:", decoded.Term, "index:", decoded.Index,
		"type:", decoded.Type, "cmd:", string(decoded.Cmd))
	fmt.Println("equal:", decoded.Term == entry.Term &&
		decoded.Index == entry.Index && decoded.Type == entry.Type &&
		bytes.Equal(decoded.Cmd, entry.Cmd))

	short := make([]byte, entry.Size()-1)
	n, err = entry.MarshalTo(short)
	fmt.Println("short buffer:", n, errors.Is(err, proto.ErrBufferTooSmall))

	// Output:
	// size: 75 written: 75 err: <nil>
	// consumed: 75 err: <nil>
	// term: 3 index: 42 type: 0 cmd: set k=v
	// equal: true
	// short buffer: 0 true
}

// ExampleGetEntrySlice is the pool docs/wire-protocol/api.md shows under
// "pool entry slices": a pooled slice starts empty, takes appends, and goes
// back with PutEntrySlice, which clears it. A nil pointer is a no-op.
func ExampleGetEntrySlice() {
	s := proto.GetEntrySlice()
	fmt.Println("len on get:", len(*s))

	*s = append(*s, proto.Entry{Index: 1}, proto.Entry{Index: 2})
	fmt.Println("len after append:", len(*s), "last index:", (*s)[1].Index)

	proto.PutEntrySlice(s)
	proto.PutEntrySlice(nil)
	fmt.Println("returned")

	// Output:
	// len on get: 0
	// len after append: 2 last index: 2
	// returned
}

// ExampleMessageTypeName is the naming helpers docs/wire-protocol/api.md
// shows under "name a constant": a known type prints its name, an
// unrecognized one prints Unknown(N), and IsFreeOrderMessage separates a
// message sent before its state is durable (Replicate) from one held for the
// commit worker (ReplicateResp).
func ExampleMessageTypeName() {
	fmt.Println(proto.MessageTypeName(proto.Replicate))
	fmt.Println(proto.MessageTypeName(9999))
	fmt.Println("Replicate free-order:", proto.IsFreeOrderMessage(proto.Replicate))
	fmt.Println("ReplicateResp free-order:", proto.IsFreeOrderMessage(proto.ReplicateResp))

	// Output:
	// Replicate
	// Unknown(9999)
	// Replicate free-order: true
	// ReplicateResp free-order: false
}
