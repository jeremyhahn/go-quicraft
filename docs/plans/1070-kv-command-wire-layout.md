# Plan: kv command wire layout (#1070)

Test gap under the v1.0.0 bar (#744). `pkg/sm/kv/protocol.go` documents the
command layout `[op:1][keyLen:2 LE][key][value]` (put, put-if-absent) and
`[op:1][keyLen:2 LE][key]` (get, delete), but existing tests only round-trip
through `DecodeCommand`.

New file `pkg/sm/kv/wire_layout_test.go` (package kv), every test named with
`WireLayout`, asserts:

- (a) exact bytes of `EncodePut`, `EncodePutIfAbsent`, `EncodeGet` and
  `EncodeDelete` for key `ab` (value `xyz`);
- (b) a 258-byte key encodes its length little-endian (`0x02 0x01`);
- (c) a key of exactly `MaxKeyLength` is accepted by all four encoders with
  length bytes `0xFF 0xFF`, and decodes to a 65535-byte key;
- (d) `EncodePutIfAbsent` with a `MaxKeyLength+1` key returns nil and a
  `*KeyTooLongError` with Length 65536;
- (e) `DecodeCommand` names each refusal Reason: "data too short",
  "unknown op" (op 5), "key length exceeds data";
- (f) decoded Key and Value alias the input slice; a get decodes a nil Value.

A divergence between code and documentation is a bug: failing test commit,
then a fix commit in `protocol.go` (or a doc fix where the code is right).

Acceptance: `go test -race -count=1 -run WireLayout ./pkg/sm/kv/`,
`go vet ./pkg/sm/kv/`, `make lint`.

Refs #1070
