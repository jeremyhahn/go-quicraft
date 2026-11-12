# Plan: #1038 shamir CombineBytes / SplitBytes reject branches

Test gap under #744 (v1.0.0 bar). No test reaches three error branches of
`pkg/crypto/shamir/shamir.go`:

1. `parseRawShare` refuses a decoded share shorter than `rawShareOverhead`+1
   with `CombineError{Reason: "share %d too short (%d bytes)"}`.
2. `parseRawShare` refuses a payload whose length differs from share 0's
   with `CombineError{Reason: "share %d payload length %d differs from share 0 length %d"}`.
3. `SplitBytes` reports a failed coefficient read as
   `SplitError{Reason: "reading random coefficients: ..."}`.

## Edit

New `pkg/crypto/shamir/combine_split_rejects_test.go` (package `shamir`),
every test name containing `CombineSplitRejects`:

- too-short share 0 (`{0x01}`) and too-short share 1 (`{0x01, 0x02}`): nil
  secret, `*CombineError`, Reason prefix `share N too short`.
- share 0 with a 4-byte payload, share 1 with a 6-byte payload: nil secret,
  `*CombineError` naming 6 and 4.
- `testOnlyRandReader(iotest.ErrReader(...))`: `SplitBytes` returns nil
  values and a `*SplitError` with Reason prefix `reading random coefficients`
  containing the reader's error; the reader is restored.

A divergence between code and documentation is a bug: failing test commit,
then a fix commit in `shamir.go`.

## Acceptance

`go test -race -count=1 -run CombineSplitRejects ./pkg/crypto/shamir/`,
`go vet ./pkg/crypto/shamir/`, `make lint` clean.
