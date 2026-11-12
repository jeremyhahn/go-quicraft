# Plan: #941 BytesLookup UnsupportedLookupResultError test gap

## Objective

`CachedDiskStateMachine.BytesLookup` (pkg/sm/cached.go) returns
`UnsupportedLookupResultError` (pkg/sm/errors.go) when the inner state machine
has no `NALookup` and its `Lookup` answers with something other than `[]byte`.
No test reaches that path. Add one that pins the documented behaviour.

## Slices

1. `docs: plan for #941` — this file.
2. `test:` — pkg/sm/unsupported_lookup_result_test.go (package sm): an inner
   disk state machine without `NALookup` whose `Lookup` result is chosen per
   sub-test, wrapped by `NewCachedDiskStateMachine`, asserting:
   - (a) a `string` and a struct-pointer result each give a nil value and an
     error `errors.As` matches to `*UnsupportedLookupResultError` with `Got`
     equal to `fmt.Sprintf("%T", v)`;
   - (b) the error text contains that type name;
   - (c) a nil result gives `(nil, nil)`; a `[]byte` result is returned with
     no error;
   - (d) an error from the inner `Lookup` is returned unchanged (`errors.Is`).
3. Per divergence between code and docs: a failing `test:` commit, then a
   `fix:` commit in pkg/sm/cached.go or pkg/sm/errors.go.

## Out of scope

Any change outside the files above; the NALookup and cache-hit paths (covered
in cached_test.go).
