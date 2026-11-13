# Plan: #1013 pkg/sm LRU cache Delete, Clear and Stats

Test gap under the v1.0.0 bar (#744). `pkg/sm/lru.go` documents:

- `Delete` removes one key from the cache.
- `Clear` removes all entries from the cache (the hit/miss counters are cumulative and are not reset).
- `Stats` returns the cumulative hit and miss counts; `Get` counts one hit or one miss per call.
- `Put` with a nil value deletes the key; a Put past capacity evicts the least recently used entry.

## Tests (pkg/sm/lru_test.go, package sm)

Every test name contains `LRUCacheDeleteClearStats`. A helper walks the list head to tail and
backwards, checking that the list and the map hold the same keys, so recency order is read
without calling `Get` (which would promote and count).

- (a) Delete of a middle key keeps the other keys and their recency order; later Puts past
  capacity evict in LRU order. Delete of a missing key changes nothing.
- (b) Delete of the head and of the tail, then Puts past capacity, evict in LRU order without a panic.
- (c) Clear makes `Len` 0 and every old key a miss; later Puts past capacity evict as on a new cache.
- (d) `Stats` counts each Get hit and miss exactly once; Clear and Delete leave the counters unchanged.
- (e) Concurrent Delete, Clear, Put and Get under -race keep `Len` at most the capacity and the
  list consistent with the map.

## Bugs

A divergence between code and documentation gets a failing test commit, then a fix commit in
`pkg/sm/lru.go`.

Acceptance: `go test -race -count=1 -run LRUCacheDeleteClearStats ./pkg/sm/`, `go vet ./pkg/sm/`, `make lint`.
