# 744: WAL landing overflow fails closed

`pkg/logdb/waldb/landing_overflow_test.go` pins the behaviour documented at
`maxCiphertextExpansion` (db.go): `reserveLanding` pre-fits a record using
`len(data) + maxCiphertextExpansion`. If a context-binding barrier then adds a
longer envelope and the sealed record overflows the segment, `writeRecord`
returns `ErrLandingSegmentOverflow`. It does not rotate, because the record's
context AAD names the reserved segment and ordinal.

**OversizeEnvelopeRefusedAtBoundary.** `oversizeEnvelopeBarrier` embeds the
real `*seal.Barrier`. It appends a 2 KiB tail to every ciphertext and strips
it on decrypt. The DB uses `WithBlockSize(1024)` and `WithMaxFileSize(64*1024)`
and saves single-entry updates until one fails. That failure must satisfy
`errors.Is(err, ErrLandingSegmentOverflow)` and come after at least one save
that succeeded. `SaveState` sends a write error to `rollbackBatch`, which
rolls back without poisoning, so the test asserts:
- the DB is not poisoned;
- a retry of the same entry is refused the same way, because the record is
  never moved;
- the acknowledged entries still read back before close.

After Close and reopen with the same barrier, exactly the acknowledged entries
read back intact and the refused entry is absent.

**PlainBarrierRotatesAcrossBoundary.** The same save sequence runs with the
plain `*seal.Barrier`, whose envelope stays within the bound. Every save
succeeds. The test also asserts that the active segment ID changed, which
proves the control crossed a segment boundary.

**Mutation check (not committed).** On the positional path, `writeRecord` was
temporarily made to rotate and retry the way the plaintext path does. The first
subtest then fails: all saves succeed and no overflow is reported.
