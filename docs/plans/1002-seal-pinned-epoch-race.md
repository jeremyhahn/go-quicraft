# Plan for #1002: pinned barrier epochs survive concurrent Rotate and PurgeEpochsBefore

Refs #1002 (v1.0.0 bar test gap, tracked under #744).

## Objective

Prove under `-race` that an epoch pinned by `Barrier.EncryptWithEpochPinned`,
or by `RegisterInFlightEpoch` before an encrypt, keeps its DEK while other
goroutines loop `Rotate` and `PurgeEpochsBefore(CurrentEpoch())`, and that the
pin is the only thing keeping it: once every pin is released with
`DeregisterInFlightEpoch`, one more purge retires the old epochs.

## Test (pkg/seal/inflight_epoch_race_test.go)

1. Workers seal concurrently: half with `EncryptWithEpochPinned`, half with
   `RegisterInFlightEpoch(CurrentEpoch())` then `EncryptWithEpoch` (retrying,
   with the pin released, when a Rotate moved the epoch between the two).
2. One goroutine loops `Rotate`, one loops `PurgeEpochsBefore(CurrentEpoch())`,
   each a bounded number of times.
3. Each sealed frame decrypts with `DecryptForEpoch` right after sealing, and
   again after the rotator and purger stop, while still pinned. Some pins are
   released during the race to exercise concurrent release.
4. After every pin is released, one `PurgeEpochsBefore(CurrentEpoch())`
   leaves only the current epoch, and every frame sealed under an older epoch
   fails with `ErrDecryptionFailed`.
5. The in-flight table is empty and no test goroutine is left running.

The race runs as two subtests, one per pinning API (`EncryptWithEpochPinned`
and `RegisterInFlightEpoch`), each on its own fresh barrier with only that
API's workers. Sharing one barrier let either API's pins on the initial epoch
protect the other's ciphertexts, so an API that stopped pinning would still
pass. Before the pins are released, each subtest asserts that a purge
completed with the current epoch past the initial one, that the in-flight
table's reference count on every epoch equals the pins the workers still hold
(more than zero on the initial epoch), and that every frame on an old epoch
still decrypts.

A divergence between the code and its documentation is a bug: a failing test
commit, then a fix commit in pkg/seal/barrier.go.
