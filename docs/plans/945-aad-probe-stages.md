# #945: AAD binding probe stages

## Objective

`verifyAADBinding` (pkg/seal/barrier_binding.go) reports a failure as an
`AADBindingProbeError` whose `Stage` is one of `probe-entropy`, `encrypt`,
`decrypt-matching-aad` or `decrypt-wrong-aad`. Only `decrypt-wrong-aad` is
tested today. Pin the other three stages, and that a matching-AAD decrypt
returning other bytes is reported as `ErrAADProbeRoundTrip` and not
`ErrAADProbeNotBound`.

## Slices

1. `docs: plan for #945` — this file.
2. `test: the AAD binding probe reports the probe-entropy, encrypt and
   decrypt-matching-aad stages, ...` — new pkg/seal/aad_probe_stages_test.go
   (package seal), with `AADSymmetricEncrypter` doubles built on
   `newHonestAADEncryptor`:
   - a reader that always fails, and one that fails after the probe bytes:
     stage `probe-entropy`, `errors.Is` reaches the reader's error;
   - `EncryptWithAAD` fails: stage `encrypt`, wraps that error;
   - matching-AAD `DecryptWithAAD` fails: stage `decrypt-matching-aad`, wraps
     that error;
   - matching-AAD decrypt returns other bytes: stage `decrypt-matching-aad`,
     `errors.Is(err, ErrAADProbeRoundTrip)`, not `ErrAADProbeNotBound`.
   Stage is read with `errors.As`. Helpers take `context.Context` first.
3. Per divergence between code and doc: a failing `test:` commit, then a
   `fix:` commit in pkg/seal/barrier_binding.go (or a doc correction there
   when the code is right).

## Out of scope

- Any change to the probe's behaviour beyond a documented-behaviour bug.
- Other packages, wire or on-disk formats.
