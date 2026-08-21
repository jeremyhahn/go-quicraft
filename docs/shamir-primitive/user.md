# Shamir Primitive — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

This page is for a developer building a key-custody layer on top of this
primitive. If you are an operator running a share ceremony, the commands you
want belong to the database layer above — this package has no commands.

## Before you start

- Go 1.26.6 or later, and `github.com/jeremyhahn/go-quicraft` as a dependency.
- A plan for **where the shares go**. This package hands them back and forgets
  them; it stores nothing.
- A plan for **how you will know the reconstruction worked**. This is not
  optional and not something the package can do for you — see "The one thing
  you must not skip" below.

## The one thing you must not skip

`Combine` does **not** check its own output. Hand it four shares of a real
5-of-9 split and it returns 32 bytes of garbage with a `nil` error. Hand it a
share someone corrupted in transit and it does the same. Hand it a share set
whose `Threshold` field was relabelled and it does the same again.

You must authenticate the reconstructed secret against something outside the
share set. In practice this is one line, because you were going to do it anyway:

```go
key, err := shamir.CombineBytes(values, threshold)
if err != nil {
    return err                       // structurally invalid share set
}
// The real check: use the key. Unseal fails on a wrong key.
if err := barrier.Unseal(ctx, strategy, seal.Credentials{Secret: key}); err != nil {
    return fmt.Errorf("reconstruction did not produce the root key: %w", err)
}
```

The AEAD tag inside `Unseal` is what tells you the shares were right. Any
authenticated decrypt, or a comparison against an independent commitment, works
the same way.

Do not wait for `IntegrityError`. It exists in the package and is never
returned.

## Task: run a ceremony in protected memory (preferred)

`ProtectedShare` keeps each share value in an mlock'd, guard-paged buffer that
`Destroy` genuinely wipes, so you never hold share material in a Go string or
manage the wiping yourself:

```go
import "github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"

shares, err := shamir.SplitProtected(rootKey, 3, 5)
if err != nil {
    return err
}
// Destroy as each is consumed — locked memory is a finite kernel resource.
defer func() {
    for _, s := range shares {
        s.Destroy()
    }
}()

for _, s := range shares {
    // WithValue holds the share open for the callback, so a concurrent
    // Destroy blocks until it returns. Do NOT keep the slice past it.
    if err := s.WithValue(func(v []byte) error {
        return deliverToCustodian(s.Index, v)
    }); err != nil {
        return err
    }
}
```

Recovering is the mirror image:

```go
// Shares that arrived from outside the process — bounded decode, never
// json.Unmarshal into a []*ProtectedShare.
shares, err := shamir.UnmarshalShareSet(submitted)
if err != nil {
    return err
}
defer func() {
    for _, s := range shares {
        s.Destroy()
    }
}()

secret, err := shamir.CombineProtected(shares)
if err != nil {
    return err
}
defer secret.Destroy()
// ... authenticate the secret before trusting it. See above.
```

Three things to remember: every share must be `Destroy`ed (including one
produced by `json.Unmarshal` — decoding is a construction), a slice from
`Value` must not outlive the share (use `WithValue` where one goroutine tears
shares down while another reads them), and a whole set is priced against
`MaxShareSetBytes` before it is allocated.

## Task: split a root key (bytes form)

```go
import "github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"

// 3-of-5: any three custodians can recover the key; any two learn nothing.
values, err := shamir.SplitBytes(rootKey, 3, 5)
if err != nil {
    return err
}
defer func() {
    for _, v := range values {
        enclave.WipeBytes(v)
    }
}()

for i, v := range values {
    // Hand values[i] to custodian i+1 — the x-coordinate is inside the
    // payload, so nothing else needs to travel with it.
    if err := deliverToCustodian(i+1, v); err != nil {
        return err
    }
}
```

Choosing M and N is your decision, not the library's. Two constraints it does
enforce: `threshold >= 2` (a 1-of-N "split" is just the secret, N times), and
both values `<= 255`.

## Task: collect shares and recover

```go
// values holds M-or-more base64-encoded share values from custodians.
// threshold came from your own records — NOT from the shares.
key, err := shamir.CombineBytes(values, threshold)
if err != nil {
    return err
}
defer enclave.WipeBytes(key)
// ... authenticate `key` before trusting it. See above.
```

You still own the input buffers and must wipe them. `CombineBytes` wipes only
the copies it decoded internally.

## Task: work with `Share` structs instead

The struct API exists for code that serializes shares as JSON:

```go
shares, err := shamir.Split(rootKey, 3, 5)   // []*Share
blob, err := json.Marshal(shares[0])          // full value — this is the storage path
...
key, err := shamir.Combine(collected)         // []*Share
```

Understand the cost before choosing it: `Share.Value` is a Go string, which
cannot be zeroed. Every share you create this way stays readable in process
memory until the garbage collector happens to reclaim it, and a ceremony holds
all N at once. Use `SplitBytes`/`CombineBytes` wherever you can keep the
material in wipeable buffers.

## Task: log or print a share safely

You do not need to do anything. Both human-facing paths are already redacted:

```go
fmt.Printf("%v\n", share)        // value omitted
logger.Info("share received", "share", share)   // value omitted, any handler
```

`MarshalJSON` is *not* redacted, because that is the serialization path. So
this **does** write share material to your log:

```go
b, _ := json.Marshal(share)
logger.Info("share received", "share", string(b))   // DO NOT DO THIS
```

Log the `Index` if you need to identify a share. A leaked share cannot be
revoked — only outrun by a rekey ceremony.

## Task: sanity-check a share set before combining

```go
if err := shamir.VerifyShare(candidate, collected); err != nil {
    return err   // nil share, threshold/total mismatch, or duplicate index
}
```

This catches operator mistakes — the same custodian submitting twice, a share
from last year's ceremony — before you spend a quorum on them. It does **not**
tell you a share is genuine; nothing can, before combining.

## Troubleshooting

**`too many shares: N exceeds the maximum of 255`** — the field has 255 non-zero
x-coordinates and that is the hard ceiling on a share set.

**`share N has unsupported format version V (legacy or foreign share)`** — the
share was not produced by this package at this version. Until the first
production release, share format changes are made in place, so this most often
means the producing and consuming binaries were built from different commits.
Rebuild both from the same commit.

**`share N payload is X bytes, exceeding the 65536-byte limit`** — you are
trying to share a payload rather than a key. Encrypt the payload under a key and
share the key.

**`share N has zero x-coordinate`** — a malformed or hand-edited share.
`x = 0` is where the secret itself sits on the polynomial and is never a valid
share coordinate.

**`share N payload length differs from share 0 length`** — the set mixes shares
of two different secrets.

**`need at least M shares, got K`** — you have not reached quorum. Note this is
checked against the **claimed** threshold; reaching it is necessary, not
sufficient.

**Recovery "succeeds" but the key does not work** — this is the expected failure
mode of a wrong share set, not a bug. `Combine` cannot distinguish it. Check
that `threshold` came from your records rather than from `shares[0].Threshold`,
and that every share belongs to the same ceremony.

**A panic on a submitted share list** — should not happen; `Validate` is
nil-receiver safe and nil elements are reported positionally. If you see one,
it is a defect worth reporting, because operator-submitted JSON containing
`null` is an anticipated input.
