# Shamir primitive: usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/crypto/shamir` · Last verified: 2026-10-13
> Conflicts with this document are defects. Fix them here first.

This page is for a developer building a key-custody layer on top of this
primitive. If you are an operator running a share ceremony, the commands you want
belong to the database layer above; this package has no commands.

## Before you start

- Go 1.27.4 or later, and `github.com/jeremyhahn/go-quicraft` as a dependency.
- A plan for where the shares go. This package hands them back and forgets them;
  it stores nothing.
- A plan for how you will know the reconstruction worked. The package cannot do
  this for you; see the next section.

## The one thing you must not skip

Combining does not check its own output. The share count is checked only against
the threshold it is told: hand `CombineBytes` four shares of a real 5-of-9 split
with `threshold` 4 and it returns 32 bytes of garbage with a `nil` error. A share
corrupted in transit, a share from another split, or a share set whose
`Threshold` field was relabelled, does the same.

You must authenticate the reconstructed secret against something outside the
share set. Usually that is the operation you were going to do anyway:

```go
secret, err := shamir.CombineProtected(shares)
if err != nil {
	return err // structurally invalid share set
}
defer secret.Destroy()

// The real check: use the secret. Unseal fails on a wrong one. This needs a
// strategy that takes its passphrase from creds (NewDeferredSoftwareStrategy);
// one built with a passphrase of its own ignores creds.
var creds seal.Credentials
if err := secret.WithBytes(func(b []byte) error {
	var err error
	creds, err = seal.NewCredentials(ctx, append([]byte(nil), b...)) // consumes the copy
	return err
}); err != nil {
	return err
}
defer creds.Secret.Destroy() // caller-owned: Unseal does not destroy it
if err := barrier.Unseal(ctx, strategy, creds); err != nil {
	return fmt.Errorf("reconstruction did not produce the right secret: %w", err)
}
```

The AEAD tag inside `Unseal` is what tells you the shares were right. Any
authenticated decrypt, or a comparison against an independent commitment, works
the same way.

Do not wait for an integrity error. The package has none: nothing it does can
tell a right reconstruction from a wrong one.

## Run a ceremony in protected memory (preferred)

`ProtectedShare` keeps each share value in an mlock'd, guard-paged buffer that
`Destroy` wipes, so you never hold share material in a Go string or manage the
wiping yourself:

```go
import "github.com/jeremyhahn/go-quicraft/pkg/crypto/shamir"

shares, err := shamir.SplitProtected(rootKey, 3, 5)
if err != nil {
	return err
}
// Destroy as each is consumed: locked memory is a finite kernel resource.
defer func() {
	for _, s := range shares {
		s.Destroy()
	}
}()

for _, s := range shares {
	// WithValue holds the share open for the callback, so a concurrent
	// Destroy blocks until it returns. Do not keep the slice past it.
	if err := s.WithValue(func(v []byte) error {
		return deliverToCustodian(s.Index, v)
	}); err != nil {
		return err
	}
}
```

Recovering is the mirror image:

```go
// Shares that arrived from outside the process: bounded decode, never
// json.Unmarshal into a []*ProtectedShare.
shares, err := shamir.UnmarshalShareSet(submitted)
enclave.Wipe(submitted) // the decoded copies are protected; wipe your buffer
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

Three things to remember:

- Every share must be `Destroy`ed, including one produced by `json.Unmarshal`
  (decoding is a construction).
- Read a share's value only inside `WithValue`, and keep nothing from the
  callback: the view is gone once it returns.
- A whole set is priced against `MaxShareSetBytes` before anything is allocated,
  so an oversized split or submission is refused up front.

If your process bounds its locked memory with a go-enclave `Allocator`, use the
`...With` forms (`SplitProtectedWith`, `CombineProtectedWith`,
`UnmarshalShareSetWith`, `NewProtectedShareWith`) so share
material is charged to it.

## Split a root key into wipeable buffers

```go
// 3-of-5: any three custodians can recover the key; any two learn nothing.
values, err := shamir.SplitBytes(rootKey, 3, 5)
if err != nil {
	return err
}
defer func() {
	for _, v := range values {
		enclave.Wipe(v)
	}
}()

for i, v := range values {
	// Hand values[i] to custodian i+1. The x-coordinate is inside the
	// payload, so nothing else needs to travel with it.
	if err := deliverToCustodian(i+1, v); err != nil {
		return err
	}
}
```

Choosing M and N is your decision. The library enforces two constraints:
`threshold >= 2` (a 1-of-N "split" is just the secret, N times), and both values
`<= 255`.

## Collect wipeable buffers and recover

```go
// values holds M-or-more base64-encoded share values from custodians.
// threshold came from your own records, NOT from the shares.
key, err := shamir.CombineBytes(values, threshold)
if err != nil {
	return err
}
defer enclave.Wipe(key)
// ... authenticate key before trusting it. See above.
```

You still own the input buffers and must wipe them. `CombineBytes` wipes only the
copies it decoded internally.

## Log or print a share safely

You do not need to do anything. Both human-facing paths of a `ProtectedShare` are
redacted:

```go
fmt.Printf("%v\n", share)                     // value omitted
logger.Info("share received", "share", share) // value omitted, any handler
```

`MarshalJSON` is not redacted, because it is the serialization path. So this does
write share material to your log:

```go
b, _ := json.Marshal(share)
logger.Info("share received", "share", string(b)) // DO NOT DO THIS
```

Log the `Index` if you need to identify a share. A leaked share cannot be revoked,
only outrun by a rekey ceremony.

## Sanity-check a share before combining

```go
if err := shamir.VerifyShare(candidate, collected); err != nil {
	return err // invalid or nil share, threshold/total mismatch, or duplicate index
}
```

This catches operator mistakes, such as the same custodian submitting twice or a
share from an old ceremony, before you spend a quorum on them. It does not tell
you a share is genuine; nothing can, before combining.

## Troubleshooting

**`too many shares: N exceeds the maximum of 255`** (or `share set has N shares,
exceeding the maximum of 255` from `UnmarshalShareSet`): the field has 255
non-zero x-coordinates, and that is the hard ceiling on a share set.

**`share N has unsupported format version V (legacy or foreign share)`**: the
share was not produced by this package at this version. Until the first
production release share format changes are made in place, so this usually means
the producing and consuming binaries were built from different commits. Rebuild
both from the same commit.

**`share N payload is X bytes, exceeding the 65536-byte limit`** (or `secret is X
bytes, exceeding the 65536-byte limit` when splitting): you are sharing a payload
rather than a key. Encrypt the payload under a key and share the key.

**`... need X bytes of locked memory, exceeding the Y-byte budget`** when
splitting (or `share set needs at least X bytes of locked memory, exceeding the
Y-byte budget` from `UnmarshalShareSet`): the whole set does not fit
`MaxShareSetBytes`. Use a smaller secret (share a key, not a
payload) or fewer shares.

**A `*ProtectedMemoryError`**: the process is out of locked memory
(`RLIMIT_MEMLOCK`) or your allocator's budget. Destroy shares you no longer need,
or raise the limit. The input was fine; for `NewProtectedShare`, the source buffer
has already been wiped.

**`share N has zero x-coordinate`**: a malformed or hand-edited share. `x = 0` is
where the secret itself sits on the polynomial and is never a valid share
coordinate.

**`share N payload length L differs from share 0 length S`**: the set mixes shares
of two different secrets.

**`need at least M shares, got K`**: you have not reached quorum. This is checked
against the claimed threshold; reaching it is necessary, not sufficient.

**Recovery "succeeds" but the key does not work**: this is the expected failure
mode of a wrong share set, not a bug, and combining cannot distinguish it. Check
that `threshold` came from your records rather than from `shares[0].Threshold`,
and that every share belongs to the same ceremony.

**A panic on a submitted share list**: should not happen. `Validate` is
nil-receiver safe and nil elements are reported by position. If you see one,
report it as a defect, because operator-submitted JSON containing `null` is an
anticipated input.
