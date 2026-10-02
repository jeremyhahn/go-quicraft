# Memory enclave: usage

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-10-02
> Conflicts with this document are defects. Fix them here first.

Use protected memory whenever your code holds a key, password, passphrase or
token and you would otherwise use a `[]byte` or a `string`.

The package is `github.com/jeremyhahn/go-enclave`, not go-quicraft. go-quicraft
has no protected-memory type of its own; import go-enclave directly, as
go-quicraft does.

## Before you start

- Go 1.27.0 or later, and `github.com/jeremyhahn/go-enclave` as a dependency.
- A process memory-lock limit (`RLIMIT_MEMLOCK`) large enough for the keys you
  hold. Containers often ship with a small default; if allocations report a
  locked-memory failure, raise the limit rather than reduce protection.
- Nothing else: no configuration file, environment variable or daemon.

## Choose a container

| You have | Use | Because |
|---|---|---|
| a key read occasionally, held for the life of a service | `SealedEnclave` | ciphertext at rest; plaintext exists only inside an `Open` |
| a key in continuous use for the length of one operation | `MemoryEnclave` | locked plaintext, no per-use decryption |
| material you are about to generate | `NewMemoryEnclaveSized` + `Freeze` | it never touches the ordinary heap |

go-quicraft holds its root key, every epoch DEK and every passphrase as
`SealedEnclave`s, and opens a `MemoryEnclave` only for the window in which the
bytes are read.

## Hold a key for the life of a service

```go
import "github.com/jeremyhahn/go-enclave"

// keyBytes came from a KDF, a file read, or a KMS response.
key, err := enclave.NewSealed(ctx, keyBytes)
if err != nil {
	return err
}
// keyBytes is now zeroed, on the error path too. Do not read it again.
```

`ctx` bounds the seal. go-enclave hands it to the process sealer's provider,
which may be a device or a remote service that blocks; a context that has ended
fails the call with an error matching both `enclave.ErrSealFailed` and
`ctx.Err()`, and still wipes `keyBytes`.

Store `key` in your struct. Its material is ciphertext until you open it.

## Use the key

Open it, use it, destroy the opened copy, every time:

```go
buf, err := key.Open(ctx)
if err != nil {
	return err
}
defer buf.Destroy()

var ciphertext []byte
if err := buf.WithBytes(func(k []byte) error {
	block, err := aes.NewCipher(k)
	if err != nil {
		return err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	ciphertext = aead.Seal(nil, nonce, plaintext, aad)
	return nil
}); err != nil {
	return err
}
```

Two rules the compiler will not enforce:

1. **`defer buf.Destroy()` right after the error check.** `Open` returns a new
   enclave each time and you own it. A missed `Destroy` leaves the plaintext
   resident for the life of the process and uses a whole page of locked-memory
   budget.
2. **Do not keep the slice.** `WithBytes` holds the enclave open for the callback
   and a concurrent `Destroy` waits, but the slice is invalid once the callback
   returns. `buf.Open()` gives you the same slice with no lock at all; use it
   only where you own the enclave and nothing else can destroy it. Storing
   either in a struct field is a use-after-free, and it faults rather than
   reading zeros, because `Destroy` unmaps the pages.
3. **Do not write into it.** The enclave `Open` returns is frozen read-only, so
   a write through either slice faults. If you need a writable copy, allocate
   your own with `NewMemoryEnclaveSized` and copy into it inside `WithBytes`.

`buf.Destroy()` returns an error. A bare `defer buf.Destroy()` discards it, which
loses nothing secret: `ErrLockedMemoryUnavailable` means the kernel refused the
release and the pages stay on go-enclave's `WipeAll` ledger for a retry, and
`ErrReleasedWithoutWipe` means a denied `mprotect` forced a bare unmap of pages
the kernel zeroes before reuse. Check it where you want to see either condition.

The `cipher.AEAD` above holds its own expanded copy of the key, and neither the
standard library nor `x/crypto` can wipe it. Build it inside the window, use it,
and let it go. Caching an AEAD past the buffer's lifetime leaves the key
unprotected.

## Retire a key

```go
key.Destroy()
```

After this, `Open` returns `ErrEnclaveDestroyed`. Calling `Destroy` twice is
fine, it is safe on a nil handle, and it is safe while another goroutine is in
`Open`. It does not wait for that call: an open already in flight completes
against the envelope it pinned, and the last open out wipes the ciphertext.

`Destroy` wipes the ciphertext and the wrapped-key blob before releasing them.
Enclaves a previous `Open` handed out are not affected; those are yours to
destroy.

## Build a key in place, then freeze it

When the key is produced rather than received (a KDF writing into a
destination, a CSPRNG read, a Shamir share being encoded), allocate locked memory
first and fill it where it lies. The material then never exists on the ordinary
heap:

```go
buf, err := enclave.NewMemoryEnclaveSized(32)
if err != nil {
	return err // the kernel refused the lock; see Troubleshooting
}
if err := buf.WithBytes(deriveInto); err != nil {
	buf.Destroy()
	return err
}
// The material is final: make the pages read-only, so a stray write through a
// retained slice faults instead of silently corrupting the key.
if err := buf.Freeze(); err != nil {
	buf.Destroy()
	return err
}
```

A sized enclave stays writable until you freeze it. That is what makes filling it possible, and what lets a retained
slice corrupt the material instead of faulting. `Freeze` is idempotent and
irreversible. A refused freeze leaves the enclave alive and writable, so destroy
it rather than hand back material whose protection was not established.

Do not freeze an enclave whose last act is to be consumed: `NewSealed` and
`NewMemoryEnclave` wipe what they are handed, and a wipe of frozen pages faults
the process.

## Clean up a slice you could not avoid

```go
enclave.Wipe(transient)
```

Use this for the copy a third-party API forced you to make. It does not replace
a `MemoryEnclave` (the bytes were swappable and dumpable while they existed), but
it bounds how long they linger.

## Supply a passphrase to go-quicraft's seal layer

`seal.Credentials.Secret` is a `*enclave.SealedEnclave`. Build one with the
constructor, which consumes your slice:

```go
creds, err := seal.NewCredentials(ctx, []byte(passphrase))
if err != nil {
	return err
}
defer creds.Secret.Destroy() // the credential is yours; the seal layer never destroys it
```

One credential serves many calls. The seal layer opens it, derives from it and
destroys the opened copy each time; it never touches the `SealedEnclave` itself.

## Troubleshooting

**`enclave: enclave has been destroyed`**: you used a handle after `Destroy`, or
a shutdown path destroyed it while you were working. Detect it with
`errors.Is(err, enclave.ErrEnclaveDestroyed)`. In go-quicraft's seal layer the
same condition on a strategy's passphrase surfaces as `ErrStrategyClosed`.

**A constructor returns `ErrLockedMemoryUnavailable`**: you have exhausted
`RLIMIT_MEMLOCK`. Only that allocation fails; every enclave you already hold is
untouched. The usual cause is leaked enclaves from a missing `Destroy`, not a
large key set. The kernel charges a whole page per enclave, so a 32-byte key
costs 4096 bytes. Check every `Open` for a paired `defer buf.Destroy()`; if the
count is legitimate, raise the limit on the process or container.

**A read faults, or the process dies with SIGSEGV near a key**: you kept a slice
from `buf.Open()` past a `Destroy()`. Copy what you need inside the window, use
`WithBytes` so `Destroy` has to wait, or move the work inside the window.

**A write faults**: you wrote through a slice from a frozen enclave (one built
by `NewMemoryEnclave` or `NewMemoryEnclaveCopy`, one returned by
`SealedEnclave.Open`, or a sized one you already froze). That fault is the protection working. The same stray write to an
unfrozen enclave corrupts the material silently, which is why `Freeze` exists.

**An open fails with `enclave: could not open sealed material`**
(`ErrOpenFailed`): the context you passed to `Open` ended, or a
per-operation provider's infrastructure failed. `errors.Is(err, ctx.Err())`
tells the first apart. The sealed value is intact; open it again with a live
context.

**Tests are flaky under `-race` around key teardown**: you are probably holding
a slice from `buf.Open()` across a `Destroy` on another goroutine. Every accessor
call holds the enclave's read lock, so `Destroy` blocks until it returns, but the
slice does not inherit that lock. Use `WithBytes` (or `ProtectedShare.WithValue`),
which holds the enclave open for the whole callback, or give each goroutine its
own `Open`.
