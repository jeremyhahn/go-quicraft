# Memory Enclave — Usage

> **Status: NORMATIVE** · Scope: go-quicraft's use of go-enclave protected memory · Last verified: 2026-09-03
> Conflicts with this document are defects. Fix them here first.

Reach for protected memory whenever your code holds a key, password, passphrase,
or token and you would otherwise use a `[]byte` or a `string`.

**The package is `github.com/jeremyhahn/go-enclave`, not go-quicraft.** This
repository declares no protected-memory type of its own — `pkg/enclave` was
deleted in #54, and its absence is the point: `docs/00 §R` is that no project
hand-rolls this. Import go-enclave directly, exactly as go-quicraft does.

## Before you start

- Go 1.27.0 or later, and `github.com/jeremyhahn/go-enclave` as a dependency.
- A process memory-lock limit (`RLIMIT_MEMLOCK`) large enough for the number of
  keys you will hold. Containers frequently ship with a small default; if the
  process reports a locked-memory failure, raise the limit rather than reducing
  protection.
- Nothing else. There is no configuration file, no environment variable, and no
  daemon.

## Choosing between the two containers

| You have | Use | Because |
|---|---|---|
| a key read twice a day, held for the life of a service | `SealedEnclave` | ciphertext at rest; plaintext exists only inside an `Open` |
| a key in continuous use for the length of one operation | `MemoryEnclave` | locked plaintext, no per-use decryption |
| material you are about to GENERATE | `NewMemoryEnclaveSized` + `Freeze` | it never touches the ordinary heap at all |

go-quicraft holds its root key, every epoch DEK and every passphrase as
`SealedEnclave`s, and materializes a `MemoryEnclave` only for the window in which
the bytes are actually read.

## Task: hold a key for the life of a service

```go
import "github.com/jeremyhahn/go-enclave"

// keyBytes came from a KDF, a file read, or a KMS response.
key, err := enclave.NewSealed(keyBytes)
if err != nil {
    return err
}
// keyBytes is now zeroed — on the error path too. Do not read it again.
```

Store `key` in your struct. Its material is ciphertext until you open it.

## Task: use the key

Open it, use it, destroy the buffer — in that order, every time:

```go
buf, err := key.Open()
if err != nil {
    return err
}
defer buf.Destroy()

var ciphertext []byte
if err := buf.WithBytes(func(k []byte) error {
    aead, err := cipher.NewGCM(mustAES(k))
    if err != nil {
        return err
    }
    ciphertext = aead.Seal(nil, nonce, plaintext, aad)
    return nil
}); err != nil {
    return err
}
```

Two rules the compiler will not enforce for you:

1. **`defer buf.Destroy()` on the line after the error check.** `Open` returns a
   NEW enclave each time and the caller owns it. A missed `Destroy` leaves the
   plaintext resident for the life of the process and burns a whole page of
   locked-memory budget.
2. **Do not keep the slice.** `WithBytes` holds the enclave open for the callback
   and a concurrent `Destroy` waits, but the slice stops being valid the moment
   the callback returns. `buf.Open()` gives you the same slice with no lock at
   all — use it only where you own the enclave and can see that nothing else can
   destroy it. Storing either in a struct field is a use-after-free, and it
   FAULTS rather than reading zeros: `Destroy` unmaps the pages.

The `cipher.AEAD` above is worth calling out: it holds its own expanded copy of
the key, and neither the standard library nor `x/crypto` offers a way to wipe
one. Build it inside the same window, use it, and let it go — do not cache an
AEAD across the buffer's lifetime and assume the key is protected.

## Task: retire a key

```go
key.Destroy()
```

After this, `Open` returns `ErrEnclaveDestroyed`. Calling `Destroy` twice is
fine, it is safe on a nil handle, and it is safe to call while another goroutine
is in `Open` — it takes the write lock and waits that call out.

**`Destroy` erases.** It wipes the ciphertext and the wrapped-key blob before
releasing them. This is a change worth knowing if you are carrying old
assumptions: go-quicraft's retired `ProtectedKey.Clear` only revoked the ability
to open a key, because memguard exposed no way to zero an `Enclave`'s ciphertext.
Enclaves a previous `Open` handed out are NOT affected — those are yours, and you
destroy them on your own paths.

## Task: build a key in place, then freeze it

When the key is produced rather than received — a KDF writing into a destination,
a CSPRNG read, a Shamir share being encoded — allocate locked memory first and
fill it where it lies. The material then never exists on the ordinary heap, not
even transiently:

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
// retained slice FAULTS instead of silently corrupting the key.
if err := buf.Freeze(); err != nil {
    buf.Destroy()
    return err
}
```

A sized enclave stays WRITABLE until you freeze it — that is what makes filling
it possible, and what makes a retained slice able to corrupt the material rather
than fault. `Freeze` is idempotent and irreversible; there is deliberately no
`Melt`. A refused freeze leaves the enclave alive and writable, so destroy it
rather than handing back material whose promised protection was not established.

Do **not** freeze an enclave whose last act is to be consumed: `NewSealed` and
`NewMemoryEnclave` wipe what they are handed, and a wipe of frozen pages faults
the process. go-quicraft's root-key enclave is deliberately left writable for
exactly that reason.

## Task: clean up a slice you could not avoid

```go
enclave.Wipe(transient)
```

Use this for the copy a third-party API forced you to make. It is not a
substitute for a `MemoryEnclave` — the bytes were swappable and dumpable for the
whole time they existed — but it bounds how long they linger.

## Task: supply a passphrase to go-quicraft's seal layer

`seal.Credentials.Secret` is a `*enclave.SealedEnclave`, not a `[]byte` (#41).
Build one with the constructor, which consumes your slice:

```go
creds, err := seal.NewCredentials([]byte(passphrase))
if err != nil {
    return err
}
defer creds.Secret.Destroy() // the credential is YOURS; the seal layer never destroys it
```

One credential serves many calls. The seal layer opens it, derives from it and
destroys the opened copy each time, and never touches the `SealedEnclave` itself.

## Troubleshooting

**`enclave: enclave has been destroyed`** — you used a handle after `Destroy`, or
a shutdown path destroyed it while you were still working. Detect it with
`errors.Is(err, enclave.ErrEnclaveDestroyed)`. In go-quicraft's seal layer the
same condition on a strategy's passphrase surfaces as `ErrStrategyClosed`, which
is the same fact named for the caller who can act on it.

**A constructor returns `ErrLockedMemoryUnavailable`** — you have exhausted
`RLIMIT_MEMLOCK`. Only that allocation fails; every enclave you already hold is
untouched, and the process keeps running. Almost always the cause is leaked
enclaves from a missing `Destroy`, not a genuinely large key set — and remember
the kernel charges a whole PAGE per enclave, so a 32-byte key costs 4096 bytes.
Audit every `Open` for a paired `defer buf.Destroy()`. If the count is
legitimate, raise the limit on the process or container.

**A read faults, or the process dies with SIGSEGV near a key** — you kept a slice
from `buf.Open()` past a `Destroy()`. `Destroy` unmaps the pages, so the read
faults rather than returning zeros. Copy what you need inside the window, use
`WithBytes` so `Destroy` has to wait, or restructure so the work happens there.

**A write faults** — you wrote through a slice taken from a FROZEN enclave: one
built by `NewMemoryEnclave` or `NewMemoryEnclaveCopy`, or a sized one you have
already frozen. That fault is the protection working. The mirror-image bug is
worse and silent: the same stray write to a sized enclave you have not frozen
corrupts the material with nothing to announce it, which is why `Freeze` exists.

**Tests are flaky under `-race` around key teardown** — you are almost certainly
holding a slice returned by `buf.Open()` across a `Destroy` on another goroutine.
Every accessor CALL is race-safe: it holds the enclave's read lock, so `Destroy`
blocks until it returns. The slice does not inherit that — it aliases protected
memory and stops being valid the moment the call returns. Use `WithBytes` (or
`ProtectedShare.WithValue`), which holds the enclave open for the whole callback,
or give each goroutine its own `Open` on the key.
