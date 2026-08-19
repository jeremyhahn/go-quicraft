# Memory Enclave — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-08-19
> Conflicts with this document are defects. Fix them here first.

Use this package whenever your code holds a key, password, passphrase, or token
in memory and you would otherwise reach for a `[]byte` or a `string`.

## Before you start

- Go 1.26.6 or later, and `github.com/jeremyhahn/go-quicraft` as a dependency.
- A process memory-lock limit (`RLIMIT_MEMLOCK`) large enough for the number of
  keys you will hold. Containers frequently ship with a small default; if the
  process aborts with a memguard message about locked memory, raise the limit
  rather than reducing protection.
- Nothing else. There is no configuration file, no environment variable, and no
  daemon.

## Task: hold a key for the life of a service

```go
import "github.com/jeremyhahn/go-quicraft/pkg/enclave"

// keyBytes came from a KDF, a file read, or a KMS response.
key := enclave.NewProtectedKey(keyBytes)
// keyBytes is now zeroed. Do not read it again.
```

Store `key` in your struct. It stays encrypted in memory until you open it.

## Task: use the key

Open it, use it, destroy the buffer — in that order, every time:

```go
buf, err := key.Open()
if err != nil {
    return err
}
defer buf.Destroy()

aead, err := cipher.NewGCM(mustAES(buf.Bytes()))
if err != nil {
    return err
}
ciphertext := aead.Seal(nil, nonce, plaintext, aad)
```

Two rules that the compiler will not enforce for you:

1. **`defer buf.Destroy()` on the line after the error check.** A missed
   `Destroy` leaves the plaintext resident for the life of the process and burns
   locked-memory budget.
2. **Do not keep `buf.Bytes()`.** The slice points into memory that `Destroy`
   zeroes and unmaps. Storing it in a struct field is a use-after-free that will
   read zeros at best.

The `cipher.AEAD` above is worth calling out: it holds its own expanded copy of
the key, and neither the standard library nor `x/crypto` offers a way to wipe
one. Build it inside the same window, use it, and let it go — do not cache an
AEAD across the buffer's lifetime and assume the key is protected.

## Task: retire a key

```go
key.Clear()
```

After this, `Open` returns `ErrEnclaveNil`. Calling `Clear` twice is fine, and
it is safe to call while another goroutine is in `Open`.

**Understand what `Clear` does not do.** It revokes the ability to open the key.
It does not erase the encrypted copy, which remains on the heap until the
garbage collector reclaims it. If your threat model requires the bytes gone, the
answer is process exit, not `Clear`. What `Clear` genuinely gives you is that
the remaining bytes are ciphertext under a session key that never left memory
and dies with the process.

## Task: build a key in place, then seal it

When the key is produced piecewise (a KDF writing into a destination buffer, for
example), allocate protected memory first and promote it afterwards:

```go
buf := enclave.NewProtectedBuffer(32)
if err := deriveInto(buf.Bytes()); err != nil {
    buf.Destroy()
    return err
}
key := enclave.SealProtectedBuffer(buf)
// buf is now consumed: Bytes() is nil, Size() is 0. Do not use it again.
if key == nil {
    return errors.New("buffer was already consumed")
}
```

The plaintext never touches the ordinary heap.

## Task: clean up a slice you could not avoid

```go
enclave.WipeBytes(transient)
```

Use this for the copy a third-party API forced you to make. It is not a
substitute for `ProtectedBuffer` — the bytes were swappable and dumpable for the
whole time they existed — but it bounds how long they linger.

## Troubleshooting

**`memprotect: enclave is nil`** — you called `Open` on a key that was `Clear`ed
or was never given material. Check for a `Clear` on a shutdown path racing your
use, and check the constructor actually ran. Detect it with
`errors.Is(err, enclave.ErrEnclaveNil)`.

**The process aborts with a memguard message about locked memory** — you have
exhausted `RLIMIT_MEMLOCK`. Almost always this is leaked `ProtectedBuffer`s from
a missing `Destroy`, not a genuinely large key set. Audit every `Open` for a
paired `defer buf.Destroy()`. If the count is genuinely legitimate, raise the
limit on the process or container.

**Reads return zeros** — you kept a slice from `Bytes()` past `Destroy()`. Copy
what you need inside the window, or restructure so the work happens there.

**A signal handler stopped firing after adding this package** — it did not. This
package never installs one, and a test asserts that. Look for a direct
`memguard.CatchSignal` or `memguard.CatchInterrupt` call in your own code:
those reset **every** handler in the process.

**Tests are flaky under `-race` around key teardown** — you are almost certainly
holding a slice returned by `Bytes` across a `Destroy` on another goroutine. The
accessor CALL is race-safe (since `0.8.0-alpha`); the returned slice aliases
protected memory and stops being valid the moment the call returns. Use
`WithBytes` (or `ProtectedShare.WithValue`), which holds the enclave open for
the whole callback so `Destroy` has to wait, or give each goroutine its own
`Open`.
