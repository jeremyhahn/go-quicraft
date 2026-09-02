# Memory Enclave — Usage

> **Status: NORMATIVE** · Scope: go-quicraft `pkg/enclave` · Last verified: 2026-09-02
> Conflicts with this document are defects. Fix them here first.

Use this package whenever your code holds a key, password, passphrase, or token
in memory and you would otherwise reach for a `[]byte` or a `string`.

## Before you start

- Go 1.26.6 or later, and `github.com/jeremyhahn/go-quicraft` as a dependency.
- A process memory-lock limit (`RLIMIT_MEMLOCK`) large enough for the number of
  keys you will hold. Containers frequently ship with a small default; if the
  process reports a locked-memory failure, raise the limit
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

Two rules that the compiler will not enforce for you:

1. **`defer buf.Destroy()` on the line after the error check.** A missed
   `Destroy` leaves the plaintext resident for the life of the process and burns
   a whole page of locked-memory budget.
2. **Do not keep the slice.** `WithBytes` holds the enclave open for the
   callback and a concurrent `Destroy` waits, but the slice stops being valid the
   moment the callback returns. `buf.Open()` gives you the same slice with no
   lock at all — use it only where you own the enclave and can see that nothing
   else can destroy it. Storing either in a struct field is a use-after-free, and
   it FAULTS rather than reading zeros: `Destroy` unmaps the pages.

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
import goenclave "github.com/jeremyhahn/go-enclave"

buf, err := goenclave.NewMemoryEnclaveSized(32)
if err != nil {
    return err // the kernel refused the lock; see Troubleshooting
}
if err := buf.WithBytes(deriveInto); err != nil {
    buf.Destroy()
    return err
}
key := enclave.SealMemoryEnclave(buf)
// buf is now consumed: it is destroyed, Size() is 0, IsAlive() is false.
if key == nil {
    return errors.New("enclave was already consumed")
}
```

The plaintext never touches the ordinary heap. Two things about the sized
constructor are worth knowing: it returns an error, because locking pages is a
syscall the kernel can decline; and the enclave it returns stays WRITABLE for its
whole life, which is what makes filling it possible and what makes a retained
slice able to corrupt the material rather than fault. There is no post-fill
freeze — `SealMemoryEnclave` or a locked-to-locked move into
`goenclave.NewMemoryEnclave` is how the material stops being writable.

## Task: clean up a slice you could not avoid

```go
enclave.WipeBytes(transient)
```

Use this for the copy a third-party API forced you to make. It is not a
substitute for a `MemoryEnclave` — the bytes were swappable and dumpable for the
whole time they existed — but it bounds how long they linger.

## Troubleshooting

**`memprotect: enclave is nil`** — you called `Open` on a key that was `Clear`ed
or was never given material. Check for a `Clear` on a shutdown path racing your
use, and check the constructor actually ran. Detect it with
`errors.Is(err, enclave.ErrEnclaveNil)`.

**A constructor returns `ErrLockedMemoryUnavailable`** — you have exhausted
`RLIMIT_MEMLOCK`. Only that allocation fails; every enclave you already hold is
untouched, and the process keeps running. (This changed with the go-enclave
adoption. The old behaviour was worse and is worth knowing if you are carrying
old assumptions: memguard unmapped every live buffer in the process and aborted.)
Almost always the cause is leaked enclaves from a missing `Destroy`, not a
genuinely large key set — and remember the kernel charges a whole PAGE per
enclave, so a 32-byte key costs 4096 bytes. Audit every `Open` for a paired
`defer buf.Destroy()`. If the count is legitimate, raise the limit on the process
or container.

**A read faults, or the process dies with SIGSEGV near a key** — you kept a slice
from `buf.Open()` past a `Destroy()`. `Destroy` unmaps the pages, so the read
faults rather than returning zeros. Copy what you need inside the window, use
`WithBytes` so `Destroy` has to wait, or restructure so the work happens there.

**A signal handler stopped firing after adding this package** — it did not. This
package never installs one, and a test asserts that. Look for a direct
`memguard.CatchSignal` or `memguard.CatchInterrupt` call in your own code:
those reset **every** handler in the process.

**Tests are flaky under `-race` around key teardown** — you are almost certainly
holding a slice returned by `buf.Open()` across a `Destroy` on another goroutine.
Every accessor CALL is race-safe: it holds the enclave's read lock, so `Destroy`
blocks until it returns. The slice does not inherit that — it aliases protected
memory and stops being valid the moment the call returns. Use `WithBytes` (or
`ProtectedShare.WithValue`), which holds the enclave open for the whole callback,
or give each goroutine its own `Open` on the key.
