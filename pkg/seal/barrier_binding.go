// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package seal

import "log/slog"

// ContextBinding reports how a barrier treats the OPTIONAL caller-supplied
// context that the *Context methods fold into the AEAD AAD (a snapshot frame's
// shard/replica/index/ordinal, a WAL record's shard/segment/ordinal).
//
// It exists because "the barrier cannot bind a context" is not one condition but
// several with different meanings, and IsHardwareMode cannot tell them apart: the
// no-encryption strategy also installs a barrier encryptor, so IsHardwareMode is
// true for it even though nothing is encrypted. A caller or operator seeing only
// "no positional binding" cannot otherwise distinguish "there is nothing to bind
// because nothing is encrypted" from "the encryptor that IS protecting this data
// cannot authenticate where the data belongs" from "the encryptor does not exist
// yet, so the question has no answer".
type ContextBinding uint8

const (
	// ContextBindingUnresolved means the barrier is not unsealed, so the
	// capability does not exist yet: no encryptor is installed and nothing has
	// chosen a strategy. It is NOT an answer of "no" — it is the absence of an
	// answer, and it is the zero value so that an unset ContextBinding can
	// never be mistaken for a barrier that binds.
	//
	// A barrier legitimately spends time here: a node may start sealed and be
	// unsealed later (the Vault-style start-then-unseal flow), so every
	// consumer that caches the capability MUST treat this state as "ask again
	// after Unseal" rather than latching it. Binds() is false for it, which
	// keeps the fail-safe direction for callers that only ask the predicate,
	// but a consumer whose own correctness depends on the answer being STABLE
	// (see Barrier.ContextBinding) must distinguish it explicitly.
	ContextBindingUnresolved ContextBinding = iota

	// ContextBindingBarrier means the barrier encrypts with its own epoch AEAD
	// and folds the caller's context into the AAD itself. This is the software
	// path.
	ContextBindingBarrier

	// ContextBindingDelegated means a delegated (hardware/KMS) encryptor owns
	// the sealing AND implements AADSymmetricEncrypter, so the caller's context
	// is authenticated end to end by that encryptor.
	ContextBindingDelegated

	// ContextBindingUnsupported means a delegated encryptor owns the sealing but
	// implements only SymmetricEncrypter, which carries no AAD. The data IS
	// encrypted, but nothing binds a frame to its position: same-epoch frames
	// can be reordered or spliced undetected by an at-rest attacker with write
	// access. The *Context methods refuse a non-empty context with
	// ErrHardwareMode rather than silently dropping it.
	ContextBindingUnsupported

	// ContextBindingPlaintext means StrategyNone is in force: nothing is
	// encrypted, so there is no AEAD, no tag, and no position to authenticate.
	// The absence of a binding is inherent to the mode, not a limitation of an
	// encryptor that is otherwise protecting the data.
	ContextBindingPlaintext
)

// String returns the binding's name for logs and operator-facing messages.
func (c ContextBinding) String() string {
	switch c {
	case ContextBindingUnresolved:
		return "unresolved"
	case ContextBindingBarrier:
		return "barrier"
	case ContextBindingDelegated:
		return "delegated"
	case ContextBindingUnsupported:
		return "unsupported"
	case ContextBindingPlaintext:
		return "plaintext"
	default:
		return "unknown"
	}
}

// Binds reports whether a caller-supplied context is actually authenticated
// under this binding. It is false for ContextBindingUnresolved: a capability
// that does not exist yet cannot be relied on to authenticate anything.
func (c ContextBinding) Binds() bool {
	return c == ContextBindingBarrier || c == ContextBindingDelegated
}

// Resolved reports whether this binding is an ANSWER rather than the absence of
// one. It is false only for ContextBindingUnresolved.
//
// Callers that CACHE the capability must gate on Resolved before caching:
// !Binds() collapses "the barrier cannot bind" and "the barrier cannot say yet"
// into one bit, and caching the latter as the former is exactly how a
// deployment ends up permanently writing unbound records against a barrier that
// would have bound them.
func (c ContextBinding) Resolved() bool {
	return c != ContextBindingUnresolved
}

// contextBindingFor classifies the barrier's context capability from the
// encryptor a strategy just produced. enc is nil for the software path.
// plaintext reports that the strategy in force is StrategyNone, whose encryptor
// is an identity pass-through rather than a real cipher.
//
// It never returns ContextBindingUnresolved: it is called from Initialize and
// Unseal with the encryptor in hand, at which point the capability is known by
// construction. Whether the barrier has REACHED that point is state, not
// encryptor shape, and is decided by Barrier.ContextBinding.
func contextBindingFor(enc SymmetricEncrypter, plaintext bool) ContextBinding {
	if enc == nil {
		return ContextBindingBarrier
	}
	if plaintext {
		return ContextBindingPlaintext
	}
	if _, ok := enc.(AADSymmetricEncrypter); ok {
		return ContextBindingDelegated
	}
	return ContextBindingUnsupported
}

// ContextBinding reports how this barrier treats a caller-supplied context.
//
// The answer is a property of the INSTALLED ENCRYPTOR, and the encryptor is
// installed by Initialize or Unseal. Before that — uninitialized, or sealed,
// including after a seal/unseal cycle that is between the two — there is no
// encryptor and therefore no capability, and this returns
// ContextBindingUnresolved. It is NOT ContextBindingBarrier: reporting the
// software capability for a barrier that has not yet chosen a strategy claims a
// binding no encryptor has promised, and a consumer that cached it would keep
// calling the *Context methods against a delegated encryptor that fails every
// one of them with ErrHardwareMode.
//
// The capability is therefore VALID ONLY WHILE THE BARRIER STAYS UNSEALED, and
// it can legitimately change across a seal/unseal cycle: the same barrier may
// be unsealed under a software strategy (ContextBindingBarrier) and, later,
// under a delegated encryptor without AAD (ContextBindingUnsupported). A
// consumer that resolves this once and caches it — a WAL that decides at open
// whether its records carry positional binding, say — MUST refuse to cache an
// unresolved answer, and must re-resolve (or fail closed) if the barrier is
// sealed and unsealed underneath it. See ContextBinding.Resolved.
//
// SupportsContextAAD is the predicate most callers want, and IsPlaintextMode
// separates "nothing to bind" from "binding unavailable".
func (b *Barrier) ContextBinding() ContextBinding {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.state != barrierUnsealed {
		return ContextBindingUnresolved
	}
	return contextBindingFor(b.hwEncryptor, b.plaintextMode)
}

// SupportsContextAAD reports whether this barrier can ACTUALLY bind a
// caller-supplied context into the authentication of its ciphertext.
//
// It is true for the software path (the barrier's own AEAD folds the context
// into the AAD) and for a delegated encryptor that implements
// AADSymmetricEncrypter. It is false for a delegated encryptor that does not,
// false in no-encryption mode where there is no AEAD at all, and false for a
// barrier that is not unsealed — where the capability does not exist yet.
//
// That last case is the reason this predicate is safe to ASK at any time but
// unsafe to REMEMBER: false means either "cannot bind" or "cannot say yet", and
// the two demand opposite handling. A caller that suppresses its context per
// operation may use this directly; a caller that latches the answer must use
// ContextBinding and check ContextBinding.Resolved, or it will pin the
// capability of a barrier that had none at the time it asked.
//
// Callers that build a positional context should suppress it when this returns
// false — passing one to a barrier that cannot carry it fails with
// ErrHardwareMode, which is deliberate: the context is never silently dropped.
func (b *Barrier) SupportsContextAAD() bool {
	return b.ContextBinding().Binds()
}

// ContextBindingResolved reports whether SupportsContextAAD currently returns a
// real answer — that is, whether the barrier is unsealed and an encryptor is
// installed. It exists for consumers that cache the capability and cannot
// import this package's ContextBinding type (the WAL asserts its barrier
// structurally); it is exactly ContextBinding().Resolved().
//
// A consumer that caches must treat false as "not answerable yet" and refuse to
// record a capability, NOT as "does not support context AAD".
func (b *Barrier) ContextBindingResolved() bool {
	return b.ContextBinding().Resolved()
}

// IsPlaintextMode reports whether the barrier performs no encryption at all
// (StrategyNone). It distinguishes the two ways SupportsContextAAD can be
// false: in plaintext mode no binding is MEANINGFUL, while in
// ContextBindingUnsupported mode the data is encrypted and a binding is
// MISSING. Operators need that difference; IsHardwareMode is true for both.
func (b *Barrier) IsPlaintextMode() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.plaintextMode
}

// checkContextAADPolicy enforces BarrierConfig.RequireContextAAD at barrier
// setup. It returns ErrContextAADUnsupported when the deployment demands
// authenticated positional binding and the encryptor about to be installed
// cannot provide it — including no-encryption mode, where the demand is
// unsatisfiable by construction (see BarrierConfig.RequireContextAAD).
func (b *Barrier) checkContextAADPolicy(binding ContextBinding) error {
	if !b.config.RequireContextAAD || binding.Binds() {
		return nil
	}
	slog.Error("quicraft/seal: barrier setup refused; RequireContextAAD is set but the barrier cannot authenticate caller context",
		"binding", binding.String(),
		"deployment_id", b.config.DeploymentID,
		"remedy", "supply an encryptor implementing seal.AADSymmetricEncrypter, select a software sealing strategy, or clear RequireContextAAD",
	)
	return ErrContextAADUnsupported
}

// delegatedEncrypt seals plaintext through the installed delegated encryptor,
// carrying the caller's context when — and ONLY when — that encryptor declares
// it authenticates AAD by implementing AADSymmetricEncrypter. Caller must hold
// at least b.mu.RLock() and must have checked b.hwEncryptor != nil.
//
// The aad bytes are built with aadForEpoch, the SAME helper the software path
// uses, so the two constructions cannot drift: a delegated frame is bound to
// [epoch][context] exactly as a software frame is.
//
// An EMPTY context takes the plain Encrypt path unchanged. That is deliberate,
// not an oversight: in delegated mode Encrypt itself has no AAD to offer, and
// the documented equivalence "EncryptWithContext with no context == Encrypt"
// must keep holding, or ciphertext written through one method would stop
// opening through the other. Nothing is downgraded — a caller that supplies no
// context asked for no binding.
//
// A non-empty context on an encryptor without the capability returns
// ErrHardwareMode. It is never dropped.
func (b *Barrier) delegatedEncrypt(plaintext, contextAAD []byte) ([]byte, error) {
	if len(contextAAD) == 0 {
		return b.hwEncryptor.Encrypt(plaintext)
	}
	aadEnc, ok := b.hwEncryptor.(AADSymmetricEncrypter)
	if !ok {
		return nil, ErrHardwareMode
	}
	return aadEnc.EncryptWithAAD(plaintext, aadForEpoch(b.epoch, contextAAD))
}

// delegatedDecrypt opens a ciphertext sealed by delegatedEncrypt under the same
// context. epoch is the epoch whose AAD the frame was sealed with: the current
// epoch for the header-implicit methods, or the caller's explicitly named epoch
// for DecryptForEpochContext. Delegated mode cannot rotate (Rotate returns
// ErrHardwareMode), so the current epoch is stable for the barrier's lifetime
// and the implicit form matches what the encrypt side used.
//
// Mirrors delegatedEncrypt exactly: an empty context takes the plain Decrypt
// path, and a non-empty context on an encryptor without the capability fails
// closed with ErrHardwareMode rather than opening the frame unbound. Caller
// must hold at least b.mu.RLock() and must have checked b.hwEncryptor != nil.
func (b *Barrier) delegatedDecrypt(ciphertext, contextAAD []byte, epoch uint64) ([]byte, error) {
	if len(contextAAD) == 0 {
		return b.hwEncryptor.Decrypt(ciphertext)
	}
	aadEnc, ok := b.hwEncryptor.(AADSymmetricEncrypter)
	if !ok {
		return nil, ErrHardwareMode
	}
	return aadEnc.DecryptWithAAD(ciphertext, aadForEpoch(epoch, contextAAD))
}

// logDelegatedMode records, once per barrier setup, exactly what the barrier
// hands to a delegated encryptor. The early return in every encrypt/decrypt
// path (see Barrier.hwEncryptor) means delegated mode is not "the same barrier
// with a different cipher": epoch DEK derivation, the epoch ciphertext header,
// the nonce-reuse tracker, and the NIST SP 800-38D invocation counter are ALL
// bypassed, because the delegated encryptor owns its own nonce and key
// management. That is intended, but it is invisible from the config alone, so
// it is stated at WARN where an operator will see it.
func (b *Barrier) logDelegatedMode(phase string, binding ContextBinding) {
	if binding == ContextBindingPlaintext {
		// Not a delegation of encryption but the absence of it. Saying
		// "safeguards bypassed" here would understate a mode in which there is
		// no ciphertext at all.
		slog.Warn("quicraft/seal: barrier performs NO encryption; at-rest data is stored in plaintext",
			"phase", phase,
			"binding", binding.String(),
			"strategy", string(b.strategyID),
			"epoch", b.epoch,
		)
		return
	}
	slog.Warn("quicraft/seal: barrier sealing is delegated to an external encryptor; barrier-side AEAD safeguards are bypassed",
		"phase", phase,
		"binding", binding.String(),
		"delegated", "epoch DEK derivation, epoch ciphertext header, nonce-reuse tracking, NIST SP 800-38D invocation counting",
		"context_binding_available", binding.Binds(),
		"strategy", string(b.strategyID),
		"hardware_backed", b.strategyHardwareBacked,
		"epoch", b.epoch,
	)
}
