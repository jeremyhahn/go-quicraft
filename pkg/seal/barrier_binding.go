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
// two with opposite meanings, and IsHardwareMode cannot tell them apart: the
// no-encryption strategy also installs a barrier encryptor, so IsHardwareMode is
// true for it even though nothing is encrypted. A caller or operator seeing only
// "no positional binding" cannot otherwise distinguish "there is nothing to bind
// because nothing is encrypted" from "the encryptor that IS protecting this data
// cannot authenticate where the data belongs".
type ContextBinding uint8

const (
	// ContextBindingBarrier means the barrier encrypts with its own epoch AEAD
	// and folds the caller's context into the AAD itself. This is the software
	// path, and it is also what an uninitialized or sealed barrier reports,
	// since no delegated encryptor is installed.
	ContextBindingBarrier ContextBinding = iota

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
// under this binding.
func (c ContextBinding) Binds() bool {
	return c == ContextBindingBarrier || c == ContextBindingDelegated
}

// contextBindingFor classifies the barrier's context capability from the
// installed encryptor. enc is nil for the software path. plaintext reports that
// the strategy in force is StrategyNone, whose encryptor is an identity
// pass-through rather than a real cipher.
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
// See the ContextBinding constants; SupportsContextAAD is the predicate most
// callers want, and IsPlaintextMode separates "nothing to bind" from "binding
// unavailable".
func (b *Barrier) ContextBinding() ContextBinding {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return contextBindingFor(b.hwEncryptor, b.plaintextMode)
}

// SupportsContextAAD reports whether this barrier can ACTUALLY bind a
// caller-supplied context into the authentication of its ciphertext.
//
// It is true for the software path (the barrier's own AEAD folds the context
// into the AAD) and for a delegated encryptor that implements
// AADSymmetricEncrypter. It is false for a delegated encryptor that does not,
// and false in no-encryption mode where there is no AEAD at all.
//
// Callers that build a positional context should suppress it when this returns
// false — passing one to a barrier that cannot carry it fails with
// ErrHardwareMode, which is deliberate: the context is never silently dropped.
func (b *Barrier) SupportsContextAAD() bool {
	return b.ContextBinding().Binds()
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
