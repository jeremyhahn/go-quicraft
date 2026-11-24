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

import (
	"bytes"
	"io"
	"log/slog"

	"github.com/jeremyhahn/go-enclave"
)

// ContextBinding reports how a barrier treats the optional context the *Context methods fold into the AAD.
type ContextBinding uint8

const (
	// ContextBindingUnresolved means the barrier is sealed and no encryptor is chosen; ask again after Unseal. The zero value.
	ContextBindingUnresolved ContextBinding = iota

	// ContextBindingBarrier means the barrier encrypts with its own epoch AEAD
	// and folds the caller's context into the AAD itself. This is the software
	// path.
	ContextBindingBarrier

	// ContextBindingDelegated means a delegated (hardware/KMS) encryptor owns
	// the sealing AND implements AADSymmetricEncrypter, so the caller's context
	// is authenticated end to end by that encryptor.
	ContextBindingDelegated

	// ContextBindingUnsupported means a delegated SymmetricEncrypter encrypts without AAD; *Context methods refuse a non-empty context with ErrHardwareMode.
	ContextBindingUnsupported

	// ContextBindingPlaintext means StrategyNone: nothing is encrypted, so there is nothing to bind.
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

// Resolved reports whether this binding is an answer; false only for ContextBindingUnresolved. Cache the capability only when it is true.
func (c ContextBinding) Resolved() bool {
	return c != ContextBindingUnresolved
}

// contextBindingFor classifies the context capability of the encryptor a strategy produced (nil for software), verifying AAD claims with verifyAADBinding; never Unresolved.
func contextBindingFor(enc SymmetricEncrypter, plaintext bool, rand io.Reader) ContextBinding {
	if enc == nil {
		return ContextBindingBarrier
	}
	if plaintext {
		return ContextBindingPlaintext
	}
	aadEnc, ok := enc.(AADSymmetricEncrypter)
	if !ok {
		return ContextBindingUnsupported
	}
	if err := verifyAADBinding(aadEnc, rand); err != nil {
		slog.Error("quicraft/seal: delegated encryptor declares seal.AADSymmetricEncrypter but does not "+
			"authenticate the AAD; treating it as incapable of positional binding",
			"error", err,
			"consequence", "caller context is refused with ErrHardwareMode instead of being silently dropped; "+
				"RequireContextAAD will refuse this barrier",
		)
		return ContextBindingUnsupported
	}
	return ContextBindingDelegated
}

// aadProbeSize is the byte length of verifyAADBinding's plaintext and AAD values.
const aadProbeSize = 32

// verifyAADBinding checks once at setup that a delegated encryptor opens a probe under its random AAD and refuses it under another.
func verifyAADBinding(enc AADSymmetricEncrypter, rand io.Reader) error {
	probe := make([]byte, aadProbeSize)
	aadA := make([]byte, aadProbeSize)
	aadB := make([]byte, aadProbeSize)
	defer enclave.Wipe(probe)
	if _, err := io.ReadFull(rand, probe); err != nil {
		return &AADBindingProbeError{Stage: "probe-entropy", Err: err}
	}
	if _, err := io.ReadFull(rand, aadA); err != nil {
		return &AADBindingProbeError{Stage: "probe-entropy", Err: err}
	}
	if _, err := io.ReadFull(rand, aadB); err != nil {
		return &AADBindingProbeError{Stage: "probe-entropy", Err: err}
	}

	ct, err := enc.EncryptWithAAD(probe, aadA)
	if err != nil {
		return &AADBindingProbeError{Stage: "encrypt", Err: err}
	}

	got, err := enc.DecryptWithAAD(ct, aadA)
	if err != nil {
		return &AADBindingProbeError{Stage: "decrypt-matching-aad", Err: err}
	}
	if !bytes.Equal(got, probe) {
		return &AADBindingProbeError{Stage: "decrypt-matching-aad", Err: ErrAADProbeRoundTrip}
	}

	if _, err := enc.DecryptWithAAD(ct, aadB); err == nil {
		return &AADBindingProbeError{Stage: "decrypt-wrong-aad", Err: ErrAADProbeNotBound}
	}
	return nil
}

// ContextBinding reports how this barrier treats a caller context, resolved by Initialize or Unseal; Unresolved while sealed, and it may change across a seal/unseal cycle.
func (b *Barrier) ContextBinding() ContextBinding {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.state != barrierUnsealed {
		return ContextBindingUnresolved
	}
	return b.binding
}

// SupportsContextAAD reports whether this barrier can bind a caller context into its ciphertext authentication; false while sealed, so do not cache it.
func (b *Barrier) SupportsContextAAD() bool {
	return b.ContextBinding().Binds()
}

// ContextBindingResolved reports ContextBinding().Resolved(), for consumers that cannot import the type; false means not answerable yet.
func (b *Barrier) ContextBindingResolved() bool {
	return b.ContextBinding().Resolved()
}

// IsPlaintextMode reports whether the barrier encrypts nothing (StrategyNone).
func (b *Barrier) IsPlaintextMode() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.plaintextMode
}

// checkContextAADPolicy returns ErrContextAADUnsupported when RequireContextAAD is set and the encryptor cannot bind context.
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

// delegatedEncrypt seals through the delegated encryptor with aadForEpoch AAD when it authenticates AAD; an empty context uses plain Encrypt, a non-empty one without support returns ErrHardwareMode. Caller holds b.mu.RLock.
func (b *Barrier) delegatedEncrypt(plaintext, contextAAD []byte) ([]byte, error) {
	if len(contextAAD) == 0 {
		return b.hwEncryptor.Encrypt(plaintext)
	}
	aadEnc, ok := b.aadEncryptorLocked()
	if !ok {
		return nil, ErrHardwareMode
	}
	return aadEnc.EncryptWithAAD(plaintext, aadForEpoch(b.epoch, contextAAD))
}

// aadEncryptorLocked returns the delegated encryptor as an AADSymmetricEncrypter only if the resolved binding says it authenticates AAD. Caller holds b.mu.RLock.
func (b *Barrier) aadEncryptorLocked() (AADSymmetricEncrypter, bool) {
	if b.binding != ContextBindingDelegated {
		return nil, false
	}
	aadEnc, ok := b.hwEncryptor.(AADSymmetricEncrypter)
	return aadEnc, ok
}

// delegatedDecrypt opens a delegatedEncrypt ciphertext under the same context and epoch, mirroring its rules. Caller holds b.mu.RLock.
func (b *Barrier) delegatedDecrypt(ciphertext, contextAAD []byte, epoch uint64) ([]byte, error) {
	if len(contextAAD) == 0 {
		return b.hwEncryptor.Decrypt(ciphertext)
	}
	aadEnc, ok := b.aadEncryptorLocked()
	if !ok {
		return nil, ErrHardwareMode
	}
	return aadEnc.DecryptWithAAD(ciphertext, aadForEpoch(epoch, contextAAD))
}

// logDelegatedMode warns once per setup that delegated mode bypasses epoch DEKs, the epoch header, nonce tracking and the invocation counter.
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
