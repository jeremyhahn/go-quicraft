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
	"context"

	"github.com/jeremyhahn/go-enclave"
)

// ProtectedSealingStrategy is the protected-memory counterpart of SealingStrategy's root-key methods, with the key and credentials protected on both sides; optional, found by type assertion.
type ProtectedSealingStrategy interface {
	SealingStrategy

	// SealRootKeyProtected seals a root key held in protected memory without copying it out; the caller keeps ownership, and a nil or destroyed enclave is an error.
	SealRootKeyProtected(ctx context.Context, rootKey enclave.ByteAccess, creds Credentials) (*SealedRootKey, error)

	// UnsealRootKeyProtected decrypts the root key into a frozen locked enclave the caller owns and must Destroy on every path.
	UnsealRootKeyProtected(ctx context.Context, sealed *SealedRootKey, creds Credentials) (enclave.ByteAccess, error)
}

// Both strategies in this package implement ProtectedSealingStrategy.
var (
	_ ProtectedSealingStrategy = (*SoftwareStrategy)(nil)
	_ ProtectedSealingStrategy = (*NoneStrategy)(nil)
)
