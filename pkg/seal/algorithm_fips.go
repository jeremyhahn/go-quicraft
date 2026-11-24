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

//go:build fips

package seal

// softwareFallbackAlgorithm returns AES-256-GCM. A FIPS build has no
// non-approved AEAD compiled in (see algorithm_chacha.go), so there is no
// ChaCha20 fallback to take on a CPU without AES-NI: such a host runs the
// software AES-GCM implementation, more slowly, rather than running an
// algorithm this build is not permitted to execute.
func softwareFallbackAlgorithm() SymmetricAlgorithm { return AlgAES256GCM }
