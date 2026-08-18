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

// defaultStandardKDFVersion is PBKDF2-SHA256 in a FIPS build. There is no
// "standard mode" distinct from FIPS mode here: Argon2id is not compiled in
// (see kdf_argon2.go), so the approved KDF is the only one DefaultKDFVersion
// can name, whatever the GOFIPS140 environment says.
func defaultStandardKDFVersion() KDFVersion { return KDFPbkdf2SHA256 }
