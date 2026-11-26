Reviewed at e616cd74, packages: pkg/proto, pkg/internal/transport, pkg/crypto, pkg/seal

| # | severity | class | file:line | defect and the inputs that trigger it | proposed fix |
|---|---|---|---|---|---|
| 1 | P2 | api | pkg/proto/membership.go:429 | The `Membership.UnmarshalFrom` doc says the version byte must be `membershipFormatVersion` "EXACTLY" and that the gate matches the exact-equality `BinVer` gate. The code at :466 accepts the closed set {current, previous} (`acceptsMembershipFormatVersion`), and the `BinVer` gates are now windowed (`WireWindow.Accepts`). The two agree only while `previousMembershipFormatVersion == membershipFormatVersion`. After the first minor release that moves the constant, the doc contradicts the code: a buffer stamped with the previous version is decoded with the current layout's offsets. | Reword the doc to describe the read set. Add a compile-time or test assertion that a release which makes `previousMembershipFormatVersion` differ from the current version must also add a per-version decode path. |

pkg/proto: reviewed fully
