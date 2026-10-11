### Added

- `make release-candidate VERSION=vX.Y.Z-rc.N` cuts a release candidate
  (release-versioning.md §1a) through the same guarded, signed, atomically
  pushed recipe as `release-alpha` and `release`. The version is never
  inferred; N is a positive integer without leading zeroes, so `v0.21.0`,
  `v0.21.0-alpha`, `v0.21.0-rc.0` and `v0.21.0-rc.01` are refused before
  anything is written. `VERSION` and the CHANGELOG heading carry
  `X.Y.Z-rc.N`.
