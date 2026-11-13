# 744: OCSP responder authorization tests

RFC 6960 §4.2.2.2 accepts an OCSP response signed by the issuing CA itself, or
by a delegate that the CA issued with the id-kp-OCSPSigning extended key usage.
`validateOCSPResponder` (pkg/internal/transport/revocation/checker.go) also
requires a delegate to be inside its validity period, widened on each side by
`revocationClockSkew`. `TestValidateOCSPResponder` pins the predicate with a
fixed `now`:

- no embedded certificate is accepted;
- the issuer's own certificate is accepted;
- the issuer's certificate with a nil issuer argument goes through the EKU check
  and is refused;
- a ServerAuth-only delegate is refused with `ErrOCSPResponderUnauthorized`;
- a valid OCSP-signing delegate is accepted;
- a delegate whose NotBefore is now+skew+1s is refused, and one at now+skew-1s
  is accepted;
- a delegate whose NotAfter is now-skew-1s is refused, and one at now-skew+1s is
  accepted.

`TestIsRevoked_UnauthorizedResponderCause` serves a "good" response signed by a
not-yet-valid OCSP-signing delegate and checks that `IsRevoked` fails closed. It
asserts that the error matches both `ErrOCSPUnavailable` and, through
`OCSPUnavailableError.Unwrap`, `ErrOCSPResponderUnauthorized`. If the NotBefore
check is removed from a local copy of checker.go, both the NotBefore case and
the IsRevoked test fail.
