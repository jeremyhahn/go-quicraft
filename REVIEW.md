# Code review passes

Copy this file to the repository root as `REVIEW.md` and edit the passes.
The `code-review.yml` template (v5.13.0 and later, ruling 1142 (A)) reads it
from the base branch, so a pull request cannot change its own review. It
reads the first 16 KiB. With no `REVIEW.md`, the review makes the three
passes below.

Give every finding a priority (P0 to P3), the file, and the line in the head.
Only a P0 or P1 finding on a line the pull request changes requests changes.
P2 and P3 findings on changed lines are advisory. Findings on other lines go
to one `review-followup` issue for the pull request and never block it.

## 1. Bugs

Correctness, data loss, concurrency, resource leaks, error handling, and
tests that do not prove the change.

## 2. Security

Authentication, authorization, input validation, injection, secrets in code
or logs, cryptography, and fail-open paths.

## 3. SPEC and plan compliance

The change against `docs/SPEC.md` and `ROADMAP.md`: what a row requires that
the change misses, and what the change does that no row allows. A SPEC-pass
finding is P1.
