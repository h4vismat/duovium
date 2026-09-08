# Release readiness implementation plan

Approved scope: implement the findings and simplifications in the pre-publication review.

Architecture: preserve the functional aggregate/executor core; isolate JSON encoding in
a configurable persistence codec; preserve PostgreSQL atomic append and projection transactions.
Ship SQLx offline metadata so consumers need no build-time database.

- [x] Simplify core errors, remove serialization bounds from Event, and reject zero executor attempts.
  Verify a non-Serde aggregate works and errors compose through std::error::Error.
- [x] Validate runner configuration; reject empty/negative batches and invalid backoff settings.
  Verify zero/negative values and saturating backoff.
- [x] Add a checkpoint revision migration. Carry the claimed revision through failed ticks;
  record only processing failures with a matching revision, advancing it on progress/resume/rebuild.
  Verify delayed failures after success/resume/rebuild cannot change the checkpoint.
- [x] Separate retryable projection infrastructure errors from rejected events, expose run observations,
  and return a halted error from the default run loop. Verify retries, notifications and halting.
- [x] Add EventCodec and default Serde JSON codec; thread through store/feed/runner.
  Verify historical payload upcasting through both store and feed, and round-trip write rejection.
- [x] Document contracts, runnable examples, migrations, codecs, runner lifecycle and limitations.
  Added MSRV and CI. License/repository details are tracked separately below.
- [x] Generate offline SQLx metadata; run core/database suites, Clippy, rustfmt, rustdoc,
  MSRV build and packaged downstream builds without DATABASE_URL.

The implementation was reviewed before its initial commit. The author subsequently
authorized pushing the repository and publishing version 0.1.0 to crates.io.

- [x] Add the author-selected license text, copyright holder and public repository URL.
  MIT; copyright (c) 2026 h4vismat; https://github.com/h4vismat/duovium.

Verification: PostgreSQL 15 regression suite, core tests and README doctests;
Rust 1.96 core/PostgreSQL checks and tests; rustfmt on 1.96 and stable; Clippy
with all features/targets; rustdoc with warnings denied; SQLx prepare --check;
offline packaging and an extracted-package consumer with both feature configurations.
The original migrations are byte-for-byte unchanged. Independent source and
release-material reviews found no blocking issues.
