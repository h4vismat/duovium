# Changelog

## 0.1.1 — 2026-09-09

- Add PostgreSQL `read_in` and `append_in` for caller-owned transactions. Multiple
  event streams and application writes can commit or roll back together.
- Protect each transactional append with a savepoint. Failed batches leave no
  partial events, and conflict reporting reuses the caller's connection.
- Share encoding, reading, and append semantics with the existing standalone API.
  Existing event formats and migrations are unchanged.
- Document transaction ownership, retries, cancellation, and application outbox
  integration; add a runnable `transactional_outbox` example.

## 0.1.0 — 2026-09-08

- Initial release of functional aggregates, command execution, event stores,
  configurable JSON codecs, and PostgreSQL projection runners.
