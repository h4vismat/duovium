# Transactional event store implementation plan

> For agentic workers: implement the tasks in order, with review before publication.

**Goal:** Publish caller-owned PostgreSQL event-store transactions before engine changes.

**Architecture:** Keep pure aggregate semantics and the EventStore interface. Add
connection-scoped reads and transaction-scoped appends in the PostgreSQL adapter;
savepoints preserve per-append atomicity inside an application transaction.

**Tech Stack:** Rust 1.96+, SQLx 0.9, PostgreSQL 15.

**Spec:** ../specs/2026-09-09-transactional-event-store.md

## Global constraints

- Rust 1.96 and SQLx 0.9 remain supported.
- Preserve existing migrations and wire formats.
- No engine changes before crates.io publication and verification.
- Never retry external effects inside aggregate decisions or replay projections.

## Task 1: Transactional persistence

Files: `src/event_store/postgres.rs`, new `src/event_store/postgres/transaction_tests.rs`.

Produces:
```rust,ignore
pub async fn read_in(&self, connection: &mut PgConnection, stream_id: &ID)
    -> Result<Vec<Envelope<ID, E>>, StoreError>;
pub async fn append_in(&self, transaction: &mut Transaction<'_, Postgres>,
    stream_id: &ID, expected: ExpectedVersion, events: Vec<Proposed<E>>)
    -> Result<Version, AppendError<StoreError>>;
```

- [x] Add real database tests exercising caller-owned commit and rollback, multiple
  streams and an application outbox row, read-your-writes, and invisible pending events.
  Assert literal event values and row counts after the outer commit/rollback.
- [x] Run tests before implementation; confirm the missing API is the failure.
- [x] Extract read and append internals using the supplied connection. Implement
  append savepoint cleanup, preserve codec validation and named conflict detection.
- [x] Add failure regressions: bad second event, collision on second insert,
  exhausted one-connection pool, concurrent caller-owned appends, empty expectations,
  custom codec, and cancelled append. Confirm outer transactions remain usable after
  recoverable append failures and no partial batches survive.
- [x] Run existing and new database tests serially against disposable PostgreSQL 15.

## Task 2: Document and release

Files: `README.md`, `Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`, and compiling example.

- [x] Document provisional versions, savepoint behavior, same-connection reads,
  deterministic stream ordering, application retries, and deduplication.
- [x] Add an example appending an event and application outbox row in one transaction.
- [x] Check registry version availability, then set 0.1.1 and update lockfile/changelog.
- [x] Run `cargo test --locked`, the serial postgres-tests suite,
  `cargo fmt --check`, `cargo clippy --locked --all-features --all-targets -- -D warnings`,
  warning-free rustdoc, Rust 1.96 tests, and `cargo sqlx prepare --check`.
- [x] Verify an extracted package compiles with postgres without DATABASE_URL.
- [ ] Commit, push, and run `cargo publish --locked`.
  Independent review found no blocking issues; its codec-failure coverage suggestion
  was implemented and verified.
- [ ] Download/resolve published duovium 0.1.1 and compile a consumer using both APIs.
  Only this completes the prerequisite for engine work.

Validation before publication: PostgreSQL 15: 123 library tests, 7 integration tests,
2 doctests passed. Default features: 31 library tests, 3 integration tests, 2 doctests.
Rust 1.96 PostgreSQL feature tests passed. Clippy, rustfmt, warning-free rustdoc,
SQLx prepare --check, counter and transactional outbox examples all passed.
Packaged PostgreSQL build passed without DATABASE_URL. Migrations are unchanged.
