# Caller-owned PostgreSQL transactions

The user authorized changes to duovium as a prerequisite for event sourcing in
mooze-engine. Publish and verify the updated crate on crates.io before changing
engine. Engine will use authoritative transaction events, verified opening-state
imports, and a maintenance window with durable callback buffering.

## Scope

Add `PostgresEventStore::read_in(&mut PgConnection, &ID)` and
`PostgresEventStore::append_in(&mut Transaction<'_, Postgres>, &ID,
ExpectedVersion, Vec<Proposed<E>>)`. Keep the existing EventStore trait,
Executor, Commands, codec behavior, migrations, and event representation compatible.
Keep Rust 1.96 and SQLx 0.9. Release as additive version 0.1.1 if available.

`read_in` reads the connection's snapshot, including that transaction's own
uncommitted appends. It does not acquire a connection from the store's pool.
`append_in` returns a provisional version: only the caller's outer commit makes
the append durable. Multiple streams and application tables can share that commit.

Each append uses a savepoint. On success release the savepoint, never commit the
outer transaction. On any append error roll back the savepoint before returning,
so a caught encoding or SQL error cannot leave a partial event batch in a transaction
the caller later commits. If savepoint cleanup fails return an infrastructure error;
the caller must discard the outer transaction. Cancellation drops the savepoint;
SQLx queues its rollback, and the caller should roll back the entire business operation.

Classify only the named stream-version uniqueness constraint as a conflict. Read
the observed current version on the same connection after savepoint rollback.
Do not acquire a second pool connection, including on errors. Under repeatable-read
or serializable isolation the caller must retry the entire outer transaction when
isolation prevents progress; the crate does not retry application work.

Existing standalone read/append delegate to shared implementation. Standalone
append commits on success and rolls back on failure. Empty batches retain existing
version-check semantics; an empty check does not reserve or lock a stream against
later writers. Ordered multi-stream access and application-wide invariant locking
remain caller responsibilities. Do not build an engine-specific transaction manager.

## Validation and release

Use real PostgreSQL 15 tests to prove visibility, read-your-writes, multi-stream
commit and rollback with application writes, partial-batch cleanup after codec and
SQL errors, conflict handling with one connection, concurrent writers, custom codec
reuse, and cancellation recovery. Existing tests protect standalone semantics.
Add a compiling usage example with an application outbox transaction. Keep replayable
projections separate from effect delivery. Explain request deduplication and unknown
commit outcomes rather than promising exactly-once external execution.

Run core/PostgreSQL suites, formatting, Clippy, warning-free rustdoc, Rust 1.96
checks, SQLx offline metadata validation, and packaged consumer checks. Review before
publishing. Verify the registry artifact version and transactional API in a consumer
before engine changes. Do not expose credentials or alter unrelated local changes.
