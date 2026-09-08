# Duovium

Functional CQRS and event-sourcing primitives for Rust: aggregates decide events,
an executor handles optimistic retries, and stores persist event streams. An
optional PostgreSQL adapter includes a transactional projection runner.

Rust **1.96 or newer**. The PostgreSQL adapter is tested against PostgreSQL 15.
Default features require neither a database nor a production async runtime.

## Getting started

```toml
[dependencies]
duovium = "0.1"
```

The domain model needs no serialization implementation:

```rust
use std::convert::Infallible;
use duovium::{Aggregate, Event, Executor, InMemoryEventStore, Metadata, Proposed};

#[derive(Debug, Clone)]
struct Added(i64);
impl Event for Added {
    fn name(&self) -> &'static str { "counter.added" }
    fn version(&self) -> u32 { 1 }
}

#[derive(Debug)]
struct Counter;
impl Aggregate for Counter {
    type Id = String;
    type Command = i64;
    type Event = Added;
    type Error = Infallible;
    type State = i64;
    fn initial_state(&self) -> i64 { 0 }
    fn apply(&self, state: i64, event: &Added) -> i64 { state + event.0 }
    fn handle(&self, _: &i64, amount: i64) -> Result<Vec<Proposed<Added>>, Infallible> {
        Ok(vec![Proposed { event: Added(amount), metadata: Metadata::new() }])
    }
}

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let executor = Executor::new(Counter, InMemoryEventStore::new(), 3)?;
let id = "visits".to_owned();
executor.execute(&id, 4, Metadata::new()).await?;
let (state, version) = executor.load(&id).await?;
assert_eq!((state, version.get()), (4, 1));
# Ok(())
# }
```

Run the complete example with `cargo run --example counter`.

## Contracts

- `Aggregate::initial_state`, `apply`, and `handle` are deterministic functions.
  A conflict can cause `handle` to run again. Put timestamps and other external
  decision inputs in commands; do not perform external side effects in the aggregate.
- `EventStore::read` returns a complete, ordered committed stream. Append batches
  are atomic and receive consecutive versions. Empty batches still check the
  expectation. A missing stream has version zero, but `Exact(0)` does not create it.
- `ExpectedVersion::Any` selects the current version; concurrent writes can still
  produce a conflict. The executor retries optimistic conflicts, up to its positive
  `max_attempts`. It does not blindly retry infrastructure errors.
- A connection failure during commit can mean the write succeeded. Request
  deduplication is an application concern; correlation IDs are not idempotency keys.
- `Executor::load` rehydrates authoritative event-store state. Projections can lag.
  Every load/command reads the full history; snapshots are not provided.
- `Commands<A>` is the object-safe dispatch port. Reads return the store's error directly. Append/execution errors implement the
  standard error traits when their contained errors support them.

## PostgreSQL

Enable `duovium = { version = "0.1", features = ["postgres"] }`. This adapter uses
SQLx 0.9 and Tokio. Its SQLx offline metadata is included in the package: consumers
can compile it without a database. Set `SQLX_OFFLINE=true` if your build environment
defines an unrelated `DATABASE_URL`.

Run migrations at application startup before constructing stores and runners:

```rust,no_run
# #[cfg(feature = "postgres")]
# async fn migrate(pool: &sqlx::PgPool) -> Result<(), sqlx::migrate::MigrateError> {
duovium::MIGRATOR.run(pool).await?;
# Ok(())
# }
```

The migrator owns `events`, `projection_checkpoints`, and its separate migration
history table `_sqlx_migrations_cqrs`. That historical table name is retained for
compatibility. Run application migrations with the application's own migrator.
The checkpoint revision is introduced by an additive migration; older migration
checksums remain unchanged. Stop old runner versions before upgrading: they do
not participate in revision guards, so mixed-version operation is unsupported.

Implement `StreamId` for a domain ID type: its `stream_type` must be globally unique,
`to_key` injective, and `from_key` its inverse. `PostgresEventStore::<Id, Event>::new(pool)`
uses `JsonEventCodec`, which requires Serde only on the stored event type.

### Event evolution

`EventCodec<E>` encodes current events and decodes stored `(name, version, payload)`.
Supply the same decoding policy to:

- `PostgresEventStore::with_codec(pool, codec)`
- `EventFeed::with_codec(codec)`
- `ProjectionRunner::with_codec(projection, pool, config, codec)`

The default `JsonEventCodec` reads self-describing JSON as-is and does not interpret
name/version. For evolving schemas, implement a codec that matches those fields,
upcasts old payloads, and rejects unsupported versions. Store and feed both invoke
that codec. Writes check that encoded payloads can be decoded before insertion;
this guarantees readability, not value equality for lossy encodings.

JSON cannot represent non-finite numbers. Non-finite metadata floats are rejected.
JSONB can normalize integral floating-point metadata into the integer variant;
do not depend on numeric variant identity across persistence.

### Projections and supervision

Implement `Projection` with `Connection = sqlx::PgConnection`. `apply` must write
through the supplied connection so its changes commit with the checkpoint.
On failure those database effects roll back; external effects and local mutable
state do not. Additive SQL updates are supported without per-event deduplication.
`reset` must clear the state owned by that projection before replay.

`ProjectionRunner::new` and `with_codec` reject nonpositive batch/failure counts,
zero polling/backoff durations, and an initial backoff above its ceiling.

Call `register` before manually calling `tick`, `resume`, or `rebuild`. `run` registers
automatically. Manual ticks report `Applied`, `CaughtUp`, `Idle`, or `Halted`.
`Idle` can also mean another runner owns the checkpoint lock. `CaughtUp` means
there is nothing ready below the feed watermark, not necessarily no newer events.

Decode errors and non-retryable projection errors consume the failure budget.
Override `Projection::is_retryable` for database failures returned from `apply`;
the default treats projection errors as event failures. Feed and checkpoint
infrastructure errors never consume the budget. A checkpoint revision guards
delayed failure updates after success, resume, or rebuild.

`run_with_observer` reports each tick result to a synchronous callback. Use it for
logging, metrics, and supervision without a prescribed telemetry dependency.
Both run methods return `RunnerError::Halted` when halted; fix the cause, call
`resume`, and start a new run. Infrastructure errors retry with capped exponential
backoff after registration; an initial registration failure returns immediately to
the caller. The observer runs outside the tick transaction and must not block or panic.
If recording an event failure also fails, `FailureRecording` retains both errors.

`rebuild` atomically resets the read model and rewinds the cursor; subsequent ticks
replay the events. It waits for an active tick and requires registration.
Cancellation drops open transactions, but a commit already in flight can still
succeed. Database effects and checkpoint progress remain atomic.

The feed orders by `(xact_id, global_seq)` below PostgreSQL's snapshot xmin to avoid
skipping late commits. An older open transaction elsewhere on the server can stall
all projections. Keep transactions short and monitor projection progress.

## Development and release checks

```sh
cargo test --locked
cargo fmt --check
SQLX_OFFLINE=true cargo clippy --locked --all-features --all-targets -- -D warnings
cargo run --locked --example counter
```

For database tests, provide `DATABASE_URL` for a disposable PostgreSQL server whose
user can create databases, then run:

```sh
SQLX_OFFLINE=true cargo test --locked --features postgres-tests -- --test-threads=1
```

Tests use separate databases. Run them serially because the feed watermark is
server-wide. To change SQL queries, apply all migrations to a separate development
database and regenerate the cache with SQLx CLI 0.9:

```sh
SQLX_OFFLINE=false cargo sqlx prepare -- --all-features --all-targets
SQLX_OFFLINE=false cargo sqlx prepare --check -- --all-features --all-targets
```

Commit the updated `.sqlx` files. CI checks the cache against a migrated schema,
runs the database suite, and builds the package offline.

## License

Licensed under the [MIT license](LICENSE). Copyright (c) 2026 h4vismat.

Repository: [h4vismat/duovium](https://github.com/h4vismat/duovium).
