//! Run against a disposable database:
//! DATABASE_URL=postgres://localhost/duovium cargo run --features postgres --example transactional_outbox
use std::convert::Infallible;

use duovium::{
    Aggregate, Event, ExpectedVersion, InvalidStreamId, Metadata, PostgresEventStore, Proposed,
    StreamId,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct CounterId(String);
impl StreamId for CounterId {
    fn stream_type() -> &'static str {
        "example.counter"
    }
    fn to_key(&self) -> String {
        self.0.clone()
    }
    fn from_key(key: &str) -> Result<Self, InvalidStreamId> {
        Ok(Self(key.into()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Added(i64);
impl Event for Added {
    fn name(&self) -> &'static str {
        "example.counter.added"
    }
    fn version(&self) -> u32 {
        1
    }
}

#[derive(Debug)]
struct Counter;
impl Aggregate for Counter {
    type Id = CounterId;
    type State = i64;
    type Command = i64;
    type Event = Added;
    type Error = Infallible;

    fn initial_state(&self) -> i64 {
        0
    }
    fn apply(&self, state: i64, event: &Added) -> i64 {
        state + event.0
    }
    fn handle(&self, _: &i64, amount: i64) -> Result<Vec<Proposed<Added>>, Infallible> {
        Ok(vec![Proposed {
            event: Added(amount),
            metadata: Metadata::new(),
        }])
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    duovium::MIGRATOR.run(&pool).await?;
    // Application-owned schema; duovium does not own delivery or request receipts.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS example_outbox (id UUID PRIMARY KEY, payload JSONB NOT NULL)",
    )
    .execute(&pool)
    .await?;

    let store = PostgresEventStore::<CounterId, Added>::new(pool.clone());
    let id = CounterId(Uuid::now_v7().to_string());
    let mut transaction = pool.begin().await?;
    let history = store.read_in(&mut transaction, &id).await?;
    let expected = history.last().map_or(ExpectedVersion::NoStream, |e| {
        ExpectedVersion::Exact(e.version)
    });
    let state = Counter.rehydrate(history.into_iter().map(|e| e.event));
    let proposed = Counter.handle(&state, 4)?;
    let provisional = store
        .append_in(&mut transaction, &id, expected, proposed)
        .await?;

    sqlx::query("INSERT INTO example_outbox (id, payload) VALUES ($1, $2)")
        .bind(Uuid::now_v7())
        .bind(serde_json::json!({"counter_id": id.0, "version": provisional.get()}))
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    println!(
        "Committed counter {} at version {} with an outbox entry",
        id.0,
        provisional.get()
    );

    // A separate application worker delivers committed outbox entries. It must
    // tolerate duplicate delivery and reconcile uncertain external outcomes.
    // On a version conflict, retry the whole decision with fresh history; on
    // an unknown commit outcome, resolve a durable request receipt before retrying.
    Ok(())
}
