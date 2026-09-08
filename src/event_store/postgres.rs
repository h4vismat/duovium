//! The durable event store.
//!
//! One table holds every stream. `StreamId` supplies the two columns that
//! address one: the aggregate a stream belongs to, and the key inside it.

use std::marker::PhantomData;

use chrono::Utc;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::{
    AppendError, CausationId, CorrelationId, Envelope, Event, EventCodec, EventId, EventStore,
    ExpectedVersion, JsonEventCodec, Metadata, Proposed, StoreError, StreamId, Value, Version,
};

/// The constraint name the migration gives the version index. Postgres reports
/// it in a 23505, which is how a lost race is told from any other unique
/// violation on the table.
const STREAM_VERSION_CONSTRAINT: &str = "events_stream_version_key";

fn store_failure(error: sqlx::Error) -> AppendError<StoreError> {
    AppendError::Store(StoreError::from(error))
}

/// Reads the current version of a stream through a caller-supplied connection.
/// `Exact` and `Any` both need it before the write, on the same connection as
/// the write that follows: reusing the transaction's connection needs no
/// second pool checkout, and it would matter under an isolation level
/// stricter than the default `READ COMMITTED`. It is not what makes the
/// pre-read safe under the default level — each statement there takes its own
/// snapshot, so the window between this read and the insert stays genuinely
/// open, and the unique constraint is what closes it.
async fn current_version_in(
    connection: &mut PgConnection,
    stream_type: &str,
    key: &str,
) -> Result<Option<Version>, StoreError> {
    let max_version = sqlx::query_scalar!(
        r#"SELECT MAX(version) AS "max?" FROM events WHERE stream_type = $1 AND stream_id = $2"#,
        stream_type,
        key,
    )
    .fetch_one(connection)
    .await
    .map_err(StoreError::from)?;

    max_version.map(to_version).transpose()
}

#[derive(Debug)]
pub struct PostgresEventStore<ID, E, C = JsonEventCodec> {
    codec: C,
    pool: PgPool,
    /// `fn() -> (ID, E)` keeps the struct covariant in both parameters and
    /// borrows no auto-trait requirement from them.
    _marker: PhantomData<fn() -> (ID, E)>,
}

impl<ID, E> PostgresEventStore<ID, E> {
    pub const fn new(pool: PgPool) -> Self {
        Self::with_codec(pool, JsonEventCodec)
    }
}

impl<ID, E, C> PostgresEventStore<ID, E, C> {
    pub const fn with_codec(pool: PgPool, codec: C) -> Self {
        Self {
            pool,
            codec,
            _marker: PhantomData,
        }
    }

    /// Reads the current version on a fresh connection. Called after a failed
    /// transaction has already rolled back, so it cannot reuse that
    /// transaction's connection.
    async fn current_version(
        &self,
        stream_type: &str,
        key: &str,
    ) -> Result<Option<Version>, StoreError> {
        let mut connection = self.pool.acquire().await.map_err(StoreError::from)?;

        current_version_in(&mut connection, stream_type, key).await
    }

    /// Inspects the error by reference and consumes it only on the
    /// non-conflict path: `sqlx::Error` is neither `Clone` nor convertible
    /// from a reference, so it cannot be classified by converting it first.
    async fn classify_insert_failure(
        &self,
        error: sqlx::Error,
        expected: ExpectedVersion,
        stream_type: &str,
        key: &str,
    ) -> AppendError<StoreError> {
        let lost_the_version = error
            .as_database_error()
            .and_then(|database_error| database_error.constraint())
            .is_some_and(|constraint| constraint == STREAM_VERSION_CONSTRAINT);

        if !lost_the_version {
            return AppendError::Store(StoreError::from(error));
        }

        match self.current_version(stream_type, key).await {
            Ok(actual) => AppendError::Conflict { expected, actual },
            Err(error) => AppendError::Store(error),
        }
    }
}

/// A stored version is a `BIGINT`, so the conversion back can fail. It never
/// silently truncates.
fn to_version(stored: i64) -> Result<Version, StoreError> {
    u64::try_from(stored)
        .map(Version::new)
        .map_err(|_| StoreError::Other(format!("stored version {stored} is negative")))
}

impl<ID, E, C> EventStore<ID, E> for PostgresEventStore<ID, E, C>
where
    ID: StreamId + Clone,
    E: Event,
    C: EventCodec<E>,
{
    type Error = StoreError;

    async fn read(&self, stream_id: &ID) -> Result<Vec<Envelope<ID, E>>, Self::Error> {
        let rows = sqlx::query!(
            r#"
            SELECT event_id, version, event_name, event_version, payload,
                   correlation_id, causation_id, metadata, recorded_at
              FROM events
             WHERE stream_type = $1 AND stream_id = $2
             ORDER BY version ASC
            "#,
            ID::stream_type(),
            stream_id.to_key(),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::from)?;

        rows.into_iter()
            .map(|row| -> Result<Envelope<ID, E>, StoreError> {
                Ok(Envelope {
                    event_id: EventId::from_uuid(row.event_id),
                    // Cloned from the argument rather than parsed back out of the
                    // column: the caller already holds the id this read is for.
                    stream_id: stream_id.clone(),
                    version: to_version(row.version)?,
                    event: self
                        .codec
                        .decode(
                            &row.event_name,
                            u32::try_from(row.event_version).map_err(|_| {
                                StoreError::Other("negative event schema version".into())
                            })?,
                            row.payload,
                        )
                        .map_err(|error| {
                            StoreError::Other(format!("stored event does not decode: {error}"))
                        })?,
                    recorded_at: row.recorded_at,
                    metadata: Metadata::from_parts(
                        row.correlation_id.map(CorrelationId::from_uuid),
                        row.causation_id.map(CausationId::from_uuid),
                        serde_json::from_value(row.metadata).map_err(|error| {
                            StoreError::Other(format!("stored metadata does not decode: {error}"))
                        })?,
                    ),
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()
    }

    async fn append(
        &self,
        stream_id: &ID,
        expected_version: ExpectedVersion,
        events: Vec<Proposed<E>>,
    ) -> Result<Version, AppendError<Self::Error>> {
        let stream_type = ID::stream_type();
        let key = stream_id.to_key();

        // An empty batch inserts nothing, so the unique constraint that
        // detects a lost race never fires. Without this guard `NoStream`
        // against an existing stream would commit and report `Version::new(0)`
        // — a version the migration's `CHECK (version > 0)` guarantees never
        // exists in storage. Resolve the same way `InMemoryEventStore` does,
        // and open no transaction: there is nothing to write or roll back.
        if events.is_empty() {
            let current = self
                .current_version(stream_type, &key)
                .await
                .map_err(AppendError::Store)?;

            return match expected_version {
                ExpectedVersion::Any => Ok(current.unwrap_or(Version::new(0))),
                ExpectedVersion::NoStream if current.is_none() => Ok(Version::new(0)),
                ExpectedVersion::Exact(expected) if current == Some(expected) => Ok(expected),
                _ => Err(AppendError::Conflict {
                    expected: expected_version,
                    actual: current,
                }),
            };
        }

        let mut transaction = self.pool.begin().await.map_err(store_failure)?;

        // `NoStream` needs no read: a non-empty stream already holds version 1,
        // so the unique constraint rejects the insert. `Exact` and `Any` both
        // need the current version, for different reasons.
        let current = match expected_version {
            ExpectedVersion::NoStream => None,
            ExpectedVersion::Exact(_) | ExpectedVersion::Any => {
                current_version_in(&mut transaction, stream_type, &key)
                    .await
                    .map_err(AppendError::Store)?
            }
        };

        let base = match expected_version {
            ExpectedVersion::NoStream => Version::new(0),
            ExpectedVersion::Any => current.unwrap_or(Version::new(0)),
            // A stream at version 3 that is handed `Exact(5)` would write
            // version 6, colliding with nothing and leaving a gap. The
            // constraint cannot catch a stale caller, only a concurrent one.
            ExpectedVersion::Exact(expected) if current == Some(expected) => expected,
            ExpectedVersion::Exact(_) => {
                let _ = transaction.rollback().await;

                return Err(AppendError::Conflict {
                    expected: expected_version,
                    actual: current,
                });
            }
        };

        let recorded_at = Utc::now();
        let mut version = base;

        for proposed in events {
            version = version.next();

            let stored_version = i64::try_from(version.get()).map_err(|_| {
                AppendError::Store(StoreError::Other(format!(
                    "version {} exceeds the stored range",
                    version.get()
                )))
            })?;
            let event_version = i32::try_from(proposed.event.version()).map_err(|_| {
                AppendError::Store(StoreError::Other(format!(
                    "event version {} exceeds the stored range",
                    proposed.event.version()
                )))
            })?;
            let payload = self.codec.encode(&proposed.event).map_err(|error| {
                AppendError::Store(StoreError::Other(format!("event does not encode: {error}")))
            })?;

            // Validate readability using the same codec as reads. This catches
            // encodings that cannot be decoded (including a required f64 encoded
            // as null). It does not prove value equality: lossy serializers and
            // JSONB numeric normalization remain the codec/domain's responsibility.
            if let Err(error) = self.codec.decode(
                proposed.event.name(),
                proposed.event.version(),
                payload.clone(),
            ) {
                return Err(AppendError::Store(StoreError::Other(format!(
                    "event `{}` serializes to a payload that cannot be read back: {error}",
                    proposed.event.name()
                ))));
            }

            // `serde_json` turns a non-finite float into `Value::Null` instead
            // of returning an error, and `Value` is `#[serde(untagged)]` with
            // no null-shaped variant. Left unchecked, the insert below would
            // succeed and store `null`, and every future `read` of this
            // stream would then fail to decode it: the whole stream becomes
            // unreadable, not just this event. Reject the write instead.
            if let Some((key, _)) = proposed
                .metadata
                .extra()
                .iter()
                .find(|(_, value)| matches!(value, Value::F64(number) if !number.is_finite()))
            {
                return Err(AppendError::Store(StoreError::Other(format!(
                    "metadata entry `{key}` is a non-finite float, which JSON cannot represent"
                ))));
            }

            let metadata = serde_json::to_value(proposed.metadata.extra()).map_err(|error| {
                AppendError::Store(StoreError::Other(format!(
                    "metadata does not encode: {error}"
                )))
            })?;

            let written = sqlx::query!(
                r#"
                INSERT INTO events (
                    event_id, stream_type, stream_id, version,
                    event_name, event_version, payload,
                    correlation_id, causation_id, metadata, recorded_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                "#,
                Uuid::now_v7(),
                stream_type,
                key,
                stored_version,
                proposed.event.name(),
                event_version,
                payload,
                proposed.metadata.correlation_id.map(|id| id.as_uuid()),
                proposed.metadata.causation_id.map(|id| id.as_uuid()),
                metadata,
                recorded_at,
            )
            .execute(&mut *transaction)
            .await;

            if let Err(error) = written {
                // Rolled back explicitly, and before the report is assembled:
                // reading the current version needs a connection of its own.
                let _ = transaction.rollback().await;

                return Err(self
                    .classify_insert_failure(error, expected_version, stream_type, &key)
                    .await);
            }
        }

        transaction.commit().await.map_err(store_failure)?;

        Ok(version)
    }
}

#[cfg(all(test, feature = "postgres-tests"))]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::PostgresEventStore;
    use crate::{
        AppendError, CausationId, CorrelationId, Event, EventStore, ExpectedVersion,
        InvalidStreamId, Metadata, Proposed, StoreError, StreamId, Value, Version,
    };

    /// Two variants, so a test can tell the `event_name` column apart.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum TestEvent {
        Added(i64),
        Removed { reason: String },
    }

    impl Event for TestEvent {
        fn name(&self) -> &'static str {
            match self {
                Self::Added(_) => "test.added",
                Self::Removed { .. } => "test.removed",
            }
        }

        fn version(&self) -> u32 {
            1
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestStreamId(String);

    impl StreamId for TestStreamId {
        fn stream_type() -> &'static str {
            "test"
        }

        fn to_key(&self) -> String {
            self.0.clone()
        }

        fn from_key(key: &str) -> Result<Self, InvalidStreamId> {
            Ok(Self(key.to_owned()))
        }
    }

    /// Carries a non-`Option` `f64`, which `TestEvent` deliberately does not
    /// have. Kept separate from `TestEvent` so its 30 existing tests stay
    /// exactly as shaped.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum MeasuredEvent {
        Rate(f64),
    }

    impl Event for MeasuredEvent {
        fn name(&self) -> &'static str {
            match self {
                Self::Rate(_) => "measured.rate",
            }
        }

        fn version(&self) -> u32 {
            1
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MeasuredStreamId(String);

    impl StreamId for MeasuredStreamId {
        fn stream_type() -> &'static str {
            "measured"
        }

        fn to_key(&self) -> String {
            self.0.clone()
        }

        fn from_key(key: &str) -> Result<Self, InvalidStreamId> {
            Ok(Self(key.to_owned()))
        }
    }

    /// Inserts one row with raw SQL. The schema is exercised on its own here,
    /// before any store code exists to exercise it.
    async fn insert_row(
        pool: &PgPool,
        event_id: Uuid,
        stream_type: &str,
        stream_id: &str,
        version: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version,
                 event_name, event_version, payload, recorded_at
             ) VALUES ($1, $2, $3, $4, 'test.added', 1, '{}'::jsonb, now())",
        )
        .bind(event_id)
        .bind(stream_type)
        .bind(stream_id)
        .bind(version)
        .execute(pool)
        .await
        .map(|_| ())
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn one_version_per_stream_is_enforced(pool: PgPool) {
        insert_row(&pool, Uuid::now_v7(), "test", "a", 1)
            .await
            .expect("the first row is accepted");

        let clash = insert_row(&pool, Uuid::now_v7(), "test", "a", 1).await;

        let error = clash.expect_err("the second row collides");
        let database_error = error.as_database_error().expect("a database error");
        assert_eq!(database_error.code().as_deref(), Some("23505"));
        assert_eq!(
            database_error.constraint(),
            Some("events_stream_version_key")
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn one_key_under_two_stream_types_is_two_streams(pool: PgPool) {
        insert_row(&pool, Uuid::now_v7(), "test", "a", 1)
            .await
            .expect("the first stream type is accepted");

        let other = insert_row(&pool, Uuid::now_v7(), "other", "a", 1).await;

        assert!(other.is_ok());
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_event_id_is_unique_across_streams(pool: PgPool) {
        let shared = Uuid::now_v7();
        insert_row(&pool, shared, "test", "a", 1)
            .await
            .expect("the first row is accepted");

        let clash = insert_row(&pool, shared, "test", "b", 1).await;

        let error = clash.expect_err("the reused event id collides");
        let database_error = error.as_database_error().expect("a database error");
        assert_eq!(database_error.constraint(), Some("events_event_id_key"));
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn version_zero_is_rejected(pool: PgPool) {
        let invalid = insert_row(&pool, Uuid::now_v7(), "test", "a", 0).await;

        let error = invalid.expect_err("version 0 breaks the check constraint");
        let database_error = error.as_database_error().expect("a database error");
        assert_eq!(database_error.code().as_deref(), Some("23514"));
    }

    /// Writes one row carrying a real payload, so a read test does not depend on
    /// the append path it is meant to be independent of.
    async fn insert_event(pool: &PgPool, stream: &TestStreamId, version: i64, event: &TestEvent) {
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version,
                 event_name, event_version, payload, recorded_at
             ) VALUES ($1, 'test', $2, $3, $4, 1, $5, now())",
        )
        .bind(Uuid::now_v7())
        .bind(stream.to_key())
        .bind(version)
        .bind(event.name())
        .bind(serde_json::to_value(event).expect("the test event serializes"))
        .execute(pool)
        .await
        .expect("the row is accepted");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn unknown_stream_reads_as_empty(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);

        let envelopes = store
            .read(&TestStreamId("missing".to_owned()))
            .await
            .expect("an absent stream is not a failure");

        assert!(envelopes.is_empty());
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn rows_are_returned_in_version_order(pool: PgPool) {
        let stream = TestStreamId("ordered".to_owned());
        insert_event(&pool, &stream, 2, &TestEvent::Added(20)).await;
        insert_event(&pool, &stream, 1, &TestEvent::Added(10)).await;
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);

        let envelopes = store.read(&stream).await.expect("the stream reads");

        assert_eq!(
            envelopes
                .iter()
                .map(|envelope| envelope.version)
                .collect::<Vec<_>>(),
            vec![Version::new(1), Version::new(2)]
        );
        assert_eq!(envelopes[0].event, TestEvent::Added(10));
        assert_eq!(envelopes[1].event, TestEvent::Added(20));
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_row_becomes_a_complete_envelope(pool: PgPool) {
        let stream = TestStreamId("full".to_owned());
        let event_id = Uuid::now_v7();
        let correlation_id = Uuid::from_u128(12);
        let causation_id = Uuid::from_u128(13);
        let recorded_at: DateTime<Utc> = Utc.with_ymd_and_hms(2026, 8, 20, 12, 30, 0).unwrap();
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version, event_name, event_version,
                 payload, correlation_id, causation_id, metadata, recorded_at
             ) VALUES ($1, 'test', $2, 1, 'test.added', 1, $3, $4, $5, $6, $7)",
        )
        .bind(event_id)
        .bind(stream.to_key())
        .bind(serde_json::to_value(TestEvent::Added(7)).expect("the event serializes"))
        .bind(correlation_id)
        .bind(causation_id)
        .bind(serde_json::json!({"source": "payments", "attempt": 2}))
        .bind(recorded_at)
        .execute(&pool)
        .await
        .expect("the row is accepted");
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);

        let envelopes = store.read(&stream).await.expect("the stream reads");

        let envelope = &envelopes[0];
        assert_eq!(envelope.event_id.as_uuid(), event_id);
        assert_eq!(envelope.stream_id, stream);
        assert_eq!(envelope.version, Version::new(1));
        assert_eq!(envelope.event, TestEvent::Added(7));
        assert_eq!(envelope.recorded_at, recorded_at);
        assert_eq!(
            envelope.metadata.correlation_id,
            Some(CorrelationId::from_uuid(correlation_id))
        );
        assert_eq!(
            envelope.metadata.causation_id,
            Some(CausationId::from_uuid(causation_id))
        );
        assert_eq!(
            envelope.metadata.get("source"),
            Some(&Value::String("payments".to_owned()))
        );
        assert_eq!(envelope.metadata.get("attempt"), Some(&Value::I64(2)));
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_row_with_no_metadata_reads_as_empty_metadata(pool: PgPool) {
        let stream = TestStreamId("bare".to_owned());
        insert_event(&pool, &stream, 1, &TestEvent::Added(1)).await;
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);

        let envelopes = store.read(&stream).await.expect("the stream reads");

        assert_eq!(envelopes[0].metadata, Metadata::new());
    }

    /// A row the store cannot interpret is a failure it reports, not a crash.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_undecodable_payload_is_reported_as_an_error(pool: PgPool) {
        let stream = TestStreamId("broken".to_owned());
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version,
                 event_name, event_version, payload, recorded_at
             ) VALUES ($1, 'test', $2, 1, 'test.unknown', 1, $3, now())",
        )
        .bind(Uuid::now_v7())
        .bind(stream.to_key())
        .bind(serde_json::json!({"Unknown": true}))
        .execute(&pool)
        .await
        .expect("the row is accepted");
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);

        let result = store.read(&stream).await;

        match result {
            Err(StoreError::Other(message)) => assert!(
                message.contains("stored event does not decode"),
                "expected a decode failure, got: {message}"
            ),
            other => panic!("expected a decode failure, got {other:?}"),
        }
    }

    fn proposed(event: TestEvent) -> Vec<Proposed<TestEvent>> {
        vec![Proposed {
            event,
            metadata: Metadata::new(),
        }]
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_first_append_starts_the_stream_at_version_one(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("fresh".to_owned());

        let version = store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(3)),
            )
            .await
            .expect("an empty stream accepts NoStream");

        let envelopes = store.read(&stream).await.expect("the stream reads");
        assert_eq!(version, Version::new(1));
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].version, Version::new(1));
        assert_eq!(envelopes[0].event, TestEvent::Added(3));
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_batch_takes_consecutive_versions_and_one_timestamp(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("batch".to_owned());

        let version = store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![
                    Proposed {
                        event: TestEvent::Added(1),
                        metadata: Metadata::new(),
                    },
                    Proposed {
                        event: TestEvent::Removed {
                            reason: "refunded".to_owned(),
                        },
                        metadata: Metadata::new(),
                    },
                ],
            )
            .await
            .expect("the batch is accepted");

        let envelopes = store.read(&stream).await.expect("the stream reads");
        assert_eq!(version, Version::new(2));
        assert_eq!(
            envelopes
                .iter()
                .map(|envelope| envelope.version)
                .collect::<Vec<_>>(),
            vec![Version::new(1), Version::new(2)]
        );
        // One append records one decision, so its events share a timestamp.
        assert_eq!(envelopes[0].recorded_at, envelopes[1].recorded_at);
        assert_ne!(envelopes[0].event_id, envelopes[1].event_id);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_appended_event_carries_its_name_and_its_version(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool.clone());
        let stream = TestStreamId("named".to_owned());

        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![
                    Proposed {
                        event: TestEvent::Added(1),
                        metadata: Metadata::new(),
                    },
                    Proposed {
                        event: TestEvent::Removed {
                            reason: "refunded".to_owned(),
                        },
                        metadata: Metadata::new(),
                    },
                ],
            )
            .await
            .expect("the batch is accepted");

        let rows: Vec<(String, i32)> = sqlx::query_as(
            "SELECT event_name, event_version FROM events
              WHERE stream_type = 'test' AND stream_id = $1 ORDER BY version",
        )
        .bind(stream.to_key())
        .fetch_all(&pool)
        .await
        .expect("the rows read back");

        assert_eq!(
            rows,
            vec![("test.added".to_owned(), 1), ("test.removed".to_owned(), 1),]
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_event_id_is_a_version_seven_uuid(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("identified".to_owned());
        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .expect("the append is accepted");

        let envelopes = store.read(&stream).await.expect("the stream reads");

        assert_eq!(envelopes[0].event_id.as_uuid().get_version_num(), 7);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn metadata_survives_the_round_trip(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("traced".to_owned());
        let correlation_id = CorrelationId::from_uuid(Uuid::from_u128(31));
        let causation_id = CausationId::from_uuid(Uuid::from_u128(32));
        let mut metadata = Metadata::new();
        metadata.correlation_id = Some(correlation_id);
        metadata.causation_id = Some(causation_id);
        metadata.insert("source", Value::String("onramp".to_owned()));
        metadata.insert("attempt", Value::I64(2));
        metadata.insert("replay", Value::Bool(true));
        // Finite, not large-integral: `Value::F64` is not variant-faithful
        // through `jsonb` for a large integral value, so that case is
        // documented on `Value` rather than asserted here.
        metadata.insert("ratio", Value::F64(1.5));

        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![Proposed {
                    event: TestEvent::Added(1),
                    metadata,
                }],
            )
            .await
            .expect("the append is accepted");

        let envelopes = store.read(&stream).await.expect("the stream reads");
        let stored = &envelopes[0].metadata;
        assert_eq!(stored.correlation_id, Some(correlation_id));
        assert_eq!(stored.causation_id, Some(causation_id));
        assert_eq!(
            stored.get("source"),
            Some(&Value::String("onramp".to_owned()))
        );
        assert_eq!(stored.get("attempt"), Some(&Value::I64(2)));
        assert_eq!(stored.get("replay"), Some(&Value::Bool(true)));
        assert_eq!(stored.get("ratio"), Some(&Value::F64(1.5)));
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_exact_append_continues_the_version_sequence(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("continued".to_owned());
        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(4)),
            )
            .await
            .expect("the first append is accepted");

        let version = store
            .append(
                &stream,
                ExpectedVersion::Exact(Version::new(1)),
                proposed(TestEvent::Added(6)),
            )
            .await
            .expect("the expected version is current");

        let envelopes = store.read(&stream).await.expect("the stream reads");
        assert_eq!(version, Version::new(2));
        assert_eq!(
            envelopes
                .iter()
                .map(|envelope| envelope.event.clone())
                .collect::<Vec<_>>(),
            vec![TestEvent::Added(4), TestEvent::Added(6)]
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn any_version_bypasses_the_optimistic_check(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("unchecked".to_owned());
        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .expect("the first append is accepted");

        let version = store
            .append(&stream, ExpectedVersion::Any, proposed(TestEvent::Added(2)))
            .await
            .expect("Any never conflicts on a stale expectation");

        assert_eq!(version, Version::new(2));
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn any_version_starts_an_empty_stream_at_one(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("unchecked-fresh".to_owned());

        let version = store
            .append(&stream, ExpectedVersion::Any, proposed(TestEvent::Added(1)))
            .await
            .expect("Any accepts an empty stream");

        assert_eq!(version, Version::new(1));
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn two_streams_of_one_type_stay_separate(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let first = TestStreamId("first".to_owned());
        let second = TestStreamId("second".to_owned());

        store
            .append(
                &first,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .expect("the first stream is accepted");
        store
            .append(
                &second,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(9)),
            )
            .await
            .expect("the second stream is accepted");

        let first_events = store.read(&first).await.expect("the first stream reads");
        let second_events = store.read(&second).await.expect("the second stream reads");
        assert_eq!(first_events.len(), 1);
        assert_eq!(second_events.len(), 1);
        assert_eq!(second_events[0].version, Version::new(1));
        assert_eq!(second_events[0].event, TestEvent::Added(9));
    }

    /// A stand-in for a second aggregate. The store is generic over the id
    /// type, so this test tells `stream_type` apart from `TestStreamId`
    /// without depending on a real second domain.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct OtherStreamId(String);

    impl StreamId for OtherStreamId {
        fn stream_type() -> &'static str {
            "other"
        }

        fn to_key(&self) -> String {
            self.0.clone()
        }

        fn from_key(key: &str) -> Result<Self, InvalidStreamId> {
            Ok(Self(key.to_owned()))
        }
    }

    /// The whole reason `stream_type` exists: one key, two aggregates.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn one_key_under_two_stream_types_appends_independently(pool: PgPool) {
        let test_store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool.clone());
        let other_store = PostgresEventStore::<OtherStreamId, TestEvent>::new(pool);
        let shared = "shared-key".to_owned();

        test_store
            .append(
                &TestStreamId(shared.clone()),
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .expect("the test aggregate is accepted");
        let other_version = other_store
            .append(
                &OtherStreamId(shared.clone()),
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(2)),
            )
            .await
            .expect("the other aggregate is accepted at version 1 too");

        let other_events = other_store
            .read(&OtherStreamId(shared))
            .await
            .expect("the other stream reads");
        assert_eq!(other_version, Version::new(1));
        assert_eq!(other_events.len(), 1);
        assert_eq!(other_events[0].event, TestEvent::Added(2));
    }

    fn assert_conflict(
        result: Result<Version, AppendError<StoreError>>,
        expected_version: ExpectedVersion,
        actual_version: Option<Version>,
    ) {
        match result {
            Err(AppendError::Conflict { expected, actual }) => {
                assert_eq!(expected, expected_version);
                assert_eq!(actual, actual_version);
            }
            other => panic!("expected a conflict, got {other:?}"),
        }
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn no_stream_against_an_existing_stream_conflicts(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("taken".to_owned());
        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .expect("the first append is accepted");

        let result = store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(2)),
            )
            .await;

        assert_conflict(result, ExpectedVersion::NoStream, Some(Version::new(1)));
        assert_eq!(
            store.read(&stream).await.expect("the stream reads").len(),
            1
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_stale_exact_expectation_conflicts(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("stale".to_owned());
        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .expect("the first append is accepted");

        let result = store
            .append(
                &stream,
                ExpectedVersion::Exact(Version::new(7)),
                proposed(TestEvent::Added(2)),
            )
            .await;

        assert_conflict(
            result,
            ExpectedVersion::Exact(Version::new(7)),
            Some(Version::new(1)),
        );
        assert_eq!(
            store.read(&stream).await.expect("the stream reads").len(),
            1
        );
    }

    /// Without the pre-read this would write version 8 and leave a gap, so this
    /// test is the reason `Exact` reads the current version at all.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_exact_expectation_on_a_missing_stream_conflicts(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("absent".to_owned());

        let result = store
            .append(
                &stream,
                ExpectedVersion::Exact(Version::new(7)),
                proposed(TestEvent::Added(1)),
            )
            .await;

        assert_conflict(result, ExpectedVersion::Exact(Version::new(7)), None);
        assert!(
            store
                .read(&stream)
                .await
                .expect("the stream reads")
                .is_empty()
        );
    }

    /// A batch is one decision, so a collision on its second event has to undo
    /// its first.
    ///
    /// `NoStream` is the only expectation that can reach this state on demand.
    /// `Exact` and `Any` both base on `MAX(version)`, so every version they
    /// write is above every version that exists and nothing collides but a
    /// concurrent writer. `NoStream` skips the pre-read and always starts at
    /// version 1. Seed version 2 and leave version 1 free: the batch takes
    /// version 1, then collides on version 2.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_batch_that_collides_partway_writes_nothing(pool: PgPool) {
        let stream = TestStreamId("atomic".to_owned());
        insert_event(&pool, &stream, 2, &TestEvent::Added(99)).await;
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool.clone());

        let result = store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![
                    Proposed {
                        event: TestEvent::Added(1),
                        metadata: Metadata::new(),
                    },
                    Proposed {
                        event: TestEvent::Added(2),
                        metadata: Metadata::new(),
                    },
                ],
            )
            .await;

        assert_conflict(result, ExpectedVersion::NoStream, Some(Version::new(2)));
        // Version 1 was written and rolled back. Only the seed survives.
        let versions: Vec<i64> = sqlx::query_scalar(
            "SELECT version FROM events
              WHERE stream_type = 'test' AND stream_id = $1 ORDER BY version",
        )
        .bind(stream.to_key())
        .fetch_all(&pool)
        .await
        .expect("the versions read back");
        assert_eq!(versions, vec![2]);
    }

    /// Two connections, one empty stream, two callers that both believe they are
    /// creating it. Exactly one wins.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn concurrent_first_appends_have_one_winner(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("contended".to_owned());

        let (left, right) = tokio::join!(
            store.append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            ),
            store.append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(2)),
            ),
        );

        let winners = usize::from(left.is_ok()) + usize::from(right.is_ok());
        let conflicts = usize::from(matches!(left, Err(AppendError::Conflict { .. })))
            + usize::from(matches!(right, Err(AppendError::Conflict { .. })));
        assert_eq!(winners, 1, "left: {left:?}, right: {right:?}");
        assert_eq!(conflicts, 1, "left: {left:?}, right: {right:?}");
        assert_eq!(
            store.read(&stream).await.expect("the stream reads").len(),
            1
        );
    }

    /// Closes the SQLSTATE arm `store.rs` cannot reach without a real server.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_real_unique_violation_maps_to_the_store_error(pool: PgPool) {
        let shared = Uuid::now_v7();
        insert_row(&pool, shared, "test", "a", 1)
            .await
            .expect("the first row is accepted");

        let error = insert_row(&pool, shared, "test", "b", 1)
            .await
            .expect_err("the reused event id collides");

        match StoreError::from(error) {
            StoreError::UniqueViolation { constraint } => {
                assert_eq!(constraint, "events_event_id_key");
            }
            other => panic!("expected a unique violation, got {other:?}"),
        }
    }

    /// `current_version_in` resolves the base version with
    /// `WHERE stream_type = $1 AND stream_id = $2`. Drive the second of two
    /// same-type streams to version 2, then let `Any` resolve its base on the
    /// first: if `stream_id` were dropped from the predicate, `Any` would read
    /// the max across the whole stream type and jump straight to version 3.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn any_resolves_its_base_from_the_addressed_stream_only(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let first = TestStreamId("any-base-first".to_owned());
        let second = TestStreamId("any-base-second".to_owned());
        store
            .append(
                &second,
                ExpectedVersion::NoStream,
                vec![
                    Proposed {
                        event: TestEvent::Added(1),
                        metadata: Metadata::new(),
                    },
                    Proposed {
                        event: TestEvent::Added(2),
                        metadata: Metadata::new(),
                    },
                ],
            )
            .await
            .expect("the second stream reaches version 2");

        let version = store
            .append(&first, ExpectedVersion::Any, proposed(TestEvent::Added(9)))
            .await
            .expect("Any accepts an empty stream");

        assert_eq!(version, Version::new(1));
    }

    /// `current_version_in` resolves the base version with
    /// `WHERE stream_type = $1 AND stream_id = $2`. Drive a `TestStreamId`
    /// stream to version 2, then let `Any` resolve its base on an
    /// `OtherStreamId` stream with the same key: if `stream_type` were dropped
    /// from the predicate, `Any` would read the max across both stream types
    /// and jump straight to version 3.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn any_resolves_its_base_within_one_stream_type_only(pool: PgPool) {
        let test_store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool.clone());
        let other_store = PostgresEventStore::<OtherStreamId, TestEvent>::new(pool);
        let shared = "any-base-shared".to_owned();
        test_store
            .append(
                &TestStreamId(shared.clone()),
                ExpectedVersion::NoStream,
                vec![
                    Proposed {
                        event: TestEvent::Added(1),
                        metadata: Metadata::new(),
                    },
                    Proposed {
                        event: TestEvent::Added(2),
                        metadata: Metadata::new(),
                    },
                ],
            )
            .await
            .expect("the test-type stream reaches version 2");

        let other_version = other_store
            .append(
                &OtherStreamId(shared),
                ExpectedVersion::Any,
                proposed(TestEvent::Added(9)),
            )
            .await
            .expect("Any accepts an empty stream in the other stream type");

        assert_eq!(other_version, Version::new(1));
    }

    /// A non-finite float would otherwise serialize to `Value::Null`, which
    /// the untagged `Value` enum cannot read back. Left unchecked, this would
    /// corrupt the stream at write time and only surface as a read failure —
    /// and every subsequent read of the whole stream, not just this event.
    /// The failure has to happen here, before anything is written.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_non_finite_metadata_float_is_rejected_at_write_time(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("non-finite".to_owned());

        let mut nan_metadata = Metadata::new();
        nan_metadata.insert("ratio", Value::F64(f64::NAN));
        let nan_result = store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![Proposed {
                    event: TestEvent::Added(1),
                    metadata: nan_metadata,
                }],
            )
            .await;
        assert!(matches!(
            nan_result,
            Err(AppendError::Store(StoreError::Other(_)))
        ));

        let mut infinite_metadata = Metadata::new();
        infinite_metadata.insert("ratio", Value::F64(f64::INFINITY));
        let infinite_result = store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![Proposed {
                    event: TestEvent::Added(1),
                    metadata: infinite_metadata,
                }],
            )
            .await;
        assert!(matches!(
            infinite_result,
            Err(AppendError::Store(StoreError::Other(_)))
        ));

        assert!(
            store
                .read(&stream)
                .await
                .expect("the stream reads")
                .is_empty()
        );
    }

    /// `MeasuredEvent::Rate` holds a non-`Option` `f64`. Serialized, `NaN`
    /// becomes `Value::Null`; deserialized back, `null` cannot fill an `f64`
    /// field. The payload guard has to catch this before the insert, or the
    /// row would write and every future `read` of the stream would fail.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_payload_with_a_nan_field_is_rejected_at_write_time(pool: PgPool) {
        let store = PostgresEventStore::<MeasuredStreamId, MeasuredEvent>::new(pool);
        let stream = MeasuredStreamId("nan-rate".to_owned());

        let result = store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![Proposed {
                    event: MeasuredEvent::Rate(f64::NAN),
                    metadata: Metadata::new(),
                }],
            )
            .await;

        match result {
            Err(AppendError::Store(StoreError::Other(message))) => {
                assert!(
                    message.contains("measured.rate"),
                    "expected the event name in the message, got: {message}"
                );
            }
            other => panic!("expected a store error naming the event, got {other:?}"),
        }
        assert!(
            store
                .read(&stream)
                .await
                .expect("the stream reads")
                .is_empty()
        );
    }

    /// Same hazard as `f64::NAN`, through the other non-finite value
    /// `serde_json` also silently turns into `Value::Null`.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_payload_with_an_infinite_field_is_rejected_at_write_time(pool: PgPool) {
        let store = PostgresEventStore::<MeasuredStreamId, MeasuredEvent>::new(pool);
        let stream = MeasuredStreamId("infinite-rate".to_owned());

        let result = store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![Proposed {
                    event: MeasuredEvent::Rate(f64::INFINITY),
                    metadata: Metadata::new(),
                }],
            )
            .await;

        match result {
            Err(AppendError::Store(StoreError::Other(message))) => {
                assert!(
                    message.contains("measured.rate"),
                    "expected the event name in the message, got: {message}"
                );
            }
            other => panic!("expected a store error naming the event, got {other:?}"),
        }
        assert!(
            store
                .read(&stream)
                .await
                .expect("the stream reads")
                .is_empty()
        );
    }

    /// The regression guard: a finite value must not trip the new check. Proof
    /// that the guard rejects only what it is meant to.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_payload_with_a_finite_field_appends_and_reads_back(pool: PgPool) {
        let store = PostgresEventStore::<MeasuredStreamId, MeasuredEvent>::new(pool);
        let stream = MeasuredStreamId("finite-rate".to_owned());

        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![Proposed {
                    event: MeasuredEvent::Rate(2.5),
                    metadata: Metadata::new(),
                }],
            )
            .await
            .expect("a finite rate is accepted");

        let envelopes = store.read(&stream).await.expect("the stream reads");
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].event, MeasuredEvent::Rate(2.5));
    }

    /// A batch is one decision: a bad second event has to undo the first, the
    /// same guarantee `a_batch_that_collides_partway_writes_nothing` proves
    /// for a version collision.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_batch_with_a_bad_second_event_writes_nothing(pool: PgPool) {
        let store = PostgresEventStore::<MeasuredStreamId, MeasuredEvent>::new(pool);
        let stream = MeasuredStreamId("batch-bad-second".to_owned());

        let result = store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![
                    Proposed {
                        event: MeasuredEvent::Rate(1.0),
                        metadata: Metadata::new(),
                    },
                    Proposed {
                        event: MeasuredEvent::Rate(f64::NAN),
                        metadata: Metadata::new(),
                    },
                ],
            )
            .await;

        assert!(matches!(
            result,
            Err(AppendError::Store(StoreError::Other(_)))
        ));
        assert!(
            store
                .read(&stream)
                .await
                .expect("the stream reads")
                .is_empty()
        );
    }

    /// `InMemoryEventStore` conflicts here because an empty batch still has to
    /// satisfy `expected_version` against the stream's actual state: `NoStream`
    /// never matches a stream already at version 3. `PostgresEventStore` must
    /// answer the identical call the identical way, or the two implementations
    /// of one trait disagree.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_empty_batch_against_an_existing_stream_conflicts_like_the_in_memory_store(
        pool: PgPool,
    ) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("empty-batch".to_owned());
        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                vec![
                    Proposed {
                        event: TestEvent::Added(1),
                        metadata: Metadata::new(),
                    },
                    Proposed {
                        event: TestEvent::Added(2),
                        metadata: Metadata::new(),
                    },
                    Proposed {
                        event: TestEvent::Added(3),
                        metadata: Metadata::new(),
                    },
                ],
            )
            .await
            .expect("the stream reaches version 3");

        let result = store
            .append(&stream, ExpectedVersion::NoStream, Vec::new())
            .await;

        assert_conflict(result, ExpectedVersion::NoStream, Some(Version::new(3)));
        assert_eq!(
            store.read(&stream).await.expect("the stream reads").len(),
            3
        );
    }

    /// An empty batch whose expectation does match the stream's actual state
    /// commits nothing and reports the version unchanged — never
    /// `Version::new(0)` as a stored fact, only as "nothing has changed".
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_empty_batch_that_matches_the_expectation_is_a_no_op(pool: PgPool) {
        let store = PostgresEventStore::<TestStreamId, TestEvent>::new(pool);
        let stream = TestStreamId("empty-batch-any".to_owned());
        store
            .append(
                &stream,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .expect("the first append is accepted");

        let version = store
            .append(&stream, ExpectedVersion::Any, Vec::new())
            .await
            .expect("Any matches whatever the stream currently holds");

        assert_eq!(version, Version::new(1));
        assert_eq!(
            store.read(&stream).await.expect("the stream reads").len(),
            1
        );
    }
}
