//! The catch-up read.
//!
//! One projection reads one `stream_type`, in an order that cannot skip a row:
//! `(xact_id, global_seq)` ascending, taking only rows strictly below the
//! snapshot's xmin.

use std::fmt::{self, Display};
use std::marker::PhantomData;
use std::str::FromStr;

use sqlx::PgConnection;

use crate::{
    CausationId, ConfigError, CorrelationId, Envelope, Event, EventCodec, EventId, JsonEventCodec,
    Metadata, StoreError, StreamId, Version,
};

/// A PostgreSQL 64-bit transaction id.
///
/// `sqlx` has no encoding for `xid8`, so one crosses the boundary as text:
/// read with `xact_id::text`, bound with `CAST($n AS xid8)`. Comparison stays
/// in the database, where the type's own operators apply — this type never
/// does arithmetic on an id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Xid(u64);

impl Xid {
    /// Precedes every real transaction id, so a projection starting here reads
    /// the table from the beginning.
    pub const START: Self = Self(0);

    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}

impl Display for Xid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A stored `xid8` is unsigned, so a value above `i64::MAX` is ordinary rather
/// than exceptional. Parsing through `u64` keeps it exact.
impl FromStr for Xid {
    type Err = StoreError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.parse()
            .map(Self)
            .map_err(|_| StoreError::Other(format!("stored transaction id is not a u64: {text}")))
    }
}

/// How far a projection has read. `seq` breaks ties inside one transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub xact: Xid,
    pub seq: i64,
}

impl Cursor {
    pub const START: Self = Self {
        xact: Xid::START,
        seq: 0,
    };
}

/// One event and the cursor that points just past it.
#[derive(Debug, Clone)]
pub struct FeedRow<ID, E> {
    pub cursor: Cursor,
    pub envelope: Envelope<ID, E>,
}

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    /// A stored column does not decode into the type the projection expects.
    /// The feed's failure, not the projection's: it owns these columns.
    #[error("stored row does not decode: {0}")]
    Decode(String),
    #[error(transparent)]
    Store(StoreError),
    #[error(transparent)]
    Config(ConfigError),
}

/// Reads one `stream_type` in an order a catch-up reader can trust.
#[derive(Debug)]
pub struct EventFeed<ID, E, C = JsonEventCodec> {
    codec: C,
    /// `fn() -> (ID, E)` keeps the struct covariant in both parameters and
    /// borrows no auto-trait requirement from them.
    _marker: PhantomData<fn() -> (ID, E)>,
}

impl<ID, E> Default for EventFeed<ID, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<ID, E> EventFeed<ID, E> {
    pub const fn new() -> Self {
        Self::with_codec(JsonEventCodec)
    }
}

impl<ID, E, C> EventFeed<ID, E, C> {
    pub const fn with_codec(codec: C) -> Self {
        Self {
            codec,
            _marker: PhantomData,
        }
    }
}

impl<ID, E, C> EventFeed<ID, E, C>
where
    ID: StreamId,
    E: Event,
    C: EventCodec<E>,
{
    /// Reads the next batch after `cursor`.
    ///
    /// `xact_id < pg_snapshot_xmin(pg_current_snapshot())` is what makes the
    /// order total: once xmin passes X, every transaction at or below X has
    /// finished, so no row can still appear at or below X, and a rolled-back
    /// one leaves nothing behind. Note that a caller which has already taken
    /// a row lock holds an id of its own and so appears in its own xmin.
    /// That is conservative, never wrong: excluding rows at or above the
    /// reader's own id can only delay them, because the cursor never
    /// advances past a row it did not return.
    ///
    /// `pg_snapshot_xmin` is a property of the whole PostgreSQL instance, not
    /// of this table or this database: any transaction left open anywhere on
    /// the server — an unrelated long report, a migration, another service
    /// sharing the cluster — holds it back and stalls every projection's
    /// progress until that transaction concludes. The feed cannot detect or
    /// work around this; the only mitigation is keeping transactions short
    /// server-wide.
    pub async fn next_batch(
        &self,
        connection: &mut PgConnection,
        cursor: Cursor,
        batch_size: i64,
    ) -> Result<Vec<FeedRow<ID, E>>, FeedError> {
        if batch_size <= 0 {
            return Err(FeedError::Config(ConfigError::InvalidBatchSize));
        }
        let rows = sqlx::query!(
            r#"
            SELECT xact_id::text AS "xact_id!", global_seq,
                   event_id, stream_id, version, event_name, event_version, payload,
                   correlation_id, causation_id, metadata, recorded_at
              FROM events
             WHERE stream_type = $1
               AND (xact_id, global_seq) > (CAST($2 AS xid8), $3)
               AND xact_id < pg_snapshot_xmin(pg_current_snapshot())
             ORDER BY xact_id, global_seq
             LIMIT $4
            "#,
            ID::stream_type(),
            // sqlx has no mapping for `xid8`. The `as String` cast skips the
            // compile-time parameter check for this argument. The value
            // still binds as text. `CAST($2 AS xid8)` converts it in the
            // database.
            cursor.xact.to_string() as String,
            cursor.seq,
            batch_size,
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| FeedError::Store(StoreError::from(error)))?;

        rows.into_iter()
            .map(|row| {
                // Read once, up front: an operator chasing a `Decode` error
                // needs the row it came from, and this stays available even
                // after the fields below are parsed or moved out of `row`.
                let global_seq = row.global_seq;

                let version = u64::try_from(row.version).map(Version::new).map_err(|_| {
                    FeedError::Decode(format!(
                        "global_seq {global_seq}: negative version {}",
                        row.version
                    ))
                })?;
                // Parsed out of the column, unlike the event store's own read:
                // this walks many streams, so the caller holds no id to clone.
                let stream_id = ID::from_key(&row.stream_id)
                    .map_err(|error| FeedError::Decode(format!("global_seq {global_seq}: {error}")))?;

                Ok(FeedRow {
                    cursor: Cursor {
                        xact: row.xact_id.parse().map_err(|error: StoreError| {
                            FeedError::Decode(format!("global_seq {global_seq}: {error}"))
                        })?,
                        seq: row.global_seq,
                    },
                    envelope: Envelope {
                        event_id: EventId::from_uuid(row.event_id),
                        stream_id,
                        version,
                        event: self.codec.decode(&row.event_name, u32::try_from(row.event_version).map_err(|_| FeedError::Decode(format!("global_seq {global_seq}: negative event schema version")))?, row.payload).map_err(|error| {
                            FeedError::Decode(format!(
                                "global_seq {global_seq}: stored event does not decode: {error}"
                            ))
                        })?,
                        recorded_at: row.recorded_at,
                        metadata: Metadata::from_parts(
                            row.correlation_id.map(CorrelationId::from_uuid),
                            row.causation_id.map(CausationId::from_uuid),
                            serde_json::from_value(row.metadata).map_err(|error| {
                                FeedError::Decode(format!(
                                    "global_seq {global_seq}: stored metadata does not decode: {error}"
                                ))
                            })?,
                        ),
                    },
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod unit_tests {
    use super::Xid;

    /// A transaction id crosses the boundary as text, because `sqlx` cannot
    /// encode `xid8`. It must survive the round trip exactly — this is the
    /// only place a transaction id is ever parsed.
    #[test]
    fn a_transaction_id_round_trips_through_text() {
        let large = Xid::new(u64::MAX);

        assert_eq!(large.to_string().parse::<Xid>().expect("it parses"), large);
        assert_eq!(Xid::START.to_string(), "0");
        assert!("not-a-number".parse::<Xid>().is_err());
    }
}

#[cfg(all(test, feature = "postgres-tests"))]
mod tests {
    //! Run this module with `--test-threads=1` for a deterministic result.
    //! `#[sqlx::test]` gives each test its own database, but
    //! `pg_snapshot_xmin` is a whole-server watermark (see `next_batch`'s doc
    //! comment): while any test elsewhere in this binary is still migrating
    //! its own database, that in-progress transaction can pin the watermark
    //! below a *different* test's already-committed rows. Under the default
    //! parallel harness this surfaces as a transient, non-reproducible
    //! subset of these tests failing — not a bug in the feed. If you hit
    //! that, re-run with `--test-threads=1` before suspecting the code.

    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;

    use super::{Cursor, EventFeed, FeedError, FeedRow};
    use crate::{
        CausationId, CorrelationId, Event, EventId, InvalidStreamId, StreamId, Value, Version,
    };

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum TestEvent {
        Added(i64),
    }

    impl Event for TestEvent {
        fn name(&self) -> &'static str {
            "test.added"
        }

        fn version(&self) -> u32 {
            1
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestStreamId(String);

    impl StreamId for TestStreamId {
        fn stream_type() -> &'static str {
            "feed_test"
        }

        fn to_key(&self) -> String {
            self.0.clone()
        }

        fn from_key(key: &str) -> Result<Self, InvalidStreamId> {
            Ok(Self(key.to_owned()))
        }
    }

    type Feed = EventFeed<TestStreamId, TestEvent>;

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_empty_table_yields_an_empty_batch(pool: PgPool) {
        let mut connection = pool.acquire().await.expect("a connection");

        let batch = Feed::new()
            .next_batch(&mut connection, Cursor::START, 10)
            .await
            .expect("the read succeeds");

        assert!(batch.is_empty());
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn rows_come_back_in_transaction_then_sequence_order(pool: PgPool) {
        insert(&pool, "a", 1, 1).await;
        insert(&pool, "a", 2, 2).await;
        insert(&pool, "b", 1, 3).await;
        let mut connection = pool.acquire().await.expect("a connection");

        let batch = Feed::new()
            .next_batch(&mut connection, Cursor::START, 10)
            .await
            .expect("the read succeeds");

        let amounts: Vec<i64> = batch
            .iter()
            .map(|row| match row.envelope.event {
                TestEvent::Added(amount) => amount,
            })
            .collect();
        assert_eq!(amounts, vec![1, 2, 3]);
        assert!(
            batch.windows(2).all(|pair| {
                (pair[0].cursor.xact, pair[0].cursor.seq)
                    < (pair[1].cursor.xact, pair[1].cursor.seq)
            }),
            "cursors increase across the batch"
        );
    }

    /// The test the whole design exists for. A row written by a transaction
    /// that has not committed sits at or above the watermark, so the feed must
    /// not return it — and must return it once that transaction commits.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_row_inside_an_open_transaction_is_withheld(pool: PgPool) {
        let mut open = pool.begin().await.expect("a transaction begins");
        insert_on(&mut open, "a", 1, 1).await;
        let mut reader = pool.acquire().await.expect("a second connection");

        let withheld = Feed::new()
            .next_batch(&mut reader, Cursor::START, 10)
            .await
            .expect("the read succeeds");
        open.commit().await.expect("the writer commits");
        let released = Feed::new()
            .next_batch(&mut reader, Cursor::START, 10)
            .await
            .expect("the read succeeds");

        assert!(withheld.is_empty(), "an uncommitted row is not readable");
        assert_eq!(released.len(), 1, "the same row appears once it commits");
    }

    /// The out-of-order commit the watermark exists for. T1 inserts and
    /// stays open, so it takes the lower `xact_id`; T2 inserts after and
    /// commits at once, taking the higher `xact_id`. T2's row is now
    /// ordinarily visible to anyone — a plain visibility check, or a naive
    /// reader ordering only by `global_seq`, would return it and could
    /// advance a cursor past it, losing T1's row forever once T1 finally
    /// commits. The watermark forbids that: `pg_snapshot_xmin` cannot pass
    /// T1's still-open id, so T2's row is withheld too, however visible it
    /// already is. Once T1 concludes, both appear, T1's first — commit order
    /// and insert order disagree, and the feed follows insert order.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_committed_row_waits_for_an_older_open_transaction(pool: PgPool) {
        let mut t1 = pool.begin().await.expect("t1 begins");
        insert_on(&mut t1, "a", 1, 1).await; // the lower xact_id; stays open

        let mut t2 = pool.begin().await.expect("t2 begins");
        insert_on(&mut t2, "b", 1, 2).await; // the higher xact_id
        t2.commit().await.expect("t2 commits");

        let mut reader = pool.acquire().await.expect("a reader connection");
        let feed = Feed::new();

        let withheld = feed
            .next_batch(&mut reader, Cursor::START, 10)
            .await
            .expect("the read succeeds");
        assert!(
            withheld.is_empty(),
            "t2's row is committed and plainly visible, but t1's older, \
             still-open transaction must withhold it too"
        );

        t1.commit().await.expect("t1 commits");

        // A sibling transaction elsewhere on the server can still pin the
        // watermark below t2's id for a moment after t1 concludes (see
        // `next_batch`'s doc comment), so this half polls with a bounded
        // retry rather than asserting on the very next read.
        let released = poll_for_rows(&feed, &mut reader, 2).await;

        let amounts: Vec<i64> = released
            .iter()
            .map(|row| match row.envelope.event {
                TestEvent::Added(amount) => amount,
            })
            .collect();
        assert_eq!(
            amounts,
            vec![1, 2],
            "t1's row precedes t2's despite committing second"
        );
    }

    /// `global_seq` alone cannot order a catch-up read; only
    /// `(xact_id, global_seq)` can. T1 opens and inserts first (the lower
    /// `xact_id`). T2 opens after, inserts, and commits before T1 does (the
    /// higher `xact_id`, but visible first). T1 inserts again before
    /// finally committing, so its second row carries the lower `xact_id`
    /// together with a `global_seq` higher than T2's. Ordering by
    /// `global_seq` alone would place T2's row between T1's two; walking the
    /// cursor by `global_seq` alone would advance it past T2's position
    /// before T1's second row is even inserted, permanently stranding that
    /// row once the walk has already moved on.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_later_transactions_row_never_comes_between_an_earlier_ones(pool: PgPool) {
        let mut t1 = pool.begin().await.expect("t1 begins");
        insert_on(&mut t1, "a", 1, 1).await; // t1's first row: the lower xact_id, the lower global_seq

        let mut t2 = pool.begin().await.expect("t2 begins");
        insert_on(&mut t2, "b", 1, 2).await; // the higher xact_id
        t2.commit().await.expect("t2 commits");

        insert_on(&mut t1, "a", 2, 3).await; // t1's second row: the lower xact_id, the higher global_seq
        t1.commit().await.expect("t1 commits");

        let mut connection = pool.acquire().await.expect("a connection");
        let feed = Feed::new();

        // Polled, exactly like `a_committed_row_waits_for_an_older_open_
        // transaction`: a sibling transaction elsewhere on the server can pin
        // the cluster-wide watermark below these rows for a moment after t1
        // concludes. This test carries the same exposure, and a spurious
        // failure in the test the whole ordering design rests on would be
        // maximally confusing.
        let whole = poll_for_rows(&feed, &mut connection, 3).await;
        let amounts: Vec<i64> = whole
            .iter()
            .map(|row| match row.envelope.event {
                TestEvent::Added(amount) => amount,
            })
            .collect();
        assert_eq!(
            amounts,
            vec![1, 3, 2],
            "t1's two rows stay adjacent and precede t2's, despite t2 committing \
             first and t1's second row carrying the higher global_seq"
        );

        let first = feed
            .next_batch(&mut connection, Cursor::START, 2)
            .await
            .expect("the read succeeds");
        let second = feed
            .next_batch(&mut connection, first.last().expect("a row").cursor, 2)
            .await
            .expect("the read succeeds");
        assert_eq!(first.len(), 2, "the batch size bounds the read");
        assert_eq!(
            second.len(),
            1,
            "the walk still reaches t2's row rather than stranding it once \
             the cursor has passed t1's second row"
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_rolled_back_transaction_contributes_nothing(pool: PgPool) {
        let mut aborted = pool.begin().await.expect("a transaction begins");
        insert_on(&mut aborted, "a", 1, 1).await;
        aborted.rollback().await.expect("the writer rolls back");
        insert(&pool, "b", 1, 2).await;
        let mut connection = pool.acquire().await.expect("a connection");

        let batch = Feed::new()
            .next_batch(&mut connection, Cursor::START, 10)
            .await
            .expect("the read succeeds");

        assert_eq!(batch.len(), 1, "only the committed row is read");
        assert_eq!(batch[0].envelope.stream_id.0, "b");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn another_stream_type_is_excluded(pool: PgPool) {
        insert(&pool, "a", 1, 1).await;
        insert_of_type(&pool, "other", "a", 1, 2).await;
        let mut connection = pool.acquire().await.expect("a connection");

        let batch = Feed::new()
            .next_batch(&mut connection, Cursor::START, 10)
            .await
            .expect("the read succeeds");

        assert_eq!(batch.len(), 1, "only this projection's stream type is read");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn the_cursor_walks_the_table_in_batches(pool: PgPool) {
        insert(&pool, "a", 1, 1).await;
        insert(&pool, "a", 2, 2).await;
        insert(&pool, "a", 3, 3).await;
        let mut connection = pool.acquire().await.expect("a connection");
        let feed = Feed::new();

        let first = feed
            .next_batch(&mut connection, Cursor::START, 2)
            .await
            .expect("the read succeeds");
        let second = feed
            .next_batch(&mut connection, first.last().expect("a row").cursor, 2)
            .await
            .expect("the read succeeds");
        let third = feed
            .next_batch(&mut connection, second.last().expect("a row").cursor, 2)
            .await
            .expect("the read succeeds");

        assert_eq!(first.len(), 2, "the batch size bounds the read");
        assert_eq!(second.len(), 1, "the cursor resumes after the first batch");
        assert!(third.is_empty(), "the cursor reaches the end");
    }

    /// The feed builds its own `Envelope` from its own row, duplicating the
    /// event store's mapping, and this is the untested copy. The other tests
    /// here read only `event`, `stream_id` and the cursor, so swapping
    /// `correlation_id` for `causation_id` — or dropping `metadata`
    /// altogether — passes all of them while handing a read model that traces
    /// by correlation the wrong id. Every column the mapping touches is
    /// asserted here.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn every_column_the_mapping_touches_comes_back(pool: PgPool) {
        let event_id = uuid::Uuid::now_v7();
        let correlation_id = uuid::Uuid::from_u128(12);
        let causation_id = uuid::Uuid::from_u128(13);
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version,
                 event_name, event_version, payload,
                 correlation_id, causation_id, metadata, recorded_at
             ) VALUES ($1, 'feed_test', 'a', 7, 'test.added', 1, $2::jsonb,
                       $3, $4, $5::jsonb, now())",
        )
        .bind(event_id)
        .bind(r#"{"Added":5}"#)
        .bind(correlation_id)
        .bind(causation_id)
        .bind(r#"{"source":"payments"}"#)
        .execute(&pool)
        .await
        .expect("the row is inserted");
        let mut connection = pool.acquire().await.expect("a connection");

        let batch = Feed::new()
            .next_batch(&mut connection, Cursor::START, 10)
            .await
            .expect("the read succeeds");

        let envelope = &batch.first().expect("the row comes back").envelope;
        assert_eq!(envelope.event_id, EventId::from_uuid(event_id));
        assert_eq!(envelope.stream_id.0, "a");
        // Above 1, so a mapping that hard-coded the first version would fail.
        assert_eq!(envelope.version, Version::new(7));
        assert_eq!(
            envelope.metadata.correlation_id,
            Some(CorrelationId::from_uuid(correlation_id)),
            "the correlation column is not the causation column"
        );
        assert_eq!(
            envelope.metadata.causation_id,
            Some(CausationId::from_uuid(causation_id)),
            "the causation column is not the correlation column"
        );
        assert_eq!(
            envelope.metadata.get("source"),
            Some(&Value::String("payments".to_owned())),
            "the metadata object survives the round trip"
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_payload_that_does_not_decode_is_reported(pool: PgPool) {
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version,
                 event_name, event_version, payload, recorded_at
             ) VALUES ($1, 'feed_test', 'a', 1, 'test.added', 1, $2::jsonb, now())",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(r#"{"Removed":{"reason":"unknown variant"}}"#)
        .execute(&pool)
        .await
        .expect("the row is inserted");
        let mut connection = pool.acquire().await.expect("a connection");

        let error = Feed::new()
            .next_batch(&mut connection, Cursor::START, 10)
            .await
            .expect_err("the payload does not decode");

        assert!(matches!(error, FeedError::Decode(_)), "got {error:?}");
    }

    /// A bounded retry for a read whose result can lag a commit because an
    /// unrelated transaction elsewhere on the server is still pinning the
    /// cluster-wide watermark. Never loops without limit: a genuine failure
    /// to appear still fails the test, just after giving the watermark a
    /// fair chance to clear first.
    async fn poll_for_rows(
        feed: &Feed,
        connection: &mut sqlx::PgConnection,
        expected_len: usize,
    ) -> Vec<FeedRow<TestStreamId, TestEvent>> {
        const ATTEMPTS: u32 = 20;
        const DELAY_SECONDS: f64 = 0.05;

        let mut batch = Vec::new();
        for attempt in 1..=ATTEMPTS {
            batch = feed
                .next_batch(&mut *connection, Cursor::START, 10)
                .await
                .expect("the read succeeds");
            if batch.len() >= expected_len {
                return batch;
            }
            assert!(
                attempt < ATTEMPTS,
                "expected {expected_len} rows after {ATTEMPTS} attempts, got {}",
                batch.len()
            );
            // Sleeps on the database side so the test needs no extra
            // dependency: the connection is otherwise idle between reads.
            sqlx::query("SELECT pg_sleep($1)")
                .bind(DELAY_SECONDS)
                .execute(&mut *connection)
                .await
                .expect("the sleep runs");
        }
        batch
    }

    async fn insert(pool: &PgPool, stream_id: &str, version: i64, amount: i64) {
        insert_of_type(pool, "feed_test", stream_id, version, amount).await;
    }

    async fn insert_of_type(
        pool: &PgPool,
        stream_type: &str,
        stream_id: &str,
        version: i64,
        amount: i64,
    ) {
        let mut connection = pool.acquire().await.expect("a connection");
        insert_row(&mut connection, stream_type, stream_id, version, amount).await;
    }

    async fn insert_on(
        transaction: &mut sqlx::PgConnection,
        stream_id: &str,
        version: i64,
        amount: i64,
    ) {
        insert_row(transaction, "feed_test", stream_id, version, amount).await;
    }

    async fn insert_row(
        connection: &mut sqlx::PgConnection,
        stream_type: &str,
        stream_id: &str,
        version: i64,
        amount: i64,
    ) {
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version,
                 event_name, event_version, payload, recorded_at
             ) VALUES ($1, $2, $3, $4, 'test.added', 1, $5::jsonb, now())",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(stream_type)
        .bind(stream_id)
        .bind(version)
        .bind(format!(r#"{{"Added":{amount}}}"#))
        .execute(connection)
        .await
        .expect("the row is inserted");
    }
}
