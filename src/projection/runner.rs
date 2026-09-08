//! The catch-up runner.
//!
//! One runner drives one projection: it reads a batch below the watermark,
//! applies it, and advances the checkpoint — all in one transaction, so the
//! read model and the cursor can never disagree.

use std::time::Duration;

use sqlx::PgPool;

use super::{Cursor, EventFeed, FeedError, Xid};
use crate::{ConfigError, EventCodec, JsonEventCodec, Projection, StoreError};

#[derive(Debug, Clone, Copy)]
pub struct RunnerConfig {
    pub batch_size: i64,
    pub poll_interval: Duration,
    /// Consecutive failed attempts at one batch before the projection halts.
    pub max_failures: i32,
    pub initial_backoff: Duration,
    /// Ceiling for the doubling, so a long outage does not push the retry
    /// interval into hours.
    pub max_backoff: Duration,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            poll_interval: Duration::from_secs(1),
            max_failures: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
        }
    }
}

impl RunnerConfig {
    /// Validates settings before a runner can perform any work.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.batch_size <= 0 {
            return Err(ConfigError::InvalidBatchSize);
        }
        if self.max_failures <= 0 {
            return Err(ConfigError::InvalidMaxFailures);
        }
        if self.poll_interval.is_zero() {
            return Err(ConfigError::InvalidPollInterval);
        }
        if self.initial_backoff.is_zero() || self.initial_backoff > self.max_backoff {
            return Err(ConfigError::InvalidBackoff);
        }
        Ok(())
    }

    fn next_backoff(&self, current: Duration) -> Duration {
        current.saturating_mul(2).min(self.max_backoff)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Tick {
    /// Applied this many events and advanced the cursor.
    Applied(usize),
    /// No event is ready below the watermark.
    CaughtUp,
    /// No checkpoint row was claimed. Two distinct causes share this variant
    /// and `Tick`'s shape does not distinguish them: another instance holds
    /// the row for the length of its own tick (transient, resolves on its
    /// own), or this projection was never `register`ed at all (permanent,
    /// and stays `Idle` forever until `register` is called).
    Idle,
    /// `max_failures` consecutive failures stopped this projection.
    Halted,
}

/// The runner's error, parameterised by the projection's own.
///
/// Decoding belongs to the feed, which owns the payload column, and the
/// checkpoint belongs to the runner, so neither is a variant a projection
/// should have to carry.
#[derive(Debug, thiserror::Error)]
pub enum RunnerError<E> {
    #[error("stored row does not decode: {0}")]
    Decode(String),
    #[error("projection failed: {0:?}")]
    Projection(E),
    #[error(transparent)]
    Store(StoreError),
    #[error(transparent)]
    Config(ConfigError),
    #[error("projection `{projection}` is halted")]
    Halted { projection: &'static str },
    #[error("{failure:?}; also failed to record the failure: {store}")]
    FailureRecording {
        failure: Box<RunnerError<E>>,
        store: StoreError,
    },
}

impl<E> From<FeedError> for RunnerError<E> {
    fn from(error: FeedError) -> Self {
        match error {
            FeedError::Decode(message) => Self::Decode(message),
            FeedError::Store(error) => Self::Store(error),
            FeedError::Config(error) => Self::Config(error),
        }
    }
}

#[derive(Debug)]
pub struct ProjectionRunner<P: Projection, C = JsonEventCodec> {
    projection: P,
    pool: PgPool,
    feed: EventFeed<P::Id, P::Event, C>,
    config: RunnerConfig,
}

impl<P: Projection> ProjectionRunner<P> {
    pub fn new(projection: P, pool: PgPool, config: RunnerConfig) -> Result<Self, ConfigError> {
        Self::with_codec(projection, pool, config, JsonEventCodec)
    }
}

impl<P: Projection, C> ProjectionRunner<P, C> {
    pub fn with_codec(
        projection: P,
        pool: PgPool,
        config: RunnerConfig,
        codec: C,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            projection,
            pool,
            feed: EventFeed::with_codec(codec),
            config,
        })
    }
}

impl<P, C> ProjectionRunner<P, C>
where
    P: Projection<Connection = sqlx::PgConnection>,
    C: EventCodec<P::Event>,
{
    /// Creates this projection's checkpoint row if it has none. Called by
    /// `run` before its first tick.
    pub async fn register(&self) -> Result<(), RunnerError<P::Error>> {
        sqlx::query!(
            r#"
            INSERT INTO projection_checkpoints (projection) VALUES ($1)
            ON CONFLICT (projection) DO NOTHING
            "#,
            self.projection.name(),
        )
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|error| RunnerError::Store(StoreError::from(error)))
    }

    /// One pass: claim the checkpoint, read a batch, apply it, advance the
    /// cursor, commit.
    ///
    /// `FOR UPDATE SKIP LOCKED` is what makes several API instances safe. One
    /// owns the projection for the length of a tick and the others return
    /// `Idle` rather than waiting, so no batch is ever applied twice and no
    /// instance blocks.
    ///
    /// Carries the claimed revision out to `tick` for conditional failure recording.
    async fn try_tick(
        &self,
        failed_revision: &mut Option<i64>,
    ) -> Result<Tick, RunnerError<P::Error>> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| RunnerError::Store(StoreError::from(error)))?;

        let claimed = sqlx::query!(
            r#"
            SELECT cursor_xact::text AS "cursor_xact!", cursor_seq, halted_at, revision
              FROM projection_checkpoints
             WHERE projection = $1
               FOR UPDATE SKIP LOCKED
            "#,
            self.projection.name(),
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|error| RunnerError::Store(StoreError::from(error)))?;

        let Some(checkpoint) = claimed else {
            return Ok(Tick::Idle);
        };
        if checkpoint.halted_at.is_some() {
            return Ok(Tick::Halted);
        }

        *failed_revision = Some(checkpoint.revision);
        let cursor = Cursor {
            xact: checkpoint
                .cursor_xact
                .parse::<Xid>()
                .map_err(RunnerError::Store)?,
            seq: checkpoint.cursor_seq,
        };
        // The feed reads through this transaction, not the pool: the batch, the
        // read-model writes below and the cursor advance all have to commit or
        // roll back together. Deref coercion reaches `&mut PgConnection` on its
        // own here, so no explicit reborrow is needed — unlike the `sqlx` call
        // sites around it, which take `impl Executor` and do need one.
        let batch = self
            .feed
            .next_batch(&mut transaction, cursor, self.config.batch_size)
            .await?;

        let Some(last) = batch.last().map(|row| row.cursor) else {
            return Ok(Tick::CaughtUp);
        };

        for row in &batch {
            self.projection
                .apply(&mut *transaction, &row.envelope)
                .await
                .map_err(RunnerError::Projection)?;
        }

        sqlx::query!(
            r#"
            UPDATE projection_checkpoints
               SET cursor_xact = CAST($2 AS xid8), cursor_seq = $3,
                   failures = 0, last_error = NULL, revision = revision + 1, updated_at = now()
             WHERE projection = $1
            "#,
            self.projection.name(),
            // sqlx has no mapping for `xid8`; see the identical cast in the
            // feed's `next_batch`.
            last.xact.to_string() as String,
            last.seq,
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| RunnerError::Store(StoreError::from(error)))?;

        transaction
            .commit()
            .await
            .map_err(|error| RunnerError::Store(StoreError::from(error)))?;

        Ok(Tick::Applied(batch.len()))
    }

    /// Records one failed attempt, halting the projection once the run of
    /// failures reaches the configured maximum.
    ///
    /// This runs in its own transaction because the failed tick's transaction
    /// has already rolled back — it cannot carry the count that describes its
    /// own failure.
    async fn record_failure(&self, revision: i64, error: &str) -> Result<(), StoreError> {
        sqlx::query!(
            r#"
            UPDATE projection_checkpoints
               SET failures = failures + 1,
                   last_error = $2,
                   -- `ELSE halted_at` is load-bearing: without it every
                   -- attempt below the threshold writes NULL, which would
                   -- clear a halt this statement never set.
                   halted_at = CASE WHEN failures + 1 >= $3 THEN now()
                                    ELSE halted_at END,
                   updated_at = now()
             WHERE projection = $1 AND revision = $4 AND halted_at IS NULL
            "#,
            self.projection.name(),
            error,
            self.config.max_failures,
            revision,
        )
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(StoreError::from)
    }

    /// Clears a halt. The cursor is left alone, so the projection restarts on
    /// the batch that broke it — fixing the cause first is the operator's job.
    pub async fn resume(&self) -> Result<(), RunnerError<P::Error>> {
        sqlx::query!(
            r#"
            UPDATE projection_checkpoints
               SET halted_at = NULL, failures = 0, last_error = NULL, revision = revision + 1,
                   updated_at = now()
             WHERE projection = $1
            "#,
            self.projection.name(),
        )
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|error| RunnerError::Store(StoreError::from(error)))
    }

    /// Clears the read model and replays this projection from the start of the
    /// table.
    ///
    /// The lock waits here, unlike a tick's `SKIP LOCKED`: skipping would let
    /// a rebuild silently do nothing while another instance held the row.
    /// Waiting also serialises the rebuild against ticking, so no tick can
    /// apply an event on top of a table a rebuild is clearing.
    ///
    /// `reset` and the cursor rewind share one transaction, so a read model is
    /// never left empty with a cursor that thinks it is caught up.
    ///
    /// An unregistered projection is an error rather than a rebuild of nothing.
    /// `FOR UPDATE` on zero rows takes no lock at all, so the serialisation
    /// above would silently not exist, and `reset` would still run — wiping
    /// read-model rows that arrived by some other route. Call `register` first.
    pub async fn rebuild(&self) -> Result<(), RunnerError<P::Error>> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| RunnerError::Store(StoreError::from(error)))?;

        let claimed = sqlx::query!(
            r#"
            SELECT projection FROM projection_checkpoints
             WHERE projection = $1
               FOR UPDATE
            "#,
            self.projection.name(),
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|error| RunnerError::Store(StoreError::from(error)))?;

        if claimed.is_none() {
            return Err(RunnerError::Store(StoreError::Other(format!(
                "cannot rebuild projection {:?}: it has no checkpoint row, so \
                 the rebuild would hold no lock; call register first",
                self.projection.name(),
            ))));
        }

        self.projection
            .reset(&mut *transaction)
            .await
            .map_err(RunnerError::Projection)?;

        sqlx::query!(
            r#"
            UPDATE projection_checkpoints
               SET cursor_xact = '0'::xid8, cursor_seq = 0, failures = 0, revision = revision + 1,
                   halted_at = NULL, last_error = NULL, updated_at = now()
             WHERE projection = $1
            "#,
            self.projection.name(),
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| RunnerError::Store(StoreError::from(error)))?;

        transaction
            .commit()
            .await
            .map_err(|error| RunnerError::Store(StoreError::from(error)))
    }

    /// Processes one batch. Decode errors and non-retryable projection errors
    /// count toward halting only if the claimed checkpoint revision is still
    /// current. Store errors and retryable projection errors never consume that
    /// budget. Failure-recording errors preserve the original processing error.
    pub async fn tick(&self) -> Result<Tick, RunnerError<P::Error>> {
        let mut failed_revision = None;
        match self.try_tick(&mut failed_revision).await {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let counts = match &error {
                    RunnerError::Decode(_) => true,
                    RunnerError::Projection(error) => !self.projection.is_retryable(error),
                    _ => false,
                };
                if let Some(revision) = failed_revision.filter(|_| counts)
                    && let Err(store) = self.record_failure(revision, &format!("{error:?}")).await
                {
                    return Err(RunnerError::FailureRecording {
                        failure: Box::new(error),
                        store,
                    });
                }
                Err(error)
            }
        }
    }

    /// Runs until cancelled or halted. Infrastructure errors are retried with
    /// capped backoff. A halt returns `RunnerError::Halted` to the supervisor;
    /// after fixing the cause, call `resume` and start a new run.
    ///
    /// Cancellation drops any open transaction. A commit already in progress
    /// can still succeed; transactional effects and the checkpoint stay atomic.
    /// Use `run_with_observer` to report intermediate errors and tick outcomes.
    pub async fn run(&self) -> Result<(), RunnerError<P::Error>> {
        self.run_with_observer(|_| {}).await
    }

    /// Calls `observe` after every tick, including errors and halted status.
    /// Registration failures return directly. The callback must be quick and
    /// nonblocking; it runs outside the tick transaction and must not panic.
    pub async fn run_with_observer<F>(&self, mut observe: F) -> Result<(), RunnerError<P::Error>>
    where
        F: FnMut(&Result<Tick, RunnerError<P::Error>>) + Send,
    {
        self.register().await?;
        let mut backoff = self.config.initial_backoff;
        loop {
            let outcome = self.tick().await;
            observe(&outcome);
            match outcome {
                Ok(Tick::Applied(_)) => backoff = self.config.initial_backoff,
                Ok(Tick::Halted) => {
                    return Err(RunnerError::Halted {
                        projection: self.projection.name(),
                    });
                }
                Ok(Tick::CaughtUp | Tick::Idle) => {
                    backoff = self.config.initial_backoff;
                    tokio::time::sleep(self.config.poll_interval).await;
                }
                Err(_) => {
                    tokio::time::sleep(backoff).await;
                    backoff = self.config.next_backoff(backoff);
                }
            }
        }
    }
}

#[cfg(all(test, feature = "postgres-tests"))]
mod tests {
    //! Run this module with `--test-threads=1` for a deterministic result.
    //! Every tick here reads through the feed, whose `next_batch` is gated on
    //! `pg_snapshot_xmin` — a whole-server watermark, not a per-database one
    //! (see `next_batch`'s doc comment). A sibling `#[sqlx::test]` still
    //! migrating its own database pins that watermark below rows this module
    //! has already committed, so under the default parallel harness a
    //! transient subset of these tests fails on a batch that arrived empty.
    //! That is the harness, not the runner. Re-run single-threaded before
    //! suspecting the code.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use serde::{Deserialize, Serialize};
    use sqlx::{PgConnection, PgPool};
    use tokio::time::timeout;

    use super::{ProjectionRunner, RunnerConfig, Tick};
    use crate::{Envelope, Event, InvalidStreamId, Projection, StreamId};

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
            "runner_test"
        }

        fn to_key(&self) -> String {
            self.0.clone()
        }

        fn from_key(key: &str) -> Result<Self, InvalidStreamId> {
            Ok(Self(key.to_owned()))
        }
    }

    /// Sums amounts per stream into a table of its own, the way a real read
    /// model would. `fail_after` drives the failure path without corrupting
    /// the schema: `apply` accepts that many events and rejects the next one.
    ///
    /// A count rather than a flag, because a flag can only reject the *first*
    /// event of a batch, and a rejected first event never wrote anything —
    /// which proves nothing about rollback. `fail_after = 1` makes `apply`
    /// write one row and then fail, so a test can demand that the written row
    /// is gone.
    #[derive(Debug)]
    struct Totals {
        fail_after: usize,
        /// Events `apply` has accepted, across every tick. Deliberately not
        /// rolled back with the transaction — a projection's own memory cannot
        /// be, which is exactly why its *writes* must go through the caller's
        /// transaction instead.
        accepted: AtomicUsize,
    }

    /// `apply` never rejects. `usize::MAX` reads better at the call sites than
    /// a magic number, and no test comes near that many events.
    const NEVER_FAILS: usize = usize::MAX;

    impl Totals {
        const fn new(fail_after: usize) -> Self {
            Self {
                fail_after,
                accepted: AtomicUsize::new(0),
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct TotalsError;

    impl Projection for Totals {
        type Id = TestStreamId;
        type Event = TestEvent;
        type Connection = PgConnection;
        type Error = TotalsError;

        fn name(&self) -> &'static str {
            "totals"
        }

        async fn apply(
            &self,
            connection: &mut Self::Connection,
            envelope: &Envelope<Self::Id, Self::Event>,
        ) -> Result<(), Self::Error> {
            if self.accepted.load(Ordering::SeqCst) >= self.fail_after {
                return Err(TotalsError);
            }
            self.accepted.fetch_add(1, Ordering::SeqCst);

            let TestEvent::Added(amount) = envelope.event;
            sqlx::query(
                "INSERT INTO totals (stream_id, total) VALUES ($1, $2)
                 ON CONFLICT (stream_id) DO UPDATE SET total = totals.total + $2",
            )
            .bind(envelope.stream_id.to_key())
            .bind(amount)
            .execute(&mut *connection)
            .await
            .map(|_| ())
            .map_err(|_| TotalsError)
        }

        async fn reset(&self, connection: &mut Self::Connection) -> Result<(), Self::Error> {
            sqlx::query("DELETE FROM totals")
                .execute(&mut *connection)
                .await
                .map(|_| ())
                .map_err(|_| TotalsError)
        }
    }

    /// The second tick is what pins the cursor to the *end* of the batch. A
    /// cursor advanced to `batch.first()` instead still moves off zero and
    /// still applies both events the first time, so the first tick alone
    /// cannot tell the two apart. On the second tick the difference shows:
    /// every event after the first is re-read, so the tick returns
    /// `Applied(1)` instead of `CaughtUp` and the total climbs to 19.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_batch_applies_and_advances_the_cursor_together(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        insert(&pool, "a", 2, 7).await;
        let runner = runner(&pool, NEVER_FAILS);
        runner.register().await.expect("registration succeeds");

        let outcome = runner.tick().await.expect("the tick succeeds");

        assert!(matches!(outcome, Tick::Applied(2)), "got {outcome:?}");
        assert_eq!(total(&pool, "a").await, 12);
        assert!(cursor_seq(&pool).await > 0, "the cursor moved");

        let again = runner.tick().await.expect("the second tick succeeds");

        assert!(matches!(again, Tick::CaughtUp), "got {again:?}");
        assert_eq!(
            total(&pool, "a").await,
            12,
            "the cursor cleared the whole batch, so nothing was applied twice"
        );
    }

    /// The atomicity promise, from inside a batch. `apply` accepts the first
    /// event — writing a real row into the tick's transaction — and then
    /// rejects the second. Both the write and the cursor must be gone
    /// afterwards: this is the only test where a read-model row genuinely
    /// existed before the tick failed, so it is the only one that can prove
    /// the rollback rather than the absence of a write.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_rejected_later_event_rolls_back_the_earlier_one(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        insert(&pool, "a", 2, 7).await;
        let runner = runner(&pool, 1);
        runner.register().await.expect("registration succeeds");

        let error = runner.tick().await.expect_err("the second event rejects");

        assert!(
            matches!(error, super::RunnerError::Projection(_)),
            "got {error:?}"
        );
        assert_eq!(
            total(&pool, "a").await,
            0,
            "the first event's write rolled back with the transaction"
        );
        assert_eq!(cursor_seq(&pool).await, 0, "the cursor did not move");
    }

    /// A poison event through the whole runner, not just the feed. An
    /// undecodable payload is the likeliest real cause of a halt, so the path
    /// from `FeedError::Decode` through `RunnerError::Decode` to a counted
    /// failure needs its own coverage: the feed's own test cannot reach the
    /// `From` mapping, and no other runner test drives a decode error at all.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn an_undecodable_payload_fails_the_tick_and_counts(pool: PgPool) {
        create_read_model(&pool).await;
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version,
                 event_name, event_version, payload, recorded_at
             ) VALUES ($1, 'runner_test', 'a', 1, 'test.added', 1, $2::jsonb, now())",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(r#"{"Removed":{"reason":"unknown variant"}}"#)
        .execute(&pool)
        .await
        .expect("the row is inserted");
        let runner = runner(&pool, NEVER_FAILS);
        runner.register().await.expect("registration succeeds");

        let error = runner
            .tick()
            .await
            .expect_err("the payload does not decode");

        assert!(
            matches!(error, super::RunnerError::Decode(_)),
            "got {error:?}"
        );
        assert_eq!(cursor_seq(&pool).await, 0, "the cursor did not move");
        assert_eq!(failures(&pool).await, 1, "the attempt was counted");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_tick_with_nothing_new_changes_nothing(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let runner = runner(&pool, NEVER_FAILS);
        runner.register().await.expect("registration succeeds");
        runner.tick().await.expect("the first tick succeeds");
        let advanced = cursor_seq(&pool).await;

        let outcome = runner.tick().await.expect("the second tick succeeds");

        assert!(matches!(outcome, Tick::CaughtUp), "got {outcome:?}");
        assert_eq!(total(&pool, "a").await, 5, "nothing was applied twice");
        assert_eq!(cursor_seq(&pool).await, advanced, "the cursor stood still");
    }

    /// Two API instances run the same projection. One holds the checkpoint row
    /// for the length of its tick; the other must decline rather than wait, and
    /// must not apply anything twice.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_second_runner_declines_while_the_row_is_held(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let runner = runner(&pool, NEVER_FAILS);
        runner.register().await.expect("registration succeeds");
        let mut holder = pool.begin().await.expect("a transaction begins");
        sqlx::query("SELECT 1 FROM projection_checkpoints WHERE projection = 'totals' FOR UPDATE")
            .fetch_one(&mut *holder)
            .await
            .expect("the row is locked");

        // Bounded rather than a bare `.await`: if `SKIP LOCKED` regressed back
        // to a plain row lock, this tick would block on `holder`'s lock
        // instead of returning `Idle`, and `holder` only releases on the next
        // line — an unbounded wait would hang the whole test run rather than
        // failing it. Five seconds is ample; the uncontended path is
        // millisecond-fast.
        let outcome = timeout(Duration::from_secs(5), runner.tick())
            .await
            .expect(
                "the tick blocked on the held checkpoint row instead of returning \
                 promptly, which means SKIP LOCKED is no longer in the claim query",
            )
            .expect("the tick succeeds");
        holder.rollback().await.expect("the holder releases");

        assert!(matches!(outcome, Tick::Idle), "got {outcome:?}");
        assert_eq!(total(&pool, "a").await, 0, "nothing was applied");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn registration_is_idempotent(pool: PgPool) {
        create_read_model(&pool).await;
        let runner = runner(&pool, NEVER_FAILS);

        runner.register().await.expect("the first registration");
        runner.register().await.expect("the second registration");

        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM projection_checkpoints WHERE projection = 'totals'",
        )
        .fetch_one(&pool)
        .await
        .expect("the query runs");
        assert_eq!(rows, 1);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_failing_apply_leaves_no_trace_and_counts(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let runner = runner(&pool, 0);
        runner.register().await.expect("registration succeeds");

        let error = runner.tick().await.expect_err("the projection rejects");

        assert!(
            matches!(error, super::RunnerError::Projection(_)),
            "got {error:?}"
        );
        assert_eq!(total(&pool, "a").await, 0, "the read model is untouched");
        assert_eq!(cursor_seq(&pool).await, 0, "the cursor did not move");
        assert_eq!(failures(&pool).await, 1, "the attempt was counted");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn repeated_failures_halt_the_projection(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let runner = runner(&pool, 0);
        runner.register().await.expect("registration succeeds");

        runner.tick().await.expect_err("the first attempt fails");
        runner.tick().await.expect_err("the second attempt fails");
        let after_halt = runner.tick().await.expect("a halted tick is not an error");

        assert!(matches!(after_halt, Tick::Halted), "got {after_halt:?}");
        assert!(halted(&pool).await, "the projection is halted");
        assert_eq!(
            failures(&pool).await,
            2,
            "a halted tick counts nothing further"
        );
    }

    /// Resume clears the halt and leaves the cursor alone, so the batch that
    /// broke the projection is the one it retries.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn resume_retries_the_batch_that_failed(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let failing = runner(&pool, 0);
        failing.register().await.expect("registration succeeds");
        failing.tick().await.expect_err("the first attempt fails");
        failing.tick().await.expect_err("the second attempt fails");

        failing.resume().await.expect("resume succeeds");
        let recovered = runner(&pool, NEVER_FAILS);
        let outcome = recovered.tick().await.expect("the retry succeeds");

        assert!(matches!(outcome, Tick::Applied(1)), "got {outcome:?}");
        assert!(!halted(&pool).await, "the halt is cleared");
        assert_eq!(total(&pool, "a").await, 5, "the withheld event landed");
        assert_eq!(failures(&pool).await, 0, "the counter reset");
    }

    /// Discriminates single-counting from double-counting. At `max_failures
    /// = 3`, single counting (the correct behaviour) halts on the tick that
    /// reaches the threshold, leaving `failures == 3`. A `run` that also
    /// called `record_failure` in its error arm — in addition to `tick`'s own
    /// call — would halt one tick sooner, leaving `failures == 4`. Driving
    /// this through `run` rather than bare `tick` calls is the point: a test
    /// that calls `tick` directly can never exercise `run`'s error arm at
    /// all, so it cannot catch a regression placed there.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn run_halts_without_double_counting_failures(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let runner = Arc::new(
            ProjectionRunner::new(
                Totals::new(0),
                pool.clone(),
                RunnerConfig {
                    batch_size: 10,
                    poll_interval: Duration::from_millis(5),
                    max_failures: 3,
                    initial_backoff: Duration::from_millis(1),
                    max_backoff: Duration::from_millis(4),
                },
            )
            .expect("valid runner config"),
        );

        let driven = runner.clone();
        let handle = tokio::spawn(async move { driven.run().await });
        poll_until_halted(&pool).await;
        // Aborting mid-tick is safe: whatever transaction was open rolls
        // back, per `run`'s own doc comment.
        handle.abort();

        assert_eq!(
            failures(&pool).await,
            3,
            "single counting halts at the configured maximum; double \
             counting would reach 4 one tick early"
        );
    }

    /// `resume` must leave an already-advanced cursor exactly where it was.
    /// Rewinding it would silently reproject everything from the start,
    /// which for a non-idempotent projection like `Totals` (its `apply` adds
    /// to a running total rather than overwriting it) means double-counted
    /// totals. Capturing the advanced value and asserting equality — rather
    /// than merely asserting non-zero — is what catches a `resume` that
    /// zeroed the cursor back to the beginning of the table: a rewound
    /// cursor is still non-zero right up until the next tick reprojects it,
    /// so a mere non-zero check would not discriminate.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn resume_leaves_an_advanced_cursor_alone(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let succeeding = runner(&pool, NEVER_FAILS);
        succeeding.register().await.expect("registration succeeds");
        succeeding.tick().await.expect("the first batch applies");
        let advanced = cursor_seq(&pool).await;
        assert!(advanced > 0, "the cursor advanced past zero");

        // Same projection name, same pool: this shares the one checkpoint
        // row `succeeding` just advanced.
        insert(&pool, "a", 2, 3).await;
        let failing = runner(&pool, 0);
        failing
            .register()
            .await
            .expect("registration is idempotent");
        failing.tick().await.expect_err("the first attempt fails");
        failing.tick().await.expect_err("the second attempt fails");
        assert!(halted(&pool).await, "the projection halted");

        failing.resume().await.expect("resume succeeds");

        assert_eq!(
            cursor_seq(&pool).await,
            advanced,
            "resume must leave the advanced cursor alone, not rewind to the start"
        );
    }

    /// A bounded retry for `run`'s background halt, which lands
    /// asynchronously on its own schedule rather than in lockstep with this
    /// test. Never loops without limit: a genuine failure to halt still
    /// fails the test, just after giving the spawned task a fair chance to
    /// run first.
    async fn poll_until_halted(pool: &PgPool) {
        const ATTEMPTS: u32 = 100;
        const DELAY: Duration = Duration::from_millis(10);

        for attempt in 1..=ATTEMPTS {
            // `fetch_optional`, not `halted`'s `fetch_one`: `run` registers
            // the row itself in the background, so an early poll can land
            // before the row exists at all, which is "not halted yet" rather
            // than an error.
            let halted: Option<bool> = sqlx::query_scalar(
                "SELECT halted_at IS NOT NULL FROM projection_checkpoints WHERE projection = 'totals'",
            )
            .fetch_optional(pool)
            .await
            .expect("the query runs");
            if halted == Some(true) {
                return;
            }
            assert!(
                attempt < ATTEMPTS,
                "expected the projection to halt within {ATTEMPTS} polls, but it never did"
            );
            tokio::time::sleep(DELAY).await;
        }
    }

    async fn failures(pool: &PgPool) -> i32 {
        sqlx::query_scalar(
            "SELECT failures FROM projection_checkpoints WHERE projection = 'totals'",
        )
        .fetch_one(pool)
        .await
        .expect("the query runs")
    }

    async fn halted(pool: &PgPool) -> bool {
        sqlx::query_scalar(
            "SELECT halted_at IS NOT NULL FROM projection_checkpoints WHERE projection = 'totals'",
        )
        .fetch_one(pool)
        .await
        .expect("the query runs")
    }

    fn runner(pool: &PgPool, fail_after: usize) -> ProjectionRunner<Totals> {
        ProjectionRunner::new(
            Totals::new(fail_after),
            pool.clone(),
            RunnerConfig {
                batch_size: 10,
                poll_interval: Duration::from_millis(10),
                max_failures: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(4),
            },
        )
        .expect("valid runner config")
    }

    async fn create_read_model(pool: &PgPool) {
        sqlx::query("CREATE TABLE totals (stream_id TEXT PRIMARY KEY, total BIGINT NOT NULL)")
            .execute(pool)
            .await
            .expect("the read model table is created");
    }

    async fn total(pool: &PgPool, stream_id: &str) -> i64 {
        // `SUM(BIGINT)` returns `NUMERIC` in Postgres, which this crate has no
        // decoder for; the cast keeps the column `BIGINT` on the wire.
        sqlx::query_scalar(
            "SELECT COALESCE(SUM(total), 0)::BIGINT FROM totals WHERE stream_id = $1",
        )
        .bind(stream_id)
        .fetch_one(pool)
        .await
        .expect("the query runs")
    }

    async fn cursor_seq(pool: &PgPool) -> i64 {
        sqlx::query_scalar(
            "SELECT cursor_seq FROM projection_checkpoints WHERE projection = 'totals'",
        )
        .fetch_one(pool)
        .await
        .expect("the query runs")
    }

    async fn insert(pool: &PgPool, stream_id: &str, version: i64, amount: i64) {
        sqlx::query(
            "INSERT INTO events (
                 event_id, stream_type, stream_id, version,
                 event_name, event_version, payload, recorded_at
             ) VALUES ($1, 'runner_test', $2, $3, 'test.added', 1, $4::jsonb, now())",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(stream_id)
        .bind(version)
        .bind(format!(r#"{{"Added":{amount}}}"#))
        .execute(pool)
        .await
        .expect("the row is inserted");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn rebuild_clears_the_read_model_and_the_cursor(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let runner = runner(&pool, NEVER_FAILS);
        runner.register().await.expect("registration succeeds");
        runner.tick().await.expect("the first tick succeeds");

        runner.rebuild().await.expect("the rebuild succeeds");

        assert_eq!(total(&pool, "a").await, 0, "the read model was cleared");
        assert_eq!(
            cursor_seq(&pool).await,
            0,
            "the cursor went back to the start"
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn the_tick_after_a_rebuild_replays_everything(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        insert(&pool, "a", 2, 7).await;
        let runner = runner(&pool, NEVER_FAILS);
        runner.register().await.expect("registration succeeds");
        runner.tick().await.expect("the first tick succeeds");
        runner.rebuild().await.expect("the rebuild succeeds");

        let outcome = runner.tick().await.expect("the replay tick succeeds");

        assert!(matches!(outcome, Tick::Applied(2)), "got {outcome:?}");
        assert_eq!(
            total(&pool, "a").await,
            12,
            "the total is rebuilt, not doubled"
        );
    }

    /// The mirror of `a_second_runner_declines_while_the_row_is_held`. A tick
    /// skips a held checkpoint row; a rebuild must wait for it, or it would
    /// clear a read model another instance is still writing into. The proof is
    /// that the call does *not* finish while the row is held: under
    /// `SKIP LOCKED` the claim would return no row and the rebuild would
    /// return promptly instead of elapsing.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn rebuild_waits_for_a_held_checkpoint_row(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let runner = runner(&pool, NEVER_FAILS);
        runner.register().await.expect("registration succeeds");
        runner.tick().await.expect("the first tick succeeds");
        let mut holder = pool.begin().await.expect("a transaction begins");
        sqlx::query("SELECT 1 FROM projection_checkpoints WHERE projection = 'totals' FOR UPDATE")
            .fetch_one(&mut *holder)
            .await
            .expect("the row is locked");

        let blocked = timeout(Duration::from_millis(500), runner.rebuild()).await;
        assert!(
            blocked.is_err(),
            "the rebuild returned while another transaction held the checkpoint \
             row, which means its claim query no longer waits: {blocked:?}"
        );

        holder.rollback().await.expect("the holder releases");

        // Bounded like the tick's own held-row test: a rebuild that never
        // acquires the freed lock should fail this run rather than hang it.
        timeout(Duration::from_secs(5), runner.rebuild())
            .await
            .expect("the rebuild acquires the freed lock")
            .expect("the rebuild succeeds");

        assert_eq!(total(&pool, "a").await, 0, "the read model was cleared");
        assert_eq!(
            cursor_seq(&pool).await,
            0,
            "the cursor went back to the start"
        );
    }

    /// An unregistered projection has no checkpoint row, and `FOR UPDATE` over
    /// zero rows takes no lock, so a rebuild there would clear the read model
    /// with nothing serialising it against a concurrent tick. It must refuse.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn rebuild_refuses_an_unregistered_projection(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        // A row that arrived by some other route: a rebuild that proceeded
        // would delete it, having locked nothing.
        sqlx::query("INSERT INTO totals (stream_id, total) VALUES ('a', 5)")
            .execute(&pool)
            .await
            .expect("the row is inserted");
        let runner = runner(&pool, NEVER_FAILS);

        let error = runner.rebuild().await.expect_err("the rebuild refuses");

        assert!(
            matches!(error, super::RunnerError::Store(_)),
            "got {error:?}"
        );
        assert!(
            format!("{error}").contains("totals"),
            "the error names the projection: {error}"
        );
        assert_eq!(total(&pool, "a").await, 5, "the read model is untouched");
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn rebuild_clears_a_halt(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let failing = runner(&pool, 0);
        failing.register().await.expect("registration succeeds");
        failing.tick().await.expect_err("the first attempt fails");
        failing.tick().await.expect_err("the second attempt fails");

        failing.rebuild().await.expect("the rebuild succeeds");

        assert!(!halted(&pool).await, "the halt is cleared");
        assert_eq!(failures(&pool).await, 0, "the counter reset");
    }
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_delayed_failure_cannot_follow_success(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let failing = runner(&pool, 0);
        failing.register().await.unwrap();
        let mut revision = None;
        failing.try_tick(&mut revision).await.unwrap_err();
        runner(&pool, NEVER_FAILS).tick().await.unwrap();
        failing
            .record_failure(revision.unwrap(), "delayed failure")
            .await
            .unwrap();
        assert_eq!(failures(&pool).await, 0);
        assert!(!halted(&pool).await);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_delayed_failure_cannot_undo_resume(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let failing = runner(&pool, 0);
        failing.register().await.unwrap();
        let mut revision = None;
        failing.try_tick(&mut revision).await.unwrap_err();
        failing.resume().await.unwrap();
        failing
            .record_failure(revision.unwrap(), "delayed failure")
            .await
            .unwrap();
        assert_eq!(failures(&pool).await, 0);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn a_delayed_failure_cannot_undo_rebuild(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let failing = runner(&pool, 0);
        failing.register().await.unwrap();
        let mut revision = None;
        failing.try_tick(&mut revision).await.unwrap_err();
        failing.rebuild().await.unwrap();
        failing
            .record_failure(revision.unwrap(), "delayed failure")
            .await
            .unwrap();
        assert_eq!(failures(&pool).await, 0);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn feed_infrastructure_errors_do_not_consume_the_failure_budget(pool: PgPool) {
        let runner = runner(&pool, NEVER_FAILS);
        runner.register().await.unwrap();
        sqlx::query("ALTER TABLE events RENAME TO temporarily_unavailable_events")
            .execute(&pool)
            .await
            .unwrap();
        for _ in 0..3 {
            runner.tick().await.unwrap_err();
        }
        assert_eq!(failures(&pool).await, 0);
        assert!(!halted(&pool).await);
    }
    #[derive(Debug)]
    struct TransientTotals(Totals);
    impl Projection for TransientTotals {
        type Id = TestStreamId;
        type Event = TestEvent;
        type Connection = PgConnection;
        type Error = TotalsError;
        fn name(&self) -> &'static str {
            self.0.name()
        }
        fn is_retryable(&self, _: &TotalsError) -> bool {
            true
        }
        async fn apply(
            &self,
            connection: &mut PgConnection,
            event: &Envelope<TestStreamId, TestEvent>,
        ) -> Result<(), TotalsError> {
            self.0.apply(connection, event).await
        }
        async fn reset(&self, connection: &mut PgConnection) -> Result<(), TotalsError> {
            self.0.reset(connection).await
        }
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn retryable_projection_errors_leave_the_budget_available(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let transient = ProjectionRunner::new(
            TransientTotals(Totals::new(0)),
            pool.clone(),
            RunnerConfig::default(),
        )
        .unwrap();
        transient.register().await.unwrap();
        for _ in 0..6 {
            transient.tick().await.unwrap_err();
        }
        assert_eq!(failures(&pool).await, 0);
        assert!(!halted(&pool).await);
        assert_eq!(
            runner(&pool, NEVER_FAILS).tick().await.unwrap(),
            Tick::Applied(1)
        );
        assert_eq!(total(&pool, "a").await, 5);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn observer_reports_each_error_and_halt_to_the_supervisor(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let runner = runner(&pool, 0);
        let mut errors = 0;
        let mut halts = 0;
        let result = timeout(
            Duration::from_secs(5),
            runner.run_with_observer(|outcome| match outcome {
                Err(super::RunnerError::Projection(_)) => errors += 1,
                Ok(Tick::Halted) => halts += 1,
                _ => {}
            }),
        )
        .await
        .unwrap();
        assert!(matches!(
            result,
            Err(super::RunnerError::Halted {
                projection: "totals"
            })
        ));
        assert_eq!((errors, halts), (2, 1));
        assert_eq!(failures(&pool).await, 2);
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn recording_failure_preserves_the_original_error_if_the_update_fails(pool: PgPool) {
        create_read_model(&pool).await;
        insert(&pool, "a", 1, 5).await;
        let runner = runner(&pool, 0);
        runner.register().await.unwrap();
        sqlx::raw_sql("CREATE FUNCTION reject_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'failure counter unavailable'; END $$; CREATE TRIGGER reject_failure BEFORE UPDATE ON projection_checkpoints FOR EACH ROW EXECUTE FUNCTION reject_failure();")
            .execute(&pool).await.unwrap();
        let error = runner.tick().await.unwrap_err();
        match error {
            super::RunnerError::FailureRecording { failure, store } => {
                assert!(matches!(
                    *failure,
                    super::RunnerError::Projection(TotalsError)
                ));
                assert!(store.to_string().contains("failure counter unavailable"));
            }
            other => panic!("expected both errors, got {other:?}"),
        }
        assert_eq!(failures(&pool).await, 0);
    }

    #[test]
    fn backoff_saturates_before_applying_the_ceiling() {
        let config = RunnerConfig {
            initial_backoff: Duration::MAX,
            max_backoff: Duration::MAX,
            ..Default::default()
        };
        assert_eq!(config.next_backoff(Duration::MAX), Duration::MAX);
        assert_eq!(
            RunnerConfig::default().next_backoff(Duration::from_secs(20)),
            Duration::from_secs(30)
        );
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn runner_construction_rejects_invalid_configuration(pool: PgPool) {
        let config = RunnerConfig {
            batch_size: 0,
            ..Default::default()
        };
        assert!(ProjectionRunner::new(Totals::new(0), pool, config).is_err());
    }
}
