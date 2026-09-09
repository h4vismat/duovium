use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::{
    AppendError, Event, EventStore, ExpectedVersion, InvalidStreamId, Metadata, PostgresEventStore,
    Proposed, StoreError, StreamId, Value, Version,
};

#[derive(Debug, Clone)]
struct Id(&'static str);

impl StreamId for Id {
    fn stream_type() -> &'static str {
        "transactional-test"
    }
    fn to_key(&self) -> String {
        self.0.into()
    }
    fn from_key(key: &str) -> Result<Self, InvalidStreamId> {
        match key {
            "a" => Ok(Self("a")),
            "b" => Ok(Self("b")),
            _ => Err(InvalidStreamId(key.into())),
        }
    }
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn concurrent_outer_transactions_have_one_committed_winner(pool: PgPool) {
    let store = PostgresEventStore::<Id, Added>::new(pool.clone());
    let mut left = pool.begin().await.unwrap();
    let mut right = pool.begin().await.unwrap();
    let (a, b) = tokio::join!(
        async {
            let result = store
                .append_in(&mut left, &Id("a"), ExpectedVersion::NoStream, events(3))
                .await;
            left.commit().await.unwrap();
            result
        },
        async {
            let result = store
                .append_in(&mut right, &Id("a"), ExpectedVersion::NoStream, events(5))
                .await;
            right.commit().await.unwrap();
            result
        },
    );
    assert!(
        matches!(
            (&a, &b),
            (Ok(_), Err(AppendError::Conflict { .. })) | (Err(AppendError::Conflict { .. }), Ok(_))
        ),
        "left={a:?}, right={b:?}"
    );
    let committed = store.read(&Id("a")).await.unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(
        committed[0].event,
        if a.is_ok() { Added(3) } else { Added(5) }
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn cancelling_a_partly_written_append_rolls_back_its_savepoint(pool: PgPool) {
    outbox(&pool).await;
    sqlx::raw_sql("CREATE FUNCTION slow_second_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.payload = '8'::jsonb THEN PERFORM pg_sleep(1); END IF; RETURN NEW; END $$; CREATE TRIGGER slow_event BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION slow_second_event();")
        .execute(&pool).await.unwrap();
    let store = PostgresEventStore::<Id, Added>::new(pool.clone());
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO outbox VALUES (7)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let batch = events(3).into_iter().chain(events(8)).collect();
    {
        let append = store.append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, batch);
        tokio::pin!(append);
        // Observe the second INSERT sleeping before cancelling; the first has
        // already run, so omitting savepoint rollback would leak Added(3).
        let observe = async {
            loop {
                let sleeping: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='PgSleep')")
                    .bind(pid).fetch_one(&pool).await.unwrap();
                if sleeping {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::select! {
            result = &mut append => panic!("append finished before cancellation: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(5), observe) => result.unwrap(),
        }
    }
    assert!(store.read_in(&mut tx, &Id("a")).await.unwrap().is_empty());
    tx.commit().await.unwrap();
    assert!(store.read(&Id("a")).await.unwrap().is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT id FROM outbox")
            .fetch_one(&pool)
            .await
            .unwrap(),
        7
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn transactional_methods_use_the_configured_codec(pool: PgPool) {
    use crate::{CodecError, EventCodec};

    #[derive(Debug)]
    struct Codec;
    impl EventCodec<Added> for Codec {
        fn encode(&self, event: &Added) -> Result<serde_json::Value, CodecError> {
            Ok(serde_json::json!({"amount": event.0}))
        }
        fn decode(
            &self,
            name: &str,
            version: u32,
            payload: serde_json::Value,
        ) -> Result<Added, CodecError> {
            if name != "transactional.added" || version != 1 {
                return Err(CodecError("unsupported schema".into()));
            }
            payload["amount"]
                .as_i64()
                .map(Added)
                .ok_or_else(|| CodecError("missing amount".into()))
        }
    }
    let store = PostgresEventStore::<Id, Added, _>::with_codec(pool.clone(), Codec);
    let mut tx = pool.begin().await.unwrap();
    store
        .append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, events(3))
        .await
        .unwrap();
    assert_eq!(
        store.read_in(&mut tx, &Id("a")).await.unwrap()[0].event,
        Added(3)
    );
    tx.commit().await.unwrap();
    assert_eq!(store.read(&Id("a")).await.unwrap()[0].event, Added(3));
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>("SELECT payload FROM events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        serde_json::json!({"amount":3})
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn codec_rejection_rolls_back_only_the_failed_batch(pool: PgPool) {
    use crate::{CodecError, EventCodec, JsonEventCodec};

    #[derive(Debug)]
    struct RejectEight;
    impl EventCodec<Added> for RejectEight {
        fn encode(&self, event: &Added) -> Result<serde_json::Value, CodecError> {
            JsonEventCodec.encode(event)
        }
        fn decode(
            &self,
            name: &str,
            version: u32,
            payload: serde_json::Value,
        ) -> Result<Added, CodecError> {
            let event: Added = JsonEventCodec.decode(name, version, payload)?;
            if event.0 == 8 {
                Err(CodecError("unreadable second event".into()))
            } else {
                Ok(event)
            }
        }
    }

    outbox(&pool).await;
    let store = PostgresEventStore::<Id, Added, _>::with_codec(pool.clone(), RejectEight);
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO outbox VALUES (7)")
        .execute(&mut *tx)
        .await
        .unwrap();
    store
        .append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, events(3))
        .await
        .unwrap();
    let batch = events(5).into_iter().chain(events(8)).collect();
    assert!(matches!(
        store
            .append_in(
                &mut tx,
                &Id("a"),
                ExpectedVersion::Exact(Version::new(1)),
                batch
            )
            .await,
        Err(AppendError::Store(StoreError::Other(_)))
    ));
    tx.commit().await.unwrap();
    let committed = store.read(&Id("a")).await.unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].event, Added(3));
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT id FROM outbox")
            .fetch_one(&pool)
            .await
            .unwrap(),
        7
    );
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Added(i64);

impl Event for Added {
    fn name(&self) -> &'static str {
        "transactional.added"
    }
    fn version(&self) -> u32 {
        1
    }
}

fn events(amount: i64) -> Vec<Proposed<Added>> {
    vec![Proposed {
        event: Added(amount),
        metadata: Metadata::new(),
    }]
}

async fn outbox(pool: &PgPool) {
    sqlx::query("CREATE TABLE outbox (id INTEGER PRIMARY KEY)")
        .execute(pool)
        .await
        .unwrap();
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn multiple_streams_and_application_writes_wait_for_outer_commit(pool: PgPool) {
    outbox(&pool).await;
    let store = PostgresEventStore::<Id, Added>::new(pool.clone());
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO outbox VALUES (7)")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        store
            .append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, events(3))
            .await
            .unwrap(),
        Version::new(1)
    );
    store
        .append_in(&mut tx, &Id("b"), ExpectedVersion::NoStream, events(5))
        .await
        .unwrap();
    store
        .append_in(
            &mut tx,
            &Id("a"),
            ExpectedVersion::Exact(Version::new(1)),
            events(8),
        )
        .await
        .unwrap();

    let own = store.read_in(&mut tx, &Id("a")).await.unwrap();
    assert_eq!(
        own.iter().map(|e| e.event.0).collect::<Vec<_>>(),
        vec![3, 8]
    );
    assert!(store.read(&Id("a")).await.unwrap().is_empty());
    assert!(store.read(&Id("b")).await.unwrap().is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM outbox")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );

    tx.commit().await.unwrap();
    let committed = store.read(&Id("a")).await.unwrap();
    assert_eq!(
        committed
            .iter()
            .map(|e| e.version.get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        committed.iter().map(|e| e.event.0).collect::<Vec<_>>(),
        vec![3, 8]
    );
    assert_eq!(store.read(&Id("b")).await.unwrap()[0].event, Added(5));
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT id FROM outbox")
            .fetch_one(&pool)
            .await
            .unwrap(),
        7
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn outer_rollback_removes_all_stream_and_application_writes(pool: PgPool) {
    outbox(&pool).await;
    let store = PostgresEventStore::<Id, Added>::new(pool.clone());
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO outbox VALUES (7)")
        .execute(&mut *tx)
        .await
        .unwrap();
    for id in [Id("a"), Id("b")] {
        store
            .append_in(&mut tx, &id, ExpectedVersion::NoStream, events(3))
            .await
            .unwrap();
    }
    tx.rollback().await.unwrap();
    assert!(store.read(&Id("a")).await.unwrap().is_empty());
    assert!(store.read(&Id("b")).await.unwrap().is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM outbox")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn invalid_second_event_leaves_no_partial_append_when_caller_commits(pool: PgPool) {
    outbox(&pool).await;
    let store = PostgresEventStore::<Id, Added>::new(pool.clone());
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO outbox VALUES (7)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let mut batch = events(3);
    let mut invalid = events(8).remove(0);
    invalid.metadata.insert("bad", Value::F64(f64::NAN));
    batch.push(invalid);
    assert!(matches!(
        store
            .append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, batch)
            .await,
        Err(AppendError::Store(_))
    ));
    assert!(store.read_in(&mut tx, &Id("a")).await.unwrap().is_empty());
    tx.commit().await.unwrap();
    assert!(store.read(&Id("a")).await.unwrap().is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT id FROM outbox")
            .fetch_one(&pool)
            .await
            .unwrap(),
        7
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn collision_on_second_insert_rolls_back_batch_and_preserves_outer_work(pool: PgPool) {
    outbox(&pool).await;
    // A deliberately sparse fixture makes NoStream collide on its second INSERT.
    sqlx::query("INSERT INTO events (event_id,stream_type,stream_id,version,event_name,event_version,payload,recorded_at) VALUES (gen_random_uuid(),'transactional-test','a',2,'transactional.added',1,'99',now())")
        .execute(&pool).await.unwrap();
    let store = PostgresEventStore::<Id, Added>::new(pool.clone());
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO outbox VALUES (7)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let batch = events(3).into_iter().chain(events(8)).collect();
    assert!(
        matches!(store.append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, batch).await,
        Err(AppendError::Conflict { expected: ExpectedVersion::NoStream, actual: Some(v) }) if v.get() == 2)
    );
    store
        .append_in(&mut tx, &Id("b"), ExpectedVersion::NoStream, events(5))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let a = store.read(&Id("a")).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].event, Added(99));
    assert_eq!(a[0].version, Version::new(2));
    assert_eq!(store.read(&Id("b")).await.unwrap()[0].event, Added(5));
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT id FROM outbox")
            .fetch_one(&pool)
            .await
            .unwrap(),
        7
    );
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn reads_and_conflict_recovery_need_only_the_callers_connection(pool: PgPool) {
    let single = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(250))
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let store = PostgresEventStore::<Id, Added>::new(single.clone());
    let mut tx = single.begin().await.unwrap();
    store
        .append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, events(3))
        .await
        .unwrap();
    assert!(
        matches!(store.append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, events(5)).await,
        Err(AppendError::Conflict { actual: Some(v), .. }) if v.get() == 1)
    );
    assert_eq!(
        store.read_in(&mut tx, &Id("a")).await.unwrap()[0].event,
        Added(3)
    );
    tx.commit().await.unwrap();
    single.close().await;
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn empty_append_checks_uncommitted_version_without_creating_stream(pool: PgPool) {
    let store = PostgresEventStore::<Id, Added>::new(pool.clone());
    let mut tx = pool.begin().await.unwrap();
    assert!(matches!(
        store
            .append_in(
                &mut tx,
                &Id("a"),
                ExpectedVersion::Exact(Version::new(0)),
                vec![]
            )
            .await,
        Err(AppendError::Conflict { actual: None, .. })
    ));
    assert_eq!(
        store
            .append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, vec![])
            .await
            .unwrap(),
        Version::new(0)
    );
    store
        .append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, events(3))
        .await
        .unwrap();
    assert!(
        matches!(store.append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, vec![]).await,
        Err(AppendError::Conflict { actual: Some(v), .. }) if v.get() == 1)
    );
    assert_eq!(
        store
            .append_in(
                &mut tx,
                &Id("a"),
                ExpectedVersion::Exact(Version::new(1)),
                vec![]
            )
            .await
            .unwrap(),
        Version::new(1)
    );
    tx.commit().await.unwrap();
    assert_eq!(store.read(&Id("a")).await.unwrap().len(), 1);
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn unrelated_sql_error_is_not_a_version_conflict(pool: PgPool) {
    sqlx::query("ALTER TABLE events ADD CONSTRAINT reject_eight CHECK (payload <> '8'::jsonb)")
        .execute(&pool)
        .await
        .unwrap();
    let store = PostgresEventStore::<Id, Added>::new(pool.clone());
    let mut tx = pool.begin().await.unwrap();
    let batch = events(3).into_iter().chain(events(8)).collect();
    assert!(matches!(
        store
            .append_in(&mut tx, &Id("a"), ExpectedVersion::NoStream, batch)
            .await,
        Err(AppendError::Store(StoreError::Other(_)))
    ));
    store
        .append_in(&mut tx, &Id("b"), ExpectedVersion::NoStream, events(5))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(store.read(&Id("a")).await.unwrap().is_empty());
    assert_eq!(store.read(&Id("b")).await.unwrap()[0].event, Added(5));
}
