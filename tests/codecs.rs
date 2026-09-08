#![cfg(feature = "postgres")]

use duovium::{CodecError, Event, EventCodec, JsonEventCodec};
use serde_json::{Value, json};

// The custom codec also permits durable events without Serde implementations.
#[derive(Debug, Clone, PartialEq)]
struct Deposited {
    cents: i64,
}
impl Event for Deposited {
    fn name(&self) -> &'static str {
        "account.deposited"
    }
    fn version(&self) -> u32 {
        2
    }
}

#[derive(Debug)]
struct AccountCodec;
impl EventCodec<Deposited> for AccountCodec {
    fn encode(&self, event: &Deposited) -> Result<Value, CodecError> {
        Ok(json!({"minor_units": event.cents}))
    }
    fn decode(&self, name: &str, version: u32, payload: Value) -> Result<Deposited, CodecError> {
        match (name, version) {
            ("account.deposited", 1) => {
                let dollars = payload["dollars"]
                    .as_i64()
                    .ok_or_else(|| CodecError("missing dollars".into()))?;
                Ok(Deposited {
                    cents: dollars * 100,
                })
            }
            ("account.deposited", 2) => Ok(Deposited {
                cents: payload["minor_units"]
                    .as_i64()
                    .ok_or_else(|| CodecError("missing minor_units".into()))?,
            }),
            _ => Err(CodecError("unsupported event schema".into())),
        }
    }
}

#[test]
fn the_default_codec_round_trips_a_stable_schema() {
    let current = 123_i64;
    let decoded: i64 = JsonEventCodec
        .decode("number", 1, JsonEventCodec.encode(&current).unwrap())
        .unwrap();
    assert_eq!(decoded, current);
}

#[test]
fn a_codec_upcasts_historical_payloads_and_rejects_unknown_versions() {
    assert_eq!(
        AccountCodec
            .decode("account.deposited", 1, json!({"dollars": 3}))
            .unwrap(),
        Deposited { cents: 300 }
    );
    assert!(
        AccountCodec
            .decode("account.deposited", 99, json!({"cents": 3}))
            .is_err()
    );
    let current = Deposited { cents: 123 };
    assert_eq!(
        AccountCodec
            .decode(
                current.name(),
                current.version(),
                AccountCodec.encode(&current).unwrap()
            )
            .unwrap(),
        current
    );
}

#[cfg(feature = "postgres-tests")]
mod database {
    use super::*;
    use duovium::{
        Cursor, Envelope, EventFeed, EventStore, ExpectedVersion, InvalidStreamId, Metadata,
        PostgresEventStore, Projection, ProjectionRunner, Proposed, RunnerConfig, StreamId, Tick,
        Version,
    };

    #[derive(Debug, Clone)]
    struct AccountId;
    impl StreamId for AccountId {
        fn stream_type() -> &'static str {
            "account"
        }
        fn to_key(&self) -> String {
            "one".into()
        }
        fn from_key(_: &str) -> Result<Self, InvalidStreamId> {
            Ok(Self)
        }
    }

    #[derive(Debug)]
    struct Balance;
    impl Projection for Balance {
        type Id = AccountId;
        type Event = Deposited;
        type Connection = sqlx::PgConnection;
        type Error = sqlx::Error;
        fn name(&self) -> &'static str {
            "balance"
        }
        fn is_retryable(&self, _: &sqlx::Error) -> bool {
            true
        }
        async fn apply(
            &self,
            connection: &mut sqlx::PgConnection,
            envelope: &Envelope<AccountId, Deposited>,
        ) -> Result<(), sqlx::Error> {
            sqlx::query("UPDATE balance SET cents = cents + $1")
                .bind(envelope.event.cents)
                .execute(connection)
                .await?;
            Ok(())
        }
        async fn reset(&self, connection: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
            sqlx::query("UPDATE balance SET cents = 0")
                .execute(connection)
                .await?;
            Ok(())
        }
    }

    #[sqlx::test(migrator = "duovium::MIGRATOR")]
    async fn store_and_feed_both_use_the_configured_codec(pool: sqlx::PgPool) {
        sqlx::query("INSERT INTO events (event_id, stream_type, stream_id, version, event_name, event_version, payload, recorded_at) VALUES ($1, 'account', 'one', 1, 'account.deposited', 1, '{\"dollars\": 3}', now())")
            .bind(uuid::Uuid::now_v7()).execute(&pool).await.unwrap();
        let store =
            PostgresEventStore::<AccountId, Deposited, _>::with_codec(pool.clone(), AccountCodec);
        assert_eq!(store.read(&AccountId).await.unwrap()[0].event.cents, 300);
        let feed = EventFeed::<AccountId, Deposited, _>::with_codec(AccountCodec);
        let mut connection = pool.acquire().await.unwrap();
        assert_eq!(
            feed.next_batch(&mut connection, Cursor::START, 10)
                .await
                .unwrap()[0]
                .envelope
                .event
                .cents,
            300
        );
        assert!(
            feed.next_batch(&mut connection, Cursor::START, 0)
                .await
                .is_err()
        );
        drop(connection);
        store
            .append(
                &AccountId,
                ExpectedVersion::Exact(Version::new(1)),
                vec![Proposed {
                    event: Deposited { cents: 25 },
                    metadata: Metadata::new(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(store.read(&AccountId).await.unwrap()[1].event.cents, 25);
        sqlx::raw_sql(
            "CREATE TABLE balance (cents BIGINT NOT NULL); INSERT INTO balance VALUES (0);",
        )
        .execute(&pool)
        .await
        .unwrap();
        let runner = ProjectionRunner::with_codec(
            Balance,
            pool.clone(),
            RunnerConfig::default(),
            AccountCodec,
        )
        .unwrap();
        runner.register().await.unwrap();
        assert_eq!(runner.tick().await.unwrap(), Tick::Applied(2));
        let balance: i64 = sqlx::query_scalar("SELECT cents FROM balance")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(balance, 325);
    }
}
