use std::convert::Infallible;
use std::fmt::Debug;
use std::sync::Mutex;
use std::{collections::HashMap, hash::Hash};

use chrono::Utc;
use uuid::Uuid;

use crate::{AppendError, Envelope, Event, EventId, EventStore, ExpectedVersion, Version};

#[derive(Debug, Default)]
pub struct InMemoryEventStore<ID, E> {
    streams: Mutex<HashMap<ID, Vec<Envelope<ID, E>>>>,
}

impl<ID, E> InMemoryEventStore<ID, E> {
    pub fn new() -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
        }
    }
}

impl<ID, E> EventStore<ID, E> for InMemoryEventStore<ID, E>
where
    ID: Clone + Eq + Hash + Debug + Send + Sync,
    E: Event + Clone,
{
    type Error = Infallible;

    async fn read(&self, stream_id: &ID) -> Result<Vec<Envelope<ID, E>>, Self::Error> {
        let streams = self.streams.lock().unwrap();
        Ok(streams.get(stream_id).cloned().unwrap_or_default())
    }

    async fn append(
        &self,
        stream_id: &ID,
        expected: crate::ExpectedVersion,
        events: Vec<crate::Proposed<E>>,
    ) -> Result<crate::Version, super::AppendError<Self::Error>> {
        let mut streams = self.streams.lock().unwrap();
        let stream = streams.entry(stream_id.clone()).or_default();

        let current = stream.last().map(|env| env.version);

        match (expected, current) {
            (ExpectedVersion::Any, _) => {}
            (ExpectedVersion::NoStream, None) => {}
            (ExpectedVersion::Exact(v), Some(cur)) if v == cur => {}
            (expected, actual) => {
                return Err(AppendError::Conflict { expected, actual });
            }
        }

        let mut version = current.unwrap_or(Version::new(0));
        for proposed in events {
            version = version.next();
            stream.push(Envelope {
                event_id: EventId::from_uuid(Uuid::now_v7()),
                stream_id: stream_id.clone(),
                version,
                event: proposed.event,
                recorded_at: Utc::now(),
                metadata: proposed.metadata,
            });
        }

        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    use super::InMemoryEventStore;
    use crate::{
        AppendError, Event, EventStore, ExpectedVersion, Metadata, Proposed, Value, Version,
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

    #[tokio::test]
    async fn unknown_stream_reads_as_empty() {
        let store = InMemoryEventStore::<String, TestEvent>::new();

        let events = store.read(&"missing".to_owned()).await.unwrap();

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn append_assigns_versions_and_preserves_envelope_data() {
        let store = InMemoryEventStore::<String, TestEvent>::new();
        let stream_id = "stream-a".to_owned();
        let mut metadata = Metadata::new();
        metadata.insert("source", Value::String("test-suite".to_owned()));
        let before = Utc::now();

        let version = store
            .append(
                &stream_id,
                ExpectedVersion::NoStream,
                vec![
                    Proposed {
                        event: TestEvent::Added(2),
                        metadata: metadata.clone(),
                    },
                    Proposed {
                        event: TestEvent::Added(3),
                        metadata: Metadata::new(),
                    },
                ],
            )
            .await
            .unwrap();
        let after = Utc::now();
        let envelopes = store.read(&stream_id).await.unwrap();

        assert_eq!(version, Version::new(2));
        assert_eq!(envelopes.len(), 2);
        assert_eq!(envelopes[0].stream_id, stream_id);
        assert_eq!(envelopes[0].version, Version::new(1));
        assert_eq!(envelopes[0].event, TestEvent::Added(2));
        assert_eq!(
            envelopes[0].metadata.get("source"),
            Some(&Value::String("test-suite".to_owned()))
        );
        assert_eq!(envelopes[1].version, Version::new(2));
        assert_eq!(envelopes[1].event, TestEvent::Added(3));
        assert_eq!(envelopes[0].event_id.as_uuid().get_version_num(), 7);
        assert_eq!(envelopes[1].event_id.as_uuid().get_version_num(), 7);
        assert!((before..=after).contains(&envelopes[0].recorded_at));
        assert!((before..=after).contains(&envelopes[1].recorded_at));
    }

    #[tokio::test]
    async fn exact_append_continues_versioning_and_streams_remain_isolated() {
        let store = InMemoryEventStore::<String, TestEvent>::new();
        let first = "first".to_owned();
        let second = "second".to_owned();
        store
            .append(
                &first,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .unwrap();
        let version = store
            .append(
                &first,
                ExpectedVersion::Exact(Version::new(1)),
                proposed(TestEvent::Added(2)),
            )
            .await
            .unwrap();
        store
            .append(
                &second,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(9)),
            )
            .await
            .unwrap();

        let first_events = store.read(&first).await.unwrap();
        let second_events = store.read(&second).await.unwrap();

        assert_eq!(version, Version::new(2));
        assert_eq!(
            first_events
                .into_iter()
                .map(|envelope| envelope.event)
                .collect::<Vec<_>>(),
            vec![TestEvent::Added(1), TestEvent::Added(2)]
        );
        assert_eq!(second_events.len(), 1);
        assert_eq!(second_events[0].version, Version::new(1));
        assert_eq!(second_events[0].event, TestEvent::Added(9));
    }

    #[tokio::test]
    async fn any_version_bypasses_the_optimistic_check() {
        let store = InMemoryEventStore::<String, TestEvent>::new();
        let stream_id = "stream".to_owned();
        store
            .append(
                &stream_id,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .unwrap();

        let version = store
            .append(
                &stream_id,
                ExpectedVersion::Any,
                proposed(TestEvent::Added(2)),
            )
            .await
            .unwrap();

        assert_eq!(version, Version::new(2));
    }

    #[tokio::test]
    async fn conflicting_appends_report_versions_without_mutating_streams() {
        let store = InMemoryEventStore::<String, TestEvent>::new();
        let existing = "existing".to_owned();
        let missing = "missing".to_owned();
        store
            .append(
                &existing,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(1)),
            )
            .await
            .unwrap();

        let no_stream = store
            .append(
                &existing,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(2)),
            )
            .await;
        let wrong_exact = store
            .append(
                &existing,
                ExpectedVersion::Exact(Version::new(7)),
                proposed(TestEvent::Added(3)),
            )
            .await;
        let absent_exact = store
            .append(
                &missing,
                ExpectedVersion::Exact(Version::new(1)),
                proposed(TestEvent::Added(4)),
            )
            .await;

        assert_conflict(no_stream, ExpectedVersion::NoStream, Some(Version::new(1)));
        assert_conflict(
            wrong_exact,
            ExpectedVersion::Exact(Version::new(7)),
            Some(Version::new(1)),
        );
        assert_conflict(absent_exact, ExpectedVersion::Exact(Version::new(1)), None);
        assert_eq!(store.read(&existing).await.unwrap().len(), 1);
        assert!(store.read(&missing).await.unwrap().is_empty());
    }

    /// An empty batch still has to satisfy `expected_version` against the
    /// stream's actual state: the match on `(expected, current)` runs before
    /// the loop that would otherwise do nothing for zero events. `NoStream`
    /// never matches a stream already at version 3, empty batch or not.
    /// `PostgresEventStore` has to answer this identical call the identical
    /// way, since both are implementations of one `EventStore` trait.
    #[tokio::test]
    async fn an_empty_batch_against_an_existing_stream_conflicts() {
        let store = InMemoryEventStore::<String, TestEvent>::new();
        let stream_id = "existing".to_owned();
        store
            .append(
                &stream_id,
                ExpectedVersion::NoStream,
                vec![
                    proposed(TestEvent::Added(1)).remove(0),
                    proposed(TestEvent::Added(2)).remove(0),
                    proposed(TestEvent::Added(3)).remove(0),
                ],
            )
            .await
            .unwrap();

        let result = store
            .append(&stream_id, ExpectedVersion::NoStream, Vec::new())
            .await;

        assert_conflict(result, ExpectedVersion::NoStream, Some(Version::new(3)));
        assert_eq!(store.read(&stream_id).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn concurrent_no_stream_appends_have_one_winner() {
        let store = InMemoryEventStore::<String, TestEvent>::new();
        let stream_id = "contended".to_owned();
        let left = store.append(
            &stream_id,
            ExpectedVersion::NoStream,
            proposed(TestEvent::Added(1)),
        );
        let right = store.append(
            &stream_id,
            ExpectedVersion::NoStream,
            proposed(TestEvent::Added(2)),
        );

        let (left, right) = tokio::join!(left, right);

        assert_eq!(
            usize::from(is_version_one(&left)) + usize::from(is_version_one(&right)),
            1
        );
        assert_eq!(
            usize::from(is_conflict(&left)) + usize::from(is_conflict(&right)),
            1
        );
        assert_eq!(store.read(&stream_id).await.unwrap().len(), 1);
    }

    fn proposed(event: TestEvent) -> Vec<Proposed<TestEvent>> {
        vec![Proposed {
            event,
            metadata: Metadata::new(),
        }]
    }

    fn assert_conflict(
        result: Result<Version, AppendError<Infallible>>,
        expected_version: ExpectedVersion,
        actual_version: Option<Version>,
    ) {
        match result {
            Err(AppendError::Conflict { expected, actual }) => {
                assert_eq!(expected, expected_version);
                assert_eq!(actual, actual_version);
            }
            other => panic!("expected conflict, got {other:?}"),
        }
    }

    fn is_version_one(result: &Result<Version, AppendError<Infallible>>) -> bool {
        matches!(result, Ok(version) if *version == Version::new(1))
    }

    fn is_conflict(result: &Result<Version, AppendError<Infallible>>) -> bool {
        matches!(result, Err(AppendError::Conflict { .. }))
    }
}
