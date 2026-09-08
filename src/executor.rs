use crate::{Aggregate, AppendError, ConfigError, EventStore, ExpectedVersion, Metadata, Version};

#[derive(Debug, thiserror::Error)]
pub enum ExecError<DomainErr, StoreErr> {
    /// The aggregate rejected the command. Business outcome, not a failure.
    #[error("command rejected: {0}")]
    Rejected(#[source] DomainErr),
    /// Retries exhausted.
    #[error("contention after {attempts} attempts")]
    Contention { attempts: u32 },
    /// Infrastructure failure from the store.
    #[error("store error: {0}")]
    Store(#[source] StoreErr),
}

#[derive(Debug)]
pub struct Executor<A, S> {
    aggregate: A,
    store: S,
    max_attempts: u32,
}

impl<A, S> Executor<A, S> {
    pub fn new(aggregate: A, store: S, max_attempts: u32) -> Result<Self, ConfigError> {
        if max_attempts == 0 {
            return Err(ConfigError::InvalidAttempts);
        }
        Ok(Self {
            aggregate,
            store,
            max_attempts,
        })
    }

    /// The store this executor writes through. Exists so a test can assert on
    /// what was appended without holding a second handle.
    pub const fn store(&self) -> &S {
        &self.store
    }
}

impl<A, S> Executor<A, S>
where
    A: Aggregate,
    A::Id: Clone,
    A::Command: Clone,
    S: EventStore<A::Id, A::Event>,
{
    pub async fn execute(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> Result<Version, ExecError<A::Error, S::Error>> {
        for _attempt in 1..=self.max_attempts {
            let envelopes = self.store.read(id).await.map_err(ExecError::Store)?;

            let expected = match envelopes.last() {
                Some(env) => ExpectedVersion::Exact(env.version),
                None => ExpectedVersion::NoStream,
            };

            let state = self
                .aggregate
                .rehydrate(envelopes.into_iter().map(|env| env.event));

            let mut proposed = self
                .aggregate
                .handle(&state, command.clone())
                .map_err(ExecError::Rejected)?;

            // The aggregate never sees the request that reached it, so the
            // caller's correlation and causation are stamped here. Anything the
            // aggregate set is left alone.
            for event in &mut proposed {
                event.metadata.fill_from(&metadata);
            }

            if proposed.is_empty() {
                return Ok(match expected {
                    ExpectedVersion::Exact(v) => v,
                    _ => Version::new(0),
                });
            }

            match self.store.append(id, expected, proposed).await {
                Ok(v) => return Ok(v),
                Err(AppendError::Conflict { .. }) => continue,
                Err(AppendError::Store(e)) => return Err(ExecError::Store(e)),
            }
        }

        Err(ExecError::Contention {
            attempts: self.max_attempts,
        })
    }

    /// The current state of one stream, and the version it was read at.
    ///
    /// A write path answers from this rather than from a projection: the
    /// projection is a catch-up read and may not carry the append that just
    /// succeeded. An empty stream answers the initial state at version zero,
    /// the same convention `execute` uses when a command proposes nothing.
    pub async fn load(&self, id: &A::Id) -> Result<(A::State, Version), S::Error> {
        let envelopes = self.store.read(id).await?;

        let version = envelopes
            .last()
            .map_or_else(|| Version::new(0), |envelope| envelope.version);

        let state = self
            .aggregate
            .rehydrate(envelopes.into_iter().map(|envelope| envelope.event));

        Ok((state, version))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use chrono::Utc;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::{ExecError, Executor};
    use crate::{
        Aggregate, AppendError, CorrelationId, Envelope, Event, EventId, EventStore,
        ExpectedVersion, InMemoryEventStore, Metadata, Proposed, Version,
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

    #[derive(Debug, Clone)]
    enum TestCommand {
        SetTotal(i64),
        Noop,
        Reject,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum DomainError {
        Rejected,
    }

    #[derive(Debug)]
    struct TestAggregate;

    impl Aggregate for TestAggregate {
        type Id = String;
        type Command = TestCommand;
        type Event = TestEvent;
        type Error = DomainError;
        type State = i64;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn apply(&self, state: Self::State, event: &Self::Event) -> Self::State {
            match event {
                TestEvent::Added(amount) => state + amount,
            }
        }

        fn handle(
            &self,
            state: &Self::State,
            command: Self::Command,
        ) -> Result<Vec<Proposed<Self::Event>>, Self::Error> {
            match command {
                TestCommand::SetTotal(total) => Ok(vec![Proposed {
                    event: TestEvent::Added(total - state),
                    metadata: Metadata::new(),
                }]),
                TestCommand::Noop => Ok(Vec::new()),
                TestCommand::Reject => Err(DomainError::Rejected),
            }
        }
    }

    #[tokio::test]
    async fn missing_stream_command_appends_first_event() {
        let executor = Executor::new(
            TestAggregate,
            InMemoryEventStore::<String, TestEvent>::new(),
            1,
        )
        .expect("valid attempts");
        let id = "aggregate".to_owned();

        let version = executor
            .execute(&id, TestCommand::SetTotal(3), Metadata::new())
            .await
            .unwrap();
        let envelopes = executor.store.read(&id).await.unwrap();

        assert_eq!(version, Version::new(1));
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].event, TestEvent::Added(3));
    }

    #[tokio::test]
    async fn existing_history_is_rehydrated_before_handling() {
        let store = InMemoryEventStore::<String, TestEvent>::new();
        let id = "aggregate".to_owned();
        store
            .append(
                &id,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(4)),
            )
            .await
            .unwrap();
        let executor = Executor::new(TestAggregate, store, 1).expect("valid attempts");

        let version = executor
            .execute(&id, TestCommand::SetTotal(10), Metadata::new())
            .await
            .unwrap();
        let envelopes = executor.store.read(&id).await.unwrap();

        assert_eq!(version, Version::new(2));
        assert_eq!(envelopes[1].event, TestEvent::Added(6));
    }

    #[tokio::test]
    async fn no_op_returns_current_version_without_appending() {
        let missing = Executor::new(
            TestAggregate,
            InMemoryEventStore::<String, TestEvent>::new(),
            1,
        )
        .expect("valid attempts");
        let missing_id = "missing".to_owned();
        let missing_version = missing
            .execute(&missing_id, TestCommand::Noop, Metadata::new())
            .await
            .unwrap();

        let store = InMemoryEventStore::<String, TestEvent>::new();
        let existing_id = "existing".to_owned();
        store
            .append(
                &existing_id,
                ExpectedVersion::NoStream,
                proposed(TestEvent::Added(4)),
            )
            .await
            .unwrap();
        let existing = Executor::new(TestAggregate, store, 1).expect("valid attempts");
        let existing_version = existing
            .execute(&existing_id, TestCommand::Noop, Metadata::new())
            .await
            .unwrap();

        assert_eq!(missing_version, Version::new(0));
        assert!(missing.store.read(&missing_id).await.unwrap().is_empty());
        assert_eq!(existing_version, Version::new(1));
        assert_eq!(existing.store.read(&existing_id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn aggregate_rejection_is_returned_without_appending() {
        let executor = Executor::new(
            TestAggregate,
            InMemoryEventStore::<String, TestEvent>::new(),
            1,
        )
        .expect("valid attempts");
        let id = "aggregate".to_owned();

        let result = executor
            .execute(&id, TestCommand::Reject, Metadata::new())
            .await;

        assert!(matches!(
            result,
            Err(ExecError::Rejected(DomainError::Rejected))
        ));
        assert!(executor.store.read(&id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_failure_is_returned_as_store_error() {
        let store = ScriptedStore::new(vec![Err(TestStoreError::Read)], Vec::new());
        let executor = Executor::new(TestAggregate, store, 1).expect("valid attempts");

        let result = executor
            .execute(
                &"aggregate".to_owned(),
                TestCommand::SetTotal(1),
                Metadata::new(),
            )
            .await;

        assert!(matches!(
            result,
            Err(ExecError::Store(TestStoreError::Read))
        ));
    }

    #[tokio::test]
    async fn append_failure_is_returned_as_store_error() {
        let store = ScriptedStore::new(
            vec![Ok(Vec::new())],
            vec![Err(AppendError::Store(TestStoreError::Append))],
        );
        let executor = Executor::new(TestAggregate, store, 1).expect("valid attempts");

        let result = executor
            .execute(
                &"aggregate".to_owned(),
                TestCommand::SetTotal(1),
                Metadata::new(),
            )
            .await;

        assert!(matches!(
            result,
            Err(ExecError::Store(TestStoreError::Append))
        ));
    }

    #[tokio::test]
    async fn conflict_retries_from_fresh_history_and_can_succeed() {
        let store = ScriptedStore::new(
            vec![Ok(Vec::new()), Ok(vec![envelope(1, TestEvent::Added(4))])],
            vec![
                Err(AppendError::Conflict {
                    expected: ExpectedVersion::NoStream,
                    actual: Some(Version::new(1)),
                }),
                Ok(Version::new(2)),
            ],
        );
        let observed = store.clone();
        let executor = Executor::new(TestAggregate, store, 2).expect("valid attempts");

        let version = executor
            .execute(
                &"aggregate".to_owned(),
                TestCommand::SetTotal(10),
                Metadata::new(),
            )
            .await
            .unwrap();
        let state = observed.state.lock().unwrap();

        assert_eq!(version, Version::new(2));
        assert_eq!(state.read_count, 2);
        assert_eq!(
            state.expected_versions,
            vec![
                ExpectedVersion::NoStream,
                ExpectedVersion::Exact(Version::new(1)),
            ]
        );
        assert_eq!(
            state.proposed_events,
            vec![vec![TestEvent::Added(10)], vec![TestEvent::Added(6)]]
        );
    }

    #[tokio::test]
    async fn exhausted_conflicts_return_configured_attempt_count() {
        let store = ScriptedStore::new(
            vec![Ok(Vec::new()), Ok(Vec::new())],
            vec![
                conflict(ExpectedVersion::NoStream, None),
                conflict(ExpectedVersion::NoStream, None),
            ],
        );
        let observed = store.clone();
        let executor = Executor::new(TestAggregate, store, 2).expect("valid attempts");

        let result = executor
            .execute(
                &"aggregate".to_owned(),
                TestCommand::SetTotal(1),
                Metadata::new(),
            )
            .await;
        let state = observed.state.lock().unwrap();

        assert!(matches!(result, Err(ExecError::Contention { attempts: 2 })));
        assert_eq!(state.read_count, 2);
        assert_eq!(state.expected_versions.len(), 2);
    }

    #[tokio::test]
    async fn the_caller_s_metadata_reaches_every_appended_event() {
        let store = InMemoryEventStore::<String, TestEvent>::new();
        let executor = Executor::new(TestAggregate, store, 3).expect("valid attempts");
        let correlation = CorrelationId::from_uuid(Uuid::now_v7());

        let mut metadata = Metadata::new();
        metadata.correlation_id = Some(correlation);

        executor
            .execute(&"stream".to_owned(), TestCommand::SetTotal(7), metadata)
            .await
            .expect("the command is accepted");

        let envelopes = executor
            .store()
            .read(&"stream".to_owned())
            .await
            .expect("the in-memory store cannot fail");

        assert!(!envelopes.is_empty());
        for envelope in envelopes {
            assert_eq!(envelope.metadata.correlation_id, Some(correlation));
        }
    }

    #[tokio::test]
    async fn loading_an_empty_stream_answers_the_initial_state_at_version_zero() {
        let executor = Executor::new(
            TestAggregate,
            InMemoryEventStore::<String, TestEvent>::new(),
            3,
        )
        .expect("valid attempts");

        let (state, version) = executor
            .load(&"absent".to_owned())
            .await
            .expect("the in-memory store cannot fail");

        assert_eq!(state, 0);
        assert_eq!(version, Version::new(0));
    }

    /// The write path answers from this, so it must see an append the projection
    /// has not caught up with yet.
    #[tokio::test]
    async fn loading_reflects_an_append_immediately() {
        let executor = Executor::new(
            TestAggregate,
            InMemoryEventStore::<String, TestEvent>::new(),
            3,
        )
        .expect("valid attempts");
        let id = "stream".to_owned();

        let version = executor
            .execute(&id, TestCommand::SetTotal(5), Metadata::new())
            .await
            .expect("the command is accepted");

        let (state, loaded) = executor.load(&id).await.expect("the store cannot fail");

        assert_eq!(state, 5);
        assert_eq!(loaded, version);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestStoreError {
        Read,
        Append,
    }

    type ReadResult = Result<Vec<Envelope<String, TestEvent>>, TestStoreError>;
    type AppendResult = Result<Version, AppendError<TestStoreError>>;

    #[derive(Debug)]
    struct ScriptedState {
        reads: VecDeque<ReadResult>,
        appends: VecDeque<AppendResult>,
        read_count: usize,
        expected_versions: Vec<ExpectedVersion>,
        proposed_events: Vec<Vec<TestEvent>>,
    }

    #[derive(Debug, Clone)]
    struct ScriptedStore {
        state: Arc<Mutex<ScriptedState>>,
    }

    impl ScriptedStore {
        fn new(reads: Vec<ReadResult>, appends: Vec<AppendResult>) -> Self {
            Self {
                state: Arc::new(Mutex::new(ScriptedState {
                    reads: reads.into(),
                    appends: appends.into(),
                    read_count: 0,
                    expected_versions: Vec::new(),
                    proposed_events: Vec::new(),
                })),
            }
        }
    }

    impl EventStore<String, TestEvent> for ScriptedStore {
        type Error = TestStoreError;

        async fn read(
            &self,
            _stream_id: &String,
        ) -> Result<Vec<Envelope<String, TestEvent>>, Self::Error> {
            let mut state = self.state.lock().unwrap();
            state.read_count += 1;
            state.reads.pop_front().expect("unexpected read")
        }

        async fn append(
            &self,
            _stream_id: &String,
            expected_version: ExpectedVersion,
            events: Vec<Proposed<TestEvent>>,
        ) -> Result<Version, AppendError<Self::Error>> {
            let mut state = self.state.lock().unwrap();
            state.expected_versions.push(expected_version);
            state
                .proposed_events
                .push(events.into_iter().map(|proposed| proposed.event).collect());
            state.appends.pop_front().expect("unexpected append")
        }
    }

    fn proposed(event: TestEvent) -> Vec<Proposed<TestEvent>> {
        vec![Proposed {
            event,
            metadata: Metadata::new(),
        }]
    }

    fn envelope(version: u64, event: TestEvent) -> Envelope<String, TestEvent> {
        Envelope {
            event_id: EventId::from_uuid(Uuid::from_u128(version as u128)),
            stream_id: "aggregate".to_owned(),
            version: Version::new(version),
            event,
            recorded_at: Utc::now(),
            metadata: Metadata::new(),
        }
    }

    fn conflict(
        expected: ExpectedVersion,
        actual: Option<Version>,
    ) -> Result<Version, AppendError<TestStoreError>> {
        Err(AppendError::Conflict { expected, actual })
    }
}
