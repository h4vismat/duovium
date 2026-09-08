//! The one way an application dispatches a command.
//!
//! `Executor<A, S>` cannot be used behind `dyn`, because `EventStore`'s methods
//! return an unnameable `impl Future`, which is not dyn-compatible. Erasing it
//! is therefore unavoidable — but it is erased here, once, keyed by the
//! aggregate, so no domain writes a dispatch port of its own.

use std::fmt::Debug;

use async_trait::async_trait;

use crate::{Aggregate, EventStore, ExecError, Executor, Metadata, StoreError, Version};

#[async_trait]
pub trait Commands<A: Aggregate>: Debug + Send + Sync {
    /// Dispatches one command at one stream.
    ///
    /// `metadata` is the caller's request context. The aggregate never
    /// invents an id, and anything it did set survives — see
    /// `Metadata::fill_from`.
    ///
    /// A `Rejected` result is the aggregate's own answer and a business
    /// outcome. `Contention` means optimistic retries were exhausted. A store
    /// failure requires classification: a connection loss during commit may
    /// have an unknown outcome, so blindly repeating a command can duplicate it.
    async fn execute(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> Result<Version, ExecError<A::Error, StoreError>>;

    /// Authoritative state, rehydrated from the stream.
    ///
    /// A write path answers from this. A projection is a catch-up read and can
    /// lag the append that just succeeded.
    async fn load(&self, id: &A::Id) -> Result<(A::State, Version), StoreError>;
}

/// Every executor is a `Commands`, whatever store it writes through, as long
/// as the store's error reaches this kernel's own. The bound is `Into` rather
/// than equality so the in-memory store, whose error is `Infallible`, is
/// usable in a test fixture without a second implementation of this trait.
#[async_trait]
impl<A, S> Commands<A> for Executor<A, S>
where
    A: Aggregate,
    A::Id: Clone + Send + Sync,
    A::Command: Clone + Send,
    A::Error: Send,
    A::State: Send,
    S: EventStore<A::Id, A::Event> + Debug + Send + Sync,
    S::Error: Into<StoreError> + Send,
{
    async fn execute(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: Metadata,
    ) -> Result<Version, ExecError<A::Error, StoreError>> {
        Executor::execute(self, id, command, metadata)
            .await
            .map_err(|error| match error {
                ExecError::Rejected(rejection) => ExecError::Rejected(rejection),
                ExecError::Contention { attempts } => ExecError::Contention { attempts },
                ExecError::Store(store) => ExecError::Store(store.into()),
            })
    }

    async fn load(&self, id: &A::Id) -> Result<(A::State, Version), StoreError> {
        Executor::load(self, id).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};

    use super::Commands;
    use crate::{
        Aggregate, Event, Executor, InMemoryEventStore, Metadata, Proposed, StoreError, Version,
    };

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum CounterEvent {
        Added(i64),
    }

    impl Event for CounterEvent {
        fn name(&self) -> &'static str {
            "counter.added"
        }

        fn version(&self) -> u32 {
            1
        }
    }

    #[derive(Debug, Clone)]
    enum CounterCommand {
        Add(i64),
    }

    #[derive(Debug)]
    struct CounterAggregate;

    impl Aggregate for CounterAggregate {
        type Command = CounterCommand;
        type Error = StoreError;
        type Event = CounterEvent;
        type Id = String;
        type State = i64;

        fn initial_state(&self) -> Self::State {
            0
        }

        fn apply(&self, state: Self::State, event: &Self::Event) -> Self::State {
            match event {
                CounterEvent::Added(value) => state + value,
            }
        }

        fn handle(
            &self,
            _state: &Self::State,
            command: Self::Command,
        ) -> Result<Vec<Proposed<Self::Event>>, Self::Error> {
            match command {
                CounterCommand::Add(value) => Ok(vec![Proposed {
                    event: CounterEvent::Added(value),
                    metadata: Metadata::new(),
                }]),
            }
        }
    }

    /// The point of the trait: an executor over any store reaches a caller as
    /// one concrete type, so `AppState` needs no type parameter.
    #[tokio::test]
    async fn an_in_memory_executor_satisfies_the_port_behind_dyn() {
        let commands: Arc<dyn Commands<CounterAggregate>> = Arc::new(
            Executor::new(
                CounterAggregate,
                InMemoryEventStore::<String, CounterEvent>::new(),
                3,
            )
            .expect("valid attempts"),
        );
        let id = "counter".to_owned();

        let version = commands
            .execute(&id, CounterCommand::Add(4), Metadata::new())
            .await
            .expect("the command is accepted");
        let (state, loaded) = commands.load(&id).await.expect("the store cannot fail");

        assert_eq!(state, 4);
        assert_eq!(loaded, version);
        assert_eq!(version, Version::new(1));
    }
}
