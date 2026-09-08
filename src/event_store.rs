use std::fmt::Debug;

use crate::{Envelope, ExpectedVersion, Proposed, Version};

mod memory;
pub use memory::*;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::*;

#[derive(Debug, thiserror::Error)]
pub enum AppendError<E> {
    #[error("stream version conflict: expected {expected:?}, actual {actual:?}")]
    Conflict {
        expected: ExpectedVersion,
        actual: Option<Version>,
    },
    #[error("store error: {0}")]
    Store(#[source] E),
}

/// Both methods are written as an explicit `impl Future` rather than an
/// `async fn` so that `Send` is part of the contract. A bare `async fn` in a
/// trait promises nothing about the future, so a caller that is generic over
/// the store cannot hand the result to a runtime that moves work between
/// threads. `Commands` is exactly that caller: it boxes an executor's future
/// as `dyn Future + Send`, and only the declaration can supply the guarantee,
/// because a generic caller cannot bound a return type it cannot name. An
/// implementor still writes `async fn`.
pub trait EventStore<ID, E>: Send + Sync {
    type Error: Debug + Send + Sync;

    /// Returns the complete committed stream in ascending, consecutive version
    /// order, starting at one. A missing stream returns an empty vector. The
    /// result must represent one consistent view and contain no partial batch.
    fn read(
        &self,
        stream_id: &ID,
    ) -> impl Future<Output = Result<Vec<Envelope<ID, E>>, Self::Error>> + Send;

    /// Appends the entire batch atomically in input order, assigning consecutive
    /// versions and unique event IDs. Checks the expectation even for an empty
    /// batch. `NoStream` requires an absent stream; `Exact(0)` does not mean absent.
    /// `Any` chooses the current version, but may still conflict under concurrent
    /// writes; callers may retry. Empty accepted batches return the current
    /// version, or zero for an absent stream.
    ///
    /// A reported optimistic conflict appends nothing. A transport error during
    /// commit can have an unknown outcome; callers must not blindly retry a
    /// non-idempotent command. This interface does not provide request deduplication.
    fn append(
        &self,
        stream_id: &ID,
        expected_version: ExpectedVersion,
        events: Vec<Proposed<E>>,
    ) -> impl Future<Output = Result<Version, AppendError<Self::Error>>> + Send;
}
