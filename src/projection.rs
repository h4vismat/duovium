use std::fmt::Debug;
use std::future::Future;

use crate::{Envelope, Event, StreamId};

#[cfg(feature = "postgres")]
mod feed;
#[cfg(feature = "postgres")]
mod runner;

#[cfg(feature = "postgres")]
pub use feed::*;
#[cfg(feature = "postgres")]
pub use runner::*;

/// A read model built by folding one aggregate's events into storage.
///
/// The trait names no database type. `Connection` is whatever the driving
/// runner hands over, and `Error` is whatever the projection's own writes can
/// fail with, so this file compiles with no database dependency. The Postgres
/// runner binds `Connection = PgConnection` and hands over its open
/// transaction, which is what makes a read-model write and the cursor advance
/// commit together.
pub trait Projection: Debug + Send + Sync {
    type Id: StreamId;
    type Event: Event;
    /// Whatever the driving runner writes through.
    type Connection: Send;
    type Error: Debug + Send + Sync;

    /// Classifies infrastructure failures produced by `apply`. Return true
    /// for errors that should be retried without consuming the poison-event
    /// budget (for example database connectivity or serialization failures).
    /// The default treats an error as a rejected event. Implementations that
    /// write to a database should classify their database errors explicitly.
    fn is_retryable(&self, _error: &Self::Error) -> bool {
        false
    }

    /// Keys the checkpoint row. Must be unique across every projection: two
    /// that return the same name share one cursor and corrupt each other, and
    /// nothing detects it.
    fn name(&self) -> &'static str;

    /// Applies one event through the supplied connection. The PostgreSQL runner
    /// commits these writes and its checkpoint atomically. Failed batches may
    /// invoke this method again after their database effects have rolled back.
    /// Keep all durable effects in that transaction: external calls and local
    /// mutable state cannot be rolled back by the runner.
    ///
    /// Additive updates are supported; per-event deduplication is not required
    /// for this runner. A rebuild resets the read model before replaying events.
    /// Other runners must document their own delivery guarantees.
    fn apply(
        &self,
        connection: &mut Self::Connection,
        envelope: &Envelope<Self::Id, Self::Event>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Removes every row this projection owns. A rebuild calls it before
    /// replaying from the start of the table, in the same transaction that
    /// resets the cursor. It has no default body on purpose: a no-op default
    /// would let a projection forget it and rebuild on top of stale rows.
    fn reset(
        &self,
        connection: &mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
