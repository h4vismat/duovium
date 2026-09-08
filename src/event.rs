use std::fmt::Debug;

use chrono::{DateTime, Utc};

use super::{EventId, Metadata, Version};

#[derive(Debug, Clone)]
pub struct Proposed<E> {
    pub event: E,
    pub metadata: Metadata,
}

#[derive(Debug, Clone)]
pub struct Envelope<ID, E> {
    pub event_id: EventId,
    pub stream_id: ID,
    pub version: Version,
    pub event: E,
    pub recorded_at: DateTime<Utc>,
    pub metadata: Metadata,
}

/// A domain fact. Serialization is supplied by a persistence adapter's codec.
/// The name and schema version must be stable and deterministic for a value.
pub trait Event: Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn version(&self) -> u32;
}
