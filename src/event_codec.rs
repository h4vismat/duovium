//! JSON persistence boundary. Domain events themselves need no serialization traits.

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::fmt::Debug;

/// Encoding, decoding, or unsupported historical schema error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct CodecError(pub String);

impl From<serde_json::Error> for CodecError {
    fn from(error: serde_json::Error) -> Self {
        Self(error.to_string())
    }
}

/// Converts between current domain events and durable JSON payloads.
///
/// `decode` receives the name and schema version recorded at write time. Use
/// them to upcast historical payloads and reject unsupported schemas. Both the
/// event store and projection feed must use the same decoding policy. Methods
/// must be deterministic and free of side effects; writes validate readability
/// by decoding the encoded value before appending it.
pub trait EventCodec<E>: Debug + Send + Sync {
    fn encode(&self, event: &E) -> Result<Value, CodecError>;
    fn decode(&self, name: &str, version: u32, payload: Value) -> Result<E, CodecError>;
}

/// Serde JSON codec for a stable, self-describing payload schema.
///
/// This default codec deserializes the payload as-is; it does not interpret the
/// stored event name/version. Supply a custom codec when those select historical
/// schemas. Serde bounds apply here, rather than to `Event` or `Aggregate`.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonEventCodec;

impl<E: Serialize + DeserializeOwned> EventCodec<E> for JsonEventCodec {
    fn encode(&self, event: &E) -> Result<Value, CodecError> {
        serde_json::to_value(event).map_err(Into::into)
    }
    fn decode(&self, _: &str, _: u32, payload: Value) -> Result<E, CodecError> {
        serde_json::from_value(payload).map_err(Into::into)
    }
}
