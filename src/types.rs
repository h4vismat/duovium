use std::collections::BTreeMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(Uuid);

impl EventId {
    pub const fn from_uuid(u: Uuid) -> Self {
        Self(u)
    }

    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CorrelationId(Uuid);

impl CorrelationId {
    pub const fn from_uuid(u: Uuid) -> Self {
        Self(u)
    }

    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CausationId(Uuid);

impl CausationId {
    pub const fn from_uuid(u: Uuid) -> Self {
        Self(u)
    }

    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// A metadata value. Untagged, so a stored entry reads as the bare JSON scalar
/// it came from rather than a wrapper object. `I64` precedes `F64` because
/// untagged deserialization takes the first variant that matches.
///
/// `F64` is not variant-faithful through a `jsonb` column for large integral
/// values. Postgres normalizes JSON numbers through `numeric`, so
/// `Value::F64(1e17)` is stored as `100000000000000000` and reads back as
/// `Value::I64(..)` — a different variant that compares unequal. This affects
/// an integral `f64` at or above roughly `1e16` within `i64` range; smaller or
/// non-integral values round-trip exactly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    String(String),
    I64(i64),
    F64(f64),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Version(u64);

impl Version {
    pub const fn new(n: u64) -> Self {
        Self(n)
    }
    pub const fn get(&self) -> u64 {
        self.0
    }
    pub const fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Metadata {
    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<CausationId>,
    extra: BTreeMap<String, Value>,
}

impl Metadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.extra.get(key)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: Value) -> Option<Value> {
        self.extra.insert(key.into(), value)
    }

    /// Rebuilds a value from stored columns. The durable store keeps the
    /// correlation and causation ids in their own columns and the rest in one
    /// JSON object, so it needs a way back that `insert` cannot give it.
    pub const fn from_parts(
        correlation_id: Option<CorrelationId>,
        causation_id: Option<CausationId>,
        extra: BTreeMap<String, Value>,
    ) -> Self {
        Self {
            correlation_id,
            causation_id,
            extra,
        }
    }

    /// Borrows the entries a store writes to its JSON column. The field stays
    /// private, so callers still add entries through `insert`.
    pub const fn extra(&self) -> &BTreeMap<String, Value> {
        &self.extra
    }

    /// Fills the gaps from a caller's request context.
    ///
    /// Values already present win: the aggregate is closer to the event than the
    /// caller is, so an aggregate that sets its own causation id keeps it.
    pub fn fill_from(&mut self, base: &Metadata) {
        if self.correlation_id.is_none() {
            self.correlation_id = base.correlation_id;
        }

        if self.causation_id.is_none() {
            self.causation_id = base.causation_id;
        }

        for (key, value) in &base.extra {
            self.extra
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    Any,
    NoStream,
    Exact(Version),
}

#[derive(Debug, thiserror::Error)]
#[error("invalid stream id: {0}")]
pub struct InvalidStreamId(pub String);

/// A stream id a durable store can address.
///
/// The in-memory store keys its map by the id value itself. A table needs two
/// text columns instead: the aggregate a stream belongs to, and the key inside
/// it. `stream_type` is an associated function rather than a method because one
/// id type belongs to exactly one aggregate, so a store cannot be handed the
/// wrong name.
/// The shared-table design depends on two requirements this trait cannot
/// enforce. `stream_type` must be unique across every implementor: two types
/// that return the same string silently merge their streams into one table
/// namespace, and nothing detects it. `to_key` must be injective for a given
/// type: two ids that map to the same key become one stream.
pub trait StreamId: Sized + Debug + Send + Sync {
    fn stream_type() -> &'static str;

    fn to_key(&self) -> String;

    /// Inverse of `to_key`. A reader that walks the whole table, rather than one
    /// stream it already holds the id of, recovers the id from the column.
    fn from_key(key: &str) -> Result<Self, InvalidStreamId>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use super::{CausationId, CorrelationId, InvalidStreamId, Metadata, StreamId, Value};

    #[test]
    fn metadata_values_encode_as_bare_json() {
        let extra = BTreeMap::from([
            ("attempt".to_owned(), Value::I64(3)),
            ("ratio".to_owned(), Value::F64(1.5)),
            ("replay".to_owned(), Value::Bool(true)),
            ("source".to_owned(), Value::String("payments".to_owned())),
        ]);

        let json = serde_json::to_string(&extra).expect("the map serializes");

        assert_eq!(
            json,
            r#"{"attempt":3,"ratio":1.5,"replay":true,"source":"payments"}"#
        );
    }

    #[test]
    fn metadata_values_round_trip_through_json() {
        let extra = BTreeMap::from([
            ("attempt".to_owned(), Value::I64(3)),
            ("ratio".to_owned(), Value::F64(1.5)),
            ("replay".to_owned(), Value::Bool(true)),
            ("source".to_owned(), Value::String("payments".to_owned())),
        ]);

        let json = serde_json::to_string(&extra).expect("the map serializes");
        let decoded: BTreeMap<String, Value> =
            serde_json::from_str(&json).expect("the map deserializes");

        assert_eq!(decoded, extra);
    }

    /// Untagged deserialization tries the variants in declaration order, so
    /// `I64` has to precede `F64` or every integer decodes as a float.
    #[test]
    fn an_integral_number_decodes_as_an_integer() {
        let value: Value = serde_json::from_str("3").expect("a number deserializes");

        assert_eq!(value, Value::I64(3));
    }

    #[test]
    fn from_parts_preserves_every_field() {
        let correlation_id = CorrelationId::from_uuid(Uuid::from_u128(12));
        let causation_id = CausationId::from_uuid(Uuid::from_u128(13));
        let extra = BTreeMap::from([("source".to_owned(), Value::String("onramp".to_owned()))]);

        let metadata =
            Metadata::from_parts(Some(correlation_id), Some(causation_id), extra.clone());

        assert_eq!(metadata.correlation_id, Some(correlation_id));
        assert_eq!(metadata.causation_id, Some(causation_id));
        assert_eq!(metadata.extra(), &extra);
        assert_eq!(
            metadata.get("source"),
            Some(&Value::String("onramp".to_owned()))
        );
    }

    /// A stand-in for a real aggregate id. The store is generic over the id
    /// type, so the tests supply their own rather than depending on a domain.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestStreamId(Uuid);

    impl StreamId for TestStreamId {
        fn stream_type() -> &'static str {
            "test"
        }

        fn to_key(&self) -> String {
            self.0.to_string()
        }

        fn from_key(key: &str) -> Result<Self, InvalidStreamId> {
            key.parse()
                .map(Self)
                .map_err(|_| InvalidStreamId(key.to_owned()))
        }
    }

    #[test]
    fn a_stream_type_names_the_aggregate_not_the_instance() {
        assert_eq!(TestStreamId::stream_type(), "test");
    }

    #[test]
    fn a_key_round_trips_through_its_text_form() {
        let id = TestStreamId(Uuid::from_u128(21));

        let restored = TestStreamId::from_key(&id.to_key()).expect("its own key parses");

        assert_eq!(restored, id);
    }

    #[test]
    fn a_key_that_does_not_parse_is_rejected() {
        let error = TestStreamId::from_key("not-a-uuid").expect_err("the key is invalid");

        assert_eq!(error.to_string(), "invalid stream id: not-a-uuid");
    }

    #[test]
    fn filling_supplies_only_the_missing_ids() {
        let correlation = CorrelationId::from_uuid(Uuid::now_v7());
        let causation = CausationId::from_uuid(Uuid::now_v7());
        let own_causation = CausationId::from_uuid(Uuid::now_v7());

        let mut event = Metadata::new();
        event.causation_id = Some(own_causation);

        let mut base = Metadata::new();
        base.correlation_id = Some(correlation);
        base.causation_id = Some(causation);

        event.fill_from(&base);

        assert_eq!(event.correlation_id, Some(correlation));
        // The aggregate is closer to the event than the caller is.
        assert_eq!(event.causation_id, Some(own_causation));
    }

    #[test]
    fn filling_never_overwrites_an_extra_entry() {
        let mut event = Metadata::new();
        event.insert("source", Value::String("aggregate".to_owned()));

        let mut base = Metadata::new();
        base.insert("source", Value::String("caller".to_owned()));
        base.insert("request_id", Value::String("abc".to_owned()));

        event.fill_from(&base);

        assert_eq!(
            event.get("source"),
            Some(&Value::String("aggregate".to_owned()))
        );
        assert_eq!(
            event.get("request_id"),
            Some(&Value::String("abc".to_owned()))
        );
    }
}
