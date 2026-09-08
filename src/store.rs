//! Storage failures independent of the application and database driver.

use std::convert::Infallible;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A uniqueness constraint rejected the write. `constraint` carries the
    /// name the store reported, so callers can tell which column collided.
    #[error("unique violation on `{constraint}`")]
    UniqueViolation { constraint: String },
    /// The store aborted the transaction to protect isolation. Postgres
    /// reports this as SQLSTATE 40001.
    #[error("serialization failure")]
    SerializationFailure,
    /// The store could not be reached, or timed out.
    #[error("store unavailable: {0}")]
    Unavailable(String),
    #[error("store error: {0}")]
    Other(String),
}

impl StoreError {
    /// True when the same append can still succeed after reloading state.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::SerializationFailure)
    }
}

/// Postgres reports a violated unique constraint with this SQLSTATE.
#[cfg(feature = "postgres")]
const UNIQUE_VIOLATION: &str = "23505";
/// Postgres reports a transaction it aborted to protect isolation with this one.
#[cfg(feature = "postgres")]
const SERIALIZATION_FAILURE: &str = "40001";

#[cfg(feature = "postgres")]
impl From<sqlx::Error> for StoreError {
    fn from(error: sqlx::Error) -> Self {
        if let sqlx::Error::Database(database_error) = &error {
            return match database_error.code().as_deref() {
                Some(UNIQUE_VIOLATION) => Self::UniqueViolation {
                    constraint: database_error.constraint().unwrap_or_default().to_owned(),
                },
                Some(SERIALIZATION_FAILURE) => Self::SerializationFailure,
                _ => Self::Other(error.to_string()),
            };
        }

        match &error {
            // The server is unreachable or the pool gave up waiting.
            // Reconnecting is the pool's concern, so this is not retryable.
            sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => {
                Self::Unavailable(error.to_string())
            }
            _ => Self::Other(error.to_string()),
        }
    }
}

/// The in-memory store's error type. A store that cannot fail still has to
/// satisfy the command port's bound, so the conversion exists and is
/// unreachable.
impl From<Infallible> for StoreError {
    fn from(error: Infallible) -> Self {
        match error {}
    }
}

#[cfg(test)]
mod tests {
    use super::StoreError;

    #[test]
    #[cfg(feature = "postgres")]
    fn an_exhausted_pool_is_unavailable() {
        let error = StoreError::from(sqlx::Error::PoolTimedOut);

        assert!(matches!(error, StoreError::Unavailable(_)));
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn a_closed_pool_is_unavailable() {
        let error = StoreError::from(sqlx::Error::PoolClosed);

        assert!(matches!(error, StoreError::Unavailable(_)));
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn a_transport_failure_is_unavailable() {
        let error = StoreError::from(sqlx::Error::Io(std::io::Error::other("socket closed")));

        assert!(matches!(error, StoreError::Unavailable(_)));
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn an_unclassified_failure_is_other() {
        let error = StoreError::from(sqlx::Error::RowNotFound);

        assert!(matches!(error, StoreError::Other(_)));
    }

    /// A lost version race never reaches this enum: the store reports it as
    /// `AppendError::Conflict`, which carries the version that won. So an
    /// aborted transaction is the only failure worth trying again.
    #[test]
    #[cfg(feature = "postgres")]
    fn only_a_serialization_failure_is_retryable() {
        assert!(StoreError::SerializationFailure.is_retryable());
        assert!(!StoreError::from(sqlx::Error::PoolTimedOut).is_retryable());
        assert!(!StoreError::from(sqlx::Error::RowNotFound).is_retryable());
        assert!(
            !StoreError::UniqueViolation {
                constraint: "events_event_id_key".to_owned(),
            }
            .is_retryable()
        );
    }

    /// The in-memory store cannot fail, and the command port's error type is
    /// concrete. Without this conversion, a test fixture could not put an
    /// in-memory executor behind `dyn Commands`.
    #[test]
    fn an_infallible_store_error_converts() {
        fn convert<E: Into<StoreError>>(error: E) -> StoreError {
            error.into()
        }

        let result: Result<(), std::convert::Infallible> = Ok(());

        assert!(result.map_err(convert).is_ok());
    }
}
