/// Invalid executor or projection runner settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("max_attempts must be greater than zero")]
    InvalidAttempts,
    #[error("batch_size must be greater than zero")]
    InvalidBatchSize,
    #[error("max_failures must be greater than zero")]
    InvalidMaxFailures,
    #[error("poll_interval must be greater than zero")]
    InvalidPollInterval,
    #[error("backoff must satisfy 0 < initial_backoff <= max_backoff")]
    InvalidBackoff,
}
