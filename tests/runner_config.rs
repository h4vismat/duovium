#![cfg(feature = "postgres")]
use duovium::RunnerConfig;
use std::time::Duration;

#[test]
fn invalid_runner_settings_are_rejected() {
    let cases = [
        RunnerConfig {
            batch_size: 0,
            ..Default::default()
        },
        RunnerConfig {
            batch_size: -1,
            ..Default::default()
        },
        RunnerConfig {
            max_failures: 0,
            ..Default::default()
        },
        RunnerConfig {
            poll_interval: Duration::ZERO,
            ..Default::default()
        },
        RunnerConfig {
            initial_backoff: Duration::ZERO,
            ..Default::default()
        },
        RunnerConfig {
            initial_backoff: Duration::from_secs(31),
            ..Default::default()
        },
    ];
    for config in cases {
        assert!(config.validate().is_err(), "accepted {config:?}");
    }
    assert!(RunnerConfig::default().validate().is_ok());
}
