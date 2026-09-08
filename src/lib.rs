#![doc = include_str!("../README.md")]

mod aggregate;
mod commands;
mod config;
mod event;
#[cfg(feature = "postgres")]
mod event_codec;
mod event_store;
mod executor;
#[cfg(feature = "postgres")]
mod migrator;
mod projection;
mod store;
mod types;

pub use aggregate::*;
pub use commands::*;
pub use config::*;
pub use event::*;
#[cfg(feature = "postgres")]
pub use event_codec::*;
pub use event_store::*;
pub use executor::*;
#[cfg(feature = "postgres")]
pub use migrator::MIGRATOR;
pub use projection::*;
pub use store::*;
pub use types::*;
